package instrumentation

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/getrepro/go-sdk/internal/capture"
)

const amqpPackage = "github.com/rabbitmq/amqp091-go"

type amqpCall struct {
	scope                        *capture.Scope
	previous                     *capture.Frame
	method, exchange, routingKey string
	payload                      any
	once                         sync.Once
}

func isAMQPDelivery(value any) bool {
	return packageOf(value) == amqpPackage && receiverName(value) == "Delivery"
}
func amqpFields(value any) reflect.Value {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	return v
}
func amqpHeaders(value any) map[string]string {
	out := map[string]string{}
	v := amqpFields(value)
	if !v.IsValid() {
		return out
	}
	headers := v.FieldByName("Headers")
	if !headers.IsValid() {
		return out
	}
	iterator := headers.MapRange()
	for iterator.Next() {
		entry := iterator.Value().Interface()
		switch val := entry.(type) {
		case string:
			out[strings.ToLower(iterator.Key().String())] = val
		case []byte:
			out[strings.ToLower(iterator.Key().String())] = string(val)
		}
	}
	return out
}
func amqpPayload(value any) map[string]any {
	v := amqpFields(value)
	if !v.IsValid() {
		return nil
	}
	out := map[string]any{"headers": amqpHeaders(value)}
	for _, key := range []string{"ContentType", "MessageId", "CorrelationId", "Exchange", "RoutingKey", "Redelivered", "DeliveryTag"} {
		field := v.FieldByName(key)
		if field.IsValid() && field.CanInterface() {
			out[strings.ToLower(key[:1])+key[1:]] = field.Interface()
		}
	}
	body := v.FieldByName("Body").Bytes()
	var data any
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if decoder.Decode(&data) != nil {
		data = string(body)
	}
	out["content"] = data
	out["bodyBytes"] = len(body)
	return out
}
func beginAMQPConsumer(value any) *amqpCall {
	headers := amqpHeaders(value)
	sid, aid, actor := headers["x-bug-session-id"], headers["x-bug-action-id"], ""
	if sid == "" {
		sid, aid, actor = capture.AdmitMessage(context.Background())
	}
	if sid == "" {
		return nil
	}
	scope, previous := capture.Begin(sid, aid, "", "amqp", actor)
	if scope == nil {
		return nil
	}
	v := amqpFields(value)
	state := &amqpCall{scope: scope, previous: previous, method: "AMQP_CONSUME", exchange: v.FieldByName("Exchange").String(), routingKey: v.FieldByName("RoutingKey").String(), payload: scope.Value(amqpPayload(value))}
	return state
}
func beforeAMQP(ctx HookContext, args []any) bool {
	if ctx.GetPackageName() != amqpPackage {
		return false
	}
	parent := capture.Current()
	if parent == nil || parent.Scope == nil || parent.Integration == "amqp-publish" {
		return true
	}
	// Context variants have one additional parameter after the receiver.
	index := 1
	if strings.Contains(ctx.GetFuncName(), "Context") {
		index = 2
	}
	if len(args) < index+5 {
		return true
	}
	original := amqpFields(args[index+4])
	if !original.IsValid() {
		return true
	}
	scope, previous := capture.Begin(parent.Scope.SessionID, parent.Scope.ActionID, "", "amqp")
	if scope == nil {
		return true
	}
	capture.MarkIntegration("amqp-publish")
	state := &amqpCall{scope: scope, previous: previous, method: "AMQP_PUBLISH", exchange: args[index].(string), routingKey: args[index+1].(string)}
	ctx.SetData(state)
	copy := reflect.New(original.Type()).Elem()
	copy.Set(original)
	field := copy.FieldByName("Headers")
	headers := reflect.MakeMap(field.Type())
	iterator := field.MapRange()
	for iterator.Next() {
		headers.SetMapIndex(iterator.Key(), iterator.Value())
	}
	existing := amqpHeaders(args[index+4])
	for key, value := range kafkaCorrelation(scope, parent) {
		if existing[key] == "" && value != "" {
			headers.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
		}
	}
	field.Set(headers)
	ctx.SetParam(index+4, copy.Interface())
	state.payload = scope.Value(amqpPayload(copy.Interface()))
	return true
}
func (s *amqpCall) finishPublish(ctx HookContext, values []any) {
	err := resultError(values)
	if p := ctx.GetKeyData("panic"); p != nil {
		err = p
	}
	if err == nil && len(values) == 2 && values[0] != nil {
		confirmation := reflect.ValueOf(values[0])
		if confirmation.Kind() == reflect.Pointer && !confirmation.IsNil() {
			wait := confirmation.MethodByName("WaitContext")
			if wait.IsValid() {
				// Observe the confirmation independently; never delay or replace the application call.
				go func() {
					defer func() {
						if recover() != nil {
							s.scope.Failure("capture", "amqp", "confirmation_observer_failed")
							s.complete(errors.New("AMQP confirmation could not be observed"), false)
						}
					}()
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					result := wait.Call([]reflect.Value{reflect.ValueOf(ctx)})
					if !result[1].IsNil() {
						s.scope.Failure("delivery", "amqp", "acknowledgement_deadline")
						s.complete(result[1].Interface(), false)
					} else if !result[0].Bool() {
						s.complete(errors.New("AMQP broker rejected publish"), false)
					} else {
						s.complete(nil, true)
					}
				}()
				return
			}
		}
	}
	s.complete(err, false)
}
func (s *amqpCall) complete(err any, confirmed bool) {
	s.once.Do(func() {
		status := 200
		var response any
		if err != nil {
			status = 500
			response = map[string]any{"error": s.scope.Value(err)}
		}
		body := map[string]any{"transport": "amqp", "exchange": s.exchange, "routingKey": s.routingKey, "queue": s.routingKey, "message": s.payload}
		if s.method == "AMQP_PUBLISH" {
			body["confirmed"] = confirmed
		}
		if p, ok := s.payload.(map[string]any); ok {
			if headers, ok := p["headers"].(map[string]any); ok {
				body["parentRequestRid"] = headers["x-repro-request-rid"]
			}
		}
		_ = s.scope.Finish(map[string]any{"method": s.method, "url": "amqp://" + s.exchange + "/" + s.routingKey, "path": "amqp://" + s.exchange + "/" + s.routingKey, "key": s.method + " " + s.routingKey, "status": status, "durMs": time.Since(s.scope.Start).Milliseconds(), "body": body, "respBody": response})
	})
}
