package instrumentation

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getrepro/go-sdk/internal/capture"
)

type kafkaCall struct {
	operation     string
	scope         *capture.Scope
	previous      *capture.Frame
	messages      []any
	topic         string
	async         bool
	once          sync.Once
	timer         *time.Timer
	remaining     int
	mu            sync.Mutex
	deliveryError any
}

var kafkaPending sync.Map

// KafkaRecords is compiler-inserted for typed record slices. Range-over-function
// preserves break, continue and return without adding manual handler wrappers.
func KafkaRecords[T any](records []T) func(func(int, T) bool) {
	return func(yield func(int, T) bool) {
		for index, message := range records {
			more := func() (more bool) {
				previous := capture.Current()
				state := beginConsumer(message)
				defer capture.Restore(previous)
				defer func() {
					p := recover()
					if state != nil {
						state.complete(p)
					}
					if p != nil {
						panic(p)
					}
				}()
				return yield(index, message)
			}()
			if !more {
				return
			}
		}
	}
}

func kafkaValue(message any) reflect.Value {
	v := reflect.ValueOf(message)
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
func kafkaHeaders(message any) map[string]string {
	out := map[string]string{}
	v := kafkaValue(message)
	if !v.IsValid() {
		return out
	}
	headers := v.FieldByName("Headers")
	if !headers.IsValid() {
		return out
	}
	for i := 0; i < headers.Len(); i++ {
		h := headers.Index(i)
		out[strings.ToLower(h.FieldByName("Key").String())] = string(h.FieldByName("Value").Bytes())
	}
	return out
}
func kafkaTopic(message any) string {
	v := kafkaValue(message)
	if !v.IsValid() {
		return ""
	}
	f := v.FieldByName("Topic")
	if !f.IsValid() {
		return ""
	}
	return f.String()
}
func kafkaPayload(message any) map[string]any {
	v := kafkaValue(message)
	if !v.IsValid() {
		return nil
	}
	out := map[string]any{"topic": kafkaTopic(message), "headers": kafkaHeaders(message)}
	for _, field := range []string{"Partition", "Offset", "Time", "Timestamp"} {
		f := v.FieldByName(field)
		if f.IsValid() && f.CanInterface() {
			out[strings.ToLower(field)] = f.Interface()
		}
	}
	for _, field := range []string{"Key", "Value"} {
		f := v.FieldByName(field)
		if !f.IsValid() {
			continue
		}
		data := f.Bytes()
		var value any
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.UseNumber()
		if decoder.Decode(&value) != nil {
			value = string(data)
		}
		out[strings.ToLower(field)] = value
		out[strings.ToLower(field)+"Bytes"] = len(data)
	}
	return out
}
func kafkaAddHeaders(message any, headers map[string]string) {
	v := kafkaValue(message)
	if !v.IsValid() {
		return
	}
	field := v.FieldByName("Headers")
	if !field.CanSet() {
		return
	}
	copy := reflect.MakeSlice(field.Type(), field.Len(), field.Len()+len(headers))
	reflect.Copy(copy, field)
	existing := kafkaHeaders(message)
	for key, value := range headers {
		if existing[key] != "" || value == "" {
			continue
		}
		header := reflect.New(field.Type().Elem()).Elem()
		header.FieldByName("Key").SetString(key)
		header.FieldByName("Value").SetBytes([]byte(value))
		copy = reflect.Append(copy, header)
	}
	field.Set(copy)
}
func kafkaCorrelation(scope *capture.Scope, parent *capture.Frame) map[string]string {
	values := map[string]string{"x-bug-session-id": scope.SessionID, "x-bug-action-id": scope.ActionID, "x-repro-request-rid": scope.RID, "x-repro-trace-id": scope.RID, "x-repro-capture-mode": scope.Mode()}
	if parent != nil {
		values["x-repro-parent-span-id"] = strconv.FormatUint(parent.Span, 10)
	}
	return values
}

func finishConsumer() {
	frame := capture.Current()
	if frame == nil || frame.Scope == nil || frame.Scope.Transport != "kafka" {
		return
	}
	if state, ok := frame.Scope.Load("kafka.consume").(*kafkaCall); ok {
		state.complete(nil)
		capture.Restore(state.previous)
	}
}

func isKafkaMessage(value any) bool {
	pkg := packageOf(value)
	name := receiverName(value)
	return (pkg == "github.com/segmentio/kafka-go" && name == "Message") || (pkg == "github.com/twmb/franz-go/pkg/kgo" && name == "Record")
}

func beginConsumer(message any) *kafkaCall {
	headers := kafkaHeaders(message)
	finishConsumer()
	sid, aid, actor := headers["x-bug-session-id"], headers["x-bug-action-id"], ""
	if sid == "" {
		sid, aid, actor = capture.AdmitMessage(context.Background())
	}
	if sid == "" {
		return nil
	}
	s, previous := capture.Begin(sid, aid, "", "kafka", actor)
	if s == nil {
		return nil
	}
	state := &kafkaCall{scope: s, previous: previous, operation: "KAFKA_CONSUME", topic: kafkaTopic(message), messages: []any{s.Value(kafkaPayload(message))}}
	s.Store("kafka.consume", state)
	return state
}

func beforeKafka(ctx HookContext, args []any) bool {
	pkg := ctx.GetPackageName()
	if pkg != "github.com/segmentio/kafka-go" && pkg != "github.com/twmb/franz-go/pkg/kgo" {
		return false
	}
	fn := ctx.GetFuncName()
	if fn == "complete" && pkg == "github.com/segmentio/kafka-go" {
		if len(args) < 2 {
			return true
		}
		msgs := reflect.ValueOf(args[0])
		for i := 0; i < msgs.Len(); i++ {
			rid := kafkaHeaders(msgs.Index(i).Interface())["x-repro-request-rid"]
			if value, ok := kafkaPending.Load(rid); ok {
				state := value.(*kafkaCall)
				state.ack(args[1])
			}
		}
		return true
	}
	if fn == "EachRecord" && len(args) > 1 {
		if frame := capture.Current(); frame != nil && frame.Integration == "kafka-iterate" {
			return true
		}
		if capture.Current() == nil {
			capture.Restore(&capture.Frame{Integration: "kafka-iterate"})
		} else {
			capture.MarkIntegration("kafka-iterate")
		}
		original := reflect.ValueOf(args[1])
		if original.Kind() != reflect.Func || original.IsNil() {
			return true
		}
		wrapped := reflect.MakeFunc(original.Type(), func(values []reflect.Value) []reflect.Value {
			previous := capture.Current()
			state := beginConsumer(values[0].Interface())
			defer capture.Restore(previous)
			if state != nil {
				defer func() {
					p := recover()
					state.complete(p)
					if p != nil {
						panic(p)
					}
				}()
			}
			return original.Call(values)
		})
		ctx.SetParam(1, wrapped.Interface())
		return true
	}
	if fn == "ReadMessage" || fn == "FetchMessage" || fn == "PollRecords" || fn == "Next" {
		if frame := capture.Current(); fn == "Next" && frame != nil && frame.Integration == "kafka-iterate" {
			return true
		}
		if frame := capture.Current(); frame != nil && frame.Integration == "kafka-poll" {
			return true
		}
		finishConsumer()
		previous := capture.Current()
		if c, ok := ctx.(*libraryContext); ok {
			c.previous = previous
		}
		if previous == nil {
			capture.Restore(&capture.Frame{Integration: "kafka-poll"})
		} else {
			capture.MarkIntegration("kafka-poll")
		}
		ctx.SetData(&kafkaCall{operation: fn, previous: previous})
		return true
	}
	if fn == "CommitMessages" || fn == "CommitRecords" || fn == "Close" {
		ctx.SetData(&kafkaCall{operation: fn})
		return true
	}
	if fn != "WriteMessages" && fn != "Produce" && fn != "TryProduce" {
		return true
	}
	parent := capture.Current()
	if parent == nil || parent.Scope == nil || parent.Integration == "kafka-produce" || len(args) < 3 {
		return true
	}
	scope, previous := capture.Begin(parent.Scope.SessionID, parent.Scope.ActionID, "", "kafka")
	if scope == nil {
		return true
	}
	capture.MarkIntegration("kafka-produce")
	state := &kafkaCall{operation: "KAFKA_PUBLISH", scope: scope, previous: previous}
	ctx.SetData(state)
	if fn == "WriteMessages" {
		messages := reflect.ValueOf(args[2])
		copy := reflect.MakeSlice(messages.Type(), messages.Len(), messages.Len())
		reflect.Copy(copy, messages)
		receiver := reflect.ValueOf(args[0]).Elem()
		state.async = receiver.FieldByName("Async").Bool()
		state.topic = receiver.FieldByName("Topic").String()
		for i := 0; i < copy.Len(); i++ {
			message := copy.Index(i).Addr().Interface()
			kafkaAddHeaders(message, kafkaCorrelation(scope, parent))
			state.messages = append(state.messages, scope.Value(kafkaPayload(message)))
			if state.topic == "" {
				state.topic = kafkaTopic(message)
			}
		}
		ctx.SetParam(2, copy.Interface())
		state.remaining = copy.Len()
		if state.async && state.remaining > 0 {
			state.arm()
		}
	} else {
		record := args[2]
		if !kafkaValue(record).IsValid() {
			state.complete(nil)
			return true
		}
		kafkaAddHeaders(record, kafkaCorrelation(scope, parent))
		state.topic = kafkaTopic(record)
		state.messages = []any{scope.Value(kafkaPayload(record))}
		state.async = true
		state.remaining = 1
		state.arm()
		original := reflect.ValueOf(args[3])
		wrapped := reflect.MakeFunc(original.Type(), func(values []reflect.Value) []reflect.Value {
			previous := capture.Current()
			capture.Restore(&capture.Frame{Scope: scope})
			defer capture.Restore(previous)
			defer state.ack(values[1].Interface())
			if !original.IsNil() {
				return original.Call(values)
			}
			return nil
		})
		ctx.SetParam(3, wrapped.Interface())
	}
	return true
}

func (s *kafkaCall) arm() {
	kafkaPending.Store(s.scope.RID, s)
	s.timer = time.AfterFunc(30*time.Second, func() {
		s.scope.Failure("delivery", "kafka", "acknowledgement_deadline")
		s.complete(map[string]any{"message": "Kafka acknowledgement not observed before capture deadline"})
	})
}
func (s *kafkaCall) ack(err any) {
	s.mu.Lock()
	if err != nil {
		s.deliveryError = err
	}
	s.remaining--
	done := s.remaining <= 0
	value := s.deliveryError
	s.mu.Unlock()
	if done {
		s.complete(value)
	}
}
func (s *kafkaCall) finish(ctx HookContext, values []any) {
	if p := ctx.GetKeyData("panic"); p != nil {
		s.complete(p)
		return
	}
	switch s.operation {
	case "ReadMessage", "FetchMessage", "Next":
		capture.Restore(s.previous)
		if len(values) > 0 && resultError(values) == nil {
			if state := beginConsumer(values[0]); state != nil {
				if c, ok := ctx.(*libraryContext); ok {
					c.previous = capture.Current()
				}
			}
		}
	case "PollRecords":
		capture.Restore(s.previous)
	case "CommitMessages", "CommitRecords", "Close":
		if resultError(values) == nil {
			finishConsumer()
			if c, ok := ctx.(*libraryContext); ok {
				c.previous = capture.Current()
			}
		}
	default:
		// Only acknowledgements finish a nonempty asynchronous publish. Reading
		// their mutable counter here races and can replace a delivery error with nil.
		if !s.async || resultError(values) != nil || len(s.messages) == 0 {
			s.complete(resultError(values))
		}
	}
}
func (s *kafkaCall) complete(err any) {
	if s.scope == nil {
		return
	}
	s.once.Do(func() {
		if s.timer != nil {
			s.timer.Stop()
		}
		kafkaPending.Delete(s.scope.RID)
		status := 200
		var response any
		if err != nil {
			status = 500
			response = map[string]any{"error": s.scope.Value(err)}
		}
		body := map[string]any{"transport": "kafka", "topic": s.topic, "messages": s.messages}
		if s.operation == "KAFKA_CONSUME" && len(s.messages) > 0 {
			body["message"] = s.messages[0]
			if message, ok := s.messages[0].(map[string]any); ok {
				body["value"] = message["value"]
				if headers, ok := message["headers"].(map[string]any); ok {
					body["parentRequestRid"] = headers["x-repro-request-rid"]
				}
			}
		}
		_ = s.scope.Finish(map[string]any{"method": s.operation, "url": "kafka://" + s.topic, "path": "kafka://" + s.topic, "key": s.operation + " " + s.topic, "status": status, "durMs": time.Since(s.scope.Start).Milliseconds(), "body": body, "respBody": response})
	})
}
