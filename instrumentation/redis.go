package instrumentation

import (
	"reflect"
	"strings"
	"time"

	"github.com/getrepro/go-sdk/internal/capture"
)

type redisCall struct {
	call     *capture.Call
	frame    *capture.Frame
	args     []any
	queries  []any
	commands []any
	pkg, fn  string
	started  time.Time
}

func beforeRedis(ctx HookContext, args []any) bool {
	if len(args) == 0 {
		return false
	}
	pkg := packageOf(args[0])
	if pkg != "github.com/redis/go-redis/v9" && pkg != "github.com/redis/rueidis" {
		return false
	}
	frame := capture.Current()
	if frame == nil || frame.Integration != "" {
		return true
	}
	state := &redisCall{frame: frame, args: args, pkg: pkg, fn: ctx.GetFuncName(), started: time.Now()}
	if len(args) > 2 {
		if reflect.TypeOf(args[2]).Kind() == reflect.Slice {
			state.commands = sliceValues(args[2])
		} else {
			state.commands = []any{args[2]}
		}
	}
	for _, command := range state.commands {
		state.queries = append(state.queries, frame.Scope.Value(redisArguments(command, pkg)))
	}
	state.call = capture.Enter(capture.Meta{Function: pkg + "." + state.fn}, state.queries)
	capture.MarkIntegration(pkg)
	state.frame = capture.Current()
	ctx.SetData(state)
	return true
}

func redisArguments(command any, pkg string) any {
	if cmd, ok := command.(interface{ Args() []any }); ok {
		return cmd.Args()
	}
	value := reflect.ValueOf(command)
	if value.Kind() == reflect.Struct && value.FieldByName("Cmd").IsValid() {
		value = value.FieldByName("Cmd")
	}
	if value.Kind() != reflect.Pointer {
		ptr := reflect.New(value.Type())
		ptr.Elem().Set(value)
		value = ptr
	}
	method := value.MethodByName("Commands")
	if method.IsValid() {
		result := method.Call(nil)
		if len(result) > 0 {
			return result[0].Interface()
		}
	}
	return map[string]any{"__kind": "omitted", "reason": "command_arguments_unavailable"}
}

func (r *redisCall) finish(values []any) {
	defer r.call.End(nil, nil)
	commands := r.commands
	results := []any{}
	if r.pkg == "github.com/redis/go-redis/v9" {
		if r.fn == "Exec" && len(values) > 0 {
			commands = sliceValues(values[0])
			for _, cmd := range commands {
				r.queries = append(r.queries, r.frame.Scope.Value(redisArguments(cmd, r.pkg)))
			}
		}
		for _, cmd := range commands {
			results = append(results, driverResult(cmd, "Val"))
		}
	} else if len(values) > 0 {
		if reflect.TypeOf(values[0]).Kind() == reflect.Slice {
			for _, result := range sliceValues(values[0]) {
				results = append(results, driverResult(result, "ToAny"))
			}
		} else {
			results = append(results, driverResult(values[0], "ToAny"))
		}
	}
	for i, query := range r.queries {
		op := "command"
		if array, ok := query.([]any); ok && len(array) > 0 {
			if text, ok := array[0].(string); ok {
				op = strings.ToLower(text)
			}
		}
		record := r.frame.Scope.Database("redis", "redis", op, map[string]any{"arguments": query, "library": r.pkg}, r.frame.Span)
		var result any
		if i < len(results) {
			result = results[i]
		}
		record.Result(r.frame.Scope.Value(result), resultError(values), time.Since(r.started))
	}
}

// These accessors are only called for allowlisted driver-owned result objects.
// They decode an already-received value and never issue another command.
func driverResult(value any, method string) any {
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	fn := v.MethodByName(method)
	if !fn.IsValid() {
		return map[string]any{"__kind": "omitted", "reason": "driver_result_unavailable"}
	}
	results := fn.Call(nil)
	out := map[string]any{}
	if len(results) > 0 {
		out["value"] = results[0].Interface()
	}
	if len(results) > 1 && !results[1].IsNil() {
		out["error"] = resultError([]any{results[1].Interface()})
	}
	if fn := v.MethodByName("Err"); fn.IsValid() {
		r := fn.Call(nil)
		if len(r) > 0 && !r[0].IsNil() {
			out["error"] = resultError([]any{r[0].Interface()})
		}
	}
	if fn := v.MethodByName("IsCacheHit"); fn.IsValid() {
		r := fn.Call(nil)
		if len(r) > 0 {
			out["cacheHit"] = r[0].Bool()
		}
	}
	return out
}

func sliceValues(value any) []any {
	v := reflect.ValueOf(value)
	if !v.IsValid() || v.Kind() != reflect.Slice {
		return nil
	}
	out := make([]any, v.Len())
	for i := range out {
		out[i] = v.Index(i).Interface()
	}
	return out
}
