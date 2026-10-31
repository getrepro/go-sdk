package instrumentation

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/getrepro/go-sdk/internal/capture"
)

type dbCall struct {
	call    *capture.Call
	frame   *capture.Frame
	args    []any
	record  *capture.DatabaseRecord
	fn, pkg string
	started time.Time
}

type cursorState struct {
	record    *capture.DatabaseRecord
	columns   []string
	exhausted bool
}
type batchState struct {
	queries []map[string]any
	index   int
}
type copyValueCall struct {
	record *capture.DatabaseRecord
	scope  *capture.Scope
}

func packageOf(value any) string {
	t := reflect.TypeOf(value)
	if t == nil {
		return ""
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.PkgPath()
}
func receiverName(value any) string {
	t := reflect.TypeOf(value)
	if t == nil {
		return ""
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}

func beforeDatabase(ctx HookContext, args []any) bool {
	if len(args) == 0 {
		return false
	}
	pkg := packageOf(args[0])
	kind := ""
	switch {
	case pkg == "database/sql" || pkg == "github.com/jackc/pgx/v5":
		kind = "postgresql"
	case strings.HasPrefix(pkg, "go.mongodb.org/mongo-driver"):
		kind = "mongo"
	default:
		return false
	}
	frame := capture.Current()
	if frame != nil && frame.Scope != nil && oneOf(receiverName(args[0]), "copyFromRows", "copyFromSlice", "copyFromFunc") && ctx.GetFuncName() == "Values" {
		if record, ok := frame.Scope.Load(args[0]).(*capture.DatabaseRecord); ok {
			ctx.SetData(&copyValueCall{record: record, scope: frame.Scope})
		}
		return true
	}
	if frame == nil || frame.Integration != "" {
		return true
	}
	fn := ctx.GetFuncName()
	state := &dbCall{frame: frame, args: args, fn: fn, pkg: pkg, started: time.Now()}
	state.call = capture.Enter(capture.Meta{Function: pkg + "." + receiverName(args[0]) + "." + fn}, args[1:])
	capture.MarkIntegration(pkg)
	if current := capture.Current(); current != nil {
		state.frame = current
	}
	ctx.SetData(state)
	if fn == "Scan" || fn == "Values" || fn == "RawValues" || fn == "Decode" || fn == "All" || fn == "Next" || fn == "Close" {
		return true
	}
	query := map[string]any{"library": pkg}
	operation := fn
	collection := ""
	if kind == "postgresql" {
		if fn == "CopyFrom" && len(args) > 4 {
			query["table"] = frame.Scope.Value(args[2])
			query["columns"] = frame.Scope.Value(args[3])
		}
		if batch, ok := frame.Scope.Load(args[0]).(*batchState); ok && oneOf(fn, "Exec", "Query") && batch.index < len(batch.queries) {
			for k, v := range batch.queries[batch.index] {
				query[k] = v
			}
			batch.index++
		}
		for i := 1; i < len(args); i++ {
			if text, ok := args[i].(string); ok {
				query["sql"] = text
				if i+1 < len(args) {
					query["parameters"] = frame.Scope.Value(args[i+1])
				}
				fields := strings.Fields(text)
				if len(fields) > 0 {
					operation = strings.ToLower(fields[0])
				}
				break
			}
		}
		if stmt, ok := args[0].(*sql.Stmt); ok {
			query["sql"] = reflect.ValueOf(stmt).Elem().FieldByName("query").String()
			if len(args) > 2 {
				query["parameters"] = frame.Scope.Value(args[2])
			}
		}
	} else {
		if named, ok := args[0].(interface{ Name() string }); ok {
			collection = named.Name()
		}
		if len(args) > 2 {
			query["arguments"] = frame.Scope.Value(args[2:])
		}
		operation = strings.ToLower(fn[:1]) + fn[1:]
	}
	state.record = frame.Scope.Database(kind, collection, operation, query, state.frame.Span)
	if fn == "CopyFrom" && len(args) > 4 {
		frame.Scope.Store(args[4], state.record)
	}
	return true
}

func (c *dbCall) finish(values []any) {
	defer c.call.End(values, nil)
	scope := c.frame.Scope
	err := resultError(values)
	if c.fn == "Scan" || c.fn == "Decode" || c.fn == "All" || c.fn == "Values" || c.fn == "RawValues" || c.fn == "Next" || c.fn == "Close" {
		cursor, _ := scope.Load(c.args[0]).(*cursorState)
		if cursor == nil {
			return
		}
		switch c.fn {
		case "Scan":
			if err == nil && len(c.args) > 1 {
				dest, _ := c.args[1].([]any)
				row := map[string]any{}
				for i, v := range dest {
					key := fieldName(cursor.columns, i)
					row[key] = scope.Value(v)
				}
				cursor.record.Row(row)
			}
		case "Decode":
			if err == nil && len(c.args) > 1 {
				cursor.record.Row(scope.Value(c.args[1]))
			}
			if err != nil || oneOf(receiverName(c.args[0]), "SingleResult", "DistinctResult") {
				cursor.record.EndRows(err == nil, err)
			}
		case "All":
			if len(c.args) > 2 {
				value := reflect.ValueOf(c.args[2])
				if value.Kind() == reflect.Pointer && !value.IsNil() {
					value = value.Elem()
					if value.Kind() == reflect.Slice {
						for i := 0; i < value.Len(); i++ {
							cursor.record.Row(scope.Value(value.Index(i).Interface()))
						}
					}
				}
				cursor.exhausted = err == nil
				cursor.record.EndRows(err == nil, err)
			}
		case "Values", "RawValues":
			if err == nil && len(values) > 0 {
				cursor.record.Row(scope.Value(values[0]))
			}
		case "Next":
			if len(values) > 0 && values[0] == false {
				cursor.exhausted = true
				if result, ok := c.args[0].(interface{ Err() error }); ok {
					if e := result.Err(); e != nil {
						err = scope.Value(e)
					}
				}
				cursor.record.EndRows(true, err)
			}
		case "Close":
			cursor.record.EndRows(cursor.exhausted, err)
		}
		return
	}
	if c.record == nil {
		return
	}
	if c.fn == "SendBatch" && len(c.args) > 2 && len(values) > 0 && values[0] != nil {
		batch := reflect.ValueOf(c.args[2])
		if batch.Kind() == reflect.Pointer && !batch.IsNil() {
			queries := batch.Elem().FieldByName("QueuedQueries")
			if queries.IsValid() {
				state := &batchState{}
				for i := 0; i < queries.Len(); i++ {
					q := queries.Index(i)
					if q.Kind() == reflect.Pointer {
						q = q.Elem()
					}
					state.queries = append(state.queries, map[string]any{"sql": q.FieldByName("SQL").String(), "parameters": scope.Value(q.FieldByName("Arguments").Interface())})
				}
				scope.Store(values[0], state)
			}
		}
	}
	if c.fn == "CopyFrom" {
		result := map[string]any{}
		if len(values) > 0 {
			result["rowsAffected"] = values[0]
		}
		c.record.CopyResult(result, err, time.Since(c.started))
		return
	}
	if len(values) > 0 && values[0] != nil && isCursor(values[0]) && err == nil {
		cursor := &cursorState{record: c.record, columns: columns(values[0])}
		scope.Store(values[0], cursor)
		c.record.EndRows(false, nil)
		return
	}
	result := scope.Value(values)
	if len(values) > 0 {
		if sqlResult, ok := values[0].(sql.Result); ok && sqlResult != nil {
			affected, e := sqlResult.RowsAffected()
			result = map[string]any{"rowsAffected": affected}
			if e != nil {
				result = map[string]any{"rowsAffectedUnavailable": true}
			}
		}
	}
	c.record.Result(result, err, time.Since(c.started))
}

func oneOf(value string, values ...string) bool {
	for _, choice := range values {
		if value == choice {
			return true
		}
	}
	return false
}

func isCursor(value any) bool {
	switch receiverName(value) {
	case "Rows", "baseRows", "Cursor", "SingleResult", "DistinctResult":
		return true
	}
	return false
}

func columns(value any) []string {
	if rows, ok := value.(interface{ Columns() ([]string, error) }); ok {
		names, _ := rows.Columns()
		return names
	}
	method := reflect.ValueOf(value).MethodByName("FieldDescriptions")
	if !method.IsValid() {
		return nil
	}
	result := method.Call(nil)
	if len(result) != 1 || result[0].Kind() != reflect.Slice {
		return nil
	}
	out := make([]string, result[0].Len())
	for i := range out {
		out[i] = result[0].Index(i).FieldByName("Name").String()
	}
	return out
}

func fieldName(names []string, index int) string {
	if index < len(names) {
		return names[index]
	}
	return "column_" + strconv.Itoa(index)
}

func resultError(values []any) any {
	for _, value := range values {
		if err, ok := value.(error); ok && err != nil {
			return map[string]any{"name": reflect.TypeOf(err).String(), "message": err.Error()}
		}
	}
	return nil
}
