package capture

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Limits struct {
	Depth       int
	Collection  int
	Keys        int
	StringBytes int
	Nodes       int
	Bytes       int
}

type snapshotter struct {
	limits Limits
	seen   map[visit]string
	nodes  int
	bytes  int
	raw    bool
}

type visit struct {
	typ     reflect.Type
	pointer uintptr
}

func omission(reason string, extra map[string]any) map[string]any {
	out := map[string]any{"__kind": "repro-omitted", "complete": false, "reason": reason}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// Snapshot does not call arbitrary String, MarshalJSON, or custom getters.
// Error messages and explicitly recognized numeric types use their value APIs.
// Input must obey Go's ordinary rule: mutable values cannot be written while read.
func Snapshot(value any, limits Limits, raw bool) (out any) {
	defer func() {
		if recover() != nil {
			out = omission("snapshot_unavailable", nil)
		}
	}()
	s := snapshotter{limits: limits, seen: make(map[visit]string), raw: raw}
	if s.limits.Bytes <= 0 {
		s.limits.Bytes = 2 << 20
	}
	return s.walk(reflect.ValueOf(value), 0, "$")
}

func (s *snapshotter) exhausted() bool { return s.bytes >= s.limits.Bytes || s.nodes >= s.limits.Nodes }

func exactInteger(value string) any { return map[string]any{"__kind": "bigint", "value": value} }

func (s *snapshotter) text(value string) any {
	n := min(len(value), s.limits.StringBytes, max(0, s.limits.Bytes-s.bytes))
	if n < len(value) {
		for n > 0 && !utf8.RuneStart(value[n]) {
			n--
		}
	}
	s.bytes += n
	if n < len(value) {
		return map[string]any{"__kind": "truncated", "complete": false, "reason": "max_string_or_value_bytes", "originalBytes": len(value), "value": strings.Clone(value[:n])}
	}
	return value
}

func sensitive(name string) bool {
	name = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "_", ""), "-", ""))
	for _, key := range []string{"password", "passwd", "secret", "authorization", "cookie", "apikey", "accesstoken", "refreshtoken", "privatekey"} {
		if strings.Contains(name, key) {
			return true
		}
	}
	return false
}

func infrastructure(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	p := t.PkgPath()
	if p == "github.com/rabbitmq/amqp091-go" && (t.Name() == "Channel" || t.Name() == "Connection" || t.Name() == "DeferredConfirmation") {
		return true
	}
	if p == "context" || p == "sync" || p == "sync/atomic" || p == "net" || p == "net/http" || p == "crypto/tls" || p == "database/sql" {
		return true
	}
	for _, prefix := range []string{"github.com/jackc/pgx", "github.com/redis/go-redis", "github.com/redis/rueidis", "github.com/segmentio/kafka-go", "github.com/twmb/franz-go", "go.mongodb.org/mongo-driver"} {
		if strings.HasPrefix(p, prefix) {
			for _, name := range []string{"Client", "Conn", "Pool", "DB", "Tx", "Rows", "Cursor", "Collection", "Database", "Session", "Reader", "Writer"} {
				if t.Name() == name {
					return true
				}
			}
		}
	}
	return false
}

func (s *snapshotter) walk(v reflect.Value, depth int, path string) any {
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		return s.walk(v.Elem(), depth, path)
	}
	if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Map || v.Kind() == reflect.Slice || v.Kind() == reflect.Chan || v.Kind() == reflect.Func) && v.IsNil() {
		return nil
	}
	if v.CanInterface() {
		if number, ok := v.Interface().(json.Number); ok {
			if n, ok := new(big.Int).SetString(number.String(), 10); ok && n.BitLen() > 53 {
				return exactInteger(number.String())
			}
			return number
		}
		if strings.HasPrefix(v.Type().PkgPath(), "go.mongodb.org/mongo-driver") {
			if v.Type().Name() == "ObjectID" && v.Kind() == reflect.Array && v.Len() == 12 {
				data := make([]byte, 12)
				for i := range data {
					data[i] = byte(v.Index(i).Uint())
				}
				return hex.EncodeToString(data)
			}
			if v.Type().Name() == "Decimal128" {
				if decimal, ok := v.Interface().(interface{ String() string }); ok {
					return map[string]any{"__type": "Decimal128", "value": decimal.String()}
				}
			}
		}
		if err, ok := v.Interface().(error); ok {
			return map[string]any{"name": v.Type().String(), "message": err.Error()}
		}
	}
	s.nodes++
	s.bytes += 16
	if s.bytes > s.limits.Bytes {
		return omission("max_value_bytes", nil)
	}
	if s.nodes > s.limits.Nodes {
		return omission("max_nodes", nil)
	}
	if depth > s.limits.Depth {
		return omission("max_depth", nil)
	}
	if infrastructure(v.Type()) {
		return map[string]any{"__kind": "repro-semantic", "category": "runtime-infrastructure", "className": v.Type().String()}
	}
	if v.Type() == reflect.TypeFor[time.Time]() && v.CanInterface() {
		return v.Interface().(time.Time).UTC().Format(time.RFC3339Nano)
	}
	if v.CanInterface() {
		// math/big keeps its value in private fields; ordinary struct traversal
		// would silently turn business amounts into empty objects.
		available := min(s.limits.StringBytes, max(0, s.limits.Bytes-s.bytes))
		switch number := v.Interface().(type) {
		case big.Int:
			if number.BitLen()/3+3 > available {
				return omission("max_numeric_bytes", map[string]any{"type": "big.Int", "bits": number.BitLen()})
			}
			return map[string]any{"__kind": "bigint", "value": s.text(number.String())}
		case big.Rat:
			if (number.Num().BitLen()+number.Denom().BitLen())/3+5 > available {
				return omission("max_numeric_bytes", map[string]any{"type": "big.Rat"})
			}
			return map[string]any{"__type": "big.Rat", "value": s.text(number.RatString())}
		case big.Float:
			if uint64(number.Prec())/3+32 > uint64(available) {
				return omission("max_numeric_bytes", map[string]any{"type": "big.Float", "precision": number.Prec()})
			}
			return map[string]any{"__type": "big.Float", "value": s.text(number.Text('g', -1)), "precision": number.Prec()}
		}
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return nil
		}
		key := visit{v.Type(), v.Pointer()}
		if prior, ok := s.seen[key]; ok {
			return map[string]any{"__kind": "reference", "path": prior}
		}
		s.seen[key] = path
		defer delete(s.seen, key)
	}
	switch v.Kind() {
	case reflect.Pointer:
		return s.walk(v.Elem(), depth, path)
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		return s.text(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n := v.Int(); n > 9007199254740991 || n < -9007199254740991 {
			return exactInteger(strconv.FormatInt(n, 10))
		}
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if n := v.Uint(); n > 9007199254740991 {
			return exactInteger(strconv.FormatUint(n, 10))
		}
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return omission("non_finite_number", nil)
		}
		return f
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 && v.Kind() == reflect.Slice {
			n := min(v.Len(), s.limits.StringBytes, max(0, (s.limits.Bytes-s.bytes)*3/4))
			s.bytes += (n + 2) / 3 * 4
			out := map[string]any{"__kind": "bytes", "encoding": "base64", "value": base64.StdEncoding.EncodeToString(v.Slice(0, n).Bytes()), "originalBytes": v.Len()}
			if n < v.Len() {
				out["truncated"] = true
				out["reason"] = "max_bytes"
			}
			return out
		}
		n := min(v.Len(), s.limits.Collection)
		out := make([]any, n)
		for i := range n {
			if s.exhausted() {
				return map[string]any{"__kind": "truncated", "complete": false, "reason": "snapshot_budget", "originalLength": v.Len(), "value": out[:i]}
			}
			out[i] = s.walk(v.Index(i), depth+1, path+"["+strconv.Itoa(i)+"]")
		}
		if n < v.Len() {
			return map[string]any{"__kind": "truncated", "complete": false, "reason": "max_collection_length", "originalLength": v.Len(), "value": out}
		}
		return out
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			entries := []any{}
			iter := v.MapRange()
			for iter.Next() && len(entries) < s.limits.Collection && !s.exhausted() {
				entries = append(entries, []any{s.walk(iter.Key(), depth+1, path+"[].key"), s.walk(iter.Value(), depth+1, path+"[].value")})
			}
			out := map[string]any{"__type": "Map", "entries": entries, "size": v.Len()}
			if len(entries) < v.Len() {
				out["__reproCapture"] = omission("max_collection_length", map[string]any{"capturedCount": len(entries), "originalCount": v.Len()})
			}
			return out
		}
		out := make(map[string]any)
		iter := v.MapRange()
		n := 0
		for iter.Next() {
			if n >= s.limits.Keys || s.exhausted() {
				break
			}
			n++
			key := iter.Key().String()
			if len(key) > s.limits.Bytes-s.bytes {
				n--
				break
			}
			s.bytes += len(key)
			if !s.raw && sensitive(key) {
				out[key] = omission("privacy_secret", nil)
				continue
			}
			out[key] = s.walk(iter.Value(), depth+1, path+"."+key)
		}
		if n < v.Len() {
			return map[string]any{"__kind": "truncated", "complete": false, "reason": "object_snapshot_budget", "originalKeys": v.Len(), "value": out}
		}
		return out
	case reflect.Struct:
		out := make(map[string]any)
		fields := v.Type()
		count := 0
		for i := 0; i < v.NumField(); i++ {
			field := fields.Field(i)
			if !field.IsExported() {
				continue
			}
			key := field.Name
			tag := strings.Split(field.Tag.Get("json"), ",")[0]
			if tag == "-" {
				continue
			}
			if tag != "" {
				key = tag
			}
			count++
			if count > s.limits.Keys || s.exhausted() {
				return map[string]any{"__kind": "truncated", "reason": "max_object_keys", "value": out}
			}
			if !s.raw && sensitive(key) {
				out[key] = omission("privacy_secret", nil)
				continue
			}
			out[key] = s.walk(v.Field(i), depth+1, path+"."+key)
		}
		return out
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return omission("runtime_infrastructure", map[string]any{"type": v.Type().String()})
	default:
		return omission("unsupported_kind", map[string]any{"type": v.Type().String()})
	}
}
