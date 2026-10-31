package capture

import (
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"testing"
)

func TestSnapshotDoesNotCallCustomerCode(t *testing.T) {
	limits := Limits{Depth: 10, Collection: 100, Keys: 100, StringBytes: 4096, Nodes: 1000}
	result := Snapshot(dangerous{Value: "business value"}, limits, true)
	data, err := json.Marshal(result)
	if err != nil || !strings.Contains(string(data), "business value") {
		t.Fatalf("snapshot: %s %v", data, err)
	}
}

func TestSnapshotAggregateBudgetAndExactLargeIntegers(t *testing.T) {
	limits := Limits{Depth: 128, Collection: 10000, Keys: 10000, StringBytes: 1 << 20, Nodes: 10000, Bytes: 4096}
	values := make([]string, 10000)
	for i := range values {
		values[i] = strings.Repeat("business", 1000)
	}
	data, err := json.Marshal(Snapshot(values, limits, true))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 8192 || !strings.Contains(string(data), "complete\":false") {
		t.Fatalf("aggregate snapshot limit not explicit/bounded: %d bytes", len(data))
	}
	got := Snapshot([]any{int64(math.MaxInt64), uint64(math.MaxUint64), json.Number("18446744073709551615")}, limits, true).([]any)
	if got[0].(map[string]any)["value"] != "9223372036854775807" || got[1].(map[string]any)["value"] != "18446744073709551615" {
		t.Fatal("integer precision lost before JSON storage")
	}
	if got[2].(map[string]any)["value"] != "18446744073709551615" {
		t.Fatal("JSON integer precision lost")
	}
}

func TestBigNumbersPreserveBusinessValues(t *testing.T) {
	limits := Limits{Depth: 10, Collection: 100, Keys: 100, StringBytes: 4096, Nodes: 1000, Bytes: 8192}
	integer, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	rational := new(big.Rat).SetFrac(big.NewInt(7), big.NewInt(13))
	decimal, _, err := big.ParseFloat("0.12345678901234567890123456789", 10, 128, big.ToNearestEven)
	if err != nil {
		t.Fatal(err)
	}
	values := Snapshot([]any{integer, rational, decimal}, limits, true).([]any)
	if value, ok := values[0].(map[string]any); !ok || value["value"] != integer.String() {
		t.Fatalf("big.Int lost: %v", values[0])
	}
	if value, ok := values[1].(map[string]any); !ok || value["value"] != "7/13" {
		t.Fatalf("big.Rat lost: %v", values[1])
	}
	value, ok := values[2].(map[string]any)
	if !ok || value["precision"] != decimal.Prec() {
		t.Fatalf("big.Float precision lost: %v", values[2])
	}
	roundtrip, _, err := big.ParseFloat(value["value"].(string), 10, value["precision"].(uint), big.ToNearestEven)
	if err != nil || roundtrip.Cmp(decimal) != 0 {
		t.Fatal("big.Float rounded during capture")
	}
	large := new(big.Int).Lsh(big.NewInt(1), 100000)
	limited := Snapshot(large, limits, true).(map[string]any)
	if limited["complete"] != false {
		t.Fatal("oversized big.Int must carry explicit omission metadata")
	}
}

type dangerous struct{ Value string }

func (d dangerous) MarshalJSON() ([]byte, error) { panic("must not execute") }
func (d dangerous) String() string               { panic("must not execute") }

func TestSnapshotCyclesSecretsAndLimits(t *testing.T) {
	limits := Limits{Depth: 10, Collection: 2, Keys: 100, StringBytes: 4096, Nodes: 1000}
	v := map[string]any{"password": "must-not-leak", "items": []int{1, 2, 3}}
	v["self"] = v
	data, err := json.Marshal(Snapshot(v, limits, false))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"reference", "max_collection_length", "privacy_secret"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s in %s", want, text)
		}
	}
	if strings.Contains(text, "must-not-leak") {
		t.Fatal("secret leaked")
	}
	if len(v["items"].([]int)) != 3 || v["password"] != "must-not-leak" {
		t.Fatal("mutated application data")
	}
}
