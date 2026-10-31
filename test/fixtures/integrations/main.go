package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"
	repro "github.com/getrepro/go-sdk"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func sqlWorkflow(ctx context.Context) (any, error) {
	db, err := sql.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS orders (id bigint PRIMARY KEY, amount bigint NOT NULL)"); err != nil {
		return nil, err
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO orders VALUES ($1,$2)", 1, 1099); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE orders SET amount=$1 WHERE id=$2", 9999, 1); err != nil {
		return nil, err
	}
	if err = tx.Rollback(); err != nil {
		return nil, err
	}
	stmt, err := db.PrepareContext(ctx, "SELECT id,amount FROM orders WHERE id=$1")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, 1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var id, amount int64
	for rows.Next() {
		if err = rows.Scan(&id, &amount); err != nil {
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if amount != 1099 {
		return nil, fmt.Errorf("rollback changed: %d", amount)
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO orders VALUES (1,7)"); err == nil {
		return nil, fmt.Errorf("expected duplicate key")
	}
	orm := bun.NewDB(db, pgdialect.New())
	var dto struct {
		ID     int64 `bun:"id"`
		Amount int64 `bun:"amount"`
	}
	if err = orm.NewSelect().TableExpr("orders").Column("id", "amount").Where("id = ?", 1).Scan(ctx, &dto); err != nil {
		return nil, err
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = db.QueryContext(canceled, "SELECT pg_sleep(10)"); err == nil {
		return nil, fmt.Errorf("cancellation ignored")
	}
	return map[string]any{"id": id, "amount": amount, "bun": dto}, nil
}

func pgxWorkflow(ctx context.Context) (any, error) {
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, "SELECT n, n*10 AS amount FROM generate_series(1,3) n")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		values, e := rows.Values()
		if e != nil {
			return nil, e
		}
		out = append(out, values)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "CREATE TEMP TABLE copied(id bigint, amount bigint)"); err != nil {
		return nil, err
	}
	n, err := tx.CopyFrom(ctx, pgx.Identifier{"copied"}, []string{"id", "amount"}, pgx.CopyFromRows([][]any{{1, 12}, {2, 24}}))
	if err != nil {
		return nil, err
	}
	if err = tx.Rollback(ctx); err != nil {
		return nil, err
	}
	batch := &pgx.Batch{}
	batch.Queue("SELECT $1::int", 41)
	batch.Queue("SELECT $1::int", 42)
	result := pool.SendBatch(ctx, batch)
	var first, second int
	if err = result.QueryRow().Scan(&first); err != nil {
		return nil, err
	}
	if err = result.QueryRow().Scan(&second); err != nil {
		return nil, err
	}
	if err = result.Close(); err != nil {
		return nil, err
	}
	if first != 41 || second != 42 || n != 2 {
		return nil, fmt.Errorf("batch/copy results changed")
	}
	return map[string]any{"rows": out, "copied": n, "batch": []int{first, second}}, nil
}

func redisWorkflow(ctx context.Context) (any, error) {
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	defer client.Close()
	if err := client.Set(ctx, "inventory", 13, time.Minute).Err(); err != nil {
		return nil, err
	}
	value, err := client.Get(ctx, "inventory").Int()
	if err != nil {
		return nil, err
	}
	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Incr(ctx, "inventory")
		pipe.HSet(ctx, "order", "amount", 1099)
		return nil
	})
	if err != nil {
		return nil, err
	}
	missing := client.Get(ctx, "missing").Err()
	if missing != redis.Nil {
		return nil, fmt.Errorf("missing key semantics changed: %v", missing)
	}
	r, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{os.Getenv("REDIS_ADDR")}})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	first, err := r.DoCache(ctx, r.B().Get().Key("inventory").Cache(), time.Minute).ToString()
	if err != nil {
		return nil, err
	}
	second := r.DoCache(ctx, r.B().Get().Key("inventory").Cache(), time.Minute)
	if !second.IsCacheHit() {
		return nil, fmt.Errorf("expected cache hit")
	}
	results := r.DoMulti(ctx, r.B().Get().Key("inventory").Build(), r.B().Hgetall().Key("order").Build())
	if len(results) != 2 || results[0].Error() != nil || results[1].Error() != nil {
		return nil, fmt.Errorf("pipeline changed")
	}
	return map[string]any{"initial": value, "current": first, "cached": second.IsCacheHit()}, nil
}

func mongoWorkflow(ctx context.Context) (any, error) {
	var monitored atomic.Int64
	client, err := mongo.Connect(options.Client().ApplyURI(os.Getenv("MONGO_URL")).SetMonitor(&event.CommandMonitor{Started: func(context.Context, *event.CommandStartedEvent) { monitored.Add(1) }}))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(context.Background())
	coll := client.Database("repro_test").Collection("orders")
	insert, err := coll.InsertOne(ctx, bson.M{"_id": "order-1", "amount": 1099, "items": bson.A{"a", "b"}})
	if err != nil {
		return nil, err
	}
	if _, err = coll.UpdateOne(ctx, bson.M{"_id": insert.InsertedID}, bson.M{"$set": bson.M{"amount": 1299}}); err != nil {
		return nil, err
	}
	var found bson.M
	if err = coll.FindOne(ctx, bson.M{"_id": "order-1"}).Decode(&found); err != nil {
		return nil, err
	}
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var all []bson.M
	for cursor.Next(ctx) {
		var row bson.M
		if err = cursor.Decode(&row); err != nil {
			return nil, err
		}
		all = append(all, row)
	}
	if cursor.Err() != nil {
		return nil, cursor.Err()
	}
	if _, err = coll.InsertOne(ctx, bson.M{"_id": "order-1"}); !mongo.IsDuplicateKeyError(err) {
		return nil, fmt.Errorf("duplicate key semantics changed: %v", err)
	}
	if err = coll.FindOne(ctx, bson.M{"_id": "missing"}).Decode(&bson.M{}); err != mongo.ErrNoDocuments {
		return nil, fmt.Errorf("missing document semantics changed: %v", err)
	}
	session, err := client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)
	err = mongo.WithSession(ctx, session, func(tx context.Context) error {
		if e := session.StartTransaction(); e != nil {
			return e
		}
		if _, e := coll.UpdateOne(tx, bson.M{"_id": "order-1"}, bson.M{"$set": bson.M{"amount": 9999}}); e != nil {
			return e
		}
		return session.AbortTransaction(tx)
	})
	if err != nil {
		return nil, err
	}
	var rolledBack bson.M
	if err = coll.FindOne(ctx, bson.M{"_id": "order-1"}).Decode(&rolledBack); err != nil {
		return nil, err
	}
	if rolledBack["amount"] != int32(1299) {
		return nil, fmt.Errorf("rollback changed business value: %v", rolledBack)
	}
	_, err = session.WithTransaction(ctx, func(tx context.Context) (any, error) {
		return coll.InsertMany(tx, []any{bson.M{"_id": "order-2", "amount": 1500}, bson.M{"_id": "order-3", "amount": 2500}})
	})
	if err != nil {
		return nil, err
	}
	bulk, err := coll.Find(ctx, bson.M{}, options.Find().SetBatchSize(1))
	if err != nil {
		return nil, err
	}
	var committed []bson.M
	if err = bulk.All(ctx, &committed); err != nil {
		return nil, err
	}
	if len(committed) != 3 {
		return nil, fmt.Errorf("transaction commit lost documents")
	}
	partial, err := coll.Find(ctx, bson.M{}, options.Find().SetBatchSize(1))
	if err != nil {
		return nil, err
	}
	if !partial.Next(ctx) {
		return nil, fmt.Errorf("early-close cursor empty")
	}
	var first bson.M
	if err = partial.Decode(&first); err != nil {
		return nil, err
	}
	if err = partial.Close(ctx); err != nil {
		return nil, err
	}
	if monitored.Load() < 10 {
		return nil, fmt.Errorf("customer command monitor not preserved")
	}
	return map[string]any{"found": found, "all": all, "committed": committed, "monitored": monitored.Load()}, nil
}

func main() {
	var mu sync.Mutex
	var events []map[string]any
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			panic(err)
		}
		mu.Lock()
		events = append(events, batch.Events...)
		mu.Unlock()
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(batch.Events), "dropped": 0})
	}))
	defer collector.Close()
	config := repro.Config{TenantID: "TENANT_go_test", AppID: "APP_go_test", AppSecret: "test-only-secret", AppName: "go-integrations", IngestBase: collector.URL, CaptureRaw: true}
	prefix := "S_go_"
	if endpoint := os.Getenv("REPRO_TEST_COLLECTOR"); endpoint != "" {
		config.IngestBase = endpoint
		config.TenantID = os.Getenv("REPRO_TEST_TENANT")
		config.AppID = os.Getenv("REPRO_TEST_APP")
		config.AppSecret = os.Getenv("REPRO_TEST_SECRET")
		prefix = os.Getenv("REPRO_TEST_SESSION_PREFIX")
	}
	if os.Getenv("REPRO_TEST_POLICY") == "1" {
		config.CaptureRaw = false
		config.Privacy = repro.PrivacyConfig{APIBase: os.Getenv("REPRO_TEST_API"), Environment: "prod"}
	}
	if os.Getenv("REPRO_TEST_LIVE") == "1" {
		config.Mode = "live"
		config.ActorLabels = map[string]string{"team": "operations"}
		config.Live = repro.LiveConfig{APIBase: os.Getenv("REPRO_TEST_API"), AdmissionTimeout: 5 * time.Second, DefineActorID: func(r *http.Request) string { return r.Header.Get("X-Test-Actor") }}
	}
	runtime, err := repro.Init(config)
	if err != nil {
		panic(err)
	}
	if os.Getenv("REPRO_TEST_LIVE") == "1" {
		liveWorkflow(runtime)
		return
	}
	workflows := map[string]func(context.Context) (any, error){"sql": sqlWorkflow, "pgx": pgxWorkflow, "redis": redisWorkflow, "mongo": mongoWorkflow, "mongo-v1": mongoV1Workflow, "redis-topology": redisTopology}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result, err := workflows[r.URL.Path[1:]](r.Context())
		if err != nil {
			w.WriteHeader(500)
			result = map[string]any{"error": err.Error()}
		}
		if os.Getenv("REPRO_TEST_LARGE_VALUE") == "1" {
			result = largeResponse(result)
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer app.Close()
	responses := map[string]any{}
	names := []string{"sql", "pgx", "redis", "mongo", "mongo-v1"}
	if selected := os.Getenv("REPRO_TEST_WORKFLOWS"); selected != "" {
		names = strings.Split(selected, ",")
	}
	for _, name := range names {
		req, _ := http.NewRequest("GET", app.URL+"/"+name, nil)
		req.Header.Set("x-bug-session-id", prefix+name)
		req.Header.Set("x-bug-action-id", "A_"+name)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			panic(string(data))
		}
		var value any
		_ = json.Unmarshal(data, &value)
		responses[name] = value
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		panic(err)
	}
	mu.Lock()
	defer mu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"responses": responses, "events": events})
}

func largeResponse(value any) any {
	return map[string]any{"result": value, "document": strings.Repeat("exact-captured-business-value/", 500), "email": "customer@example.test", "apiConfigService": map[string]any{"secret": "never-leave-the-app"}}
}
