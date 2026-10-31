package build

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
)

// Read signatures from the actual dependency sources, not guessed arities.
func addIntegrationRules(rules map[string]any, packages []Package) error {
	for _, pkg := range packages {
		if !integrationPackage(pkg.ImportPath) {
			continue
		}
		rules["repro_library_bridge_"+pkg.ImportPath] = map[string]any{"target": pkg.ImportPath, "do": []any{map[string]any{"add_file": map[string]string{"file": "library_bridge.go", "path": sdkPath + "/instrumentation/gls"}}}}
		for _, file := range pkg.GoFiles {
			fs := token.NewFileSet()
			tree, err := parser.ParseFile(fs, filepath.Join(pkg.Dir, file), nil, 0)
			if err != nil {
				return err
			}
			for _, decl := range tree.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || fn.Recv == nil {
					continue
				}
				var out bytes.Buffer
				_ = format.Node(&out, fs, fn.Recv.List[0].Type)
				recv := out.String()
				if !selectedIntegration(pkg.ImportPath, strings.TrimPrefix(recv, "*"), fn.Name.Name) {
					continue
				}
				name := fmt.Sprintf("repro_lib_%d", len(rules))
				params := append(rawNames(fn.Recv, false), rawNames(fn.Type.Params, false)...)
				if pkg.ImportPath == "github.com/segmentio/kafka-go" && recv == "*writeBatch" && fn.Name.Name == "complete" {
					params = []string{"&" + fn.Recv.List[0].Names[0].Name + ".msgs", "&err"}
				}
				results := rawNames(fn.Type.Results, true)
				raw := fmt.Sprintf("__reproLibraryState := _reproLibraryEnterV1(%s,%s,[]interface{}{%s}); defer func(){ __reproPanic := recover(); _reproLibraryExitV1(__reproLibraryState,[]interface{}{%s},__reproPanic); if __reproPanic != nil { panic(__reproPanic) } }()", strconv.Quote(pkg.ImportPath), strconv.Quote(fn.Name.Name), strings.Join(params, ","), strings.Join(results, ","))
				rules[name] = map[string]any{"target": pkg.ImportPath, "where": map[string]string{"func": fn.Name.Name, "recv": recv}, "do": []any{map[string]any{"inject_code": map[string]string{"raw": raw}}}}
			}
		}
	}
	return nil
}

func rawNames(fields *ast.FieldList, results bool) []string {
	var out []string
	if fields == nil {
		return out
	}
	unnamed := 0
	for _, field := range fields.List {
		if len(field.Names) == 0 {
			if results {
				out = append(out, fmt.Sprintf("&_unnamedRetVal%d", unnamed))
				unnamed++
			} else {
				out = append(out, "nil")
			}
			continue
		}
		for _, name := range field.Names {
			if name.Name == "_" {
				out = append(out, "nil")
			} else {
				out = append(out, "&"+name.Name)
			}
		}
	}
	return out
}

func fieldCount(fields *ast.FieldList) int {
	n := 0
	if fields != nil {
		for _, field := range fields.List {
			n += max(1, len(field.Names))
		}
	}
	return n
}

func integrationPackage(pkg string) bool {
	switch pkg {
	case "github.com/rabbitmq/amqp091-go":
		return true
	case "net/http", "database/sql", "github.com/jackc/pgx/v5", "github.com/redis/go-redis/v9", "github.com/redis/rueidis", "github.com/segmentio/kafka-go", "github.com/twmb/franz-go/pkg/kgo", "go.mongodb.org/mongo-driver/mongo", "go.mongodb.org/mongo-driver/v2/mongo":
		return true
	}
	return false
}

func one(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}

func selectedIntegration(pkg, recv, fn string) bool {
	switch pkg {
	case "github.com/rabbitmq/amqp091-go":
		return recv == "Channel" && one(fn, "Publish", "PublishWithContext", "PublishWithDeferredConfirm", "PublishWithDeferredConfirmWithContext")
	case "net/http":
		return (recv == "serverHandler" && fn == "ServeHTTP") || (recv == "Client" && fn == "do")
	case "database/sql":
		return (one(recv, "DB", "Conn", "Tx", "Stmt") && one(fn, "ExecContext", "QueryContext", "PrepareContext", "BeginTx", "Commit", "Rollback")) || (recv == "Rows" && one(fn, "Scan", "Next", "Close"))
	case "github.com/jackc/pgx/v5":
		return (recv == "Conn" && one(fn, "Exec", "Query", "SendBatch", "CopyFrom")) || (recv == "baseRows" && one(fn, "Scan", "Values", "RawValues", "Next", "Close")) || (one(recv, "batchResults", "pipelineBatchResults") && one(fn, "Exec", "Query", "Close")) || (one(recv, "copyFromRows", "copyFromSlice", "copyFromFunc") && fn == "Values")
	case "github.com/redis/go-redis/v9":
		return (one(recv, "Client", "Conn", "Tx", "ClusterClient", "Ring", "SentinelClient") && fn == "Process") || (recv == "Pipeline" && fn == "Exec")
	case "github.com/redis/rueidis":
		return one(recv, "singleClient", "sentinelClient", "clusterClient", "dedicatedSingleClient", "dedicatedClusterClient") && one(fn, "Do", "DoMulti", "DoCache", "DoMultiCache")
	case "go.mongodb.org/mongo-driver/mongo", "go.mongodb.org/mongo-driver/v2/mongo":
		return (recv == "Collection" && one(fn, "InsertOne", "InsertMany", "UpdateOne", "UpdateMany", "ReplaceOne", "DeleteOne", "DeleteMany", "Find", "FindOne", "FindOneAndUpdate", "FindOneAndDelete", "FindOneAndReplace", "Aggregate", "BulkWrite", "CountDocuments", "Distinct")) || (one(recv, "Cursor", "SingleResult", "DistinctResult") && one(fn, "Decode", "All", "Next", "Close")) || (recv == "Session" && one(fn, "CommitTransaction", "AbortTransaction"))
	case "github.com/segmentio/kafka-go":
		return (recv == "Writer" && fn == "WriteMessages") || (recv == "writeBatch" && fn == "complete") || (recv == "Reader" && one(fn, "ReadMessage", "FetchMessage", "CommitMessages", "Close"))
	case "github.com/twmb/franz-go/pkg/kgo":
		return (recv == "Client" && one(fn, "Produce", "TryProduce", "PollRecords", "CommitRecords", "Close")) || (one(recv, "Fetches", "FetchTopic", "FetchPartition", "FetchTopicPartition") && fn == "EachRecord") || (recv == "FetchesRecordIter" && fn == "Next")
	}
	return false
}
