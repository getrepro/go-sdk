// Package instrumentation supplies library hooks to the compile-time engine.
package instrumentation

import (
	"github.com/getrepro/go-sdk/internal/bridge"
	"github.com/getrepro/go-sdk/internal/capture"
)

type HookContext = bridge.HookContext

func before(ctx HookContext, args []any) {
	if beforeAMQP(ctx, args) {
		return
	}
	if beforeKafka(ctx, args) {
		return
	}
	if ctx.GetPackageName() == "net/http" && ctx.GetFuncName() == "ServeHTTP" {
		beforeHTTPServer(ctx, args)
		return
	}
	if capture.Current() == nil || capture.Current().Scope == nil {
		return
	}
	if ctx.GetPackageName() == "net/http" && ctx.GetFuncName() == "do" {
		beforeHTTPClient(ctx, args)
		return
	}
	if beforeDatabase(ctx, args) || beforeRedis(ctx, args) {
		return
	}
	call := capture.Enter(capture.Meta{Function: ctx.GetPackageName() + "." + ctx.GetFuncName()}, args)
	ctx.SetData(call)
}

func after(ctx HookContext, values []any) {
	if amqp, ok := ctx.GetData().(*amqpCall); ok {
		amqp.finishPublish(ctx, values)
		return
	}
	if copy, ok := ctx.GetData().(*copyValueCall); ok {
		if len(values) > 0 && resultError(values) == nil {
			copy.record.Row(copy.scope.Value(values[0]))
		}
		return
	}
	if kafka, ok := ctx.GetData().(*kafkaCall); ok {
		kafka.finish(ctx, values)
		return
	}
	if client, ok := ctx.GetData().(*httpClientCapture); ok {
		client.finish(values)
		return
	}
	if redis, ok := ctx.GetData().(*redisCall); ok {
		redis.finish(values)
		return
	}
	if db, ok := ctx.GetData().(*dbCall); ok {
		db.finish(values)
		return
	}
	if server, ok := ctx.GetData().(*httpServerCapture); ok {
		server.finish(ctx.GetKeyData("panic"))
		return
	}
	if call, ok := ctx.GetData().(*capture.Call); ok {
		call.End(values, nil)
	}
}

// Enter and Exit are generated into application functions; callers do not wrap them.
func Enter(name, file string, line int, args []any) *capture.Call {
	var consumer *kafkaCall
	var amqpConsumer *amqpCall
	if frame := capture.Current(); frame == nil || frame.Scope == nil || frame.Scope.Closed() {
		for _, arg := range args {
			if isAMQPDelivery(arg) {
				amqpConsumer = beginAMQPConsumer(arg)
				if amqpConsumer != nil {
					break
				}
			}
			if isKafkaMessage(arg) {
				consumer = beginConsumer(arg)
				if consumer != nil {
					break
				}
			}
		}
	}
	call := capture.Enter(capture.Meta{Function: name, File: file, Line: line}, args)
	if call != nil && amqpConsumer != nil {
		call.OnEnd = func(results []any, p any) {
			if p == nil {
				p = resultError(results)
			}
			amqpConsumer.complete(p, false)
			capture.Restore(amqpConsumer.previous)
		}
	}
	if call != nil && consumer != nil {
		call.OnEnd = func(results []any, p any) {
			if p == nil {
				p = resultError(results)
			}
			consumer.complete(p)
			capture.Restore(consumer.previous)
		}
	}
	return call
}

func Exit(call *capture.Call, results []any, panicValue any) { call.End(results, panicValue) }
func RegisterPackage(name string)                            { capture.RegisterPackage(name) }
