# Backend-only applications

For an API-only application, start with the
[three-step README quickstart](../README.md#api-only-quickstart).
It includes a diagram and a timestamped example explaining when sessions start,
continue and become complete. No frontend SDK is required.

## More than one API

Initialize Repro once in each service, build each with `repro-go`, and give each
a distinct service name. Use the Repro application credentials assigned to that
service; services in the same Repro app may share app credentials.

Instrumented outgoing HTTP calls propagate session correlation to instrumented
downstream services. Those calls remain part of the original captured work,
rather than starting unrelated sessions just because they cross a service boundary.

## Sampling and customer identity

The quickstart uses 100% sampling and a stable service actor, `orders-api`.
That groups the service's traffic, not individual customers.

- Lower `SampleRate`, for example to `0.1`, for sampling. It is not a promise
  that exactly one in ten requests is recorded.
- For per-customer HTTP grouping, set `Live.DefineActorID` to return a stable,
  trusted customer identity.
- The resolver runs before the application handler. Identity added by middleware
  later in the handler chain may not be available yet. Do not trust an unvalidated
  caller-supplied identity header.
- A blank resolved HTTP actor skips capture. Without a resolver, the configured
  service `ActorID` is used; its default is `AppID`.
- `ActorLabels` describe the service. `sdk.SetLiveCapture` supports expiring actor
  or label targets. See the [configuration examples](../README.md#live).
- Sampling and targeting remain subject to quotas.

The API retains the sampling decision across continuous activity; it does not
draw a new decision for every request. After five minutes without admission
activity for that actor, the next claim starts a new sampling chain. This is
separate from the three-minute session windows described in the README.

## Message-driven backends

| Existing workload | Backend-only live capture |
| --- | --- |
| Incoming HTTP request | Can start a session automatically. |
| Supported Kafka consumer | An uncorrelated message can start a session; correlated messages keep their existing session. |
| RabbitMQ with `amqp091-go` | Consumer admission starts at a compiled application function receiving a typed `amqp.Delivery`. |
| Correlated goroutine started during captured work | Retains capture context, subject to drain and memory limits. |
| Standalone cron, timer or polling loop with only outgoing calls | Does not currently start a Go capture scope by itself. |

Message consumers use the configured service actor, not `Live.DefineActorID`.
A bare inline RabbitMQ channel-receive loop without a typed delivery handler is
not covered by the consumer hook. This SDK version has no public manual
job-session API; initialization alone does not instrument arbitrary jobs as
independent sessions.

## Troubleshooting: no backend session appears

1. Use `Mode: "live"`; staging mode needs existing upstream correlation.
2. Build with `repro-go build`, not ordinary `go build`.
3. Check `Init` errors, app credentials and the service's approved policy.
4. Check sampling, quota admission and delivery errors.
5. Send traffic through a supported incoming HTTP or consumer boundary.

An HTTP 200 from your API proves the application responded, not that recording
succeeded. Sessions may remain processing until their capture window closes and
expected evidence has been ingested. Call `sdk.Shutdown(ctx)` after stopping
incoming work during graceful shutdown, and check its returned error.
