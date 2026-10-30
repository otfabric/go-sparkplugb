# Observability

The library is silent by default: a nil `Logger` logs nothing, and `Metrics` defaults to `NopMetrics`.

## Logging

Set `Config.Logger` (`sparkplugb.Logger`). `sparkplugb.NewSlogLogger` adapts an `slog.Handler` and `sparkplugb.NewStdLogger` a `*log.Logger`. A `FieldLogger` also gets `component=edge`, `host` or `app`.

| Level | Used for |
|---|---|
| Info | State transitions, rebirth requests, server walking |
| Warn | Lost transport, ignored malformed topics and payloads, seq and bdSeq problems |
| Error | Failed auto-rebirth, server walking giving up |

A `Logger` is called from many goroutines and must not block. Credentials are never logged (`TestE2E_brokerLogin` checks that). Broker URLs are, so keep credentials out of them.

## Metrics

Set `Config.Metrics`. Methods are synchronous: safe for concurrent use, quick and non-blocking. Embed `NopMetrics` to implement only some.

| Runtime | Methods |
|---|---|
| `edge` | `OnConnect`, `OnPublish`, `OnMessage`, `OnStateChange` |
| `host`, `app` | `OnConnect`, `OnMessage`, `OnStateChange`, `OnNodeBirth`, `OnNodeDeath` |

`OnNodeBirth` and `OnNodeDeath` carry group and node IDs, so their cardinality is that of the fleet.

## State

`Config.OnState` and `State()` give the state of a runtime. `host` and `app` expose the tracked nodes through `GetNode` and `Nodes`, which return copies.
