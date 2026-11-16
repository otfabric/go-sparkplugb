# Observability

The library is silent by default: a nil `Logger` logs nothing, and `Metrics` defaults to `NopMetrics`.

## Logging

Set `Config.Logger` (`sparkplugb.Logger`). `sparkplugb.NewSlogLogger` adapts an `slog.Handler` and `sparkplugb.NewStdLogger` a `*log.Logger`. A `FieldLogger` also gets `component=edge`, `host` or `app`.

| Level | Used for |
|---|---|
| Info | State transitions, rebirth requests, server walking |
| Warn | Lost transport, ignored malformed topics and payloads, seq and bdSeq problems |
| Error | Failed auto-rebirth, server walking giving up, application code that did not return normally (`callback did not return normally: callback=<name> value=<value>`, with the stack) |

`host` and `app` quote the group, edge node and device IDs of a received topic in their entries (`"group/node"`, Go syntax), since the IDs may hold control characters.

A `Logger` is called from many goroutines, sometimes with a lock of the runtime held: it must not block or call the runtime. A `Logger` method that does not return normally loses its entry, and is reported once through the `Logger`'s `Errorf`. Credentials are never logged (`TestE2E_brokerLogin` checks that). Broker URLs are, so keep credentials out of them.

## Metrics

Set `Config.Metrics`. Methods are synchronous, called without a lock of the runtime held: safe for concurrent use, quick and non-blocking. `OnStateChange` is called with each transition, in order, just before `OnState`. Embed `NopMetrics` to implement only some.

| Runtime | Methods |
|---|---|
| `edge` | `OnConnect`, `OnPublish`, `OnMessage`, `OnStateChange` |
| `host`, `app` | `OnConnect`, `OnMessage`, `OnStateChange`, `OnNodeBirth`, `OnNodeDeath` |

`host` and `app` call `OnMessage` for every received message whose topic parses, STATE included, before the payload is decoded. `OnNodeBirth` and `OnNodeDeath` carry group and node IDs, so their cardinality is that of the fleet.

A `host` or `app` `Metrics` that also implements `sparkplugb.SessionDropMetrics` gets `OnSessionDrop(groupID, edgeNodeID, messageType)` for each message dropped as belonging to a session that has ended: out of sequence and older than the NBIRTH, always, and what `Config.SessionChecks` drops ([API.md](API.md#session-checks)). Each drop is also logged at debug level, with the check that made it.

A `host` or `app` `Metrics` that also implements `sparkplugb.HeldDropMetrics` gets `OnHeldDrop(groupID, edgeNodeID, messageType)` for each message dropped from those held for reordering to keep them within their bounds over all edge nodes, 4096 messages and 8 MiB of payload ([SECURITY.md](SECURITY.md)): the oldest held message of any edge node makes room for the next. The message type is NDATA, DDATA, DBIRTH or DDEATH. Each drop is also logged at debug level. A held message that waited 30 s for the gap before it is dropped without this call. Its group and node IDs give it the cardinality of the fleet.

## State

`Config.OnState` and `State()` give the state of a runtime. `OnState` may call the runtime ([API.md](API.md#callbacks)). `host` and `app` expose the tracked nodes through `GetNode` and `Nodes`, which return copies.
