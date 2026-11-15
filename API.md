# API

The packages of `github.com/otfabric/go-sparkplugb`. `go doc -all ./<package>` has every signature; the README has the quick starts and configuration tables.

| Package | Use |
|---|---|
| `edge` | `edge.New`, `Node`: Edge Node runtime |
| `host` | `host.New`, `App`: Host Application runtime, publishes STATE |
| `app` | `app.New`, `Application`: consumer that publishes no STATE |
| `mqtt` | `Transport`, `ConnectOpts`, `PahoTransport`, `ErrConnectionLost` |
| `payload` | Metrics, `Encode`, `Decode`, `GetValue`, `SetValue`, DataSets, Templates |
| `topic` | `Parse`, `Topic`, the `For*` builders |
| `proto` | Generated Protobuf code (EPL-2.0, see [docs/LICENSING.md](docs/LICENSING.md)) |
| `sparkplugb` (root) | `Logger`, `FieldLogger`, `RetryPolicy`, `ExponentialBackoff`, `NoRetry`, `SessionChecks`, `SessionDropMetrics` |

## Lifecycle

All three runtimes take a transport and a `Config`: `New(transport, cfg)`, then `Connect(ctx)`, then `Disconnect(ctx)` or `Close(ctx)`. `Connect` after `Close` returns `ErrClosed`. After a lost connection the state is `Disconnected`, and the caller calls `Connect` again; an edge node with `ServerURLs` and a `PrimaryHostID` walks to the next server on its own when the primary host goes offline (`walkServers` in `edge/edge.go`, with `Config.RetryPolicy`).

`New` validates required fields, refuses a `Password` without a `Username`, and refuses an ID that cannot be a topic segment (`GroupID`, `EdgeNodeID`, `PrimaryHostID`, `HostID`: empty where required, not valid UTF-8, or with `/`, `+` or `#`) with `topic.ErrInvalidID`.

`Close` is safe to call more than once and from several goroutines: the first call closes the runtime, the others return nil at once. A `Connect` in progress when `Close` is called, the edge node's walk to another server included, returns `ErrClosed` and leaves no connection up; the state stays `Closed`.

An edge node goes through `PublishBirth`, `PublishDeviceBirth`, then `PublishNodeData`, `PublishDeviceData` and `PublishDeviceDeath`. Data before the birth returns `ErrNoBirth`. Aliases must be unique across the node: a birth that breaks that returns `ErrDuplicateID`. Template definitions go in the NBIRTH only, and every template instance of the node and its devices refers to one of them: a message that breaks that returns `payload.ErrUnknownTemplate` or `payload.ErrInvalidTemplate` and is not published. `payload.NewTemplateDefs` and `TemplateDefs.Check` do the same check for a caller; `host` and `app` use them on receipt and log what breaks it.

## Callbacks

| Callback | Called on | Locks of the runtime held | May call the runtime |
|---|---|---|---|
| `OnState` of `edge`, `host`, `app`; `Metrics.OnStateChange` | The goroutine that made the transition (the caller of `Connect`, `Disconnect` or `Close`, the transport's for a lost connection or a STATE, the edge node's while it walks), or one that was delivering an earlier transition | none | yes: `State`, `PublishBirth` on `Online`, `Close`, `Connect` after `Disconnected`. A transition the call makes is delivered after the handler returns |
| `edge`: `OnNodeCmd`, `OnDeviceCmd`; `Metrics.OnMessage` | The transport's, per message | none | yes |
| `host`, `app`: `OnNodeBirth`, `OnNodeData`, `OnNodeDeath`, `OnDeviceBirth`, `OnDeviceData`, `OnDeviceDeath`; `Metrics.OnNodeBirth`, `OnNodeDeath` | The transport's, per message | none | yes |
| `host`, `app`: `Config.SessionChecksFor`; `SessionDropMetrics.OnSessionDrop` | The transport's, per message | none | `SessionChecksFor`: no, it must not block; `OnSessionDrop`: yes |
| `edge`: `RetryPolicy.ShouldRetry`, `Metrics.OnConnect` | The edge node's, while it walks to another server | none | yes |
| `edge`: `BdSeqStore.Load`, `Store` | The caller of `Connect` | the node's | no |
| `Logger` | Any goroutine of the runtime | sometimes | no |

`OnState` is called once per transition, one call at a time, in the order of the transitions. `Disconnected` after `Disconnect`, or after a `Connect` that failed, is delivered once the runtime has ended the connection, so a `Connect` from the handler is not ended with it. The edge node does not publish an NBIRTH on its own: publish it from `OnState` when the state becomes `Online`, which also covers the session after a walk to another server ([examples/server-walking](examples/server-walking/main.go)). The next message waits for a message callback to return, so it should not block (ARCHITECTURE.md, "Goroutines").

A callback, `Metrics` method, `Logger` method, `BdSeqStore` or `RetryPolicy` that does not return normally is recovered where the runtime calls it and logged at error level, `callback did not return normally: callback=<name> value=<value>` with the stack, once the runtime holds none of its locks. The runtime goes on: the message is applied as without the callback, a `BdSeqStore` fails `Connect`, a `RetryPolicy` ends the walk, a `Logger` loses the entry.

## Session checks

`host.Config` and `app.Config` take `SessionChecks`, a set of `sparkplugb.SessionChecks`, or `SessionChecksFor` to choose them per edge node. None is set by default. They go beyond Sparkplug B 3.0: each drops an NDATA or DDATA of the session that ended, which a broker delivered among the new session's and which would otherwise be applied to it (go-sparkplugb#24, [LIMITATIONS.md](LIMITATIONS.md)), and each drops valid data of an edge node that breaks its assumption.

| Check | Drops | Also drops, from edge nodes that break the assumption |
|---|---|---|
| `CheckBirthMillisecond` | An NDATA or DDATA stamped in the millisecond of the NBIRTH, that arrived before it | A new-session NDATA or DDATA in that millisecond that the broker delivered before the NBIRTH |
| `CheckOlderInSequence` | An NDATA or DDATA stamped before the NBIRTH, within 30 s of it, that carries the `seq` the session expects next | Data stamped earlier than its time of publishing (a sample time), or by a clock that went back |

Births and deaths are never dropped by these checks, and a message without a timestamp is not judged. A valid message dropped leaves a gap in the sequence, which costs a rebirth like a lost message. Use them for edge nodes that stamp a payload with its time of publishing, from a clock that does not go back, as this module's `edge` does. `New` refuses bits other than `SessionChecksAll`. A `Metrics` that implements `sparkplugb.SessionDropMetrics` has `OnSessionDrop` called for every message dropped as of an ended session, by these checks or by the check that is always made (out of sequence and older than the NBIRTH).

## Ownership

| Value | Whose |
|---|---|
| Metrics given to `edge` `PublishBirth`, `PublishDeviceBirth` | The caller's. A metric without a timestamp is stamped. The node keeps copies for `Rebirth`: a later change of the caller's is not in a rebirth until it is sent as data |
| Metrics given to `PublishNodeData`, `PublishDeviceData` | The caller's. A metric without a timestamp is stamped; the payload is encoded before the call returns, and the node keeps copies of the values for the rebirth |
| Metrics a callback receives (`edge` commands, `host`/`app` births and data) | The callback's: the runtime keeps copies of what it stores |
| `host`/`app` `GetNode`, `Nodes` | The caller's: snapshot copies |
| `payload.GetValue` | The `[]byte` of Bytes and File, the DataSet and the Template are the metric's own; arrays are new slices |
| `payload.SetValue`, `NewMetric` | A `[]byte` of Bytes or File, a DataSet and a Template are kept, not copied; arrays are encoded into new bytes |
| `payload.GetDataSetValues` | `columns` is the DataSet's own slice; `types` and `rows` are new |
| `payload.NewDataSet` | `columns` is kept, not copied |
| `payload.NewTemplateDefinition`, `NewTemplateInstance` | The `metrics` and `params` slices are kept, not copied |
| `payload.GetPropertyValue`, `NewPropertyValue` | A PropertySet or PropertySetList is the PropertyValue's own, and is kept, not copied |
| The payload of an `mqtt.MessageHandler` | The handler's: the `host` and `app` runtimes keep it while they hold a message for reordering |

## Custom transports

`mqtt.Transport` has `Connect`, `Publish`, `Subscribe` and `Disconnect`. An implementation honours the fields of `ConnectOpts`, `Username` and `Password` included, and reports a lost connection through `OnConnectionLost` with an error wrapping `ErrConnectionLost`. It gives each message a payload slice of its own, which it does not use or change after the handler is called.

`PahoTransport` returns from `Connect` when the broker accepts the connection, `ctx` ends or `ConnectTimeout` passes, whichever comes first, and an attempt that ended with `ctx` or the timeout does not connect later. It does not hold its lock while it waits: `Disconnect` during `Connect` returns at once and ends the attempt. Its zero value uses the timeouts of `NewPahoTransport`.

Errors: [ERRORS.md](ERRORS.md).
