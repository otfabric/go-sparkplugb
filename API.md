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
| `sparkplugb` (root) | `Logger`, `FieldLogger`, `RetryPolicy`, `ExponentialBackoff`, `NoRetry` |

## Lifecycle

All three runtimes take a transport and a `Config`: `New(transport, cfg)`, then `Connect(ctx)`, then `Disconnect(ctx)` or `Close(ctx)`. `Connect` after `Close` returns `ErrClosed`. After a lost connection the state is `Disconnected`, and the caller calls `Connect` again; an edge node with `ServerURLs` and a `PrimaryHostID` walks to the next server on its own when the primary host goes offline (`walkServers` in `edge/edge.go`, with `Config.RetryPolicy`).

`New` validates required fields and refuses a `Password` without a `Username`.

An edge node goes through `PublishBirth`, `PublishDeviceBirth`, then `PublishNodeData`, `PublishDeviceData` and `PublishDeviceDeath`. Data before the birth returns `ErrNoBirth`. Aliases must be unique across the node: a birth that breaks that returns `ErrDuplicateID`.

## Custom transports

`mqtt.Transport` has `Connect`, `Publish`, `Subscribe` and `Disconnect`. An implementation honours the fields of `ConnectOpts`, `Username` and `Password` included, and reports a lost connection through `OnConnectionLost` with an error wrapping `ErrConnectionLost`.

Errors: [ERRORS.md](ERRORS.md).
