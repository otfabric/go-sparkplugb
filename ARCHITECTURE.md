# Architecture

```text
edge, host, app      runtimes: session state, seq, bdSeq, births, reordering
   |      |
   |      +--> payload --> proto     metrics and Protobuf
   +--> topic                        spBv1.0 namespace
   +--> mqtt.Transport <-- PahoTransport (eclipse/paho.mqtt.golang)
```

The runtimes import `mqtt`, `payload`, `proto`, `topic` and the root package, and not each other. Only `mqtt/paho.go` imports Paho, so a runtime sees the broker through `mqtt.Transport` alone, which is also what its tests fake.

## State

Each runtime keeps one mutex-protected state machine. Edge node states: `Disconnected`, `Connecting`, `Subscribing`, `WaitingForPrimaryHost`, `Online`, `Closed`. A generation counter (`gen`) in the edge node keeps callbacks of an old connection from acting on the new one.

`PahoTransport` turns Paho's automatic reconnection off (`SetAutoReconnect(false)`): reconnecting is the decision of the runtime or caller, since an edge node needs a new `bdSeq` and births for each session.

## Goroutines

- Messages arrive on the goroutines of the transport, which call the handler given to `Subscribe`. Callbacks in `Config` run there, so a slow callback holds up the message flow.
- The edge node starts one goroutine per auto-rebirth (`edge.go`, on an NCMD rebirth request) and one for server walking.
- `host` and `app` start one goroutine per rebirth request they send.

## Reordering

`host` and `app` hold messages out of `seq` order, at most 256 per node and 30 seconds: [INTEROPERABILITY.md](INTEROPERABILITY.md#host-and-application-behaviour).

## Generated code

`proto/sparkplug_b.pb.go` is generated from the pinned `proto/sparkplug_b.proto`; `make generate-check` and the `generated` CI job keep them equal. Provenance: [docs/LICENSING.md](docs/LICENSING.md).
