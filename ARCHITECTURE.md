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

Each runtime keeps one mutex-protected state machine. Edge node states: `Disconnected`, `Connecting`, `Subscribing`, `WaitingForPrimaryHost`, `Online`, `Closed`. A generation counter (`gen`) in the edge node keeps callbacks of an old connection from acting on the new one. `Close` sets a closed flag under the lock, once; `Connect` checks it again after each step it takes without the lock (the transport's `Connect` and `Subscribe`, and the host's STATE birth), and ends with `ErrClosed` and a disconnected transport when it is set.

A transition is queued under the lock and delivered to `Metrics.OnStateChange` and `OnState` after the lock is released, in order, by one goroutine at a time (`internal/callback.Queue`): the one that made it, or one that is delivering an earlier transition. So `OnState` may call the runtime. `Disconnect` and a `Connect` that fails deliver after the transport's `Disconnect` has returned: a handler that connects again finds the transport free. The host also holds a second lock, `stateMu`, from its STATE birth until the transition to `Connected`, so that its STATE death never goes out before its birth.

Every call of application code (the callbacks of `Config`, `Metrics`, the `Logger`, a `BdSeqStore`, a `RetryPolicy`) goes through `internal/callback`, which recovers a call that does not return normally and logs it with its stack once no lock of the runtime is held ([API.md](API.md#callbacks)).

`PahoTransport` turns Paho's automatic reconnection off (`SetAutoReconnect(false)`): reconnecting is the decision of the runtime or caller, since an edge node needs a new `bdSeq` and births for each session.

## Goroutines

- Messages arrive on the goroutines of the transport, which call the handler given to `Subscribe`. Callbacks in `Config` run there, without a lock of the runtime held, so a slow callback holds up the message flow.
- The edge node publishes one message at a time: a message takes its `seq` and goes to the transport under one lock, which a rebirth holds for its NBIRTH and all its DBIRTHs. Publishes from several goroutines are safe and reach the wire in `seq` order, given the ordering `mqtt.Transport.Publish` documents; a slow `Publish` of the transport holds up the others. `PahoTransport.Publish` keeps that ordering by returning, on its context or timeout too, only once paho has the message in line, or once the connection has ended.
- The edge node starts one goroutine per auto-rebirth (`edge.go`, on an NCMD rebirth request) and one for server walking.
- `host` and `app` start one goroutine per rebirth request they send, at most 16 at a time over all edge nodes.
- `PahoTransport.Connect` waits for the CONNACK, its context and its timeout without holding the transport's lock.

## Reordering

`host` and `app` hold messages out of `seq` order, at most 256 per node, 4096 and 8 MiB of payload over all nodes, and 30 seconds: [INTEROPERABILITY.md](INTEROPERABILITY.md#host-and-application-behaviour). When a message is held or an NDEATH handled, at most once a second, the messages of every node that waited longer are dropped, and the rebirth requests and deaths whose time has passed are forgotten, so a node that sends nothing more leaves nothing behind.

## Generated code

`proto/sparkplug_b.pb.go` is generated from the pinned `proto/sparkplug_b.proto`; `make generate-check` and the `generated` CI job keep them equal. Provenance: [docs/LICENSING.md](docs/LICENSING.md).
