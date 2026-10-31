# go-sparkplugb Releases

## v0.2.1

**Date:** 2026-10-31
**Previous release:** v0.2.0

### Changed

- Policy 1.2: build output in `bin/` and `release/`, a per-package coverage floor, and more tests and CI. No change in behaviour.

---

## v0.2.0

**Date:** 2026-10-30
**Previous release:** v0.1.3

## Summary

Broker logins without a custom transport, aliases that are checked to be unique, and string limits that are enforced.

## Changes

### Added

- **`Username` and `Password` in `Config` of `edge`, `host` and `app`**, passed to `ConnectOpts`, so a broker that requires a login no longer needs a custom `mqtt.Transport`. They are never logged. `New` refuses a `Password` without a `Username`.
- **`payload.ErrStringTooLarge`.**

### Fixed

- **`edge`: an alias that is not unique across the edge node is refused** with `ErrDuplicateID` by `PublishBirth` and `PublishDeviceBirth`; a later metric used to overwrite the earlier one without a word. A device repeating its birth keeps its aliases.
- **`payload.GetValue` enforces its limits:** a string or StringArray element above `MaxStringBytes` returns `ErrStringTooLarge`, and a StringArray above `MaxArrayElements` returns `ErrArrayTooLarge`. `MaxStringBytes` was exported and unused.

### Deprecated

- **`edge.ErrInvalidState`:** never returned. It stays for source compatibility.

### Documentation

- **MQTT versions:** the Paho adapter asks for 3.1.1 and falls back to 3.1, with no MQTT 5. `docs/LICENSING.md` no longer lists `paho.golang`, which is not a dependency, and lists the indirect ones.
- **New:** API.md, ARCHITECTURE.md, OBSERVABILITY.md, TESTING.md, docs/FEATURE-STATUS.md.

### Build

- `make e2e`, `make bench`.

### Dependencies

- `golang.org/x/net` → v0.62.0 (govulncheck).

---

## v0.1.3

**Date:** 2026-10-27
**Previous release:** v0.1.2

## Summary

A restarted edge node's DBIRTH delivered before its NBIRTH is no longer
lost; current dependencies and Go releases; and a README that compares
go-sparkplugb with the other Sparkplug B stacks it is tested against.

## Changes

### Fixed

- **`host` and `app`: a message behind the expected `seq` is held for the node's next session** and applied after its NBIRTH. When a node restarted, NanoMQ and HiveMQ CE sometimes delivered the new session's DBIRTH (seq 1) before its NBIRTH; the host took it for a gap in the old session and dropped it, so the device stayed offline unless a rebirth came. Found by the interop tests against pysparkplug on NanoMQ.

### Changed

- **Requires Go 1.26 or later** (was 1.24): `golang.org/x/net` v0.61.0 and `golang.org/x/sync` v0.24.0, their latest releases, need it. CI tests Go 1.26 and 1.27, the releases the Go project supports.

### Documentation

- **README:** a comparison with Eclipse Tahu, pysparkplug, SparkplugNet, srad and sparkplughost, from their source at the tested versions; the compatibility matrix by reference stack and broker; the configuration tables list every field (`BdSeqStore`, `RetryPolicy`, `Logger`, `Metrics`, `OnDeviceCmd`) and no longer a `UseAliases` field that does not exist.

---

## v0.1.2

**Date:** 2026-10-27
**Previous release:** v0.1.1

## Summary

The host and the application no longer flood an edge node with rebirth
requests when a broker delivers its messages out of order. Found by the
interop tests against srad on HiveMQ CE.

## Changes

### Fixed

- **`host` and `app`: at most one rebirth request per edge node every 2 seconds**, for a seq gap and a reorder timeout as already for a node without a birth. A broker that reorders messages across topics (HiveMQ CE, NanoMQ) made several messages in a row look out of sequence, each answered with its own rebirth request, so the node published one NBIRTH per request and the host was still applying them after the node's NDEATH.

### Changed

- **Interop tests:** the reference roles get a 60 s MQTT connect timeout instead of the contract's 10 s: Eclipse Tahu on a busy runner sometimes did not reach a HiveMQ that had just started in time.

---

## v0.1.1

**Date:** 2026-10-27
**Previous release:** v0.1.0

## Summary

The edge node follows its primary host as Sparkplug 3.0.0 requires when the
host goes offline, also with a single MQTT server. Found by running
sparkplugbctl's edge simulator against its host simulator.

## Changes

### Fixed

- **`edge`: when the primary host's STATE goes offline the node disconnects and starts over** (tck-id-operational-behavior-edge-node-termination-host-offline and -reconnect): NDEATH, DISCONNECT, then a new session on the next server of `ServerURLs`, or on the same server when there is one, waiting for the host's STATE online before the next birth. With a single server the node stayed connected in state `Disconnected` and never came back.
- **`edge`: an offline STATE older than the latest online STATE is ignored** (tck-id-operational-behavior-edge-node-termination-host-offline-timestamp): it ends an earlier session of the host.

---

## v0.1.0

**Date:** 2026-10-27
**Previous release:** none

## Summary

The first release: a Sparkplug B 3.0.0 edge node, primary host application
and application for Go, qualified on every push against six independent
Sparkplug stacks on four MQTT brokers, through the images of
[otfabric/sparkplugb-interop v0.1.0](https://github.com/otfabric/sparkplugb-interop/releases/tag/v0.1.0),
pinned by digest. Running against them found the defects fixed below.

## Changes

### Added

- **Packages** `edge` (edge node with devices, aliases, NCMD/DCMD handling, rebirth, primary host STATE), `host` (primary host application with STATE, `bdSeq` and `seq` tracking, rebirth requests), `app` (application without STATE), `payload` (every scalar, array, DataSet and Template datatype), `topic`, `mqtt` (transport over Eclipse Paho) and `proto` (the Sparkplug B protobuf).
- **Interop suite** (`make interop`, module `interop/`): the library's host and application against the reference edge nodes of Eclipse Tahu, pysparkplug, SparkplugNet, srad and Eclipse Tahu JavaScript, and the library's edge node against the reference hosts of Eclipse Tahu, pysparkplug, SparkplugNet, srad and sparkplughost, on Mosquitto, HiveMQ CE, EMQX and NanoMQ. See [INTEROPERABILITY.md](INTEROPERABILITY.md).

### Fixed (found by the interop suite)

- **`host` and `app`: `GetNode` and `Nodes` return snapshots.** They returned the live state, which the MQTT goroutine kept changing under the caller.
- **`host` and `app`: a metric of a data message that carries only an alias keeps the name and datatype of its birth.** Its value was stored without them.
- **`payload`: integer values are read from whichever of `int_value` and `long_value` carries them.** Eclipse Tahu (Java and JavaScript) encodes UInt32 in `long_value`; the library read 0.
- **`edge`: NBIRTH carries `Node Control/Rebirth`** (Boolean, false), as the specification requires, unless the caller already has it.
- **`edge`: a rebirth publishes the current values**, not those of the first birth.
- **`edge`: a primary host whose STATE is retained is seen.** The node subscribed to STATE before it started waiting for it, and missed a retained online message that arrived in between.
- **`host`: data from an edge node without a birth is answered with a rebirth request** (at most one per node every 2 seconds), as the specification requires, instead of being dropped.
- **`host`: messages of a node that arrive out of `seq` order are reordered.** MQTT orders messages only per topic; a broker may deliver a DBIRTH before its NBIRTH (NanoMQ does). Messages ahead of the expected `seq` are held, at most 256 per node and 30 seconds, and applied in order.

---
