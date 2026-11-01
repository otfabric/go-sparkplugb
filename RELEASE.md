# go-sparkplugb Releases

## v0.3.0

**Date:** 2026-11-01
**Previous release:** v0.2.3

### Changed

- **`GetDataSetValues` and `GetTemplateParameterValue` return the Go type of the datatype, an API change.** An Int8 cell or parameter is an `int8`, not the raw `uint32`; Int64 an `int64`, not a `uint64`; the rest as `GetValue` returns them. A DateTime cell is a `time.Time`, not a `uint64`. Code with type assertions on `uint32` or `uint64` for those datatypes has to change. It compiles unchanged, and fails at run time.
- **Every integer out of its datatype's range is rejected, a behaviour change.** Metrics (`GetValue`), DataSet cells, template parameters and properties return `ErrTypeMismatch` for a UInt8 above 255, a UInt16 above 65535, a UInt32 or Int32 that does not fit in `long_value`, a DateTime above `math.MaxInt64` milliseconds and an Int8 or Int16 in neither wire form, and for an integer metric whose value is of another kind. Until now they were truncated, or read as 0. A peer that sends such values now gets an error from `GetValue`. Valid values decode as before.
- **`NewDataSet` and `NewTemplateParameter` take the Go type of the datatype** (`int8(-8)` for an Int8). The raw `uint32` and `uint64` still work, but they have to be values of the datatype, which fails for a value out of range that was written before, and they go out in the canonical form: a width-sized Int8 is sign-extended.
- **`SetValue` refuses a `time.Time` before 1970 for DateTime and DateTimeArray,** which wrapped around to a date far in the future. `ErrTypeMismatch`.
- `GetDataSetValues` returns `ErrColumnMismatch` for a row with more cells than the DataSet has types.
- `GetQuality` reads the property as an Int32 and returns false for a value that is not one.

### Added

- `NewPropertyValue` and `GetPropertyValue`, with the same rules, for a metric's PropertySet.
- DateTime in DataSet columns and template parameters, which `NewDataSet` and `NewTemplateParameter` refused.

### Interoperability

- **The interop suite pins sparkplugb-interop v0.3.0,** which it had at v0.1.0. It runs the third update of `scalars` and the new `composite` fixture: DataSets, templates and properties against Tahu, tahu-js and SparkplugNet edges and Tahu, SparkplugNet and sparkplughost hosts, with the form of each Int8 and Int16 checked on the wire. Exclusions and the two SparkplugNet findings are in `INTEROPERABILITY.md`.

### Documentation

- `INTEROPERABILITY.md`, section Integers: what the specification states, and what is this library's reading.

### Tests

- `FuzzIntegerPaths` holds the metric, DataSet cell, template parameter and PropertyValue paths to one statement of the rule. Table tests per path.

---

## v0.2.3

**Date:** 2026-10-31
**Previous release:** v0.2.2

### Changed

- **`payload.GetValue` rejects an Int8 or Int16 that is not a valid wire value, a behaviour change.** It used to truncate every value to the width, so `0x100` read as Int8 0 and `0xFFFFFF7F` as 127. It now accepts the width-sized form and the sign-extended one, which the specification leaves open, and returns `ErrTypeMismatch` for anything else. Valid values decode as before. Negative Int8 and Int16 are still encoded sign-extended to 32 bits, as Eclipse Tahu does: nothing changes on the wire.

### Documentation

- The Int8 and Int16 encoding is recorded as a specification ambiguity, not a defect of any stack (`INTEROPERABILITY.md`, `LIMITATIONS.md`, `docs/CONFORMANCE.md`).

### Tests

- `FuzzDecodeSignedInt`. The interop suite checks which form each reference edge node has on the wire.

---

## v0.2.2

**Date:** 2026-10-31
**Previous release:** v0.2.1

### Fixed

- `PahoTransport` Publish and Subscribe now honour ctx, fail at once when the connection ends, and stay within `PublishTimeout`. paho could block for 30s in the call on a connection that had just dropped, and ctx was ignored.

---

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
