# go-sparkplugb Releases

## v0.4.3

**Date:** 2026-11-14
**Previous release:** v0.4.2

No API change. The findings of the policy 1.6 review that a patch can carry.

### Fixed

- **`payload`: templates nested more than 32 deep return `ErrInvalidTemplate`,** from `TemplateDefs.Check`, `NewTemplateDefs` and so from the edge node's publish and the `host` and `app` checks on receipt. The checks take time and memory in proportion to the templates they check.
- **`host`, `app`: the messages held for reordering are bounded over all edge nodes,** known or not: at most 4096 messages and 8 MiB of payload, besides 256 per node. Past either bound the oldest held message is dropped. A held message waits at most 30 s for the gap before it, for every edge node, also one that sends nothing more; the rebirth requests and deaths an edge node leaves behind are forgotten once their time has passed. At most 16 rebirth requests are on their way at once, one per edge node.
- **`payload`: the getters return `ErrTypeMismatch` for a value carried in another field than its datatype's,** for every datatype: `GetValue`, `GetPropertyValue`, `GetTemplateParameterValue` and the cells of `GetDataSetValues`, as ERRORS.md documents. `GetDataSetValues` returns `ErrColumnMismatch` for a DataSet whose columns and types differ in count, or whose `num_of_columns` differs from them, and `ErrUnknownType` for a value in a column whose type is not basic. `GetQuality` returns false for a `quality` property whose type is not Int32.
- **`edge`, `host`, `app`: `Close` is safe to call more than once and from several goroutines.** The first call closes the runtime; the others return nil at once.
- **`edge`, `host`, `app`: a `Connect` in progress when `Close` is called returns `ErrClosed`,** the edge node's walk to another server included. The transport is disconnected and the state stays `Closed`.
- **`edge`, `host`, `app`: `OnState` and `Metrics.OnStateChange` are called with no lock of the runtime held,** once per transition, one call at a time, in order. They may call the runtime: `State`, `PublishBirth` on `Online`, `Close`, `Connect` after `Disconnected`, as the message callbacks already could. `Disconnected` after `Disconnect` or a failed `Connect` is delivered once the connection has ended. API.md has a Callbacks table: which goroutine calls each callback and what it may do.
- **`edge`: the metrics of a birth are copied.** `PublishBirth` and `PublishDeviceBirth` keep copies of the caller's metrics for `Rebirth`, so a later change of the caller's is not in a rebirth until it is sent as data. **`host`, `app`:** the birth callbacks receive copies, not the metrics the registry keeps. API.md has an Ownership table, and the doc comments of the getters and constructors say what they keep and return.
- **`edge`, `host`, `app`: a callback, `Metrics` or `Logger` method, `BdSeqStore` or `RetryPolicy` that does not return normally is recovered** where the runtime calls it, and logged at error level with its stack once the runtime holds none of its locks. The runtime goes on.
- **`mqtt`: `PahoTransport.Connect` returns when `ctx` ends,** with an error wrapping `ctx.Err()`, as well as when the broker accepts the connection or `ConnectTimeout` passes. It does not hold its lock while it waits, so `Disconnect` during `Connect` returns at once and ends the attempt, and an attempt that ended with `ctx` or the timeout does not connect later. The zero value of `PahoTransport` uses the timeouts of `NewPahoTransport`; its fields have doc comments.
- **`edge.New`, `host.New` refuse an ID that cannot be a topic segment** (`GroupID`, `EdgeNodeID`, `PrimaryHostID`, `HostID`: not valid UTF-8, or with `/`, `+` or `#`) with `topic.ErrInvalidID`. **`topic.Parse` returns `topic.ErrInvalidID` for an ID that is not valid UTF-8,** as docs/CONFORMANCE.md stated.

### Documentation

- docs/CONFORMANCE.md: the ID rows say what is checked (group and edge node IDs **Y**, device IDs **P**: the device ID given to the edge node's device methods is not checked), and the NCMD rebirth row matches the code. README: the blocking calls and `context.Context`, the ID rules, `OnState` per runtime.
- examples/server-walking publishes the NBIRTH from `OnState` when the node becomes `Online`, also after a walk to another server.
- SECURITY.md: the resource limits above; INTEROPERABILITY.md, ERRORS.md, ARCHITECTURE.md and OBSERVABILITY.md for the changes above.
- TESTING.md: the lifecycle matrix L1–L16 for `edge.Node`, `host.App`, `app.Application` and `mqtt.PahoTransport`, the fuzz targets and allocation budgets, and how the tests wait for events and time.

### Tests

- Lifecycle tests for each runtime: concurrent `Close` (L2), `Close` during `Connect` and during the server walk (L5), `OnState` that calls the runtime (L9, L10) and connects again when `Disconnected`, a callback that does not return normally. `mqtt`: `Connect` with a broker that does not answer, `Disconnect` during `Connect`, the zero value.
- Bounds of the held messages and the rebirth requests in `host` and `app`, with `testing/synctest`; template nesting at and past 32; the getters for every datatype and value kind; birth metric copies; ID tables for `New` and `topic.Parse`.
- Fuzz targets for the message handling of `edge`, `host` and `app`, and for the getters; `FuzzDecode`, `FuzzParse` and `FuzzTemplateDefs` assert invariants. `TestDecodeLimitsAllocationBudget` bounds what the getters allocate. `make fuzz` finds every target with `go test -list`.

### CI

- The release workflow builds and tests otfabric/sparkplugbctl and otfabric/sparkplugb-interop (`own/go-sparkplugb`) against the candidate before it is tagged (`consumers`).

---

## v0.4.2

**Date:** 2026-11-13
**Previous release:** v0.4.1

### Fixed

- **`edge`, `host`, `app`: a `Connect` that fails after the CONNACK closes the connection.** When a subscription failed, or the host's STATE birth could not be published, `Connect` returned the error with the connection still up: its will armed, the runtime left in `Subscribing`, `WaitingForPrimaryHost` or `Connecting`, and the next `Connect` opened a second connection beside it. The connection is now closed with a DISCONNECT and the state is `Disconnected`, so `Connect` can be called again. A DISCONNECT discards the will, so when the host's STATE birth may have reached the broker, the host publishes its STATE death first: edge nodes would otherwise keep its retained STATE online. The edge node published no NBIRTH on such a connection and owes no NDEATH.
- **`app`: the default client ID is random.** An application without `ClientID` took `sparkplug_app_` and the clock in nanoseconds modulo 100000: two applications had 1 chance in 100000 of the same ID, and a broker disconnects a client when another connects with its ID (MQTT 3.1.1, MQTT-3.1.4-2), so they took each other over. The default is now `sparkplug_app_` and 8 hex digits from `crypto/rand`, 22 bytes.

### Tests

- `edge`, `host`, `app`: each failing step after the CONNACK leaves the runtime `Disconnected` with the connection closed, publishes nothing on it, or the STATE death after a failed STATE birth, and a second `Connect` succeeds.
- `app`: the default client ID has its format, differs between applications, and a configured one is kept.

---

## v0.4.1

**Date:** 2026-11-13
**Previous release:** v0.4.0

No API change. A static analysis pass (gosec, opengrep at severity and confidence low) and the move to otfabric/engineering policy 1.5.

### Fixed

- **`payload.GetValue`: the element count of a BooleanArray is compared with `MaxArrayElements` as the 32-bit value read from the payload,** before it becomes an `int`. Where `int` is 32 bits wide (`386`, `arm`), a count of 2^31 or more was not held to that limit until now; such a payload now returns `ErrArrayTooLarge` on every platform.

### Security

- SAST review (gosec, opengrep at severity and confidence low) of the module and of the `interop` module: 35 findings, all reviewed in the code and recorded as false positives or accepted risks in the organisation's private triage, not in this repository: the integer conversions of the Sparkplug wire encodings (a signed integer travels in an unsigned field, the datatype in a `uint32`; each decoded value is range-checked before it is narrowed), the millisecond timestamp of a birth, and `math/rand/v2` for the jitter of reconnect backoff. The `interop` module has none. The BooleanArray count above was found in that review, not by the scanners. There is no `//nolint` or `#nosec`. Gosec and Opengrep do not run in public CI (otfabric/engineering ADR-0011): a maintainer scans before each release.
- SECURITY.md has a Static analysis section.

### Changed

- otfabric/engineering policy 1.5 (was 1.4): `.otfabric/policy.yaml`, the policy checker v1.5.0 in CI and the docs check, and the shared workflows v3.6.0. `otfabric-policy check` v1.5.0: compliant, no failing rule.

### Tests

- `TestBooleanArrayCountHighBit`: counts of 2^31 and 2^32-1 return `ErrArrayTooLarge`; run on linux/386 as well, where it fails without the fix.

---

## v0.4.0

**Date:** 2026-11-13
**Previous release:** v0.3.6

### Added

- **`payload.NewTemplateDefs` and `TemplateDefs.Check`** check the templates of a node's messages against Sparkplug 3.0.0 sections 6.4.13–6.4.14 and tck-id-topics-nbirth-templates: definitions only in the NBIRTH, with `is_definition` true and no `template_ref`; every instance with `is_definition` false and a `template_ref` naming a definition of the NBIRTH; only members and parameters of its definition, and in a birth every member; versions, references and parameter names UTF-8; parameters of a definition with a name and a basic type. Nested instances are checked too. Errors wrap the new sentinels **`payload.ErrUnknownTemplate`** (no such definition in the NBIRTH) and **`payload.ErrInvalidTemplate`** (any other rule). The template version has no matching rule in Sparkplug and is not compared.

### Changed

- **`edge`: a message whose templates break those rules is not published.** `PublishBirth`, `PublishDeviceBirth`, `PublishNodeData` and `PublishDeviceData` return the error, take no `seq` and change no state; until now an edge node could publish an instance of a definition its NBIRTH did not have, or a definition in a DBIRTH or data message. A caller that did must now put its definitions in the NBIRTH.
- **`host` and `app` log a warning for a received template that breaks the rules,** naming the node, the message type and the template, and still apply and deliver the message. A definition that breaks a rule is kept, so its instances are still checked against it: the SparkplugNet 1.4.1 edge node sends an empty `template_ref` with its definitions, which Sparkplug requires to be omitted.
- **`payload.NewDataSet` returns `ErrUnknownType` for a column type that is not basic** (Int8 to Text, tck-id-payloads-dataset-types-value), also when the DataSet has no rows; such a type was accepted before when no row needed encoding.

### Fixed

- **`edge`: a rebirth after data with some of a template instance's members published the instance with only those members.** The stored birth took the data metric as a whole; it now takes the members and parameters the data has and keeps the others, with their datatypes, as an instance in a birth must have every member of its definition (tck-id-payloads-template-instance-members-birth).

### Documentation

- docs/CONFORMANCE.md lists every template, DataSet and PropertySet requirement of Sparkplug 3.0.0 with its TCK ID and status, and what `SendComplexDataTest` checks: its template checks are payload-level, the NBIRTH rules are checked by the TCK's `SendDataTest` and monitor, and three of its requirements are the caller's (alias-only data metrics, no datatype in data, quality values). The quality value 0, 192 or 500 is P: `SetQuality` takes any `int32`. "Complex Data" stays Y, as implemented; the TCK has not been run.
- LIMITATIONS.md, ERRORS.md, API.md, INTEROPERABILITY.md, docs/FEATURE-STATUS.md and the README describe the template checks, how `host` and `app` report them, and the SparkplugNet deviation.

### Tests

- Unit tests for every rule, valid and malformed, nested instances, by-alias data, the edge refusals (nothing published, no `seq` taken), the rebirth merge, and the host and application warnings; `FuzzTemplateDefs`.
- The interop host and application fail on a template warning, with SparkplugNet's empty `template_ref` recorded as a known deviation. Locally (linux/arm64 containers on macOS, sparkplugb-interop v0.4.0) 856 tests and subtests pass and 82 skip for a missing capability or a known upstream bug; 12 application runs on HiveMQ CE and one on EMQX failed because the broker container did not come up in time on the shared Docker host, and pass on a rerun of the application tests on those two brokers (115 pass, none fail).

---

## v0.3.6

**Date:** 2026-11-12
**Previous release:** v0.3.5

### Changed

- **The interop suite pins sparkplugb-interop v0.4.0** (container contract 0.3), which it had at v0.3.0: `interop/images.go` and the Interop workflow take the reference images by the digests of its `manifest.json`. The reference adapters, their upstream versions, the fixtures and the brokers do not change. Contract 0.3 lets the DBIRTHs of a rebirth come in any order; the suite never checked that order, so no test changes. The library does not change.
- The suite passes against the v0.4.0 images on all four brokers: locally (linux/arm64 containers on macOS) 905 tests and subtests pass, 82 skip for a missing capability or a known upstream bug, none fail; the Interop workflow passes on amd64 for Mosquitto, HiveMQ CE, EMQX and NanoMQ.

### Documentation

- README.md, INTEROPERABILITY.md and LIMITATIONS.md name sparkplugb-interop v0.4.0.

---

## v0.3.5

**Date:** 2026-11-12
**Previous release:** v0.3.4

### Fixed

- **`edge`: a message published during a rebirth no longer carries a `seq` of the ending session.** A `Publish*` call took its `seq` under the lock but stamped and published the message after releasing it, and `Rebirth` did not stop other publishes. A DDATA could take its `seq` before the NBIRTH reset the counter and go out after it, with a timestamp no older than the NBIRTH's, and data could go out between the NBIRTH and its device's DBIRTH. A host applied the old value in the place of the new session's message with that `seq`, which was lost (go-sparkplugb#24). A message now takes its `seq` and is published under one lock, which `Rebirth` holds for the NBIRTH and all DBIRTHs; data published meanwhile waits and follows them. Publishes from several goroutines also reach the transport in `seq` order now, where before they could overtake each other.
- **`edge`: an NBIRTH is stamped in a later millisecond than the message before it,** waiting up to a millisecond for the clock; a clock that went back is not waited for (LIMITATIONS.md). A message of the ending session in the millisecond of the NBIRTH, delivered before it, was taken by a host for one of the new session; older than the NBIRTH, it is dropped as of the ending session.
- **`mqtt`: a `PahoTransport.Publish` that returns on its context or timeout can no longer be overtaken.** It returned while paho could still be handing the message to the connection's writer, and a later publish could get there first; with the context already ended, the message was still sent, after the next one. A publish whose context has ended no longer starts, and one that gives up waits until paho has the message in line, bounded by `PublishTimeout`, or until the connection ends. Without this the `edge` serialization above could still let a message of the ending session follow an NBIRTH.

### Changed

- **`mqtt.Transport.Publish` documents the ordering a transport must keep:** when it returns, with or without an error, the message is ahead of every later one or never sent. A custom transport that returns earlier breaks the `seq` order of an edge node.

### Documentation

- LIMITATIONS.md: the two cases in which `host` and `app` cannot tell a message of the ending session from one of the new session, which of them this library's edge node rules out (the same millisecond as the NBIRTH, while its clock does not go back) and which remains with any edge node (older, but delivered after the NBIRTH with the `seq` expected next), and how `host` and `app` recover: a rebirth once 2 s pass without an NBIRTH, which restores the state if the edge node answers it, but not the value delivered meanwhile.

### Tests

- Four goroutines publishing node and device data during a hundred rebirths: every session counts from 0 in order on the wire, no DDATA precedes its DBIRTH, and every NBIRTH is stamped after the message before it. `birthTime` with an injected clock: later, the same millisecond, and a clock that went back.
- `PahoTransport` against a broker that records the order: a publish with an ended context is not sent, and one whose context ends while paho holds it back keeps its place.
- `host` and `app`: both cases of a message of the ending session taking the place of the new session's, the value delivered, the rebirth requested, and the state once the edge node answers it.

---

## v0.3.4

**Date:** 2026-11-06
**Previous release:** v0.3.3

### Fixed

- **`edge`: a rebirth publishes the DBIRTHs in the order the devices were first born.** `Node.Rebirth`, and so the answer to an NCMD rebirth, ranged over a map, so the order of the DBIRTHs and their `seq` changed from one rebirth to the next. Sparkplug 3.0.0 fixes no order among them, so hosts were not wrong to accept it, but a run could not be repeated. Seen in sparkplugb-interop, whose cross-stack test expects the order of the fixture. A device born again after its death keeps its place.
- **`edge`: `Rebirth` takes the devices' state under the lock.** It read whether a device was online, and its metrics, after releasing it, a data race with a device death or data published at the same time.

### Tests

- The order and `seq` of eight devices' DBIRTHs over twenty rebirths, one of them dead and one born again.
- A failed NBIRTH or DBIRTH in a rebirth: the error names it and no later DBIRTH follows.

---

## v0.3.3

**Date:** 2026-11-01
**Previous release:** v0.3.2

### Fixed

- **`host` and `app` keep an update stamped in the millisecond of the NBIRTH that follows it.** Delivered before that NBIRTH, it was wiped with the state of the old session, and the value stayed stale until the next rebirth. Seen against srad on NanoMQ with delayed NBIRTHs.
- **`edge` no longer publishes an NBIRTH after its NDEATH.** `Disconnect` kept the node online while it published the NDEATH, so a rebirth requested at that moment followed it and a host kept the node online. Seen with a Tahu host on EMQX. The session now ends first, and a message on its way goes out before the NDEATH; a publish after that returns `ErrNotConnected`. The state changes to `Disconnected` before the NDEATH is published.

### Tests

- A message in the millisecond of the next NBIRTH, for `host` and `app`; a rebirth during `Disconnect`, for `edge`.

---

## v0.3.2

**Date:** 2026-11-01
**Previous release:** v0.3.1

### Fixed

- **An NBIRTH delivered just after the NDEATH of the same `bdSeq` no longer brings a dead edge node back online.** MQTT orders messages per topic only, so a will can overtake a rebirth that was in flight when the edge node died; `host` and `app` then showed the node online for good. For 250 ms after an NDEATH, an NBIRTH with its `bdSeq` is dropped, and a rebirth is requested 2 seconds later. A node whose NDEATH had a `bdSeq` ahead of the known session goes offline on that NBIRTH. A DBIRTH on an offline node no longer puts the device online.

### Changed

- An edge node that restarts within 250 ms with the `bdSeq` of its NDEATH comes online only on the rebirth that follows, about 2 seconds later. srad and SparkplugNet start again at `bdSeq` 0, and SparkplugNet does not answer that rebirth.

### Tests

- `host` and `app`: late NBIRTHs after an NDEATH, a will before its NBIRTH, a restart with the same `bdSeq`, and the limits of the 250 ms.

---

## v0.3.1

**Date:** 2026-11-01
**Previous release:** v0.3.0

### Fixed

- **`host` and `app` keep the sessions of an edge node apart when a broker reorders messages across topics.** MQTT orders messages per topic only, and data messages carry no session number; with rebirths overlapping, as a reordering broker causes, the host took messages of one session for another. Three outcomes, all seen against srad on NanoMQ and reproduced with a unit test each: devices missing after the births until the next rebirth; a device with the values of an earlier session after a restart; and the state frozen, because held data of the ended session carried `seq` past the edge node's, after which every message was behind and held, and no rebirth was requested. Now a message held or arriving out of sequence that is older than the current NBIRTH is dropped, the newest of a `seq` held twice is the one applied, and messages of the new session that were applied to the old one before its NBIRTH arrived are applied again after it.
- **A message behind the expected `seq` requests a rebirth when no NBIRTH follows within 2 seconds.** It used to wait for that NBIRTH for ever. A duplicate delivered late now costs one rebirth.

### Tests

- A simulated broker that keeps the order per topic only, and an edge node with overlapping rebirths, against `host` and `app`: 400 seeds each. Before the fix about 11 in 100 ended without the edge node's state.

---

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
