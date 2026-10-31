# Research — go-sparkplugb

This document records the findings from Phase 0 research for the
`github.com/otfabric/go-sparkplugb` library. It covers the Sparkplug B 3.0.0
specification, the Eclipse Tahu reference project, the official Protobuf
schema, available implementations, and the established OT Fabric engineering
patterns that this library will follow.

---

## 1. How Sparkplug Differs from Plain MQTT and OPC UA PubSub

### Sparkplug vs Plain MQTT

Plain MQTT defines a transport protocol with topics and payloads but prescribes
no topic namespace, payload encoding, or session state management. Sparkplug
adds:

- **Defined topic namespace** — `spBv1.0/{group_id}/{message_type}/{edge_node_id}/[device_id]`
- **Defined payload encoding** — Google Protocol Buffers (proto2)
- **Session state management** — Birth/Death certificates track online/offline
  state of every Edge Node, Device, and Host Application
- **Metric definitions** — NBIRTH/DBIRTH define all metrics (name, datatype,
  value) upfront; subsequent DATA messages send only changed values
  (report-by-exception)
- **Sequence numbering** — `seq` (0-255 wrapping) ensures message ordering;
  `bdSeq` (0-255 wrapping) correlates NBIRTH↔NDEATH
- **Host STATE** — JSON-encoded retained messages on
  `spBv1.0/STATE/{host_id}` track Host Application availability
- **Aliases** — compact numeric aliases replace metric names in DATA messages
- **Commands** — NCMD/DCMD provide bidirectional write capability

### Sparkplug vs OPC UA PubSub

OPC UA PubSub (Part 14) supports MQTT and UADP over UDP/multicast. Key
differences:

| Aspect | Sparkplug B | OPC UA PubSub |
|--------|------------|---------------|
| Transport | MQTT only | MQTT, UADP, AMQP |
| Type system | Flat metrics + Templates | Rich AddressSpace with NodeClasses |
| Discovery | Subscribe `spBv1.0/#`, process BIRTHs | Server-side discovery services |
| Security | Defers to MQTT/TLS | Application-level signing/encryption |
| Complexity | Simple to implement | Significant specification weight |
| Target | SCADA/IIoT edge gateways | Factory automation, vendor interop |

---

## 2. Normative Specification and Release

**Target**: Eclipse Sparkplug Specification Version 3.0.0

| Property | Value |
|----------|-------|
| Repository | https://github.com/eclipse-sparkplug/sparkplug |
| Commit | `e45b610` (Merge pull request #505) |
| Format | AsciiDoc under `specification/src/main/asciidoc/` |
| Published PDF | https://www.eclipse.org/tahu/spec/sparkplug_spec.pdf |
| License | EPL-2.0 (Eclipse Foundation Specification License) |

The Sparkplug 3.0 MQTT namespace remains `spBv1.0` (not `spBv3.0`). The "3.0"
is the specification version; the namespace version identifies the payload
encoding.

There is no finalized Sparkplug 4.0 specification. This library implements only
3.0.0 semantics — in particular the 3.0 JSON STATE format (not the 2.2
format).

---

## 3. What Eclipse Tahu Provides

Eclipse Tahu (https://github.com/eclipse-tahu/tahu) is the reference
implementation project for Sparkplug. It provides:

- **Canonical Protobuf schema**: `sparkplug_b/sparkplug_b.proto` (EPL-2.0)
- **Java client libraries**: Full Edge Node and Host Application
- **Python, JavaScript, C/C++ clients**: Edge Node implementations
- **Reference applications**: Example Edge Nodes and Host Applications

| Property | Value |
|----------|-------|
| Commit | `5736e404889d4b95910613040a99ba79589ffb13` |
| License | EPL-2.0 |

Tahu is useful as a reference but may contain implementation-specific behavior.
The specification is authoritative.

---

## 4. Official Protobuf Schema

| Property | Value |
|----------|-------|
| File | `sparkplug_b/sparkplug_b.proto` |
| SHA-256 | `4432c5c483b7fb9732d0594c98a2e97dca5e517e39c5374a8b918d837f0b4a19` |
| Syntax | `proto2` |
| Package | `org.eclipse.tahu.protobuf` |
| License | EPL-2.0 (SPDX header in file) |

### Key characteristics

- `optional` fields with presence semantics (important for `is_null`)
- `oneof` for metric values, property values, dataset values, template
  parameter values
- `extensions` ranges on most messages for third-party extensions
- Nested messages: `Payload.Metric`, `Payload.Template`, `Payload.DataSet`,
  `Payload.PropertySet`, etc.
- Enum `DataType` with 35 values (0-34)
- No `go_package` option — must be supplied via build-time mapping

### DataType values

| Range | Types | Protobuf field |
|-------|-------|---------------|
| 1-4 | Int8, Int16, Int32, Int64 | `int_value` (uint32) / `long_value` (uint64) |
| 5-8 | UInt8, UInt16, UInt32, UInt64 | `int_value` (uint32) / `long_value` (uint64) |
| 9-10 | Float, Double | `float_value` / `double_value` |
| 11 | Boolean | `boolean_value` |
| 12, 14-15 | String, Text, UUID | `string_value` |
| 13 | DateTime | `long_value` (ms since epoch) |
| 16 | DataSet | `dataset_value` |
| 17-18 | Bytes, File | `bytes_value` |
| 19 | Template | `template_value` |
| 20-21 | PropertySet, PropertySetList | (property values only) |
| 22-34 | Array types | `bytes_value` (packed little-endian) |

**Critical**: Int8, Int16, Int32 are signed integers transported in a `uint32`
protobuf field, and the specification does not say how a negative Int8 or Int16
goes in (INTEROPERABILITY.md). BooleanArray
is bit-packed with a 4-byte count prefix. StringArray uses null-terminated
strings.

---

## 5. Edge Node and Host Application Roles

### Edge Node

- Represents a Sparkplug MQTT client at the edge of the network
- Manages one or more downstream Sparkplug Devices
- Publishes: NBIRTH, NDEATH (Will), DBIRTH, DDEATH, NDATA, DDATA
- Receives: NCMD, DCMD
- Must implement `Node Control/Rebirth` command
- Optionally watches Primary Host STATE

### Host Application

- Consumes data from Edge Nodes
- Publishes STATE Birth/Death certificates (JSON, retained, QoS 1)
- Sends commands via NCMD/DCMD
- Maintains registry of all discovered infrastructure
- Validates message sequence ordering
- Manages metric staleness on node/device death

### Primary Host Application

- Optional designation by Edge Nodes
- Edge Nodes with a configured Primary Host wait for STATE `online=true`
  before publishing births
- If Primary Host goes offline, Edge Nodes disconnect and walk to next server

---

## 6. Lifecycle State Machines

### Edge Node Lifecycle

1. **CONNECT** — Register NDEATH as Will Message (QoS 1, not retained,
   Protobuf payload containing bdSeq)
2. **SUBSCRIBE** — `spBv1.0/{group}/{NCMD}/{node}` (QoS 1); optionally DCMD
   and STATE topics
3. **Wait for Primary Host** (if configured) — Subscribe to
   `spBv1.0/STATE/{host_id}`, wait for `online=true`
4. **NBIRTH** — Publish all node metrics (QoS 0, not retained, seq=0,
   matching bdSeq)
5. **DBIRTH** — Publish all device metrics for each online device (QoS 0, not
   retained, seq increments)
6. **NDATA/DDATA** — Report by exception (QoS 0, not retained, seq increments)
7. **DDEATH** — Published by Edge Node when a device goes offline (includes
   seq)
8. **NDEATH** — Intentional disconnect: publish NDEATH then DISCONNECT

### Host Application Lifecycle

1. **CONNECT** — Register STATE Death as Will Message (QoS 1, retained, JSON
   with `online=false` and timestamp)
2. **SUBSCRIBE** — Own STATE topic and `spBv1.0/#`
3. **STATE Birth** — Publish STATE `online=true` with same timestamp as Will
   (QoS 1, retained)
4. **Process messages** — Handle NBIRTH, DBIRTH, NDATA, DDATA, NDEATH, DDEATH
5. **Send commands** — NCMD/DCMD (QoS 0, not retained)
6. **Disconnect** — Publish STATE Death (`online=false`, current timestamp)
   then DISCONNECT

### Sequence Numbers

- **bdSeq**: 0-255, incremented per MQTT CONNECT, shared between NDEATH Will
  and NBIRTH. Persisted across restarts.
- **seq**: 0-255 wrapping, starts at 0 in NBIRTH, increments for each
  subsequent message (DBIRTH, NDATA, DDATA, DDEATH). Shared across node and
  all device messages. NDEATH does NOT contain seq. NCMD/DCMD do NOT contain
  seq.

---

## 7. MQTT 3.1.1 versus MQTT 5.0 Requirements

| Requirement | MQTT 3.1.1 | MQTT 5.0 |
|------------|-----------|---------|
| Clean Session | MUST be true | Clean Start true, Session Expiry Interval 0 |
| Edge NDEATH Will | QoS 1, not retained | QoS 1, not retained |
| Host STATE Will | QoS 1, retained | QoS 1, retained |
| Intentional disconnect (Edge) | Publish NDEATH, then DISCONNECT | Publish NDEATH, then DISCONNECT with reason code 0x04 |
| Intentional disconnect (Host) | Publish STATE Death, then DISCONNECT | Publish STATE Death, then DISCONNECT |

MQTT 5.0 reason code 0x04 ("Disconnect with Will Message") tells the broker to
deliver the Will even though the client sent a clean DISCONNECT. In MQTT 3.1.1,
sending DISCONNECT suppresses the Will — the Edge Node must publish NDEATH
explicitly before disconnecting.

### Go MQTT Client Libraries

| Library | Protocol | Module | Status |
|---------|----------|--------|--------|
| paho.mqtt.golang | MQTT 3.1.1 | `github.com/eclipse/paho.mqtt.golang` | Mature, widely deployed |
| paho.golang | MQTT 5.0 | `github.com/eclipse-paho/paho.golang` | Active, v0.23.0 (Sep 2025), Go 1.24+ |

Both Paho libraries support Will Message configuration. The `autopaho`
sub-package in paho.golang provides automatic reconnection, but Sparkplug
requires owning the reconnection lifecycle (incrementing bdSeq, re-running the
full birth sequence). The library must manage reconnection above the MQTT
client layer.

---

## 8. Available Implementations

### Go

| Project | Spec | License | Edge | Host | TCK |
|---------|------|---------|------|------|-----|
| joyautomation/nautilus | 3.0 | AGPL-3.0 | Yes (embedded) | No | Yes (Edge) |
| weekaung/sparkplugb-client | Partial 3.0 | MIT | Partial | No | No |
| adilbekes/sparkbridge | Partial | MIT | Partial | No | No |

### Python

| Project | Spec | License | Edge | Host |
|---------|------|---------|------|------|
| pysparkplug | 3.0 | Apache-2.0 | Yes | Partial |
| mqtt-spb-wrapper | 3.0 | MIT | Yes | Yes |

### JavaScript/TypeScript

| Project | Spec | License | Edge | Host |
|---------|------|---------|------|------|
| node-red-contrib-mqtt-sparkplug-plus | 3.0 | Apache-2.0 | Yes | Yes |
| joyautomation/synapse | 3.0 | Unknown | Yes | Yes |

### Java

| Project | Spec | License | Edge | Host |
|---------|------|---------|------|------|
| Eclipse Tahu | 3.0 | EPL-2.0 | Yes | Yes |

### .NET

| Project | Spec | License | Edge | Host |
|---------|------|---------|------|------|
| HiveMQtt.Sparkplug | 3.0 | Apache-2.0 | Yes | Yes |
| SparkplugNet | 3.0 | MIT | Yes | Yes |

### Interoperability Test Candidates

1. **Eclipse Tahu (Java)** — reference implementation, both roles, EPL-2.0,
   containerizable
2. **node-red-contrib-mqtt-sparkplug-plus** — both roles, Apache-2.0, has
   TCK automation
3. **pysparkplug** — Edge Node, Apache-2.0, pip-installable
4. **HiveMQtt.Sparkplug (.NET)** — both roles, Apache-2.0, active

### Brokers

- **Eclipse Mosquitto** — MQTT 3.1.1 and 5.0, lightweight
- **HiveMQ Community Edition** — required for TCK, MQTT 3.1.1 and 5.0
- **EMQX** — MQTT 3.1.1 and 5.0, clustered deployment option

---

## 9. Test Frameworks and Official TCK

### Eclipse Sparkplug TCK

The TCK is a HiveMQ extension that intercepts all MQTT traffic and validates
Sparkplug conformance.

**Location**: `tck/` directory in the sparkplug specification repository.

**Components**:
- HiveMQ extension JAR — intercepts CONNECT, PUBLISH, SUBSCRIBE, DISCONNECT
- Web console (Node.js/Vue.js) — controls test execution via WebSocket
- Test classes organized by profile and scenario

### Conformance Profiles

1. **Edge Node** — SessionEstablishment, SessionTermination, SendData,
   SendComplexData (optional), ReceiveCommand, PrimaryHost, MultipleBroker
2. **Host Application** — SessionEstablishment, SessionTermination,
   SendCommand, MessageOrdering, EdgeSessionTermination, MultipleBroker
3. **MQTT Broker** — Compliant and Aware (optional)

### Optional Edge Groups

- **Aliases** — metric alias support
- **DataSets** — DataSet metric types
- **Properties** — PropertySet on metrics
- **Templates** — Template definitions and instances

### Running the TCK

1. Install HiveMQ CE with sparkplug-tck extension
2. Start web console (`yarn install && yarn build && yarn start`)
3. Select profile and tests
4. Connect implementation under test
5. Results in `SparkplugTCKResults.log`, report via `python3 report.py`

The TCK can be automated headless. Reference:
https://github.com/joyautomation/sparkplug-tck-go provides Go-specific TCK
automation.

---

## 10. Licensing, Trademark, and Compatibility Constraints

### License Summary

| Component | License |
|-----------|---------|
| Sparkplug specification | EPL-2.0 |
| Eclipse Tahu (including sparkplug_b.proto) | EPL-2.0 |
| TCK | EPL-2.0 |
| OT Fabric original code | MIT |

### EPL-2.0 Implications

The `.proto` file is EPL-2.0 source. Code generated from it is a derivative
work. The recommended approach:

- `proto/` directory: EPL-2.0 (contains copied `.proto` and generated `.go`)
- All other directories: MIT (original OT Fabric code)
- `docs/LICENSING.md` explains the dual-license structure

The Tahu proto file does NOT designate a secondary license.

### Trademark

- "Sparkplug", "Sparkplug Compatible", and the Sparkplug Logo are trademarks
  of the Eclipse Foundation
- The library refers to "Sparkplug B" or "Eclipse Sparkplug" with attribution
- "Sparkplug Compatible" requires passing the TCK and being listed

---

## 11. OT Fabric Engineering Patterns

The following patterns are established across the OT Fabric Go repositories
(go-modbus, go-dnp3, go-iec104, go-ethernetip, go-opcua, go-sunspec) and will
be adopted by go-sparkplugb.

### Module & Go Version

- Module path: `github.com/otfabric/go-sparkplugb`
- Minimum Go version: 1.23 (CI tests across 1.23-1.26)

### Package Structure

```
proto/         — EPL-2.0 generated Protobuf types
payload/       — typed metric encoding/decoding (MIT)
topic/         — topic namespace parsing (MIT)
mqtt/          — MQTT transport abstraction (MIT)
edge/          — Edge Node runtime (MIT)
host/          — Host Application runtime (MIT)
cmd/sparkplugctl/ — diagnostic CLI (MIT)
internal/      — private implementation details
e2e/           — end-to-end tests
interop/       — interoperability tests (build-tagged)
examples/      — runnable examples
docs/          — documentation
testdata/      — test fixtures
```

### Public API

- Functional options pattern: `New(addr string, opts ...Option)`
- Constructor validates config but does NOT create network connections
- Explicit `Connect(ctx)` / `Close(ctx)` lifecycle methods
- `context.Context` on every operation
- Clients safe for concurrent use

### Error Handling

- Sentinel errors: `var ErrX = errors.New("sparkplugb: ...")`
- Protocol prefix on all sentinels
- Rich typed errors wrapping sentinels
- Test with `errors.Is` / `errors.As`
- `ERRORS.md` documenting the error taxonomy

### Logging

```go
type Logger interface {
    Debugf(format string, args ...any)
    Infof(format string, args ...any)
    Warnf(format string, args ...any)
    Errorf(format string, args ...any)
}
```

- Optional `FieldLogger` and `ContextLogger` extensions
- `NewSlogLogger(h slog.Handler) Logger` adapter
- Silent by default (nil Logger = no-op)

### Metrics / Observability

- Callback interfaces, not Prometheus dependency
- `nil` metrics = disabled, must be non-blocking
- State enum with handler callback

### Testing

- 90% coverage target (codecov.yml + CI gate)
- `go test -race` always
- Fuzz targets for codecs/parsers
- Chaos testing with fault injection
- E2E tests against real protocol sessions
- Interop tests with `-tags=interop`, Docker-based, pinned digests

### CI

- Shared `otfabric/.github/.github/workflows/go-ci.yml@v2`
- Multi-platform matrix (Linux amd64/arm64, macOS, Windows)
- `make generate-check` for generated code freshness
- 90% coverage gate

### File Headers

- `// SPDX-License-Identifier: MIT` on every original `.go` file
- `// SPDX-License-Identifier: EPL-2.0` on proto-derived files

### Code Generation

- `generate.go` with `//go:generate`
- `make generate` and `make generate-check` in CI
- Generated files committed, reproducibility enforced
- Pin protoc, protoc-gen-go, and upstream proto by commit SHA + SHA-256

### Key Divergence from Other OT Fabric Libraries

| Aspect | Existing Pattern | Sparkplug B Adjustment |
|--------|-----------------|----------------------|
| Dependencies | Zero or minimal | MQTT client + Protobuf runtime required |
| Transport | Own TCP/serial | Delegate to MQTT client library |
| Wire format | Custom binary codecs | Protobuf (generated) |
| Connection | Client-server TCP | MQTT pub/sub with stateful sessions |
| State machine | Simple connect/disconnect | Complex lifecycle (birth/death/rebirth, STATE) |
| Message ordering | TCP guarantees order | MQTT QoS 0 may reorder; need reorder buffer |
| Code generation | From JSON/XML schemas | From proto2 schema with protoc |
| Conformance | Against spec clauses | Official Eclipse Sparkplug TCK |
