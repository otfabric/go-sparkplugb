# go-sparkplugb

Production-quality [Eclipse Sparkplug B 3.0.0](https://sparkplug.eclipse.org/specification/version/3.0/documents/sparkplug-specification-3.0.0.pdf) library for Go.

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/otfabric/go-sparkplugb/actions/workflows/ci.yaml/badge.svg)](https://github.com/otfabric/go-sparkplugb/actions/workflows/ci.yaml)
[![Coverage](https://img.shields.io/badge/coverage-91%25-brightgreen)](docs/CONFORMANCE.md)
[![Go Reference](https://pkg.go.dev/badge/github.com/otfabric/go-sparkplugb.svg)](https://pkg.go.dev/github.com/otfabric/go-sparkplugb)
[![Release](https://img.shields.io/github/v/release/otfabric/go-sparkplugb?include_prereleases)](https://github.com/otfabric/go-sparkplugb/releases)

go-sparkplugb handles the full Sparkplug B 3.0.0 protocol lifecycle for Edge
Nodes, Host Applications and Applications: topic namespace, Protobuf codecs,
typed metrics (including arrays, templates and datasets), sequence numbering,
alias management, birth/death certificates, primary host awareness with server
walking, and message reordering. It ships an MQTT transport over Eclipse Paho, which
asks for MQTT 3.1.1 and falls back to 3.1 if the broker refuses it, and has no
MQTT 5. Any other MQTT client plugs in through the `mqtt.Transport` interface.

**Quick links:**
[Install](#installation) ·
[Edge Node](#quick-start--edge-node) ·
[Host Application](#quick-start--host-application) ·
[Application](#quick-start--application) ·
[Packages](#packages) ·
[Comparison](#go-sparkplugb-vs-other-sparkplug-b-stacks) ·
[Compatibility](#compatibility-matrix) ·
[Examples](#examples) ·
[Conformance](#specification-conformance) ·
[Related projects](#related-projects)

---

## Features

| Category | Details |
|----------|---------|
| **Edge Node runtime** | Full lifecycle (birth/death/rebirth), sequence numbering, alias support, primary host awareness, server walking across multiple brokers |
| **Host Application runtime** | STATE birth/death certificates, node/device registry, sequence validation, out-of-order messages held and applied in `seq` order, rebirth requests (at most one pending per node), reorder timeout |
| **Application runtime** | Non-primary-host consumer: subscribes to spBv1.0/#, tracks nodes/devices, alias resolution, sends NCMD/DCMD — no STATE messages |
| **Typed metric codec** | All scalar types, signed-integer handling (Int8–Int64 in wire fields), all array types (little-endian packed, bit-packed BooleanArray, null-terminated StringArray, DateTimeArray), quality codes |
| **Templates** | Template definitions (is_definition, version, member metrics, parameters) and instances (template_ref, parameter overrides) |
| **DataSets** | Columnar data with typed columns (num_of_columns, columns, types, rows), round-trip encode/decode |
| **Topic namespace** | Parser and builder for `spBv1.0/{group}/{type}/{node}/[{device}]` with validation |
| **Observability** | Pluggable `Logger` (`log/slog`, standard `log`) and `Metrics` callbacks per role; `context.Context` on every blocking call |
| **Protobuf types** | Generated from the canonical Eclipse Tahu `sparkplug_b.proto` (pinned commit, SHA-256 verified) |
| **MQTT transport** | `mqtt.Transport` interface; Eclipse Paho adapter included (MQTT 3.1.1, with fallback to 3.1, over TCP, TLS or WebSocket; no MQTT 5) |

## go-sparkplugb vs other Sparkplug B stacks

go-sparkplugb is tested against five other implementations (see the
[compatibility matrix](#compatibility-matrix)). All are good libraries; this
table compares them, to help you pick the right one for your project.

| Capability | **otfabric/go-sparkplugb** | Eclipse Tahu (Java) | pysparkplug | SparkplugNet | srad | sparkplughost |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Language | Go | Java | Python | C# | Rust | Go |
| License | MIT (`proto/` EPL-2.0) | EPL-2.0 | Apache-2.0 | MIT | MIT or Apache-2.0 | Apache-2.0 |
| Edge node | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Primary host application (STATE) | ✅ | ✅ | — ¹ | ✅ | ✅ | ✅ |
| Application without STATE | ✅ | ✅ | ✅ plain client | ✅ | — | — |
| STATE of Sparkplug 3.0 / 2.2 | 3.0 | 3.0 | 3.0 | 3.0 and 2.2 | 3.0 | 3.0 |
| MQTT | 3.1.1 with fallback to 3.1 (Paho), any client through `mqtt.Transport` | 3.1.1 (Paho) | 3.1.1 and 5 (paho-mqtt) | 3.1.1 and 5 (MQTTnet) | 5 (rumqttc) | 3.1.1 (Paho) |
| TLS, WebSocket | ✅ ✅ | by URL scheme, no TLS settings | ✅ ✅ | ✅ ✅ | not documented | ✅ TLS |
| Edge: devices | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Edge: metric aliases | ✅ caller-assigned | ✅ generated | ✅ caller-assigned | ✅ caller-assigned | ✅ | — |
| Edge: births wait for the primary host's STATE | ✅ | ✅ | — | — ² | — ³ | — |
| Edge: server walking when the primary host goes offline | ✅ | ✅ | — | — | — ³ | — |
| Host: `seq` check with rebirth request | ✅ | ✅ | — | — | ✅ | ✅ |
| Host: out-of-order messages held and resequenced | ✅ across the NBIRTH too, configurable timeout | ✅ fixed 5 s | — | — | ✅ default 3 s | ✅ default 5 s |
| Host: NDEATH of an earlier session (`bdSeq`) ignored | ✅ | ✅ | — | — | ✅ | ✅ |
| Typed scalars and arrays | ✅ | ✅ | ✅ | ✅ | ✅ | — raw protobuf |
| DataSet | ✅ | ✅ | — | ✅ | raw protobuf only | — raw protobuf |
| Template, definition and instance | ✅ | ✅ | — | ✅ | ✅ | — raw protobuf |
| Properties and property sets | ✅ | ✅ | — | ✅ | ✅ | — raw protobuf |
| Cancellation | `context.Context` per call | — callbacks, timers | — callbacks | `CancellationToken` | async (tokio) | `context.Context` on `Run` |
| Logging | `log/slog` or standard `log`, plus metrics callbacks | SLF4J | `logging` | `Microsoft.Extensions.Logging` | `log` crate | `log/slog` |

¹ pysparkplug decodes STATE but has no host application; the application
publishes STATE itself.
² SparkplugNet's edge subscribes to the primary host's STATE but publishes
its births at once.
³ srad's edge ignores STATE; waiting for the primary host and several
brokers are on its to-do list.

✅ = available · — = not available. For Eclipse Tahu 1.0.21, pysparkplug
0.6.1, SparkplugNet 1.4.1, srad 0.5.0 and EvergenEnergy/sparkplughost
v0.4.0, the versions go-sparkplugb is tested against, from their source at
those tags as of October 2026. If you spot an inaccuracy, please
[open an issue](https://github.com/otfabric/go-sparkplugb/issues) and we will
correct it.

**Choose go-sparkplugb if** you write Go and want all three roles in one
library: an edge node that follows its primary host across brokers, a host
that copes with brokers that reorder messages, the full datatype set
including DataSets and Templates, and `context` control.

**Choose Eclipse Tahu if** you are on the JVM and want the reference
implementation of the Eclipse Sparkplug project.

**Choose sparkplughost if** you need only a Go host application and work
with the raw protobuf types.

## Compatibility matrix

Every role of go-sparkplugb runs on every push against the six reference
stacks of
[sparkplugb-interop v0.3.0](https://github.com/otfabric/sparkplugb-interop/releases/tag/v0.3.0),
on four MQTT brokers, with the images pinned by digest in
[interop/images.go](interop/images.go).

| Reference | Version | go-sparkplugb host and application ← reference edge | go-sparkplugb edge → reference host |
|-----------|---------|:---:|:---:|
| Eclipse Tahu (Java) | 1.0.21 | ✅ | ✅ |
| pysparkplug | 0.6.1 | ✅ ¹ | ✅ ² ⁵ |
| SparkplugNet | 1.4.1 | ✅ ³ | ✅ ⁴ |
| srad | 0.5.0 | ✅ | ✅ ⁵ |
| Eclipse Tahu sparkplug-client (JavaScript) | 3.2.4 | ✅ | (edge only) |
| EvergenEnergy/sparkplughost | v0.4.0 | (host only) | ✅ ⁵ |

| Broker | Version | Result |
|--------|---------|:---:|
| Eclipse Mosquitto | 2.1.2 | ✅ |
| HiveMQ CE | 2026.5 | ✅ |
| EMQX | 6.3.2 | ✅ |
| NanoMQ | 0.25.7 | ✅ |

✅ = every step passes, except the steps that need a capability the
reference lacks or that hit one of its upstream bugs:
¹ pysparkplug's NDEATH carries the next `bdSeq`: the will after a crash is
checked instead.
² pysparkplug's host cannot decode a negative Int8 or Int16 sign-extended to
32 bits, a [specification ambiguity](INTEROPERABILITY.md#negative-int8-and-int16):
the scalars fixture is skipped.
³ without device writes: SparkplugNet's edge drops a DCMD for a device.
⁴ without the steps after a death: SparkplugNet's host keeps dead nodes
online.
⁵ not on NanoMQ and HiveMQ CE: these brokers reorder a node's messages
across topics, which these hosts do not resequence across the NBIRTH.

Each step (births, aliases, updates of all fourteen scalar datatypes, NCMD
and DCMD writes, rebirth, device death and birth, NDEATH, the will, the
primary host going offline) and the wire behaviour are described in
[INTEROPERABILITY.md](INTEROPERABILITY.md). Run it with `make interop`
(Docker).

## Installation

```bash
go get github.com/otfabric/go-sparkplugb
```

Requires Go 1.26 or later.

## Packages

| Package | Import | Description |
|---------|--------|------------|
| `proto` | `github.com/otfabric/go-sparkplugb/proto` | Generated Protobuf types (EPL-2.0) |
| `topic` | `github.com/otfabric/go-sparkplugb/topic` | Topic namespace parsing, construction and validation |
| `payload` | `github.com/otfabric/go-sparkplugb/payload` | Typed metric encoding/decoding, templates, datasets, arrays, quality codes |
| `edge` | `github.com/otfabric/go-sparkplugb/edge` | Edge Node runtime (lifecycle, sequencing, aliases, server walking) |
| `host` | `github.com/otfabric/go-sparkplugb/host` | Host Application runtime (STATE, registry, seq validation, reorder timeout) |
| `app` | `github.com/otfabric/go-sparkplugb/app` | Application runtime (subscriber, node tracking, commands — no STATE) |
| `mqtt` | `github.com/otfabric/go-sparkplugb/mqtt` | MQTT transport interface and Paho adapter |

---

## Quick Start — Edge Node

```go
package main

import (
    "context"
    "log"

    "github.com/otfabric/go-sparkplugb/edge"
    "github.com/otfabric/go-sparkplugb/mqtt"
    "github.com/otfabric/go-sparkplugb/payload"
    pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
    transport := mqtt.NewPahoTransport()

    node, _ := edge.New(transport, edge.Config{
        GroupID:    "plant",
        EdgeNodeID: "gateway-01",
        BrokerURL:  "tcp://broker:1883",
    })

    ctx := context.Background()
    node.Connect(ctx)
    defer node.Close(ctx)

    temp, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
    node.PublishBirth(ctx, []*pb.Payload_Metric{temp})

    // Report by exception
    updated, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(23.1))
    node.PublishNodeData(ctx, []*pb.Payload_Metric{updated})
}
```

## Quick Start — Host Application

```go
app, _ := host.New(transport, host.Config{
    HostID:    "scada-01",
    BrokerURL: "tcp://broker:1883",
    OnNodeBirth: func(group, node string, metrics []*pb.Payload_Metric) {
        log.Printf("node online: %s/%s with %d metrics", group, node, len(metrics))
    },
    OnNodeDeath: func(group, node string) {
        log.Printf("node offline: %s/%s", group, node)
    },
})

app.Connect(ctx)
defer app.Close(ctx)
```

## Quick Start — Application

An Application subscribes to the Sparkplug namespace and tracks edge
nodes/devices without publishing STATE birth/death certificates. Use it for
dashboards, loggers, analytics pipelines, or any consumer that does not need
to be a Primary Host.

```go
application, _ := app.New(transport, app.Config{
    BrokerURL: "tcp://broker:1883",
    OnNodeBirth: func(group, node string, metrics []*pb.Payload_Metric) {
        log.Printf("node online: %s/%s with %d metrics", group, node, len(metrics))
    },
    OnNodeData: func(group, node string, metrics []*pb.Payload_Metric) {
        log.Printf("data from %s/%s: %d metrics", group, node, len(metrics))
    },
    OnNodeDeath: func(group, node string) {
        log.Printf("node offline: %s/%s", group, node)
    },
})

application.Connect(ctx)
defer application.Close(ctx)

// Send a command to an edge node
application.SendNodeCommand(ctx, "plant", "gw1", []*pb.Payload_Metric{rebirth})
```

## Quick Start — Templates

```go
// Define a template with member metrics and parameters.
tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(0))
unitParam, _ := payload.NewTemplateParameter("unit", pb.DataType_String, "celsius")

def, _ := payload.NewTemplateDefinition("SensorTemplate", "1.0",
    []*pb.Payload_Metric{tempMetric},
    []*pb.Payload_Template_Parameter{unitParam})

// Create an instance with parameter overrides.
instMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(72.5))
instParam, _ := payload.NewTemplateParameter("unit", pb.DataType_String, "fahrenheit")

inst, _ := payload.NewTemplateInstance("sensor1", "SensorTemplate",
    []*pb.Payload_Metric{instMetric},
    []*pb.Payload_Template_Parameter{instParam})
```

## Quick Start — DataSets

```go
columns := []string{"name", "value", "active"}
types := []pb.DataType{pb.DataType_String, pb.DataType_Double, pb.DataType_Boolean}
rows := [][]any{
    {"sensor1", float64(22.5), true},
    {"sensor2", float64(33.1), false},
}

m, _ := payload.NewDataSet("readings", columns, types, rows)

// Extract after decode
cols, dts, gotRows, _ := payload.GetDataSetValues(m)
```

---

## Examples

Runnable examples are in the [`examples/`](examples/) directory:

| Example | Description |
|---------|------------|
| [`examples/edge-node`](examples/edge-node) | Edge Node with birth, device birth, and periodic data publishing |
| [`examples/host-app`](examples/host-app) | Host Application with node/device lifecycle callbacks |
| [`examples/templates`](examples/templates) | Template definitions and instances with parameter overrides |
| [`examples/datasets`](examples/datasets) | DataSet metrics with typed columnar data |
| [`examples/application`](examples/application) | Application (non-host subscriber) with node/device event tracking |
| [`examples/aliases`](examples/aliases) | Edge Node with metric aliases and alias resolution |
| [`examples/commands`](examples/commands) | Host Application sending NCMD and DCMD commands to edge nodes |
| [`examples/server-walking`](examples/server-walking) | Edge Node with multi-broker ServerURLs and primary host awareness |

Run any example with:

```bash
MQTT_BROKER=tcp://localhost:1883 go run ./examples/edge-node
```

---

## Configuration

### Edge Node Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `GroupID` | `string` | required | Sparkplug group identifier |
| `EdgeNodeID` | `string` | required | Edge node identifier |
| `BrokerURL` | `string` | | MQTT broker URL (`tcp://`, `ssl://`) |
| `ServerURLs` | `[]string` | | Multiple broker URLs for server walking |
| `Username`, `Password` | `string` | | Broker login, for every server. A `Password` needs a `Username`. Never logged; use `ssl://` |
| `ClientID` | `string` | `sparkplug_<group>_<node>` | MQTT client ID |
| `PrimaryHostID` | `string` | | Primary Host Application ID: births wait for its STATE online; its STATE offline makes the node disconnect and walk to the next server |
| `BdSeqStore` | `BdSeqStore` | in memory | Where `bdSeq` is kept across restarts |
| `RetryPolicy` | `sparkplugb.RetryPolicy` | `ExponentialBackoff{}` | Reconnect policy while walking servers |
| `Logger` | `sparkplugb.Logger` | none | Logger (`NewSlogLogger`, `NewStdLogger`) |
| `Metrics` | `edge.Metrics` | none | Connect, publish, message and state-change callbacks |
| `OnState` | `func` | | State change callback |
| `OnNodeCmd` | `func` | | NCMD received callback |
| `OnDeviceCmd` | `func` | | DCMD received callback |

Aliases are set by the caller on the birth metrics (`Alias`); data may then
carry the alias only, and `ResolveAlias` maps it back.

### Host Application Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `HostID` | `string` | required | Host Application identifier |
| `BrokerURL` | `string` | | MQTT broker URL |
| `ServerURLs` | `[]string` | | Multiple broker URLs |
| `Username`, `Password` | `string` | | Broker login, for every server. A `Password` needs a `Username`. Never logged; use `ssl://` |
| `ClientID` | `string` | `sparkplug_host_<host id>` | MQTT client ID |
| `ReorderTimeout` | `time.Duration` | `0` | How long a `seq` gap may stay open before a rebirth request; `0` requests it at once |
| `Logger` | `sparkplugb.Logger` | none | Logger |
| `Metrics` | `host.Metrics` | none | Connect, message, state-change, node birth and death callbacks |
| `OnState` | `func` | | State change callback |
| `OnNodeBirth` | `func` | | NBIRTH callback |
| `OnDeviceBirth` | `func` | | DBIRTH callback |
| `OnNodeData` | `func` | | NDATA callback |
| `OnDeviceData` | `func` | | DDATA callback |
| `OnNodeDeath` | `func` | | NDEATH callback |
| `OnDeviceDeath` | `func` | | DDEATH callback |

### Application Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `BrokerURL` | `string` | | MQTT broker URL |
| `ServerURLs` | `[]string` | | Multiple broker URLs (fallback) |
| `Username`, `Password` | `string` | | Broker login, for every server. A `Password` needs a `Username`. Never logged; use `ssl://` |
| `ClientID` | `string` | `sparkplug_app_<n>` | MQTT client ID |
| `Logger` | `sparkplugb.Logger` | none | Logger |
| `Metrics` | `app.Metrics` | none | Connect, message, state-change, node birth and death callbacks |
| `OnState` | `func` | | State change callback |
| `OnNodeBirth` | `func` | | NBIRTH callback |
| `OnDeviceBirth` | `func` | | DBIRTH callback |
| `OnNodeData` | `func` | | NDATA callback |
| `OnDeviceData` | `func` | | DDATA callback |
| `OnNodeDeath` | `func` | | NDEATH callback |
| `OnDeviceDeath` | `func` | | DDEATH callback |

### Connection loss

Edge nodes, host applications and applications move to `Disconnected` (and
fire `OnState`) as soon as the transport reports the connection gone — a
broker drop, a client-ID takeover, or `Disconnect` called on the transport
directly. Call `Connect` again to resume; edge nodes bump `bdSeq` and must
re-publish NBIRTH. Custom `mqtt.Transport` implementations report this by
calling `ConnectOpts.OnConnectionLost` with an error wrapping
`mqtt.ErrConnectionLost`.

---

## Specification Conformance

This library targets full conformance with the Eclipse Sparkplug B 3.0.0
specification. See [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for the
clause-by-clause requirements matrix.

### Conformance Summary

| Profile | Mandatory | Optional |
|---------|-----------|----------|
| Edge Node | Session, Termination, Data, Commands, Primary Host, Multi-Broker | Aliases, Templates, DataSets, Properties |
| Host Application | Session, Termination, Commands, Edge Termination; Multi-Broker partial (`ServerURLs`, one broker at a time) | Message Reordering |
| Application | Subscribe, Track State, Alias Resolution, Seq Validation, Commands | — |

---

## Project Structure

```
go-sparkplugb
├── proto/            Generated Protobuf types (EPL-2.0)
├── topic/            Topic namespace parsing and construction
├── payload/          Typed metric codec, templates, datasets, arrays
├── edge/             Edge Node runtime
├── host/             Host Application runtime
├── app/              Application runtime (non-host consumer)
├── mqtt/             MQTT transport interface and Paho adapter
├── interop/          Interop suite against sparkplugb-interop (own module, Docker)
├── examples/
│   ├── edge-node/        Edge Node lifecycle
│   ├── host-app/         Host Application lifecycle
│   ├── templates/        Template definitions and instances
│   ├── aliases/          Metric aliases and alias resolution
│   ├── application/      Application (non-host consumer)
│   ├── commands/         Sending NCMD and DCMD commands
│   ├── datasets/         DataSet columnar metrics
│   └── server-walking/   Multi-broker server walking
├── docs/
│   ├── CONFORMANCE.md    Specification conformance matrix
│   ├── LICENSING.md      License details
│   └── RESEARCH.md       Design research
├── INTEROPERABILITY.md   Wire behaviour and the interop matrix
└── RELEASE.md            Release notes
```

## Building and Development

| Target | Description |
|--------|-------------|
| `make test` | Run all tests |
| `make cover` | Run tests with coverage report |
| `make lint` | Run staticcheck |
| `make lint-ci` | Run golangci-lint |
| `make e2e` | The runtimes over Paho against a fake broker on loopback TCP |
| `make bench` | Benchmarks |
| `make build` | Build examples |
| `make check` | Lint + test + coverage gate (90%) |
| `make interop` | Edge, host and application against the six reference stacks of sparkplugb-interop, on Mosquitto, HiveMQ CE, EMQX and NanoMQ (`interop/` module, Docker) |

```bash
git clone https://github.com/otfabric/go-sparkplugb.git
cd go-sparkplugb
make check
```

## Further Reading

[API.md](API.md), [ARCHITECTURE.md](ARCHITECTURE.md), [OBSERVABILITY.md](OBSERVABILITY.md), [TESTING.md](TESTING.md), [INTEROPERABILITY.md](INTEROPERABILITY.md), [ERRORS.md](ERRORS.md), [LIMITATIONS.md](LIMITATIONS.md), [docs/FEATURE-STATUS.md](docs/FEATURE-STATUS.md), [docs/CONFORMANCE.md](docs/CONFORMANCE.md).

## Related Projects

| Repository | Description |
|-----------|-------------|
| [sparkplugbctl](https://github.com/otfabric/sparkplugbctl) | CLI for monitoring, publishing, simulating and debugging Sparkplug B infrastructure |
| [sparkplugb-interop](https://github.com/otfabric/sparkplugb-interop) | Reference Sparkplug B edge nodes and host applications (Eclipse Tahu, pysparkplug, SparkplugNet, srad, sparkplug-client, sparkplughost) as fixture-driven containers, which `make interop` runs this library against |

## License

Original OT Fabric code is MIT licensed. The `proto/` directory contains code
derived from the Eclipse Tahu project under EPL-2.0. See
[docs/LICENSING.md](docs/LICENSING.md) for details.
