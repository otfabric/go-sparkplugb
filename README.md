# go-sparkplugb

Production-quality [Eclipse Sparkplug B 3.0.0](https://sparkplug.eclipse.org/specification/version/3.0/documents/sparkplug-specification-3.0.0.pdf) library for Go.

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/otfabric/go-sparkplugb/actions/workflows/ci.yaml/badge.svg)](https://github.com/otfabric/go-sparkplugb/actions/workflows/ci.yaml)
[![Coverage](https://img.shields.io/badge/coverage-95%25-brightgreen)](docs/CONFORMANCE.md)
[![Go Reference](https://pkg.go.dev/badge/github.com/otfabric/go-sparkplugb.svg)](https://pkg.go.dev/github.com/otfabric/go-sparkplugb)
[![Release](https://img.shields.io/github/v/release/otfabric/go-sparkplugb?include_prereleases)](https://github.com/otfabric/go-sparkplugb/releases)

go-sparkplugb handles the full Sparkplug B 3.0.0 protocol lifecycle for Edge
Nodes, Host Applications and Applications: topic namespace, Protobuf codecs,
typed metrics (including arrays, templates and datasets), sequence numbering,
alias management, birth/death certificates, primary host awareness with server
walking, and message reordering. Plug in any MQTT 3.1.1 or 5.0 client through
the transport interface.

**Quick links:**
[Install](#installation) ·
[Edge Node](#quick-start--edge-node) ·
[Host Application](#quick-start--host-application) ·
[Application](#quick-start--application) ·
[Packages](#packages) ·
[Examples](#examples) ·
[Conformance](#specification-conformance) ·
[Related projects](#related-projects)

---

## Features

| Category | Details |
|----------|---------|
| **Edge Node runtime** | Full lifecycle (birth/death/rebirth), sequence numbering, alias support, primary host awareness, server walking across multiple brokers |
| **Host Application runtime** | STATE birth/death certificates, node/device registry, sequence validation, automatic rebirth requests, message reordering timeout |
| **Application runtime** | Non-primary-host consumer: subscribes to spBv1.0/#, tracks nodes/devices, alias resolution, sends NCMD/DCMD — no STATE messages |
| **Typed metric codec** | All scalar types, signed-integer handling (Int8–Int64 in wire fields), all array types (little-endian packed, bit-packed BooleanArray, null-terminated StringArray, DateTimeArray), quality codes |
| **Templates** | Template definitions (is_definition, version, member metrics, parameters) and instances (template_ref, parameter overrides) |
| **DataSets** | Columnar data with typed columns (num_of_columns, columns, types, rows), round-trip encode/decode |
| **Topic namespace** | Parser and builder for `spBv1.0/{group}/{type}/{node}/[{device}]` with validation |
| **Protobuf types** | Generated from the canonical Eclipse Tahu `sparkplug_b.proto` (pinned commit, SHA-256 verified) |
| **MQTT transport** | Interface-based — plug in Paho, EMQX, or any MQTT client |

## Installation

```bash
go get github.com/otfabric/go-sparkplugb
```

Requires Go 1.24 or later.

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
| `ClientID` | `string` | auto | MQTT client ID |
| `PrimaryHostID` | `string` | | Primary Host Application ID to wait for |
| `UseAliases` | `bool` | `false` | Use metric aliases in DATA messages |
| `OnState` | `func` | | State change callback |
| `OnNodeCmd` | `func` | | NCMD received callback |

### Host Application Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `HostID` | `string` | required | Host Application identifier |
| `BrokerURL` | `string` | | MQTT broker URL |
| `ServerURLs` | `[]string` | | Multiple broker URLs |
| `ClientID` | `string` | auto | MQTT client ID |
| `ReorderTimeout` | `time.Duration` | `0` | Wait before requesting rebirth on sequence gap |
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
| `ClientID` | `string` | auto | MQTT client ID |
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
| Host Application | Session, Termination, Commands, Edge Termination | Message Reordering |
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
├── examples/
│   ├── edge-node/        Edge Node lifecycle
│   ├── host-app/         Host Application lifecycle
│   ├── templates/        Template definitions and instances
│   ├── aliases/          Metric aliases and alias resolution
│   ├── application/      Application (non-host consumer)
│   ├── commands/         Sending NCMD and DCMD commands
│   ├── datasets/         DataSet columnar metrics
│   └── server-walking/   Multi-broker server walking
└── docs/
    ├── CONFORMANCE.md    Specification conformance matrix
    └── LICENSING.md      License details
```

## Building and Development

| Target | Description |
|--------|-------------|
| `make test` | Run all tests |
| `make cover` | Run tests with coverage report |
| `make lint` | Run golangci-lint |
| `make build` | Build examples |
| `make check` | Lint + test + coverage gate (90%) |
| `make interop` | Edge/host/app against Mosquitto, HiveMQ CE, EMQX and NanoMQ in Docker (`interop/` module) |

```bash
git clone https://github.com/otfabric/go-sparkplugb.git
cd go-sparkplugb
make check
```

## Related Projects

| Repository | Description |
|-----------|-------------|
| [sparkplugbctl](https://github.com/otfabric/sparkplugbctl) | CLI for monitoring, publishing, simulating and debugging Sparkplug B infrastructure |
| [sparkplugb-interop](https://github.com/otfabric/sparkplugb-interop) | Docker-based interoperability and chaos testing against Mosquitto, HiveMQ, EMQX and NanoMQ |

## License

Original OT Fabric code is MIT licensed. The `proto/` directory contains code
derived from the Eclipse Tahu project under EPL-2.0. See
[docs/LICENSING.md](docs/LICENSING.md) for details.
