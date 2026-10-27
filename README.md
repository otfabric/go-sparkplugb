# go-sparkplugb

Production-quality [Eclipse Sparkplug B 3.0.0](https://www.eclipse.org/tahu/spec/sparkplug_spec.pdf) library for Go.

## Features

- **Edge Node runtime** — full lifecycle management (birth/death/rebirth), sequence numbering, alias support, Primary Host awareness
- **Host Application runtime** — STATE birth/death certificates, node/device registry, sequence validation with automatic rebirth requests
- **Typed metric codec** — correct signed-integer handling (Int8/Int16/Int32 in uint32 wire fields), all array encodings (little-endian packed, bit-packed BooleanArray, null-terminated StringArray), quality codes
- **Topic namespace** — parser and builder for `spBv1.0/{group}/{type}/{node}/[{device}]`
- **Protobuf types** — generated from the canonical Eclipse Tahu `sparkplug_b.proto` (pinned commit, SHA-256 verified)
- **MQTT transport abstraction** — plug in any MQTT 3.1.1 or 5.0 client

## Packages

| Package | Description |
|---------|------------|
| `proto` | Generated Protobuf types (EPL-2.0) |
| `topic` | Topic namespace parsing and construction |
| `payload` | Typed metric encoding/decoding, quality codes, builder helpers |
| `edge` | Edge Node runtime (lifecycle, sequencing, commands) |
| `host` | Host Application runtime (STATE, registry, seq validation) |

## Installation

```bash
go get github.com/otfabric/go-sparkplugb
```

Requires Go 1.23 or later.

## Quick Start — Edge Node

```go
node, _ := edge.New(transport, edge.Config{
    GroupID:    "plant",
    EdgeNodeID: "gateway-01",
    BrokerURL:  "tcp://broker:1883",
})

node.Connect(ctx)

tempMetric, _ := payload.NewMetric("temperature", payload.Double, float64(22.5))
node.PublishBirth(ctx, []*proto.Payload_Metric{tempMetric})

// Report by exception
updated, _ := payload.NewMetric("temperature", payload.Double, float64(23.1))
node.PublishNodeData(ctx, []*proto.Payload_Metric{updated})

node.Close(ctx)
```

## Quick Start — Host Application

```go
app, _ := host.New(transport, host.Config{
    HostID:    "scada-01",
    BrokerURL: "tcp://broker:1883",
    OnNodeBirth: func(group, node string, metrics []*proto.Payload_Metric) {
        log.Printf("node online: %s/%s with %d metrics", group, node, len(metrics))
    },
})

app.Connect(ctx)
defer app.Close(ctx)
```

## Specification Conformance

This library targets full conformance with the Eclipse Sparkplug B 3.0.0 specification. See [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for the clause-by-clause requirements matrix.

## License

Original OT Fabric code is MIT licensed. The `proto/` directory contains code derived from the Eclipse Tahu project under EPL-2.0. See [docs/LICENSING.md](docs/LICENSING.md) for details.
