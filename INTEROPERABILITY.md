# Interoperability: otfabric/go-sparkplugb

What the library does on the wire where Sparkplug B 3.0.0 leaves room or
implementations disagree, and how it was verified against independent
implementations. [docs/CONFORMANCE.md](docs/CONFORMANCE.md) maps the
normative requirements clause by clause.

## On the wire

| Item | go-sparkplugb |
|------|---------------|
| MQTT | 3.1.1, falling back to 3.1 if the broker refuses it (Eclipse Paho); no MQTT 5: TCP, TLS (`TLSConfig`) or WebSocket, by the scheme of the broker URL |
| Namespace | `spBv1.0` |
| STATE | Sparkplug 3.0 (`spBv1.0/STATE/<host id>`, JSON `{"online", "timestamp"}`, QoS 1, retained) |
| Int8, Int16 encoded as | `int_value` sign-extended to 32 bits (as Eclipse Tahu and SparkplugNet) |
| Int8, Int16 decoded from | either form: sign-extended or width-sized (as pysparkplug and srad send it). Anything else, such as `0x100` or `0xFFFFFF7F` for an Int8, is `ErrTypeMismatch` |
| UInt32 encoded as | `int_value` (protobuf `uint32`, as the specification says) |
| Integers decoded from | `int_value` or `long_value`, whichever carries the value (Eclipse Tahu puts UInt32 in `long_value`). A value out of the datatype's range is `ErrTypeMismatch`, see [Integers](#integers) |
| Datatype in NDATA, DDATA, NCMD, DCMD | as the caller builds the metric (`payload.NewMetric` sets it); a received metric without one takes the datatype of its birth |
| `seq` in NCMD and DCMD | never sent |
| Metric timestamp | on every metric of NBIRTH, DBIRTH, NDATA and DDATA |
| NBIRTH | `bdSeq`, `Node Control/Rebirth` (Boolean, false), then the node metrics |
| Aliases | assigned by the caller; data may carry the alias only |
| Primary host goes offline | NDEATH, DISCONNECT, then a new session on the next server (the same one when there is one), waiting for the host's STATE online; an offline STATE older than the latest online one is ignored |

### Negative Int8 and Int16

Specification ambiguity. Sparkplug B 3.0.0 gives Int8 and Int16 the protobuf
type `uint32` and does not say how a negative value goes in. Eclipse Tahu,
SparkplugNet and the Sparkplug TCK's encoder sign-extend it to 32 bits;
pysparkplug and srad send the width. Each form keeps the two's-complement
bits of the value, and for a value that is not negative the two coincide.

go-sparkplugb follows Eclipse Tahu on encoding and accepts both on decoding:

| Value | Encoded | Decoded from |
|-------|---------|--------------|
| `int8(-8)` | `0xFFFFFFF8` | `0xFFFFFFF8`, `0x000000F8` |
| `int16(-8)` | `0xFFFFFFF8` | `0xFFFFFFF8`, `0x0000FFF8` |

`payload.GetValue` returns `ErrTypeMismatch` for any other value; it does not
truncate. DataSet cells, template parameters and PropertyValues follow the
same rule, and a template's members are metrics.

### Integers

Metric, DataSet.DataSetValue, Template.Parameter and PropertyValue carry
integers in the same two unsigned fields, and the `payload` package treats them
alike. What the specification states: the datatype's width and signedness, and
the protobuf type (`uint32` up to 32 bits, `uint64` for Int64, UInt64 and
DateTime). What it does not state: how a signed value goes in, or what a
receiver does with a value out of range. The readings below are this library's.

| Datatype | Read from `int_value` or `long_value` as | Anything else |
|----------|------------------------------------------|---------------|
| Int8, Int16 | the width-sized form, or the sign extension to 32 or 64 bits | `ErrTypeMismatch` |
| Int32 | the 32-bit form, or its sign extension to 64 bits | `ErrTypeMismatch` |
| Int64 | two's complement of all 64 bits; from `int_value`, zero-extended | none |
| UInt8, UInt16, UInt32 | the value | above the type's maximum, `ErrTypeMismatch` |
| UInt64 | the value | none |
| DateTime | milliseconds since the epoch | above `math.MaxInt64`, which a `time.Time` cannot hold from milliseconds: `ErrTypeMismatch` |

A value of another kind, a string in an Int8, is `ErrTypeMismatch` too. A metric
or parameter with no value at all reads as zero, as before, and a DataSet cell
with no value as `nil`.

Encoding, in all four places: Int8, Int16 and Int32 in `int_value`,
sign-extended to 32 bits; UInt8, UInt16 and UInt32 in `int_value`; Int64, UInt64
and DateTime in `long_value`. The Go type has to be the datatype's, so nothing
can be out of range, except a `time.Time` before the epoch, which `uint64` cannot
hold (`ErrTypeMismatch`, also for DateTimeArray). `NewDataSet`,
`NewTemplateParameter` and `NewPropertyValue` take the wire type as well, `uint32`
or `uint64`: it is checked by the table above and written in the form above.

## Host and application behaviour

| Situation | What the host (package `host`) and the application (package `app`) do |
|-----------|-------------------------------------------------------------|
| Messages of a node out of `seq` order | Held, at most 256 per node and 30 seconds, and applied in `seq` order once the gap is filled. MQTT orders messages per topic only, and NanoMQ and HiveMQ CE do deliver a node's DBIRTH before its NBIRTH and data out of order |
| Message behind the expected `seq` | Held for the node's next session and applied after its NBIRTH, without a rebirth request: a restarted node's DBIRTH may arrive before its NBIRTH |
| A gap that is not filled | NCMD `Node Control/Rebirth` after `ReorderTimeout` (host), at once without it |
| Data or DBIRTH from a node without a birth | NCMD `Node Control/Rebirth` |
| Any of the rebirth requests above | One pending per node: the next waits for the NBIRTH or 2 seconds, however many messages are out of order |
| NDEATH whose `bdSeq` is not the birth's | Ignored: it is the will of an earlier session, which a broker may deliver late |
| Data metric with only an alias | Stored by the name and datatype of its birth |
| `GetNode`, `Nodes` | Return copies, safe to read while messages arrive |

## Verification status

Besides its own tests, the library is run on every push against six
independent implementations, in every role they have, on four MQTT brokers.
The reference builds are the images of
[otfabric/sparkplugb-interop v0.3.0](https://github.com/otfabric/sparkplugb-interop/releases/tag/v0.3.0),
pinned in [interop/images.go](interop/images.go):

| Reference | Version | go-sparkplugb host and application ← reference edge | go-sparkplugb edge → reference host |
|-----------|---------|------------------------------------------------------|-------------------------------------|
| Eclipse Tahu (Java) | 1.0.21 | pass | pass |
| pysparkplug | 0.6.1 | pass ¹ | pass ² ⁵ |
| SparkplugNet | 1.4.1 | pass ³ | pass ⁴ |
| srad | 0.5.0 | pass | pass ⁵ |
| Eclipse Tahu sparkplug-client (JavaScript) | 3.2.4 | pass | (edge only) |
| EvergenEnergy/sparkplughost | v0.4.0 | (host only) | pass ⁵ |

on Eclipse Mosquitto 2.1.2, HiveMQ CE 2026.5, EMQX 6.3.2 and NanoMQ 0.25.7.

The `composite` fixture runs DataSets, templates and properties with integers at
each type's limits. Tahu, tahu-js and SparkplugNet run it as edges against the
library's host and application, Tahu, SparkplugNet and sparkplughost as hosts
against its edge. The suite checks the form of each Int8 and Int16 on the wire, in
each of those places. On the SparkplugNet host, properties are not compared, and
UInt32 DataSet cells neither: it drops the first and reads the second from
`long_value` only, where the library writes `int_value` as the specification says.

¹ pysparkplug's NDEATH on a clean stop carries `bdSeq + 1`, so the
node does not go offline on it; the will after a crash is checked instead.
² pysparkplug's host cannot decode a negative Int8 or Int16 sign-extended to
32 bits, as go-sparkplugb (and Tahu) encode it: the scalars fixture is
skipped. This is a [specification ambiguity](#negative-int8-and-int16), not
a defect of either side.
³ without device writes: SparkplugNet's edge drops a DCMD for a device.
⁴ without the steps after a death: SparkplugNet's host keeps dead nodes and
devices online.
⁵ not on NanoMQ and HiveMQ CE: these hosts apply messages in arrival order,
and the brokers reorder messages across topics in some runs.

A reference that lacks a capability (a datatype, a call, a feature) skips
the steps that need it; each skip and known difference is logged with its
reason. The differences are described in
[sparkplugb-interop's COMPATIBILITY.md](https://github.com/otfabric/sparkplugb-interop/blob/main/docs/COMPATIBILITY.md).

The suite (`make interop`, module `interop/`):

- **TestHost**, the go-sparkplugb host against each reference edge on each
  fixture: births, aliases, every update, writes by NCMD and DCMD, rebirth on
  request and by call, device death and birth, NDEATH on a clean stop, the
  will after a crash, and the edge's view of the primary host going offline.
- **TestApplication**, the go-sparkplugb application (no STATE) against each
  reference edge: births, updates, device death and birth, and the node's
  death.
- **TestEdge**, the go-sparkplugb edge against each reference host: births with
  aliases, alias-only data, the host's view of the node, commands, rebirth on
  request, device death and birth, NDEATH, the will, and the primary host's
  STATE going offline.

The two fixtures cover a node with two devices and writable metrics, and all
fourteen scalar datatypes at their limits. Templates, DataSets, arrays and
properties are covered by this repository's codec tests only: no fixture of
the reference images has them.

It has **not** been run against the Sparkplug TCK or commercial
implementations. Interoperability reports are welcome.
