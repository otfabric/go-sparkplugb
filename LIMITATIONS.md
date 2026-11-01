# Limitations

What go-sparkplugb does not do, and what has not been verified. The status of
each requirement is in [docs/CONFORMANCE.md](docs/CONFORMANCE.md), the test
evidence in [INTEROPERABILITY.md](INTEROPERABILITY.md).

## Scope not implemented

- MQTT 5: the Paho adapter asks for MQTT 3.1.1 and falls back to 3.1 if the broker
  refuses it, so an intentional disconnect does not send DISCONNECT with reason
  code 0x04. `mqtt.Transport` has no way to pass that reason code either.
- Complex data in the edge node (the optional SendComplexData group of the
  conformance profile).
- Host Application multi-broker support is partial: `ServerURLs`, one broker at a
  time.
- The library does not enforce that NDATA and DDATA metrics are a subset of the
  birth, nor that metric names appear in births only: that is left to the caller.

## Verification

- Run against six independent implementations (Eclipse Tahu Java and JavaScript,
  pysparkplug, SparkplugNet, srad, sparkplughost) through
  [sparkplugb-interop](https://github.com/otfabric/sparkplugb-interop) v0.3.0,
  on Mosquitto, HiveMQ CE, EMQX and NanoMQ. Roles, exclusions and upstream
  defects are in [INTEROPERABILITY.md](INTEROPERABILITY.md#verification-status).
- Arrays are covered by this repository's codec tests only: no fixture of the
  reference images has them. DataSets, templates and properties are run with
  integers, against Tahu, tahu-js and SparkplugNet edges and Tahu, SparkplugNet and
  sparkplughost hosts; pysparkplug and srad do not run them.
- Not run against the Sparkplug TCK, or against commercial implementations.
- Certification: none. Eclipse Sparkplug compatibility certification has not been attempted.

## Known deviations

Interpretations where the specification leaves room, and how they differ between
stacks, are in [INTEROPERABILITY.md](INTEROPERABILITY.md#on-the-wire).

- Negative Int8 and Int16: go-sparkplugb sign-extends on encoding, which the
  pysparkplug host cannot decode, and accepts both forms on decoding
  ([specification ambiguity](INTEROPERABILITY.md#negative-int8-and-int16)).
- A value out of its datatype's range is rejected on decoding, in metrics,
  DataSet cells, template parameters and properties
  ([Integers](INTEROPERABILITY.md#integers)). The specification does not say what
  a receiver does with one: this library's reading. A stack that truncates accepts
  what this library refuses.
- An Int64 or UInt64 in `int_value`, against the specification's `uint64`, is read
  zero-extended: a negative Int64 sent that way reads as a large positive value.
- The member of a template instance without a datatype, which data messages may
  have, is `ErrUnknownType` from `GetValue`: the caller has the definition, the
  `payload` package does not. Set its datatype from the definition first.
- `GetQuality`, `bdSeq` and `seq` take the types the specification gives them;
  `host` and `app` read `bdSeq` from `long_value` only.
- A session is told from another by payload timestamps, in milliseconds, of the edge node's clock. Two rebirths in the same millisecond, or an edge node that stamps a message earlier than the NBIRTH before it and has it reordered, cost a rebirth to recover. An NBIRTH that a broker delivers more than 250 ms after the NDEATH of the same `bdSeq` cannot be told from a restart, so such a node stays online until its next death. Within those 250 ms it is dropped: an edge node that restarts that fast with the `bdSeq` of its NDEATH, which breaks the increment Sparkplug requires, comes online only if it answers the rebirth that follows.
