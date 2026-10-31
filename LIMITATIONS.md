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
  [sparkplugb-interop](https://github.com/otfabric/sparkplugb-interop) v0.1.0,
  on Mosquitto, HiveMQ CE, EMQX and NanoMQ. Roles, exclusions and upstream
  defects are in [INTEROPERABILITY.md](INTEROPERABILITY.md#verification-status).
- Templates, DataSets, arrays and properties are covered by this repository's
  codec tests only: no fixture of the reference images has them.
- Not run against the Sparkplug TCK, or against commercial implementations.
- Certification: none. Eclipse Sparkplug compatibility certification has not been attempted.

## Known deviations

Interpretations where the specification leaves room, and how they differ between
stacks, are in [INTEROPERABILITY.md](INTEROPERABILITY.md#on-the-wire).

- Negative Int8 and Int16: go-sparkplugb sign-extends on encoding, which the
  pysparkplug host cannot decode, and accepts both forms on decoding
  ([specification ambiguity](INTEROPERABILITY.md#negative-int8-and-int16)).
- DataSet values and template parameters of type Int8 and Int16 are raw
  `uint32` values in the API: neither form is applied or checked.
