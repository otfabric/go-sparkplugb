# Limitations

What go-sparkplugb does not do, and what has not been verified. The status of
each requirement is in [docs/CONFORMANCE.md](docs/CONFORMANCE.md), the test
evidence in [INTEROPERABILITY.md](INTEROPERABILITY.md).

## Scope not implemented

- MQTT 5: the Paho adapter speaks MQTT 3.1.1, so an intentional disconnect does
  not send DISCONNECT with reason code 0x04.
- Complex data in the edge node (the optional SendComplexData group of the
  conformance profile).
- Host Application multi-broker support is partial: `ServerURLs`, one broker at a
  time.
- The library does not enforce that NDATA and DDATA metrics are a subset of the
  birth, nor that metric names appear in births only: that is left to the caller.
- `edge`, `host` and `app` do not pass broker credentials: they never set
  `ConnectOpts.Username` or `ConnectOpts.Password`. A broker that requires a login
  needs an `mqtt.Transport` that supplies them.

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
