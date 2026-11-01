# Feature status

Levels: **implemented**, **tested** (this repository), **independently verified** (against implementations not written here). Nothing is certified, and the Sparkplug TCK has not been run. [CONFORMANCE.md](CONFORMANCE.md) has the status per requirement.

| Feature | Level | Evidence |
|---|---|---|
| Edge Node: birth, death, rebirth, seq, bdSeq, data | independently verified | [INTEROPERABILITY.md](../INTEROPERABILITY.md#verification-status) |
| Host Application: STATE, session tracking, commands, reordering | independently verified | same |
| Application without STATE | independently verified | same |
| Scalar metric codecs | independently verified | same |
| Templates, DataSets, properties | independently verified, with integers | `composite` fixture, [INTEROPERABILITY.md](../INTEROPERABILITY.md#verification-status) |
| Arrays | tested | `payload` tests only ([LIMITATIONS.md](../LIMITATIONS.md#verification)) |
| Server walking, primary host awareness | tested | `edge/edge_test.go` |
| TLS, WebSocket through Paho | implemented | `mqtt/paho.go`; the login and interop tests use plain TCP |
| Broker login (`Username`, `Password`) | tested | `mqtt/e2e_test.go`, fake broker only |
| Alias uniqueness across the edge node | tested | `TestAliasUniqueness` |
| Decode limits (`MaxArrayElements`, `MaxStringBytes`) | tested | `TestDecodeLimits` |
| MQTT 5 | not implemented | [LIMITATIONS.md](../LIMITATIONS.md) |
| Complex data in the edge node | not implemented | [LIMITATIONS.md](../LIMITATIONS.md) |
