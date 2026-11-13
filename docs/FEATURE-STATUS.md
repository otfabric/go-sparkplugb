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
| Template rules: definitions in the NBIRTH only, instances refer to them, members and parameters of the definition | independently verified | `payload/templatecheck_test.go`, `TestTemplateDefinitionsInNBIRTH`, `TestTemplateInstanceUnknownDefinition`; the interop host and application fail on a template warning, against the Tahu, tahu-js and SparkplugNet edges, with SparkplugNet's empty `template_ref` a known deviation ([INTEROPERABILITY.md](../INTEROPERABILITY.md#verification-status)) |
| Decode limits (`MaxArrayElements`, `MaxStringBytes`) | tested | `TestDecodeLimits` |
| MQTT 5 | not implemented | [LIMITATIONS.md](../LIMITATIONS.md) |
