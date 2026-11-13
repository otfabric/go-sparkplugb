# Limitations

What go-sparkplugb does not do, and what has not been verified. The status of
each requirement is in [docs/CONFORMANCE.md](docs/CONFORMANCE.md), the test
evidence in [INTEROPERABILITY.md](INTEROPERABILITY.md).

## Scope not implemented

- MQTT 5: the Paho adapter asks for MQTT 3.1.1 and falls back to 3.1 if the broker
  refuses it, so an intentional disconnect does not send DISCONNECT with reason
  code 0x04. `mqtt.Transport` has no way to pass that reason code either.
- Host Application multi-broker support is partial: `ServerURLs`, one broker at a
  time.
- The library does not enforce that NDATA and DDATA metrics are a subset of the
  birth, nor that metric names appear in births only: that is left to the
  caller. The template rules of Sparkplug are enforced
  ([CONFORMANCE.md](docs/CONFORMANCE.md#templates)): the edge node refuses to
  publish a template that breaks them, and `host` and `app` log a warning and
  still apply the message.

## Verification

- Run against six independent implementations (Eclipse Tahu Java and JavaScript,
  pysparkplug, SparkplugNet, srad, sparkplughost) through
  [sparkplugb-interop](https://github.com/otfabric/sparkplugb-interop) v0.4.0,
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
- Templates are checked by member and parameter name. A template member sent by
  alias only is not checked against its definition, since the `payload` package
  has no alias map. An instance is matched to its definition by `template_ref`
  alone: Sparkplug gives the template version no matching rule, so an instance
  with another version than its definition is accepted.
- `host` and `app` report a template that breaks Sparkplug on the `Logger` and
  apply the message: an edge node's error is not a reason to lose its data, and a
  rebirth would bring the same NBIRTH.
- The SparkplugNet 1.4.1 edge node sends an empty `template_ref` with its
  template definitions: `host` and `app` log a warning for every NBIRTH of such a
  node ([INTEROPERABILITY.md](INTEROPERABILITY.md#verification-status)).
- The member of a template instance without a datatype, which data messages may
  have, is `ErrUnknownType` from `GetValue`: the caller has the definition, the
  `payload` package does not. Set its datatype from the definition first.
- `GetQuality`, `bdSeq` and `seq` take the types the specification gives them;
  `host` and `app` read `bdSeq` from `long_value` only.
- A session is told from another by payload timestamps, in milliseconds, of the edge node's clock. Two rebirths in the same millisecond, or an edge node that stamps a message earlier than the NBIRTH before it and has it reordered, cost a rebirth to recover. An NBIRTH that a broker delivers more than 250 ms after the NDEATH of the same `bdSeq` cannot be told from a restart, so such a node stays online until its next death. Within those 250 ms it is dropped: an edge node that restarts that fast with the `bdSeq` of its NDEATH, which breaks the increment Sparkplug requires, comes online only if it answers the rebirth that follows.
- Sparkplug carries no session in a data message, and MQTT orders messages per topic only, so a broker may deliver a message of the ending session among those of the new one. `host` and `app` tell it apart by its timestamp, and in two cases cannot (go-sparkplugb#24):
  - it is stamped in the millisecond of the next NBIRTH and delivered before that NBIRTH: births and the data right after them share a millisecond too, so it is taken for the new session's;
  - it is older than the NBIRTH, but delivered after it with the `seq` the new session expects next: a message in sequence is applied whatever its timestamp, because an edge node may stamp a DBIRTH before its NBIRTH, and dropping such a DBIRTH would drop it at every rebirth.

  Either way its value is delivered, and the new session's message with that `seq` is then behind the sequence and not delivered. When no NBIRTH follows within 2 s of it, a rebirth is requested: if the edge node answers it, that restores the current state, never the value delivered meanwhile.

  The `edge` node of this library rules out two things: a message of the ending session published after the NBIRTH, since data published during a rebirth waits for it and its messages reach the broker in `seq` order (given a transport that keeps the ordering `mqtt.Transport.Publish` documents); and the first case, since it stamps an NBIRTH in a later millisecond than the message before it, as long as its clock does not go back. After a step back it stamps the NBIRTH by the clock, earlier than the message before it, rather than hold every publish until the clock catches up, so the first case is possible again around that step. The second case remains with any edge node, this library's included: a message published just before the NBIRTH can still be delivered after it, with the `seq` the new session expects.
