# Conformance — go-sparkplugb

This document maps every normative requirement from the Eclipse Sparkplug B
3.0.0 specification to its implementation status in this library. Requirements
are grouped by specification chapter and identified by their TCK test IDs where
applicable.

Legend: **Y** = implemented, **P** = partial, **N** = not yet, **N/A** = not
applicable (broker or server requirement).

---

## Topics (Chapter 4)

### Topic Namespace Structure

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Topic namespace `spBv1.0/{group}/{type}/{node}/[{device}]` | — | Y | topic package |
| Group ID: valid UTF-8, no `/`, `+`, `#`, reserved words | tck-id-topics-group-id | Y | topic validation |
| Edge Node ID: valid UTF-8, no `/`, `+`, `#`, reserved words | tck-id-topics-edge-node-id | Y | topic validation |
| Device ID: valid UTF-8, no `/`, `+`, `#`, reserved words | tck-id-topics-device-id | Y | topic validation |

### Message Types

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| NBIRTH: QoS 0, not retained | tck-id-topics-nbirth-mqtt | Y | edge package |
| NDEATH: QoS 1, not retained (Will Message) | tck-id-topics-ndeath-mqtt | Y | edge package |
| DBIRTH: QoS 0, not retained | tck-id-topics-dbirth-mqtt | Y | edge package |
| DDEATH: QoS 0, not retained | tck-id-topics-ddeath-mqtt | Y | edge package |
| NDATA: QoS 0, not retained | tck-id-topics-ndata-mqtt | Y | edge package |
| DDATA: QoS 0, not retained | tck-id-topics-ddata-mqtt | Y | edge package |
| NCMD: QoS 0, not retained | tck-id-topics-ncmd-mqtt | Y | host package |
| DCMD: QoS 0, not retained | tck-id-topics-dcmd-mqtt | Y | host package |
| STATE: QoS 1, retained | tck-id-topics-state-mqtt | Y | host package |
| All template definitions of the instances of the edge node and its devices in the NBIRTH | tck-id-topics-nbirth-templates | Y | `edge` refuses the message with `payload.ErrUnknownTemplate`; `host` and `app` log a warning ([Templates](#templates)) |

---

## Operational Behavior (Chapter 5)

### Edge Node Session Establishment

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| CONNECT with Clean Session true | tck-id-message-flow-edge-node-birth-publish-connect | Y | |
| Register NDEATH as Will Message | tck-id-message-flow-edge-node-birth-publish-will-message | Y | |
| Will payload contains bdSeq metric | tck-id-message-flow-edge-node-birth-publish-will-message-payload | Y | |
| Will QoS 1, not retained | tck-id-message-flow-edge-node-birth-publish-will-message-qos | Y | |
| Subscribe to NCMD topic | tck-id-message-flow-edge-node-birth-subscribe-ncmd | Y | |
| Optionally subscribe to DCMD topics | tck-id-message-flow-edge-node-birth-subscribe-dcmd | Y | |
| Publish NBIRTH with seq=0 | tck-id-message-flow-edge-node-birth-publish-nbirth-seq | Y | |
| NBIRTH contains all node metrics | tck-id-message-flow-edge-node-birth-publish-nbirth-metrics | Y | |
| NBIRTH bdSeq matches Will bdSeq | tck-id-message-flow-edge-node-birth-publish-nbirth-bdseq | Y | |
| Publish DBIRTH for each online device | tck-id-message-flow-edge-node-birth-publish-dbirth | Y | |

### Edge Node Session Termination

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Publish NDEATH on intentional disconnect | tck-id-message-flow-edge-node-termination-publish-ndeath | Y | |
| MQTT 5.0: DISCONNECT with reason code 0x04 | tck-id-message-flow-edge-node-termination-mqtt5 | N | Needs MQTT 5.0 transport |
| Increment bdSeq on next CONNECT | tck-id-message-flow-edge-node-termination-bdseq-increment | Y | |

### Edge Node Data Publication

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| NDATA metrics are a subset of NBIRTH | tck-id-message-flow-edge-node-data-publish-ndata | P | Not enforced by library |
| DDATA metrics are a subset of DBIRTH | tck-id-message-flow-edge-node-data-publish-ddata | P | Not enforced by library |
| seq increments for each message | tck-id-message-flow-edge-node-data-publish-seq | Y | On the wire in `seq` order, also when publishing from several goroutines and during a rebirth |
| seq wraps from 255 to 0 | tck-id-message-flow-edge-node-data-publish-seq-wrap | Y | |

### Edge Node Rebirth

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Handle NCMD with Node Control/Rebirth=true | tck-id-message-flow-edge-node-rebirth-action | Y | Sets born=false |
| On rebirth: re-publish NBIRTH + all DBIRTHs | tck-id-message-flow-edge-node-rebirth-publish | Y | Auto-rebirth via Rebirth() |

### Host Application Session Establishment

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| CONNECT with Clean Session true | tck-id-host-topic-phid-connect | Y | |
| Register STATE Death as Will (QoS 1, retained) | tck-id-host-topic-phid-death-payload | Y | |
| Will payload: JSON `{"online":false,"timestamp":T}` | tck-id-host-topic-phid-death-payload-connect | Y | |
| Publish STATE Birth (QoS 1, retained) | tck-id-host-topic-phid-birth-payload | Y | |
| Birth payload: JSON `{"online":true,"timestamp":T}` | tck-id-host-topic-phid-birth-payload-connect | Y | |
| Birth timestamp matches Will timestamp | tck-id-host-topic-phid-birth-timestamp | Y | Same `ts` variable |
| Subscribe to `spBv1.0/#` | tck-id-host-topic-phid-subscribe | Y | |

### Host Application Session Termination

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Publish STATE Death on intentional disconnect | tck-id-host-topic-phid-death-publish | Y | |
| Death timestamp is current time (not Will timestamp) | tck-id-host-topic-phid-death-timestamp | Y | |

### Host Application Data Handling

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Track online/offline state of all Edge Nodes | tck-id-host-topic-phid-state-tracking | Y | |
| Validate seq ordering per Edge Node | tck-id-host-topic-phid-seq-validation | Y | |
| On seq gap: request rebirth via NCMD | tck-id-host-topic-phid-seq-rebirth | Y | At most one pending request per node (2 s) |
| Mark metrics STALE on NDEATH/DDEATH | tck-id-host-topic-phid-stale-metrics | Y | Stale flag on EdgeNodeState/DeviceState |
| Message reordering timeout (optional) | tck-id-host-topic-phid-reorder | Y | ReorderTimeout config, timer-based |

### Primary Host Behavior

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Edge waits for Primary Host STATE online=true | tck-id-message-flow-edge-node-birth-primary-host | Y | |
| Edge walks server list if STATE online=false | tck-id-message-flow-edge-node-birth-primary-host-walk | Y | walkServers() cycles ServerURLs |
| Edge disconnects if Primary Host goes offline | tck-id-message-flow-edge-node-birth-primary-host-offline | Y | |

---

## Payloads (Chapter 6)

### Metric Structure

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Metric name in BIRTH only | — | P | Not enforced by library |
| Metric alias in BIRTH and DATA | — | Y | Edge tracks, Host resolves |
| Alias unique across entire Edge Node | — | Y | Edge returns `ErrDuplicateID` from a birth that breaks it; Host alias map |
| DataType field set on every metric | — | Y | payload package |
| Timestamp on every metric | — | Y | NewMetric/StampMetrics sets per-metric ts |
| is_null for null values | — | Y | payload package |
| is_historical for historical data | — | Y | Proto field available |
| is_transient for non-stored data | — | Y | Proto field available |

### Signed Integer Types

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Int8 in uint32 field | — | Y | The specification does not say how a negative value goes in: encoded sign-extended, decoded from either form ([INTEROPERABILITY.md](../INTEROPERABILITY.md#negative-int8-and-int16)) |
| Int16 in uint32 field | — | Y | As Int8 |
| Int32 in uint32 field, interpret as signed | — | Y | payload package; out of range is rejected ([INTEROPERABILITY.md](../INTEROPERABILITY.md#integers)) |
| Int64 in uint64 field, interpret as signed | — | Y | payload package |
| UInt8, UInt16, UInt32 in uint32 field | — | Y | payload package; out of range is rejected, also from `long_value` |
| The same in DataSet cells, template parameters and property values | — | Y | payload package; `GetDataSetValues`, `GetTemplateParameterValue`, `GetPropertyValue` |

### Array Types

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Numeric arrays: little-endian packed in bytes_value | — | Y | payload package |
| BooleanArray: 4-byte count prefix + bit-packed | — | Y | payload package |
| StringArray: null-terminated strings in bytes_value | — | Y | payload package |
| DateTimeArray: packed uint64 little-endian ms | — | Y | payload package |

### Quality Codes

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Quality as Int32 PropertyValue named "quality" | — | Y | payload package |
| 0 = BAD, 192 = GOOD, 500 = STALE | — | Y | payload package |

### Templates

Sparkplug 3.0.0 section 6.4.13 (Template) and 6.4.14 (Template.Parameter).
**Edge** is what `edge.Node` checks before it publishes: a message that breaks
the rule is not published, and its error wraps `payload.ErrInvalidTemplate` or
`payload.ErrUnknownTemplate`. **Host** is what `host` and `app` check on
receipt: a warning on the `Logger`, and the message is still applied and
delivered. Both use `payload.NewTemplateDefs` and `TemplateDefs.Check`
(payload/templatecheck.go), which a caller can also use.

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Definitions only in the NBIRTH | tck-id-payloads-template-definition-nbirth-only | Y | Edge and host: a definition in a DBIRTH, NDATA or DDATA |
| A definition has is_definition=true | tck-id-payloads-template-definition-is-definition, tck-id-payloads-template-is-definition-definition | Y | `NewTemplateDefinition` sets it; a template without it is taken for an instance |
| A definition omits template_ref | tck-id-payloads-template-definition-ref, tck-id-payloads-template-ref-definition | Y | Edge and host |
| A definition has all members its instances will have | tck-id-payloads-template-definition-members | Y | Checked as its converse: an instance member the definition lacks |
| The NBIRTH has the definition of every instance in the NBIRTH and DBIRTHs | tck-id-payloads-template-definition-nbirth | Y | Edge and host, `ErrUnknownTemplate`; also for NDATA and DDATA, and for instances nested in members |
| A definition has all parameters its instances will have | tck-id-payloads-template-definition-parameters | Y | Checked as its converse: an instance parameter the definition lacks |
| A definition may give parameter defaults | tck-id-payloads-template-definition-parameters-default | Y | MAY; `NewTemplateParameter` always sets a value |
| An instance has is_definition=false | tck-id-payloads-template-instance-is-definition, tck-id-payloads-template-is-definition-instance | Y | `NewTemplateInstance` sets it |
| is_definition in every template | tck-id-payloads-template-is-definition | Y | Edge and host: a template without it is an invalid instance |
| An instance's template_ref names the definition's metric | tck-id-payloads-template-instance-ref, tck-id-payloads-template-ref-instance | Y | Edge and host: missing, empty, not UTF-8, or no such definition |
| An instance has only members of its definition | tck-id-payloads-template-instance-members | Y | Edge and host, by member name; a member by alias only is not checked |
| An instance in an NBIRTH or DBIRTH has all members of its definition | tck-id-payloads-template-instance-members-birth | Y | Edge and host. A rebirth after data with some members keeps the others (`withCurrent`) |
| An instance in NDATA or DDATA may have a subset | tck-id-payloads-template-instance-members-data | Y | |
| An instance may have parameters of its definition | tck-id-payloads-template-instance-parameters | Y | A parameter the definition lacks is refused |
| version, if present, is UTF-8 | tck-id-payloads-template-version | Y | Sparkplug gives the version no matching rule: an instance is matched to its definition by template_ref only, whatever the versions |
| Parameter name in every parameter definition, UTF-8 | tck-id-payloads-template-parameter-name-required, tck-id-payloads-template-parameter-name-type | Y | Name required in definitions; UTF-8 everywhere |
| Parameter type: a basic datatype, uint32; required in definitions | tck-id-payloads-template-parameter-value-type, tck-id-payloads-template-parameter-type-value, tck-id-payloads-template-parameter-type-req | Y | Int8 to Text; `NewTemplateParameter` refuses others with `ErrUnknownType` |
| Parameter value: uint32, uint64, float, double, bool or string | tck-id-payloads-template-parameter-value | Y | Protobuf oneof; integers per [Integers](../INTEROPERABILITY.md#integers) |

### DataSets

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| num_of_columns: uint64, the column count | tck-id-payloads-dataset-column-size | Y | `NewDataSet` sets it |
| columns and types have the same count | tck-id-payloads-dataset-column-num-headers, tck-id-payloads-dataset-types-num | Y | `NewDataSet` and `GetDataSetValues` return `ErrColumnMismatch` |
| types: uint32 basic datatypes, always present | tck-id-payloads-dataset-types-def, tck-id-payloads-dataset-types-type, tck-id-payloads-dataset-types-value, tck-id-payloads-dataset-parameter-type-req | Y | `NewDataSet` refuses a type outside Int8 to Text with `ErrUnknownType`, also without rows |
| DataSetValue: uint32, uint64, float, double, bool or string | tck-id-payloads-template-dataset-value | Y | payload/dataset.go |

### PropertySets

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| keys and values have the same count | tck-id-payloads-propertyset-keys-array-size, tck-id-payloads-propertyset-values-array-size | Y | `SetQuality` keeps them paired; a PropertySet built by the caller is the caller's |
| PropertyValue type: uint32, a basic datatype or PropertySet/PropertySetList; required in births | tck-id-payloads-metric-propertyvalue-type-type, tck-id-payloads-metric-propertyvalue-type-value, tck-id-payloads-metric-propertyvalue-type-req | Y | `NewPropertyValue` sets it |
| PropertySetList: repeated PropertySet | — | Y | Proto fields available |
| Quality: type Int32 (3), int_value | tck-id-payloads-propertyset-quality-value-type | Y | `SetQuality` |
| Quality: one of 0, 192, 500 | tck-id-payloads-propertyset-quality-value-value | P | `SetQuality` takes any int32: the value is the caller's |

---

## High Availability (Chapter 8)

| Requirement | Status | Notes |
|------------|--------|-------|
| Multiple MQTT server support (Edge) | Y | ServerURLs + walkServers() on primary host offline |
| Multiple MQTT server support (Host) | P | ServerURLs config, single-broker connect |
| Reconnection with bdSeq increment | Y | |

---

## Conformance Profiles (Chapter 10)

### Sparkplug Edge Node (mandatory)

| Group | Status |
|-------|--------|
| Session Establishment | Y |
| Session Termination | Y |
| Send Data | Y |
| Receive Command | Y |
| Primary Host | Y |
| Multiple Broker | Y |

### Sparkplug Edge Node (optional)

| Group | Status |
|-------|--------|
| Aliases | Y |
| DataSets | Y |
| Properties | Y |
| Templates | Y |
| Complex Data (SendComplexData) | Y |

Complex Data is the TCK's `SendComplexDataTest`. It checks the DataSet,
Template and PropertySet requirements above, together with the requirements
on every edge node message it sees: a seq, timestamps and names, datatypes in
births. Its template checks are payload-level; the TCK checks that an instance
refers to a definition of the NBIRTH in `SendDataTest`
(tck-id-payloads-template-definition-nbirth) and in its monitor
(tck-id-topics-nbirth-templates). Y means implemented: the TCK has not been
run. Three of the requirements it checks depend on the metrics the caller
gives the edge node, which publishes them as they are: NDATA and DDATA with
aliases carry the alias only (tck-id-payloads-alias-data-cmd-requirement,
`payload.NewAliasMetric`), data metrics without a datatype
(tck-id-payloads-metric-datatype-not-req, a SHOULD), and quality values of 0,
192 or 500.

### Sparkplug Host Application (mandatory)

| Group | Status |
|-------|--------|
| Session Establishment | Y |
| Session Termination | Y |
| Send Command | Y |
| Edge Session Termination | Y |
| Multiple Broker | P |

### Sparkplug Host Application (optional)

| Group | Status |
|-------|--------|
| Message Reordering | Y |

### Sparkplug Application (non-host consumer)

| Group | Status |
|-------|--------|
| Subscribe to spBv1.0/# | Y |
| Track Node/Device state | Y |
| Alias resolution | Y |
| Sequence validation | Y |
| Send NCMD/DCMD | Y |
| No STATE birth/death | Y |
