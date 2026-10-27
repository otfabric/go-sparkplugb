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
| seq increments for each message | tck-id-message-flow-edge-node-data-publish-seq | Y | |
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
| On seq gap: request rebirth via NCMD | tck-id-host-topic-phid-seq-rebirth | Y | |
| Mark metrics STALE on NDEATH/DDEATH | tck-id-host-topic-phid-stale-metrics | Y | Stale flag on EdgeNodeState/DeviceState |
| Message reordering timeout (optional) | tck-id-host-topic-phid-reorder | N | Config placeholder only |

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
| Alias unique across entire Edge Node | — | Y | Edge alias map, Host alias map |
| DataType field set on every metric | — | Y | payload package |
| Timestamp on every metric | — | Y | NewMetric/StampMetrics sets per-metric ts |
| is_null for null values | — | Y | payload package |
| is_historical for historical data | — | Y | Proto field available |
| is_transient for non-stored data | — | Y | Proto field available |

### Signed Integer Types

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Int8 in uint32 field, sign-extend correctly | — | Y | payload package |
| Int16 in uint32 field, sign-extend correctly | — | Y | payload package |
| Int32 in uint32 field, interpret as signed | — | Y | payload package |
| Int64 in uint64 field, interpret as signed | — | Y | payload package |

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

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| Template definition: is_definition=true, version | — | N | optional |
| Template instance: template_ref set | — | N | optional |
| Parameters on instances | — | N | optional |

### DataSets

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| num_of_columns, columns, types, rows | — | N | optional |
| DataSetValue typed per column | — | N | optional |

### PropertySets

| Requirement | TCK ID | Status | Notes |
|------------|--------|--------|-------|
| PropertySet: keys[] + values[] | — | Y | Proto fields available |
| PropertySetList: repeated PropertySet | — | Y | Proto fields available |
| PropertyValue type + is_null | — | Y | Proto fields available |

---

## High Availability (Chapter 8)

| Requirement | Status | Notes |
|------------|--------|-------|
| Multiple MQTT server support (Edge) | Y | ServerURLs + walkServers() on primary host offline |
| Multiple MQTT server support (Host) | N | Per-server STATE |
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
| DataSets | N |
| Properties | Y |
| Templates | N |
| Complex Data (SendComplexData) | N |

### Sparkplug Host Application (mandatory)

| Group | Status |
|-------|--------|
| Session Establishment | Y |
| Session Termination | Y |
| Send Command | Y |
| Edge Session Termination | Y |
| Multiple Broker | N |

### Sparkplug Host Application (optional)

| Group | Status |
|-------|--------|
| Message Reordering | N |
