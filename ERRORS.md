# Errors

Errors are wrapped with the operation (`sparkplugb: connect: ...`): test them
with `errors.Is`. Error text is for people and may change. Each package has its
own sentinels; `ErrClosed` and `ErrNotConnected` of `edge`, `host` and `app` are
distinct values.

## edge

| Error | Returned when |
|---|---|
| `ErrClosed` | `Connect` is called on a closed node |
| `ErrNotConnected` | `PublishBirth` or `Rebirth` is called when the node is not online |
| `ErrNoBirth` | Node data or a device birth, data or death comes before the node's birth |
| `ErrDuplicateID` | `PublishBirth` or `PublishDeviceBirth` uses an alias twice, or one of another part of the edge node |
| `payload.ErrUnknownTemplate` | A template instance in `PublishBirth`, `PublishDeviceBirth`, `PublishNodeData` or `PublishDeviceData` refers to a definition that the NBIRTH does not have |
| `payload.ErrInvalidTemplate` | A template in one of those breaks another rule of Sparkplug for templates, for one a definition outside the NBIRTH ([payload](#payload)) |
| `ErrInvalidState` | Deprecated. Not returned: an operation in the wrong state returns `ErrNotConnected`, `ErrNoBirth` or `ErrClosed` |

## host

| Error | Returned when |
|---|---|
| `ErrClosed` | `Connect` is called on a closed host application |
| `ErrNotConnected` | An NCMD or DCMD is sent without a connection |

A received template that breaks Sparkplug is not an error: `host` logs a warning
with the `payload` error, and applies and delivers the message.

## app

| Error | Returned when |
|---|---|
| `ErrClosed` | `Connect` is called on a closed application |
| `ErrNotConnected` | An NCMD or DCMD is sent without a connection |

Received templates: as `host`.

## mqtt

| Error | Returned when |
|---|---|
| `ErrConnectionLost` | Passed to `ConnectOpts.OnConnectionLost` when the connection ends: a broker drop, a client-ID takeover, or `Disconnect` on the transport. A custom `Transport` must wrap it |

## payload

| Error | Returned when |
|---|---|
| `ErrNilMetric` | The metric is nil |
| `ErrTypeMismatch` | The value does not have the datatype: on `SetValue`, `NewDataSet`, `NewTemplateParameter` and `NewPropertyValue` its Go type, a wire value out of range, or a `time.Time` before the epoch; on `GetValue`, `GetDataSetValues`, `GetTemplateParameterValue` and `GetPropertyValue` an integer that is out of the datatype's range or another kind of value ([Integers](INTEROPERABILITY.md#integers)) |
| `ErrUnknownType` | The datatype is not one the codec knows; `NewDataSet` with a column type that is not basic (Int8 to Text) |
| `ErrInvalidArray` | Array bytes are not a valid encoding of the datatype |
| `ErrArrayTooLarge` | A BooleanArray or StringArray has more than `MaxArrayElements` elements |
| `ErrStringTooLarge` | `GetValue` meets a string or StringArray element above `MaxStringBytes` |
| `ErrColumnMismatch` | A DataSet's columns, types or rows disagree in count; `GetDataSetValues` meets a row with more cells than the DataSet has types |
| `ErrUnknownTemplate` | `NewTemplateDefs` or `TemplateDefs.Check` meets a template instance whose `template_ref` names no definition of the NBIRTH |
| `ErrInvalidTemplate` | `NewTemplateDefs` or `TemplateDefs.Check` meets a template that breaks another rule of Sparkplug 3.0.0 6.4.13–6.4.14: a definition outside the NBIRTH, with a `template_ref`, without a name or twice; an instance without `is_definition` or `template_ref`; a member or parameter its definition lacks; an instance in a birth without every member of its definition; a version, `template_ref` or parameter name that is not UTF-8; a parameter of a definition without name or type, or of a type that is not basic |

## topic

| Error | Returned when |
|---|---|
| `ErrInvalidTopic` | The topic does not have the structure of the namespace |
| `ErrInvalidNamespace` | The topic is not under `spBv1.0` |
| `ErrInvalidMessageType` | The message type is not a Sparkplug one |
| `ErrInvalidID` | A group, edge node or device ID is empty or contains `/`, `+` or `#` |

## Errors without a sentinel

`New` of `edge`, `host` and `app` returns an unexported error for missing
required configuration. `mqtt.PahoTransport` returns unexported errors for
timeouts and for an operation without a connection (`sparkplugb: connect timeout`,
`publish timeout`, `subscribe timeout`, `not connected`).
