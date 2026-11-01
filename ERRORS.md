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
| `ErrInvalidState` | Deprecated. Not returned: an operation in the wrong state returns `ErrNotConnected`, `ErrNoBirth` or `ErrClosed` |

## host

| Error | Returned when |
|---|---|
| `ErrClosed` | `Connect` is called on a closed host application |
| `ErrNotConnected` | An NCMD or DCMD is sent without a connection |

## app

| Error | Returned when |
|---|---|
| `ErrClosed` | `Connect` is called on a closed application |
| `ErrNotConnected` | An NCMD or DCMD is sent without a connection |

## mqtt

| Error | Returned when |
|---|---|
| `ErrConnectionLost` | Passed to `ConnectOpts.OnConnectionLost` when the connection ends: a broker drop, a client-ID takeover, or `Disconnect` on the transport. A custom `Transport` must wrap it |

## payload

| Error | Returned when |
|---|---|
| `ErrNilMetric` | The metric is nil |
| `ErrTypeMismatch` | The value does not have the datatype: on `SetValue`, `NewDataSet`, `NewTemplateParameter` and `NewPropertyValue` its Go type, a wire value out of range, or a `time.Time` before the epoch; on `GetValue`, `GetDataSetValues`, `GetTemplateParameterValue` and `GetPropertyValue` an integer that is out of the datatype's range or another kind of value ([Integers](INTEROPERABILITY.md#integers)) |
| `ErrUnknownType` | The datatype is not one the codec knows |
| `ErrInvalidArray` | Array bytes are not a valid encoding of the datatype |
| `ErrArrayTooLarge` | A BooleanArray or StringArray has more than `MaxArrayElements` elements |
| `ErrStringTooLarge` | `GetValue` meets a string or StringArray element above `MaxStringBytes` |
| `ErrColumnMismatch` | A DataSet's columns, types or rows disagree in count; `GetDataSetValues` meets a row with more cells than the DataSet has types |

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
