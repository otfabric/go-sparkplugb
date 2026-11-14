# Security policy

## Reporting a vulnerability

Do not open a public issue. Email **security@otfabric.io**, or open a private
advisory on GitHub (<https://github.com/otfabric/go-sparkplugb/security/advisories/new>),
with a description, the steps to reproduce and the affected version.

## Supported versions

| Version | Supported |
|---|---|
| Latest release | Yes |
| Older releases | No |

## Threat model

Sparkplug B has no security of its own: payloads are neither signed nor
encrypted, and every client of a broker that may publish to `spBv1.0/#` can
issue NCMD and DCMD, forge births and deaths, or publish a STATE. Confidentiality,
authentication and authorization are those of the MQTT connection and the
broker's access control.

Anything received from the broker is untrusted: topics, Protobuf payloads and
the JSON of STATE messages. The decoders, the getters of `payload`, the
template checks, `topic.Parse` and the message handling of the three runtimes
have fuzz targets (`make fuzz`, [TESTING.md](TESTING.md#fuzz-targets-and-budgets)).
Application code that the runtimes call and that does not return normally is
logged and does not stop them ([API.md](API.md#callbacks)).

## Secure defaults

- The transport is chosen by the scheme of the broker URL. `tcp://` is plaintext:
  use `ssl://` outside a closed network.
- `mqtt.PahoTransport.TLSConfig` is passed to Paho as given. The library never
  sets `InsecureSkipVerify`.
- The runtimes log nothing unless a `Logger` is set.
- `Config.Username` and `Config.Password` of `edge`, `host` and `app` reach the
  transport only. They are never logged, and a `Config` printed by the application
  shows them, so do not print it. MQTT carries them in clear: use `ssl://`. Do not
  put them in a broker URL, which does get logged.

## Resource limits

| Limit | Default | Option |
|---|---|---|
| Elements of a BooleanArray or StringArray (`payload.GetValue`) | `payload.MaxArrayElements` (1 << 20) | none |
| Bytes of a string or StringArray element (`payload.GetValue`) | `payload.MaxStringBytes` (1 << 24) | none |
| Template nesting (`payload.NewTemplateDefs`, `TemplateDefs.Check`, and so `host` and `app` on receipt) | 32 levels: deeper is `ErrInvalidTemplate` | none |
| Messages held for reordering (`host`, `app`) | 256 per edge node; 4096, and 8 MiB of payload, over all edge nodes, the oldest dropped first; a message above 8 MiB is not held; every held message at most 30 seconds, of every edge node | none |
| Pending rebirth requests (`host`, `app`) | one per node, for the next NBIRTH or 2 seconds; 16 on their way over all nodes | none |
| Connect timeout (`PahoTransport`) | 10 s | `ConnectTimeout` |
| Publish and subscribe timeout (`PahoTransport`) | 5 s | `PublishTimeout` |

## Static analysis

Gosec and Opengrep scans run on a maintainer's machine before each release, with `cd ../engineering && make sast REPO=go-sparkplugb` and `sast/scan.sh scan go-sparkplugb --module-dir interop` for the interop module, from a checkout of otfabric/engineering beside this one ([ADR-0011](https://github.com/otfabric/engineering/blob/main/docs/adr/0011-shared-sast.md)), never in a GitHub workflow, so that a public CI log does not publish a finding before it is fixed. Findings are fixed in code: no `//nolint` or `#nosec`. The accepted findings, the integer conversions of the Sparkplug wire encodings (a signed integer carried in an unsigned field, the datatype in a `uint32`, each decoded value range-checked before it is narrowed) and `math/rand/v2` for reconnect jitter, are recorded in the organisation's private triage, not in this repository. A clean scan is not a proof that no weakness is left.

## Known security limitations

- No library limit on the size of a received payload: `payload.Decode` takes what
  the broker delivers, so bound the message size at the broker. The limits above apply
  afterwards, in `payload.GetValue`, and not to DataSet cells or template parameters.
- `MaxArrayElements` bounds the elements of a StringArray, not the memory of the
  decoded slice: each element takes a string header besides its bytes, and the
  elements are counted as they are decoded (`TestDecodeLimitsAllocationBudget`
  has the budget).
