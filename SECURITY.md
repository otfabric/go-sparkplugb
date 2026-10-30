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
the JSON of STATE messages. `payload.Decode` and `topic.Parse` have fuzz
targets (`make fuzz`).

## Secure defaults

- The transport is chosen by the scheme of the broker URL. `tcp://` is plaintext:
  use `ssl://` outside a closed network.
- `mqtt.PahoTransport.TLSConfig` is passed to Paho as given. The library never
  sets `InsecureSkipVerify`.
- The runtimes log nothing unless a `Logger` is set.
- `edge`, `host` and `app` do not set `ConnectOpts.Username` or
  `ConnectOpts.Password`: see [LIMITATIONS.md](LIMITATIONS.md).

## Resource limits

| Limit | Default | Option |
|---|---|---|
| Elements of a BooleanArray | `payload.MaxArrayElements` (1 << 20) | none |
| Messages held per edge node for reordering (`host`, `app`) | 256, at most 30 seconds | none |
| Pending rebirth requests per node (`host`, `app`) | one, for the next NBIRTH or 2 seconds | none |
| Connect timeout (`PahoTransport`) | 10 s | `ConnectTimeout` |
| Publish and subscribe timeout (`PahoTransport`) | 5 s | `PublishTimeout` |

## Known security limitations

- No library limit on the size of a received payload: `payload.Decode` takes what
  the broker delivers, so bound the message size at the broker. `payload.MaxStringBytes`
  is exported but not enforced.
- `edge`, `host` and `app` keep no broker credentials of their own.
