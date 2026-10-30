# Testing

| Command | What it runs |
|---|---|
| `make test` | Library tests with `-race`; needs no broker |
| `make stress` | The same, shuffled, twice |
| `make coverage` / `make coverage-gate` | Coverage over `COVER_PKGS`; fails below `COVER_MIN` (90) |
| `make fuzz` | `FuzzDecode`, `FuzzDecodeBooleanArray`, `FuzzDecodeStringArray`, `FuzzParse`, for `FUZZTIME` each (30s) |
| `make bench` | Benchmarks |
| `make e2e` | `TestE2E*`: the three runtimes over `PahoTransport` against a fake broker on loopback TCP |
| `make interop` | The `interop/` module against the sparkplugb-interop images and four brokers (Docker) |
| `make generate-check` | Fails if regenerating `proto/` changes the committed code |
| `make check` | Format, lint, vet, vulnerability scan, tests, coverage gate |

## Levels

- **Unit:** `edge`, `host` and `app` tests drive the runtimes through a `mockTransport`, which records `ConnectOpts` and publishes.
- **Fake broker:** `mqtt/e2e_test.go` holds a broker of about a hundred lines that answers CONNECT, SUBSCRIBE, PUBLISH and PINGREQ. It checks the login on the wire, not MQTT behaviour.
- **Interop:** real brokers and six independent stacks, in the separate `interop/` module: [INTEROPERABILITY.md](INTEROPERABILITY.md).

## Coverage

`proto/` (generated) and `examples/` are excluded; the `coverpkg` of `.github/workflows/ci.yaml` matches `COVER_PKGS` of the Makefile. `make coverage` shows the current figure.

## Fuzz regressions

Keep a failing input as a `testdata/fuzz/<Target>/` file next to the target and fix the code.
