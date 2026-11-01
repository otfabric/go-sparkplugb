# Testing

| Command | What it runs |
|---|---|
| `make test` | Library tests with `-race`; needs no broker |
| `make stress` | The same, shuffled, twice |
| `make coverage` / `make coverage-gate` | Coverage over `COVER_PKGS`; fails below `COVER_MIN` (90) |
| `make fuzz` | `FuzzDecode`, `FuzzDecodeSignedInt`, `FuzzIntegerPaths`, `FuzzDecodeBooleanArray`, `FuzzDecodeStringArray`, `FuzzParse`, for `FUZZTIME` each (30s) |
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

`proto/` (generated) and `examples/` are excluded; the `coverpkg` of `.github/workflows/ci.yaml` matches `COVER_PKGS` of the Makefile. `make coverage` shows the current figure. `make coverage-pkg-check` requires `COVER_PKG_MIN` (80) of each package of `COVER_PKGS`; `mqtt` is the lowest, at 85 %.

## Prerequisites

The interop suite (build tag `interop`) skips without Docker, or fails with `SPARKPLUGB_REQUIRE_INTEROP=1`, which `make interop` sets.

## Leaks

`edge`, `host` and `app` start goroutines: their tests end with `goleak.VerifyTestMain` (test-only dependency).

## Fuzz regressions

Keep a failing input as a `testdata/fuzz/<Target>/` file next to the target and fix the code.
