# Contributing to go-sparkplugb

## Development setup

- Go 1.26 or later (see [go.mod](go.mod))
- [staticcheck](https://staticcheck.dev/), [golangci-lint](https://golangci-lint.run/) and [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) for the checks
- Docker, for `make interop` only
- protoc and protoc-gen-go, for `make generate` only, at the versions in the `generated` job of [.github/workflows/ci.yaml](.github/workflows/ci.yaml)

```sh
make check           # format, lint, vet, vulnerability scan, tests with -race, coverage gate (90 %)
make test            # the library tests with the race detector
make stress          # the tests shuffled, twice, with the race detector
make fuzz            # every fuzz target (FUZZTIME=30s each by default)
make e2e             # the runtimes over Paho against a fake broker
make bench           # benchmarks
make generate-check  # fail if regenerating proto/ changes the committed code
make interop         # against the sparkplugb-interop images (the interop/ module; needs Docker)
make help            # every target
```

## Pull requests

- Work on a branch and open a PR against `main`. CI must be green.
- Add tests for new behaviour, including error paths, and a fuzz target for a new decoder of untrusted input.
- Do not edit `proto/sparkplug_b.proto`, which is the pinned Eclipse Tahu file, or `proto/sparkplug_b.pb.go`, which is generated. See [docs/LICENSING.md](docs/LICENSING.md): `proto/` is EPL-2.0, the rest of the module MIT.
- Update the document that owns the topic: [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for a requirement, [INTEROPERABILITY.md](INTEROPERABILITY.md) for behaviour where the specification leaves room and for what was tested against what, [ERRORS.md](ERRORS.md) for errors, [LIMITATIONS.md](LIMITATIONS.md) for gaps, and add an entry to [RELEASE.md](RELEASE.md).
- Claim interoperability only from passing runs against the pinned references.

## Security

Report vulnerabilities as [SECURITY.md](SECURITY.md) says, not in a public issue.
