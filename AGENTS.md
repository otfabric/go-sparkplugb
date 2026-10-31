# AGENTS.md — go-sparkplugb

## What this is

Eclipse Sparkplug B 3.0.0 library for Go: Edge Node, Host Application and Application runtimes, topic namespace, Protobuf codecs and typed metrics, over MQTT 3.1.1 through Eclipse Paho or any `mqtt.Transport`. Profile: **protocol-library**. Nothing is certified, and the Sparkplug TCK has not been run.

## Organisation standards

This repository follows **otfabric/engineering policy 1.2**, profile **protocol-library** (`.otfabric/policy.yaml`, mode `enforce`). Where this file and the organisation standards disagree, this file wins here: say so in your report.

## Where the truth lives

| Question | Source |
|---|---|
| Wire behaviour | Sparkplug B 3.0.0; requirement by requirement in docs/CONFORMANCE.md |
| Where the specification leaves room, and what is verified against which stack | INTEROPERABILITY.md (sparkplugb-interop images pinned in `interop/images.go`) |
| Scope not implemented, what is not verified | LIMITATIONS.md |
| Licensing of `proto/` (EPL-2.0) and of the rest (MIT) | docs/LICENSING.md |
| Errors | ERRORS.md |
| Release history | RELEASE.md (`## vX.Y.Z` … `---`) |

## Hard rules

1. Never lower `COVER_MIN` (90) or `coverage-min` in `.github/workflows/ci.yaml`, add lint suppressions, or skip or delete tests to make CI pass.
2. `proto/sparkplug_b.proto` is the upstream Eclipse Tahu file, pinned by SHA-256 (docs/LICENSING.md, `.github/workflows/check-spec.yaml`); `proto/sparkplug_b.pb.go` is generated from it. Regenerate with the tool versions pinned in the `generated` CI job.
3. Never invent protocol semantics: cite the specification, or record the reading in INTEROPERABILITY.md.
4. Interop claims only from passing runs against the pinned sparkplugb-interop release.
5. No new runtime dependency without an ADR.
6. No incidental breaking changes to exported API; this library stays on v0 or v1.

## Commands

| When you change… | Run |
|---|---|
| anything | `make check` |
| concurrency or lifecycle | `make stress` |
| a decoder | `make fuzz` |
| `proto/` | `make generate-check` |
| interop-relevant behaviour | `make interop` (Docker; the `interop/` module) |

## Definition of Done

`make check` green; tests for new behaviour including error paths; a fuzz target for a new decoder of untrusted input; docs updated in the same change, docs/CONFORMANCE.md for a requirement's status; ERRORS.md for an exported error; RELEASE.md section for user-visible changes.

## Git and releases

Branches and PRs. Do not push to `main`, tag or run the Release workflow unless the maintainer asks in the session.
