# Testing

| Command | What it runs |
|---|---|
| `make test` | Library tests with `-race`; needs no broker |
| `make stress` | The same, shuffled, twice |
| `make coverage` / `make coverage-gate` | Coverage over `COVER_PKGS`; fails below `COVER_MIN` (90) |
| `make fuzz` | Every `Fuzz*` target of the library packages, found with `go test -list '^Fuzz'`, for `FUZZTIME` each (30s) |
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

## Time and events

New tests wait for events, not for time: a gate in a test transport that reports when a call arrived and holds it until the test opens it, a channel the transport sends its publishes and subscriptions to, a `Metrics` that reports each connect. A bound far above the time the event takes (10 s) turns a missed event into a failure, not a test that never ends. Tests of what time does, the age of held messages and the sweep, run in a `testing/synctest` bubble (`host/bounds_test.go`, `app/bounds_test.go`).

## Fuzz targets and budgets

| Target | Input | Invariant |
|---|---|---|
| `FuzzDecode` | A payload | `Decode(Encode(p))` equals `p`; every getter of a metric, its properties, its template parameters and its DataSet returns a documented error or the documented Go type |
| `FuzzDataSetValues`, `FuzzTemplateParameterValue`, `FuzzPropertyValue` | A DataSet, a template parameter, a PropertyValue | as `FuzzDecode`, for that getter |
| `FuzzTemplateDefs` | A payload | `NewTemplateDefs` and `TemplateDefs.Check` return nil, `ErrInvalidTemplate` or `ErrUnknownTemplate` |
| `FuzzDecodeSignedInt`, `FuzzIntegerPaths`, `FuzzDecodeBooleanArray`, `FuzzDecodeStringArray` | Wire values, array bytes | The integer rule on every path, the array round trips |
| `FuzzParse` | A topic | Accepted topics are valid UTF-8, with IDs without `/`, `+` or `#`, and round-trip |
| `FuzzEdgeMessages` | The STATE, an NCMD or a DCMD of an edge node | A STATE brings the node online exactly when it is JSON that says online; a command reaches its handler exactly when it decodes |
| `FuzzHostMessages`, `FuzzApplicationMessages` | A message on every topic type of a known edge node | The runtime stays connected, the node stays known, the held messages stay within their bounds |

Allocation budgets: `TestDecodeLimitsAllocationBudget` (`MaxArrayElements`, `MaxStringBytes`, fixed-width arrays), `TestTemplateChecksAllocationBudget` and `TestTemplateNestingDepth` (the nesting depth of 32), `TestHostNBIRTHWithDeepTemplates` and its `app` twin (a received NBIRTH nested 1000 deep), `TestHostHeldMessagesBoundedOverAllNodes` and its `app` twin (the held messages: 256 per node, 4096 and 8 MiB over all nodes).

## Lifecycle matrix

The long-lived types: `edge.Node`, `host.App`, `app.Application` and `mqtt.PahoTransport`. A row is covered by the tests named, n/a with the reason, or a gap.

| Row | `edge.Node` | `host.App` | `app.Application` | `mqtt.PahoTransport` |
|---|---|---|---|---|
| L1 Close twice | `TestCloseIsIdempotent` | `TestHostCloseIdempotent` | `TestApplicationCloseIdempotent` | `TestZeroValuePahoTransport` (`Disconnect` twice) |
| L2 Two goroutines call Close | `TestCloseFromTwoGoroutines` | `TestHostCloseFromTwoGoroutines` | `TestApplicationCloseFromTwoGoroutines` | gap: no test of two concurrent `Disconnect` calls |
| L3 Close during a blocked read | n/a: no reader of its own; message callbacks in flight may run after `Close` (not tested) | as `edge` | as `edge` | `TestOperationsEndWithTheConnection` |
| L4 Close during a write | `TestPublishAfterSessionEnded`, `TestNoBirthAfterNDEATH` | gap: no test of `Close` during a command | gap: as `host` | `TestPublishOnEndedContextKeepsItsPlace`, `TestPublishWithEndedContextIsNotSent` |
| L5 Close during the handshake | `TestCloseDuringConnect` | `TestHostCloseDuringConnect` | `TestApplicationCloseDuringConnect` | `TestDisconnectDuringConnect` |
| L6 Cancel a pending request | n/a: the context goes to the transport | n/a: as `edge` | n/a: as `edge` | `TestOperationsHonourContext`, `TestConnectThatEndsDoesNotConnectLater`, `TestConnectWithEndedContext` |
| L7 Response after the request timed out | n/a: no request/response correlation | n/a: as `edge` | n/a: as `edge` | n/a: Paho correlates |
| L8 Response after reconnect | `TestStaleConnectionGeneration` | n/a: a session is per edge node, not per connection | n/a: as `host` | `TestConnectThatEndsDoesNotConnectLater` (a CONNACK after the attempt ended) |
| L9 Callback calls a public method | `TestStateHandlerCallsNode`, `TestStateHandlerConnectsWhenDisconnected` | `TestHostStateHandlerCallsHost`, `TestHostStateHandlerConnectsWhenDisconnected` | `TestApplicationStateHandlerCallsApplication`, `TestApplicationStateHandlerConnectsWhenDisconnected` | n/a: `OnConnectionLost` is called without the transport's lock |
| L10 Callback calls Close | `TestStateHandlerCallsNode` ("Close when disconnected") | `TestHostStateHandlerCallsHost` | `TestApplicationStateHandlerCallsApplication` | n/a: as L9 |
| L11 Slow callback | gap: the head-of-line wait is documented (ARCHITECTURE.md), not tested | gap: as `edge` | gap: as `edge` | n/a |
| L12 Queue at capacity | n/a: no queue; a publish waits for the transport | `TestHostHeldMessagesBoundedOverAllNodes`, `TestHostRebirthRequestsBoundedOverAllNodes`, `TestHostForgetsSilentNodes`, `TestHostThrottlesSeqGapRebirths` | the `app` twins | n/a |
| L13 Concurrent sessions | n/a: one session | `TestHostSessionCollisionInSequence`, `TestHostConvergesOnReorderedSessions` | `TestApplicationSessionCollisionInSequence`, `TestApplicationNodeReplacement` | n/a: one connection |
| L14 Failure during construction or startup | `TestConnectFailure`, `TestConnectSetupFailureDisconnects`, `TestConnectBdSeqLoadError`, `TestBdSeqStoreThatDoesNotReturnNormally`, `TestNewRefusesInvalidIDs` | `TestHostConnectFailure`, `TestHostConnectSetupFailureDisconnects`, `TestHostNewRefusesInvalidHostID` | `TestApplicationConnectError`, `TestApplicationSubscribeFailureDisconnects` | `TestConnectThatEndsDoesNotConnectLater` |
| L15 `Shutdown(ctx)` | n/a: no graceful drain; `Close(ctx)` gives its context to the last publish and the disconnect | n/a: as `edge` | n/a: as `edge` | n/a |
| L16 Close racing reconnect | `TestCloseDuringWalk` | n/a: no reconnect of its own | n/a: as `host` | n/a: automatic reconnection is off |

`Close` does not wait for the goroutines its runtime started (auto-rebirth, server walking, rebirth requests) to end, and an operation after `Close` returns `ErrNotConnected` or `ErrNoBirth` ([ERRORS.md](ERRORS.md)).

## Fuzz regressions

Keep a failing input as a `testdata/fuzz/<Target>/` file next to the target and fix the code.

## When CI runs

| Event | Workflow | What runs |
|---|---|---|
| Pull request | `pr.yml` | `docs.yml` (docsync, otfabric-policy) always; `ci.yaml`, `interop.yml` unless every changed file is documentation; then `OT Fabric Required Checks` |
| Push to a branch | none | nothing; dispatch `ci.yaml`, `interop.yml` to test a branch without a pull request |
| Push to `main` | `docs.yml` | the light checks; the merged commit was tested as the pull request's merge commit |
| Schedule | `check-spec.yaml`, `interop.yml` | the scheduled runs, on `main` |
| Release | `release.yaml` | the release workflow tests the release commit again |

Documentation means an added or modified `*.md` file outside `.github/`, `testdata/` and `vendor/`, other than `AGENTS.md` and `CLAUDE.md`. A deleted or renamed document, or any other file, runs everything. `OT Fabric Required Checks` fails unless every job that applies succeeded; its summary lists them.
