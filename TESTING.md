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

New tests wait for events, not for time: a gate in a test transport that reports when a call arrived and holds it until the test opens it, a channel the transport sends its publishes and subscriptions to, a `Metrics` that reports each connect. A bound far above the time the event takes (10 s) turns a missed event into a failure, not a test that never ends. Tests of what time does, the age of held messages, the sweep, the reorder and session waits, the throttling of rebirth requests, run in a `testing/synctest` bubble (in `host` and `app`), and so do tests that wait for the goroutines of a runtime to finish: `synctest.Wait` returns once every other goroutine of the bubble is blocked, where a sleep guessed how long they take. `TestHostConvergesOnReorderedSessions` and its `app` twin run each seed in a bubble. A test that waits for an event outside a bubble polls with a deadline (`waitFor`, `eventually`, `waitLive`).

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

Allocation budgets: `TestDecodeLimitsAllocationBudget` (`MaxArrayElements`, `MaxStringBytes`, fixed-width arrays), `TestDecodeLimits` (every array datatype at and past `MaxArrayElements`) and `TestDecodeLimitsPropertyAndParameterStrings` (`MaxStringBytes` in `GetPropertyValue` and `GetTemplateParameterValue`), `TestTemplateChecksAllocationBudget` and `TestTemplateNestingDepth` (the nesting depth of 32), `TestTemplateBirthCheckFollowsInstance` (a birth check costs the instance's members, also against a definition that repeats a member name), `TestTemplateCheckAllocsIndependentOfDefinition` (`Check` allocates the same against a definition of 1 or 10000 members), `TestTemplateCheckDefinitionChanged` (a definition changed after `NewTemplateDefs`), `TestTemplateDeeperCopiesPath` (sibling error paths), `TestHostNBIRTHWithDeepTemplates` and its `app` twin (a received NBIRTH nested 1000 deep), `TestHostHeldMessagesBoundedOverAllNodes` and its `app` twin (the held messages: 256 per node, 4096 and 8 MiB over all nodes).

## Lifecycle matrix

The long-lived types: `edge.Node`, `host.App`, `app.Application` and `mqtt.PahoTransport`. A row is covered by the tests named, n/a with the reason, or a gap.

| Row | `edge.Node` | `host.App` | `app.Application` | `mqtt.PahoTransport` |
|---|---|---|---|---|
| L1 Close twice | `TestCloseIsIdempotent` | `TestHostCloseIdempotent` | `TestApplicationCloseIdempotent` | `TestZeroValuePahoTransport` (`Disconnect` twice) |
| L2 Two goroutines call Close | `TestCloseFromTwoGoroutines` | `TestHostCloseFromTwoGoroutines` | `TestApplicationCloseFromTwoGoroutines` | `TestDisconnectFromTwoGoroutines` |
| L3 Close during a blocked read | no reader of its own: `TestCommandHandlerRunningAtClose` (a command callback in flight at `Close`: `Close` returns, what it calls afterwards returns `ErrClosed`) | `TestHostCallbackRunningAtClose` (as `edge`, with `OnNodeData`) | `TestApplicationCallbackRunningAtClose` | `TestOperationsEndWithTheConnection` |
| L4 Close during a write | `TestPublishAfterSessionEnded`, `TestNoBirthAfterNDEATH`, `TestOperationsAfterClose` ("during Close"), `TestCloseEndsAPublishInFlight` (a publish waiting for the transport ends with `Close`, matching `ErrClosed` and the transport's error) | `TestHostCloseDuringCommand` (`Close` returns; a command the transport completes returns nil), `TestHostCloseEndsCommandsInFlight` (a command waiting for the transport ends with `Close`, matching `ErrClosed` and `mqtt.ErrConnectionLost` or the transport's error), `TestHostCommandsAfterClose` | `TestApplicationCloseDuringCommand`, `TestApplicationCloseEndsCommandsInFlight`, `TestApplicationCommandsAfterClose` | `TestPublishOnEndedContextKeepsItsPlace`, `TestPublishWithEndedContextIsNotSent` |
| L5 Close during the handshake | `TestCloseDuringConnect` | `TestHostCloseDuringConnect` | `TestApplicationCloseDuringConnect` | `TestDisconnectDuringConnect` |
| L6 Cancel a pending request | n/a: the context goes to the transport | n/a: as `edge` | n/a: as `edge` | `TestOperationsHonourContext`, `TestConnectThatEndsDoesNotConnectLater`, `TestConnectWithEndedContext` |
| L7 Response after the request timed out | n/a: no request/response correlation | n/a: as `edge` | n/a: as `edge` | n/a: Paho correlates |
| L8 Response after reconnect | `TestStaleConnectionGeneration` | n/a: a session is per edge node, not per connection | n/a: as `host` | `TestConnectThatEndsDoesNotConnectLater` (a CONNACK after the attempt ended) |
| L9 Callback calls a public method | `TestStateHandlerCallsNode`, `TestStateHandlerConnectsWhenDisconnected` | `TestHostStateHandlerCallsHost`, `TestHostStateHandlerConnectsWhenDisconnected` | `TestApplicationStateHandlerCallsApplication`, `TestApplicationStateHandlerConnectsWhenDisconnected` | n/a: `OnConnectionLost` is called without the transport's lock |
| L10 Callback calls Close | `TestStateHandlerCallsNode` ("Close when disconnected"), `TestCloseFromTheWalk` (`OnState`, `Metrics.OnConnect`, `RetryPolicy` during a walk), `TestCloseFromOnPublish` (`Metrics.OnPublish` during a rebirth) | `TestHostStateHandlerCallsHost` | `TestApplicationStateHandlerCallsApplication` | n/a: as L9 |
| L11 Slow callback | `TestSlowCommandHandler`: the node's state, data and `Rebirth` go on; the next message waits (ARCHITECTURE.md) | `TestHostSlowCallback`: as `edge`, with `State`, `GetNode`, `Nodes` and a command | `TestApplicationSlowCallback` | n/a |
| L12 Queue at capacity | n/a: no queue; a publish waits for the transport | `TestHostHeldMessagesBoundedOverAllNodes`, `TestHostRebirthRequestsBoundedOverAllNodes`, `TestHostForgetsSilentNodes`, `TestHostThrottlesSeqGapRebirths` | the `app` twins | n/a |
| L13 Concurrent sessions | n/a: one session | `TestHostSessionCollisionInSequence`, `TestHostConvergesOnReorderedSessions` | `TestApplicationSessionCollisionInSequence`, `TestApplicationNodeReplacement` | n/a: one connection |
| L14 Failure during construction or startup | `TestConnectFailure`, `TestConnectSetupFailureDisconnects`, `TestConnectBdSeqLoadError`, `TestBdSeqStoreThatDoesNotReturnNormally`, `TestNewRefusesInvalidIDs` | `TestHostConnectFailure`, `TestHostConnectSetupFailureDisconnects`, `TestHostNewRefusesInvalidHostID` | `TestApplicationConnectError`, `TestApplicationSubscribeFailureDisconnects` | `TestConnectThatEndsDoesNotConnectLater` |
| L15 `Shutdown(ctx)` | n/a: no graceful drain; `Close(ctx)` gives its context to the last publish and the disconnect | n/a: as `edge` | n/a: as `edge` | n/a |
| L16 Close racing reconnect | `TestCloseDuringWalk` (`Close` returns once the walk ended) | n/a: no reconnect of its own | n/a: as `host` | n/a: automatic reconnection is off |

`Close` returns once the goroutines and timers its runtime started have ended: `TestCloseEndsTheRebirth` (an auto-rebirth waiting for the broker), `TestCloseDuringWalk`, `TestHostCloseEndsRebirthRequests`, `TestHostCloseStopsTimersArmedAfterConnectionLost` and their `app` twins, `internal/routine` for the group they are kept in. An operation once `Close` was called returns an error that matches `ErrClosed` and the error it returned before, `ErrNotConnected` or `ErrNoBirth` ([ERRORS.md](ERRORS.md)): `TestOperationsAfterClose`, `TestHostCommandsAfterClose`, `TestApplicationCommandsAfterClose`. An operation on its way to the transport when `Close` is called ends with it, with an error that matches `ErrClosed` and the transport's: `TestCloseEndsAPublishInFlight`, `TestHostCloseEndsCommandsInFlight`, `TestApplicationCloseEndsCommandsInFlight`. `goleak` checks at the end of each package that no goroutine is left.

## Fuzz regressions

Keep a failing input as a `testdata/fuzz/<Target>/` file next to the target and fix the code.

## When CI runs

| Event | Workflow | What runs |
|---|---|---|
| Pull request | `pr.yml` | `docs.yml` (docsync, otfabric-policy) always; `ci.yaml`, `interop.yml` unless every changed file is documentation; then `OT Fabric Required Checks` |
| Push to a branch | none | nothing; dispatch `ci.yaml`, `interop.yml` to test a branch without a pull request |
| Push to `main` | `docs.yml` | the light checks; the merged commit was tested as the pull request's merge commit |
| Schedule | `check-spec.yaml`, `interop.yml` | the scheduled runs, on `main`: `interop.yml` nightly; `check-spec.yaml` weekly, the check of the upstream proto and the tests on a 32-bit architecture (job `test-386`, `GOARCH=386 go test -count=1 ./...`) |
| Release | `release.yaml` | the release workflow tests the release commit again |

The tiers are those of otfabric/engineering `standards/testing.md#ci-tiers`. Pull requests run the fast gate and the interop suite; the 32-bit run is in the scheduled tier, not on pull requests: dispatch `check-spec.yaml` to run it on a branch. It runs without `-race`, which `386` does not support.

Documentation means an added or modified `*.md` file outside `.github/`, `testdata/` and `vendor/`, other than `AGENTS.md` and `CLAUDE.md`. A deleted or renamed document, or any other file, runs everything. `OT Fabric Required Checks` fails unless every job that applies succeeded; its summary lists them.
