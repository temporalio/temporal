# Umpire — Driver (the mechanics): spec & plan

> **Status: component reference; implemented for current Umpire workflows.** Concrete workflow
> and kitchensink drivers, action realizers, the generic `Drive` runtime, and the sparse regression
> harness are built. The remaining work is broader protocol adoption and new realizers, not a
> first concrete driver.

The Driver is Umpire's active **mechanics** — the arm, not the brain. The **Planner**
(see [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md)) decides *what* states to reach and plans
routes over the model; the Driver realizes each abstract route step as real traffic against
the running server, and injects faults. The **Monitor** ([`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md))
then judges the result. For the whole-system pitch read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md).

This document records the driver seam and reach discipline implemented by the current action and
sparse-regression runtimes. Some proposed realizers remain future work.

## The seam

A `Plan`'s routes are *abstract model events* (`"admit"`, `"accept"`, …). The Driver is the
single seam that turns each event into real traffic against the SUT:

```go
type Driver interface {
    Do(ctx context.Context, event string) error
}
```

A concrete driver is constructed for one target entity and maps events onto RPCs / worker
polls / fault injection:

```go
// Illustrative update-specific driver; current workflow and Nexus drivers use action realizers.
type updateDriver struct {
    client         workflowservice.WorkflowServiceClient
    wfID, updateID string
}

func (d *updateDriver) Do(ctx context.Context, event string) error {
    switch event {
    case "admit":    return d.startAndSendUpdate(ctx)
    case "accept":   return d.pollAndAccept(ctx)
    case "complete": return d.pollAndComplete(ctx)
    // ...
    }
    return fmt.Errorf("updateDriver: unhandled event %q", event)
}
```

That is the whole contract with the Planner: the Planner guarantees a route is legal and
reachable; the Driver only has to realize one event at a time.

## Design decisions

- **Events and facts are symmetric.** The Monitor has a `FactDecoder` that turns wire/spans
  **into** facts; the Driver is the inverse, turning a planned event **into** wire calls. They
  meet at the same `EntityPath` addressing and the same deterministic identifiers, so the event
  the Driver realizes and the fact it provokes name the same entity. Reuse `entity_key.go`.
- **Eventual-consistency waits are polled, never slept.** Realizing an event that depends on
  server state (poll until a task is dispatchable, then accept) polls a predicate over the
  Monitor's model rather than `time.Sleep`. Every action/result is labelled **strong** or
  **eventual**; eventual results are polled to their predicate. This is what keeps driven
  concurrency scenarios deterministic instead of flaky.
- **Fault injection rides the dormant hook that already exists.** The framework's
  `FaultInjector.Inject(ctx, info, request) error` and the interceptor's `inj` slot
  (`common/testing/umpire/interceptor.go`, `NewUnaryServerInterceptor`) are built and wired but
  no-op. The Driver is the first real `FaultInjector`: drop → return error; delay →
  sleep-then-proceed; corrupt → mutate request. Faults are just events with a grey-box reach.
- **Timing control is an event class of its own.** Remove client deadlines to hold a request
  open; trip an in-flight context deadline early; fire a timer task before it is due. These
  need paired client+server interceptors and are white/grey-box.
- **Prefer the SDK worker over a raw poller** where possible, hooking "before/after worker
  receives WFT" so one driver works with and without a real worker and can stub work that isn't
  built yet. Keeps black-box realizations honest.

## Capability-honest reach

Each event declares the **drive-capability** it needs, mirroring the observe-capability the
Monitor's facts carry (see *Environments & capabilities* in [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md)
and [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md)):

- **`rpcDrive`** — realized through the public frontend API / SDK
  (Start/Signal/Update/Poll/Describe/GetHistory). Runnable **anywhere**, including `cicd` and
  `canary`.
- **`faults`** — injection at internal RPC / persistence / timing seams. `local-*` only.
- **`directDrive`** — CHASM transitions called directly, no wire. `local-chasm` only.

A run realizes only events whose capability its environment grants, and **skips the rest
explicitly** — never a silently-dropped action. A `canary` run literally cannot schedule a
`faults` or `directDrive` event. (The Planner declares each route's required capabilities; the
Driver's per-environment realizer enforces them.)

## Determinism & replay

- **Deterministic & replayable.** Given the same seed and inputs, a run reproduces —
  buying flaky-test *verification* and reproducible bug repro. Requires seeded randomness and
  the stable/derived identifiers the Monitor already uses.
- **Separate run from eval.** A run records the events it realized + the Monitor's `FactLog`;
  the rulebook check can run inline *or* be replayed offline against the capture, so checks are
  re-runnable and tweakable without re-driving the server.
- **Known-bug dismissal.** A run can mark a specific expected violation as a known bug so an
  unrelated defect doesn't block a developer.

## Integration needs (the execution seams)

These already exist or are half-built in the Umpire code; the Driver consumes them.

1. **`FaultInjector` (the active hook).** `interceptor.go` already threads an `inj FaultInjector`
   through `NewUnaryServerInterceptor`; `tests/umpirev1`'s `NewUnaryServerInterceptor(u, inj)`
   accepts it and passes `nil` today. *Need:* a driver implementing `Inject`, passed where `nil`
   is now. **No framework change to start** — the cleanest entry point.
2. **A client handle.** RPC events need a frontend client (and, for worker-based realizations, an
   SDK worker hook). *Need:* the Driver owns/receives the same client the test would have used,
   per-namespace, matching the Monitor's namespace scoping so a run and its checks share a
   namespace.
3. **Namespace lifecycle.** Reuse `CheckNamespace` / `PurgeNamespace`: a run executes in a
   namespace, the Monitor checks it, then it's purged — coverage survives the purge. The Driver
   creates/owns that namespace per run.
4. **Timing interceptors (later).** Client+server interceptors for deadline/timer control don't
   exist yet — a genuine new seam, needed only for white-box timing events.

## Status & build order

- **Implemented:** `Driver`, `Plan.Run`, the generic action `Drive` runtime, Workflow and Nexus
  realizers, kitchensink drivers, footprint-derived fault scheduling, and the sparse regression
  harness.
- **Next extensions:** add realizers only when a protocol action gap or migrated regression needs
  one; keep deadline/timer control as explicit future work rather than a prerequisite for the
  existing driver.

## Open questions / risks

1. **Determinism under real concurrency.** Seeded choice is easy; a *reproducible* interleaving
   of injected faults against a live multi-goroutine server is the hard part. May need the fault
   seams to expose synchronization points ("hold request between points X and Y").
2. **Guard polling vs. event-driven.** When realizing an eventual-consistency event, does the
   Driver poll the `ModelState` or get notified on generation bumps? Polling is simplest and
   matches the existing generation watermark; a notify seam may be needed for tight timing.
3. **Overlap with existing pollers/testvars.** The Driver should *replace* the fragmented
   poller/testvars style, not become a fourth style beside them. Plan a migration, not an
   addition.
