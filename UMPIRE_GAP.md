# Umpire completion audit

Assessment date: 2026-08-15.

## Outcome

The bounded Umpire milestone is implemented. One compiled behavioral protocol now feeds runtime
monitoring, planning, sparse regressions, semantic coverage, model-family projection, and generated
verification. The framework also provides an evidence-qualified discovery-to-regression campaign,
portable environment profiles, and a guarded canary controller.

This is a bounded assurance platform, not a promise of exhaustive verification or deterministic
distributed scheduling. Deployment and canary drivers remain explicitly supplied by the caller;
Umpire does not embed production credentials, silently enable faults, or infer authority from a
local test environment.

## Delivered slices

### Trustworthy sparse regressions

- [x] Validate every completed suite before compilation succeeds.
- [x] Report missing action, policy, and resource realizations as structured compile errors.
- [x] Validate resource dependency chains and cycles before environment allocation.
- [x] Preserve completed semantic plans, grounded identities, observations, facts, qualified
  verdicts, and environment profiles for replay.

Authoritative implementation and tests:

- [`compiler.go`](./common/testing/umpire/regress/compiler.go) and
  [`realization.go`](./common/testing/umpire/regress/realization.go);
- [`executor.go`](./common/testing/umpire/regress/executor.go) and
  [`artifact.go`](./common/testing/umpire/regress/artifact.go);
- [`compiler_test.go`](./common/testing/umpire/regress/compiler_test.go),
  [`artifact_test.go`](./common/testing/umpire/regress/artifact_test.go), and the defensive live
  preflight in [`regression_preflight.go`](./tests/umpire2/action/regression_preflight.go).

### Generated verification trust

- [x] Lower canonical lifecycle, cross-entity, speculative Workflow Task, Nexus closure/timeout,
  routing, backlog, Workflow delivery, Activity delivery, and Callback properties.
- [x] Enforce unsupported progress and backend semantics without reporting success.
- [x] Normalize native counterexamples, recover action bindings and deltas, and replay them through
  the canonical Go interpreter.
- [x] Classify timeout, interruption, depth, state, step, memory, schedule, and tool limits as
  inconclusive.
- [x] Expand backend equivalence and mutation testing.
- [x] Keep tool versions, artifact coordinates, and checksums in one Go-owned manifest and generate
  CI installation metadata from it.

The semantic mutation corpus covers missing guards, missing frame clauses, reversed cardinality,
omitted choices, identity reuse, weakened properties, and incorrect refinements. Supporting
backends must expose the mutation or fail closed during normalized replay. TLC and FizzBee expose a
comparable finite state count; Apalache, P/PEx, and Ivy explicitly record why their result form is
not a reachable-state count instead of treating a clean run as equivalence.

Authoritative implementation and tests:

- [`model.go`](./common/testing/umpire/verify/model.go),
  [`family.go`](./common/testing/umpire/verify/family.go), and
  [`project.go`](./common/testing/umpire/verify/project.go);
- [`counterexample.go`](./common/testing/umpire/verify/counterexample.go),
  [`result.go`](./common/testing/umpire/verify/result.go), and
  [`runner`](./common/testing/umpire/verify/runner);
- deterministic generators for [TLA+](./common/testing/umpire/verify/tla),
  [P](./common/testing/umpire/verify/p), [Ivy](./common/testing/umpire/verify/ivy), and
  [FizzBee](./common/testing/umpire/verify/fizz);
- the backend equivalence and mutation gates in
  [`main_test.go`](./cmd/umpire-genmodels/main_test.go);
- the generated [target index](./tests/umpire2/genmodels/manifest.json) and pinned
  [`tools.env`](./tests/umpire2/genmodels/tools.env).

### Model breadth and boundaries

The model family generates twelve owned targets:

1. `protocol-atomic`;
2. `foundation-delivery-safety`;
3. `integration-workflow-delivery`;
4. `integration-activity-delivery`;
5. `feature-workflow-speculative-delivery`;
6. `feature-nexus`;
7. `integration-nexus-activity`;
8. `integration-callback-nexus`;
9. `integration-callback-workflow`;
10. `foundation-routing-isolation`;
11. `foundation-ownership-fencing`; and
12. `foundation-backlog-ack`.

These targets cover the L0/L1 delivery contract, Workflow and Activity adapters, speculative
Workflow Task delivery, Nexus and Callback integrations, L2 ownership/routing, and L3 backlog
acknowledgement/garbage collection. Each target records owners, interfaces, properties, minimum
bounds, failure policy, backend requirements, abstractions, refinements, and identity pools. The
foundation delivery manifest is a representative example:
[`foundation-delivery-safety/manifest.json`](./tests/umpire2/genmodels/foundation-delivery-safety/manifest.json).

Mutation gates and non-vacuity checks live in
[`verification_test.go`](./tests/umpire2/protocol/verification_test.go). The capability-owned model
sources are in the adjacent `verification_delivery*`, `verification_workflow*`,
`verification_activity*`, `verification_nexus`, and `verification_callback` files.

### Discovery to durable regression

- [x] Accept one behavioral template, declared risk, explicit bounds, seed, environment profile,
  semantic coverage collector, pairwise dimensions, lifecycle exploration, and fault targets.
- [x] Rank candidates by declared risk and unmet semantic coverage and record every selection or
  omission reason.
- [x] Deduplicate a bounded corpus by canonical semantic intent without runtime identities or
  environment-specific realizations.
- [x] Execute through an isolated executor boundary and reject unqualified or mismatched evidence.
- [x] Minimize actions, policies, faults, resources, and unused bindings monotonically while the
  same qualified violation remains.
- [x] Replay the minimized semantic experiment and distinguish matching evidence, schedule drift,
  observation drift, and violation drift.
- [x] Emit a deterministic sparse regression candidate only after complete evidence, cleanup, and
  stable replay.

The deep module is [`campaign`](./common/testing/umpire/campaign/campaign.go). Its seeded end-to-end
test, corpus test, exploration test, drift test, budget test, and qualification test are in
[`campaign_test.go`](./common/testing/umpire/campaign/campaign_test.go).

### Evidence-aware portability

- [x] Declare public API, history, telemetry, and in-process observation profiles.
- [x] Bind evidence profiles to local, CI, deployment, and approved canary environment kinds.
- [x] Qualify properties as established, violated, unsupported, or inconclusive.
- [x] Treat missing sources as unsupported and observation loss, ambiguous identity, conflicting
  lineage, or incomparable ordering as inconclusive.
- [x] Preserve causal references, source sequences, and clock domains; never infer cross-clock
  causality from wall time.
- [x] Compile unchanged completed behavioral intent for all four portable environment kinds while
  retaining the profile as result evidence; concrete deployment drivers remain caller-owned.
- [x] Split the Temporal functional harness into scoped, public API, participant, in-process
  observation, and fault capability interfaces.

Authoritative implementation and tests:

- [`environment_profile.go`](./common/testing/umpire/environment_profile.go) and
  [`environment_profile_test.go`](./common/testing/umpire/environment_profile_test.go);
- [`trace.go`](./common/testing/umpire/trace.go) and clock-skew tests in
  [`trace_test.go`](./common/testing/umpire/trace_test.go);
- profile-bearing regression artifacts and portability tests in
  [`artifact_test.go`](./common/testing/umpire/regress/artifact_test.go);
- capability-owned Temporal interfaces in
  [`environment.go`](./tests/umpire2/action/environment.go) and live qualified verdicts in
  [`regression_observation.go`](./tests/umpire2/action/regression_observation.go).

### Guarded deployment and canary use

- [x] Require campaign, namespace, and tenant isolation before any environment preparation.
- [x] Require explicit action and fault allowlists.
- [x] Enforce action, fault, concurrency, and evidence reservations under concurrency; propagate
  execution and cleanup deadlines to drivers that enforce cancellation at their transport or
  process boundary.
- [x] Stop on invariant violation, observation loss, action failure, timeout, cancellation, or
  budget exhaustion.
- [x] Run cleanup through an uncancelled bounded context and retain recovery-safe resource metadata.
- [x] Redact sensitive fields and configured secrets from retained observations, audit records, and
  cleanup diagnostics.
- [x] Prevent local fault capabilities from granting canary authority; destructive behavior needs
  canary-specific permission in both the profile and envelope.

The controller is [`canary`](./common/testing/umpire/canary/canary.go). Concurrency, cancellation,
stop, cleanup, redaction, isolation, and authority tests are in
[`canary_test.go`](./common/testing/umpire/canary/canary_test.go). A caller must still supply an
approved `Driver`; this is the intentional seam for deployment credentials and operator policy.

## Completion criteria audit

| Criterion | Evidence | Result |
| --- | --- | --- |
| One declaration supplies monitoring, driving, regression, coverage, and verification views | Protocol compilation plus assurance inventory and model-family projection | Complete for the declared protocol; gaps fail compilation or remain explicit abstractions |
| One regression intent is portable across local, CI, deployment, and canary profiles | Environment profiles, capability-owned drivers, and unchanged-suite portability tests | Complete at the framework boundary; environment credentials and drivers are caller-owned |
| Every result says what was explored, observed, omitted, and justified | Qualified claims, campaign selections/omissions, bounds, manifests, artifacts, and summaries | Complete |
| A seeded unknown failure is found, minimized, replayed, and emitted as a stable candidate | Campaign seeded end-to-end test | Complete |
| Clock skew cannot be mistaken for causal proof | Clock domains, source sequences, causal graph validation, and skew tests | Complete |
| Principal Temporal behaviors have executable, observable, non-vacuous slices | Twelve generated targets plus live Workflow/Nexus regression realization and mutation gates | Complete for the bounded catalog |
| Canary execution stays within its enforceable safety envelope | Preflight authority, locked budget reservation, deadline propagation, cleanup, and redaction tests | Complete for drivers that enforce context cancellation at their transport or process boundary |

## Assurance and operational boundaries

The following are explicit boundaries, not silently omitted work:

- Umpire replays a completed semantic experiment; it does not promise to reproduce an uncontrolled
  distributed schedule exactly.
- All exploration is finite and caller-bounded. A reached campaign or backend limit produces an
  incomplete or inconclusive result.
- Unequal evidence profiles justify unequal claims. Public-interface-only execution cannot inherit
  an in-process verdict.
- FizzBee is an explicit smoke backend. Native progress/fairness, actorized FizzBee/P refinements,
  symmetry, and model-based test generation require separate targets before they can make claims.
- Production and deployment drivers, credentials, namespace policy, and approvals remain outside
  the repository-neutral framework. The canary controller rejects missing safeguards and never
  enables destructive behavior by default.
- Exact wire compatibility, authorization, performance, schema migration, metrics, and low-level
  synchronization remain specialized tests below Umpire's behavioral abstraction.

## Verification

The completion gate is:

```shell
go test -tags test_dep ./common/testing/umpire/... ./tests/umpire2/action/... \
  ./tests/umpire2/regress/... ./tests/umpire2/protocol/... \
  ./tests/umpire2/assurance/... ./cmd/umpire-genmodels/...
make umpire-genmodels
make umpire-check-genmodels
make umpire-verify-smoke
make lint-code
```

Tool-backed mutation and equivalence tests run when their pinned executable variables are present.
The CI workflow installs those tools from the generated pins and runs the required verification
matrix; FizzBee remains an explicit non-default smoke lane.
