# Umpire Follow-up Observability and Regression Design

## Objective

Complete the five follow-ups in `UMPIRE_PLAN.md` after the initial v2 parity cutover:

1. triage the v2 default against the Umpire, Nexus, Workflow, and testcore suites;
2. model callback-to-operation and callback-to-handler identities with duplicate-response
   idempotency;
3. add canonical payload/link and explicit terminal-storage observations so sparse regressions can
   replace their remaining imperative mechanics;
4. connect action and verdict coverage/tracing to both action execution runtimes and provide a
   Temporal protocol adapter for pairwise matrices; and
5. add checked-in causal trace footprints and validator-backed error domains.

V1 remains in `tests/umpirev1` as an explicit compatibility implementation. This work neither
deletes v1 nor requires v1 to adopt v2-only relations and observability.

## Scope

The broad functional triage is deliberately limited to top-level tests whose names cover Umpire,
Nexus, or Workflow behavior, plus `tests/testcore` and `tests/umpire2`. Unrelated functional,
replication, persistence, and repository-wide suites are outside this pass. Failures outside the
selected scope are reported but do not expand implementation automatically.

The change reuses existing public request/response, history, and admin mutable-state observations.
It does not add production telemetry or change Temporal wire formats. No new third-party library is
introduced.

## Selected approach

Use observation-first vertical slices. Every sparse predicate is grounded in a canonical fact or
relation that the monitor can also consume. Both action runtimes emit through one optional neutral
observer contract. Temporal-specific adapters remain under `tests/umpire2`; reusable stores,
recorders, matrix generation, refinement, and validation-domain machinery remain under
`common/testing/umpire`.

Two alternatives were rejected:

- Harness-only assertions are quicker, but preserve the `localFacts` duplication that prevents the
  monitor and sparse regressions from sharing a source of truth.
- New server telemetry provides more internal detail, but is invasive and unnecessary while the
  required callback, history, and mutable-state values are available through existing boundaries.

## Callback identity and relations

### Canonical observations

Add a non-lifecycled `Callback` entity to the v2 protocol. A callback identity is a SHA-256 digest
over its canonical non-secret routing identity. Raw callback URLs, headers, and callback tokens are
never stored in facts, relations, diagnostics, coverage, or trace events.

Two observations feed the entity:

- A Nexus callback observation is produced when a Nexus start request is delivered. Normal
  worker-target delivery is decoded from `PollNexusTaskQueue` responses. The in-process external
  test handler emits the same fact through an optional v2 fact-observation capability when it
  captures `StartOperationOptions`.
- A workflow callback attachment is produced from a successful `StartWorkflowExecution`
  request/response pair containing completion callbacks. The request supplies the callback and
  request ID; the response supplies the actual handler run ID, including the existing-run result
  for `WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING`.

Callback tokens are decoded only in memory with the existing Nexus token helpers. The normalized
fact retains the callback digest, operation request ID, operation business/workflow ID, handler
workflow ID, and handler run ID. Invalid tokens produce a scoped conformance observation rather
than being silently ignored.

### Typed edges

Declare these relations in the v2 protocol:

- `callback-operation`: one operation target per callback; an operation may have multiple callback
  observations across retries or reset-compatible identities.
- `callback-handler-run`: one handler run target per callback; a handler run may own callbacks for
  multiple operations.

Applying an identical fact or edge twice is idempotent. A second target for the same callback is a
cardinality violation; the relation mutation is rejected atomically and retained as a monitor
violation scoped to the namespace. Accepted indexes are never overwritten by a conflict.

The shared-handler sparse regression must query these relations instead of setting
`nexus.handler_workflow` in harness-local state. Completion-before-start must observe the same
callback identity before delivering completion, so a duplicate handler response proves idempotency
through the relation store rather than timing.

## Completion values and terminal storage

### Canonical payload and link identity

Add reusable deterministic protobuf hashing that returns a labeled SHA-256 digest. The helper
rejects nil or unencodable values and never returns serialized bytes. Completion facts retain:

- the operation and scheduled-event identities;
- the terminal event kind;
- a result or failure digest, as applicable; and
- sorted canonical link digests plus normalized link endpoint identities.

History response decoding emits these facts from Nexus terminal events. Sparse predicates compare
expected canonical digests and link endpoints, not raw payload contents or protobuf byte order.

### Explicit deletion observation

Decode successful admin `DescribeMutableState` request/response pairs into a workflow-scoped Nexus
storage snapshot. The snapshot lists the operation identifiers currently present in the HSM
sub-state-machine map and the CHASM operation-node map. An empty list is an explicit observed fact;
a terminal lifecycle state alone is never treated as proof of deletion.

The ordinary-completion sparse proof becomes an embedded synchronous operation and requires the
same result, handler link, and storage-deletion observations as the existing imperative oracle.
Only after that functional proof passes will the duplicate imperative orchestration be replaced by
the sparse helper. The public test names remain so coverage and historical intent are preserved.

## Execution observer and semantic recording

### Neutral optional contract

Add a small optional `ExecutionObserver` contract in `common/testing/umpire`. It receives stable
action start, action finish, and verdict-checkpoint records. Observer errors are returned to the
caller; an enabled recorder cannot silently lose an event.

The generic `Drive` runtime discovers the observer through the existing `RealizeContext`. The
sparse executor discovers it through an optional harness/path capability. Existing implementations
remain source-compatible and pay only a failed type assertion when observation is disabled.

Action records use stable action names and phases. Error fields contain only normalized error
classes. Verdict records contain checkpoint identity and pass/violation counts, not arbitrary error
text. Both runtimes record proactive, reactive, and rejected actions consistently; reactive action
windows begin at installation and close at reconciliation.

### Coverage and trace sinks

A reusable semantic recorder fans each execution record into the existing `Coverage` and
`TraceRecorder` modules:

- action starts mark `CoverageAction` and create `TraceAction` events;
- action finishes causally reference their start events;
- monitor checks create `TraceVerdict` events and mark evaluated/violated rule coverage; and
- facts, transitions, and relations observed while an action window is active reference that
  action where the execution boundary is synchronous and known.

The monitor remains usable with coverage only, tracing only, both, or neither. Event and byte caps
remain authoritative and turn overflow into explicit test failures.

## Checked-in causal footprints

Add a `CausalFootprint` declaration keyed by stable action name. A footprint contains required and
forbidden semantic trace patterns plus ordering/causal constraints. It does not contain request
IDs, timestamps, callback tokens, raw payloads, or generated entity IDs.

The comparator selects the action window from action-start through action-finish/reconciliation,
checks required facts/transitions/relations in order, permits explicitly allowed extras, and
reports the first missing, forbidden, misordered, or causally disconnected observation. Protocol
actions and sparse realizations keep their footprint catalogs in `tests/umpire2`, while comparison
remains generic.

Initial checked-in footprints cover ordinary completion, completion-before-start, cancellation
failure followed by cancellation, and shared-handler attachment. Footprints are diagnostic and
conformance artifacts; they do not replace lifecycle or safety rules.

## Temporal pairwise matrix adapter

Add a read-only adapter over the compiled v2 protocol. It derives ordered dimensions from:

- entity type;
- lifecycle edge;
- standalone or embedded hosting;
- executable action versus declared action gap; and
- caller-provided regression profiles/capabilities.

The adapter supplies validity constraints to the existing generic pairwise generator, so invalid
entity/edge/hosting/profile combinations are never candidates. Returned cases have deterministic
names and retain the underlying action key or gap reason. Generation does not provision a cluster
or execute a case; callers decide which cases become functional tests.

## Validator-backed domains

Add a reusable validator registry keyed by protobuf message full name and field path. A registered
adapter validates a value and may return a canonical normalized value. `ValidatorDomain` composes
an existing mutation domain with one of these adapters, so variants and normalization are judged by
the same validator rather than a copied rule.

The Temporal registry adapts established validators for:

- protobuf durations through Temporal timestamp/duration validation;
- link collections through `common/links.Validate`;
- payload metadata/decoding through existing payload conversion helpers;
- enum membership through the protobuf descriptor; and
- request-specific signed integer ranges through explicit registered bounds.

Reflection uses a validator-backed domain when an adapter exists. A missing adapter returns
`ErrUnsupportedDomain` with the message and field name; it never silently accepts the value.
Validation functions that normalize mutable protos receive clones so error exploration cannot
mutate the known-good request base.

## Runtime flow

The integrated path is:

1. begin or install an action and open its semantic observation window;
2. drive the action through the generic or sparse runtime;
3. decode request, response, history, handler-capture, and mutable-state observations into facts;
4. route entity facts and atomically derive callback, lineage, and link relations;
5. record normalized fact, transition, relation, action, and verdict events;
6. reconcile the action and validate its checked-in causal footprint;
7. evaluate safety at action/observation/quiescence checkpoints and liveness at teardown; and
8. finalize bounded trace/coverage artifacts atomically when requested.

## Error and failure semantics

- Protocol, relation-schema, matrix-catalog, validator-registration, and footprint declaration
  errors fail construction before a functional environment starts.
- Malformed external observations are retained as scoped monitor violations where an owning
  namespace is known; they do not crash the server under test.
- Relation conflicts reject the complete mutation without changing forward or reverse indexes.
- Missing payload, link, callback, or deletion evidence leaves a sparse milestone unresolved and
  fails with the expected semantic identity.
- Observer, trace-cap, and artifact errors propagate from the execution runtime.
- Unsupported validation fields return typed errors and remain visible in mutation coverage.
- A test-process crash may lose in-memory observations but cannot change Temporal state. Atomic
  artifact replacement prevents a partial file from appearing complete.

## Performance, scalability, complexity, and security

Disabled observation adds only optional capability checks. Enabled fact, relation, coverage, and
trace processing remains synchronous and in memory. Indexed relation lookup is proportional to
returned edges. Pairwise generation remains bounded and avoids automatically running a Cartesian
product. At ten times the current event load, retained state grows linearly until configured trace
and matrix caps turn growth into explicit failures.

Callback tokens and URLs are decoded transiently and never retained. Payloads and links are
represented by deterministic digests and normalized endpoint identities. Authorization,
credential, token, payload, and header fields remain covered by trace redaction. Hashing provides
stable comparison identity, not authentication.

## Test strategy

Every production behavior follows red-green-refactor development.

Focused tests cover:

- callback digest stability without secret retention;
- token decoding for HSM and CHASM operation targets;
- callback-to-operation and callback-to-handler derivation;
- duplicate idempotency and atomic conflict rejection;
- canonical payload/link hashing and deterministic ordering;
- explicit HSM/CHASM storage snapshots and absence handling;
- action and verdict events in both generic and sparse runtimes;
- disabled observer compatibility and observer error propagation;
- causal footprint success and first-mismatch diagnostics;
- deterministic valid protocol/profile matrix cases; and
- validator registration, cloning, normalization, invalid values, and unsupported fields.

Functional verification is limited to:

```text
go test -tags test_dep ./common/testing/umpire/...
go test -tags test_dep ./tests/umpire2/...
go test -tags test_dep ./tests/testcore/...
go test -tags test_dep ./tests -run '^(TestUmpire|TestSparseRegression|TestNexus|Test.*Workflow)' -count=1
```

The exact top-level regex may be split into smaller commands for diagnosis, but it may not expand
to unrelated suites. Changed imports are formatted, scoped lint is run without auto-fixing
unrelated files, `make lint-code` is attempted as required, and the final diff is checked for v1
changes, raw secret/payload retention, unstable ordering, and unrelated modifications.

## Completion criteria

The follow-up is complete when:

- selected v2-default suites pass or every remaining failure has a concrete out-of-scope baseline
  classification;
- callback relations are fact-derived, idempotent, conflict-safe, and used by sparse regressions;
- ordinary completion proves canonical result/link identity and explicit state deletion;
- both executors emit action observations and monitor checks emit verdict observations;
- protocol pairwise cases are deterministic and validity-constrained;
- checked-in causal footprints cover the four initial regressions; and
- validator-backed domains use registered Temporal validators with typed unsupported gaps.
