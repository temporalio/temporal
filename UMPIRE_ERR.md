# UMPIRE — The Error / Divergence Model

> **Status: component reference; partially implemented.** Synchronous rejection capture,
> string/duration/enum domains, reusable integer bounds, bounded payload mutations, canonical
> non-sensitive normalization, rejection facts, and grounded contracts are built. Request-specific
> integer overlays and validator-registry-backed domains remain explicit follow-ups.

## Why

The [Actions model](./UMPIRE_ACTIONS.md) makes the *happy path* declarative: an `Action` has
preconditions and a single unconditional `Effects` list — it always succeeds. But a driver that
can only issue *valid* requests tests half the server. The other half is the **validation
surface**: what happens when an ID is unknown, a RunID is stale, an integer is out of range, an
enum is unspecified, a payload is malformed.

`UMPIRE_SPEC.md` already commits to the discipline for this — **input mutation** as its own
action class, distinct from a transport fault:

> a **mutation** replaces a valid request with a malformed-but-plausible one … carrying its
> *expected rejection* as the oracle — the total model predicts the reject, so negative-space
> coverage needs no hand-written per-case assertion. Invalid cases are a valid base plus exactly
> one *labeled* mutation, so the expected error is unambiguous and any failure minimizes cleanly.

What's missing is *where that lives in the actions model*. This document defines it.

> entity model + actions model + **validity domains on params** ⇒ umpire generates well-formed
> requests, derives their malformed neighbors, drives both, and judges the outcome — rejection,
> alternate path, or deferred failure — against the *same* conformance oracle it already uses for
> legal transitions.

## The core reframe

Today an `Action` is a single operator with one outcome. Encoding error behavior means an action
becomes **a valid base plus a set of labeled divergences, each with its own expected outcome.**

The current `Effects` list is simply the *default* branch — the `WellFormed` variant. Everything
that exists today is the happy variant of a larger family.

Three concerns are separable, and each reuses machinery that already exists:

1. **Where validity lives** — on the action's *params* (typed domains).
2. **What a divergence leads to** — a branched *outcome*, which is *not always an error*.
3. **Who judges it** — the existing `Reconcile` / `Classify` oracle, extended one notch. No
   second judging subsystem.

## 0. The reuse principle: assert a contract, don't restate the rules

The obvious trap is to re-encode the server's validation surface into the model — one hand-written
`Reject{InvalidArgument, "operation ID exceeds…"}` per field per RPC. That duplicates logic the
server already owns, and it *will* drift. There is nothing to reflect over either: the Temporal API
protos carry **no** `protovalidate` / `buf.validate` annotations — validation lives entirely in
server Go code (`serviceerror.NewInvalidArgument(…)`), declared nowhere machine-readable.

So umpire does not restate *what* is valid. It asserts the **meta-properties every rejection must
satisfy** — uniform across all fields and all RPCs, and *not* tested by hand today — and *learns*
the specifics. Three reusable pillars, none of which restates a field rule:

1. **Type-derived domains — one reflection walk, reused everywhere.** The machine-readable source
   is the proto *descriptor*, not annotations. Walk the request `MessageDescriptor` once,
   generically: enum → valid = member set (+ derived `unknown`/`unspecified`); numeric → type range
   (+ `negative`/`zero`/`max`); string → (+ `empty`/`huge`/`non-utf8`); Duration → (+
   `negative`/`zero`). You never hand-write a `Param` for a plain typed field — reflection
   enumerates them and mints the standard mutants. You annotate only the **semantic overlay**
   reflection can't infer: "this string is an `IDRef` to entity X," "this int is a timeout that must
   be positive." (§1)

2. **A generic rejection *contract* as the default oracle — the actual win.** Defined **once**, it
   is the oracle for every field of every RPC. A single-field mutation of an accepted request must:
   - **(a)** be rejected synchronously — *or* accepted (some fields are lenient; grounding tells you
     which);
   - **(b)** if rejected, with a **client-error class** (`InvalidArgument` / `NotFound` /
     `FailedPrecondition` / `AlreadyExists`) — never `Internal`/`Unknown`/`Unavailable`, never a
     panic, never a hang;
   - **(c)** with **no effect** — the target entity is not created / does not advance, and no *other*
     entity gains an effect (no partial write);
   - **(d)** **stably** — same mutation → same class as the last grounded run.

   Nothing in (a)–(d) names a field or a message. It is the same predicate for `OperationId=""` and
   `ScheduleToCloseTimeout=-1`. That is the reuse: written zero times per field. Where the server
   exposes a per-field validator registry, this contract sharpens into a *differential* oracle that
   runs the server's own validators as the model — see §6.

3. **Ground-then-pin for the specifics — metamorphic, not absolute.** The exact code + message is
   *observed on first run and pinned*, never authored. The oracle is **relational**: base accepted →
   base + one mutation rejected-cleanly, and the delta is the signal. `Reconcile` already does
   declared-vs-observed; it extends to record-vs-compare. The only thing ever hand-written is a
   *deviation* from the contract ("this field is deliberately lenient," "this one returns
   `FailedPrecondition` by design"). Everything conforming needs no declaration. (§3)

The server stays the single source of truth for *what* is valid; umpire asserts the *contract* every
rejection obeys and pins the observed specifics for regression. No mirrored rule table to keep in
sync. The rest of this document is how the schema (§1–2) and the oracle (§3–4) realize these three
pillars.

## 1. Validity lives on a typed param domain

`Action` gains `Params []Param`. A param is a field path into the request plus a **Domain** — the
thing that both *generates* valid values and *classifies* a given one:

```go
type Param struct {
    Path     string    // field path into the request, e.g. "OperationId"
    Domain   Domain
    Variants []Variant // usually derived from Domain; may be hand-augmented
}

type Domain interface {
    Generate(rng *rand.Rand) any     // a fresh valid value
    Classify(v any) ValidityClass    // WellFormed | Malformed | Unknown | Stale | OutOfRange | ...
}
```

Most `Param`s and their `Variant`s are **not hand-written** — they are enumerated by reflecting the
request `MessageDescriptor` (pillar 1, §0). The domain is **typed**, and the type is the payoff:
each domain kind ships a **default variant catalog** — its standard invalid neighbors — so error
cases are not authored any more than conformance assertions are today. This is the negative-space
analog of the model's "no vacuous pass": every param type comes with the invalid values you'd
otherwise forget.

The **expected class** column below is not a per-field declaration — it is the *contract's*
prediction (pillar 2): the family of client error the mutant should provoke. The **specific** code
and message are grounded and pinned on first observation (pillar 3), never authored here.

| Domain | valid | derived variants → expected *class* (specific = grounded) |
|---|---|---|
| `Enum{members}` (reflected) | a declared member | `unknown`, `unspecified` (zero) → client error |
| `IntRange{lo,hi}` | in `[lo,hi]` | `below`, `above`, `negative`, `zero` → client error (or *accepted/clamped*, grounded) |
| `Str{maxLen}` (reflected) | within bounds | `empty`, `too-long`, `bad-charset` → client error |
| `Payload{codec}` | encodable | `oversize`, `bad-encoding`, `wrong-type` → client error |
| `IDRef{entityVar}` (semantic overlay) | id of a live bound entity | `malformed` → client error; `unknown` → **NotFound**; `stale` → **policy-dependent** |

Only `IntRange`, `Payload`, and `IDRef` carry information reflection can't derive (a semantic range,
a codec, an entity reference); the rest fall out of the descriptor.

### The ID / RunID distinction falls out of the domain

The cases you'd otherwise conflate are distinct **validity classes**, not ad-hoc strings:

- **malformed** (bad UUID) — rejected before any lookup → `InvalidArgument`.
- **unknown** (well-formed, never created) — passes syntax, fails lookup → `NotFound`.
- **stale** (well-formed, *was* valid, since superseded — e.g. a previous RunID) — requires
  `IDRef` to carry the entity's *identity history*, not be a plain string. Its outcome is
  deliberately **not fixed** (see §4).

## 2. Divergence forks the outcome — it is not always an error

The mistake would be to model divergence as "error vs. not." A perturbed input has **three**
possible shapes. A `Variant` is:

```go
type Variant struct {
    Label  string        // "unknown-id", "stale-runid", "negative-timeout", "unknown-enum"
    Class  ValidityClass
    Mutate func(valid any) any // produce the perturbed value from a valid base
    Expect Outcome
}

// Outcome is exactly one of:
type Outcome struct {
    Reject    *Reject    // synchronous RPC rejection; nil Reject = "reject per the contract" (the default)
    Normalize *Normalize // accepted, but the server rewrites the field first (see §6)
    Alternate []Effect   // valid, but a different legal edge (incl. deferred failure)
    OneOf     []Outcome  // deliberate nondeterminism — any listed outcome is acceptable (see §4)
}

// Reject is an OPTIONAL pin/override on the generic rejection contract (§0 pillar 2). A derived
// variant leaves it nil — "reject cleanly, class grounded." You fill it in only to assert a
// specific code by design, or grounding fills it on first observation.
type Reject struct {
    Code    codes.Code // gRPC / HTTP status  (nil Outcome.Reject ⇒ any client-error class)
    Message Matcher    // substring / regex on the error message (optional)
}

// Normalize: the server accepts the request after rewriting the field (e.g. empty RequestId → a
// fresh UUID). Not a reject and not a plain accept — the observed entity carries the *rewritten*
// value, which matters for RequestID-based identity routing. See §6.
type Normalize struct {
    Rule Matcher // predicate over the accepted value (e.g. "is a UUID"); usually derived, see §6
}
```

- **Reject** — invalid IDs, bad enums, out-of-range ints: the RPC fails and *no entity is
  created*. The default (`Reject == nil`) asserts only the **contract** — rejected cleanly, some
  client-error class, no effect, stable — with the specific code grounded, so nothing is authored
  per field.
- **Normalize** — a field the server rewrites rather than rejects. Enumerated for free where a
  validator registry exists (§6); otherwise grounded on first observation.
- **Alternate** — a stale RunID the server *accepts* against the current run, or any
  valid-but-different value. Just a different `Effects` branch of the same action.
- **Deferred failure** — accepted synchronously, entity created, then transitions to a terminal
  whose `Disposition == Failure`. This is *already expressible*: it's an `Alternate` whose effects
  land on a `Failure` terminal the lifecycle already models. No new mechanism.

The base action's existing `Effects` is the default `WellFormed` outcome; a `Variant` overrides it.

## 3. Judge with the machinery that already exists

No second judging path for errors. Two moves keep everything inside the current oracle.

### a) Make the error a fact

The Monitor already decodes gRPC traffic into facts, and *an error response is a fact*. Teach the
decoder to map a request/error-response pair, keyed by `RequestID`, into a lifecycle event on the
would-be entity, and add rejection terminals to the lifecycle:

```
unspecified --reject:NotFound-->         rejected_not_found  (Disposition: Failure)
unspecified --reject:InvalidArgument-->  rejected_invalid    (Disposition: Failure)
unspecified --reject:FailedPrecondition-->rejected_precond   (Disposition: Failure)
```

These terminals are a **small fixed set keyed by client-error class**, shared by every entity — not
one state per field or per rule. A rejection is now an ordinary transition to a `Failure`-disposition
terminal, the same shape a modeled timeout or cancellation already has. Nothing new is needed to
*represent* it, and the set does not grow as fields or RPCs are added.

### b) Check the branch with `Reconcile`, not `Classify`

`Classify` alone won't catch "server accepted an invalid request": the `schedule` edge is legal in
the abstract, so `unspecified --schedule--> scheduled` is not an illegal transition. The divergence
is at the **variant** level, and that is exactly what `Reconcile` already does — compare *declared
effects* against *observed edges* and emit `Drift`.

Generalize `Reconcile` one notch: a driven variant declares its expected terminal event
(`reject:NotFound`); reconcile confirms the entity actually traversed it and flags `Drift` if the
server did `schedule` instead. This is the existing `Drift` struct in
`common/testing/umpire/action.go`, one layer up. The error model is a straight extension of the
conformance check, not a parallel system.

```
declared:  variant "unknown-id"  Expect Reject{nil}   ⇒ entity must reach some rejected_* terminal
observed:  entity reached scheduled                    ⇒ Drift: "accepted an unknown id"
```

### c) The contract clauses are generic rules, authored once

Contract clauses (b)–(d) from §0 are **not** re-stated per variant — they are ordinary Monitor
rules over the reject facts, written once and reused for every mutation:

- **(b) client-error class** — a safety rule: a `reject:*` fact whose class is `Internal` /
  `Unknown` / `Unavailable` (or a decoded panic/timeout) is a violation, for *any* entity.
- **(c) no partial write** — a relational rule: when an action's driven variant expects rejection,
  no entity bound by that action may hold a non-`unspecified` effect. Reuses the cross-entity reach
  the rulebook already has.
- **(d) stable** — grounding: the pinned `reject:*` class for a `(variant)` is compared on each run;
  a change is `Drift` unless re-grounded deliberately.

So the per-variant author writes *nothing* for a contract-conforming field; the reusable rules and
the grounded pin do all the judging. Only a deliberate `Reject{Code, Message}` override is authored.

## 4. The stale-RunID case specifically

Stale identity is the case that is *genuinely not knowable a priori* — it's a server policy, not a
syntax rule. Don't pretend it's fixed. Two supported answers, both already idiomatic:

1. **Disjunction** (`Outcome.OneOf`) — "either `Reject{NotFound}` or `Alternate → scheduled`." Both
   are acceptable; the model *documents* the ambiguity instead of guessing, and reconcile passes if
   the observed outcome is any listed one.
2. **Ground-then-pin** — the first drive *observes* what the server does; `Reconcile` records it;
   thereafter that is the pinned expectation and a change is a regression. This is literally the
   "happy-path run grounds the model" step from `UMPIRE_ACTIONS.md`'s auto-drive loop, applied to a
   variant.

Default to **ground-then-pin** for `Stale`; reserve **disjunction** for behavior that is
*deliberately* nondeterministic.

## 5. How it plugs into the loop

- **Generate valid** — `Domain.Generate` per param replaces the hardcoded literals in the current
  realizers (e.g. `rpcStartStandalone.Fire`'s 5-minute timeout, `"service"`/`"operation"` names).
- **Enumerate variants** — reflect the request `MessageDescriptor` (pillar 1) to mint the standard
  mutants per field; hand-declaration is only the semantic overlay (`IDRef`, semantic ranges).
- **Generate invalid** — a valid base plus *exactly one* variant's `Mutate` applied to *one* param.
  One labeled mutation ⇒ the expected error is unambiguous and any failure minimizes cleanly (the
  `rapid` discipline `UMPIRE_SPEC.md` borrows).
- **Coverage goal extends** — today "cover every entity edge"; now also "cover every derived
  variant." Each variant is a coverpoint; negative-space coverage = every mutant was *exercised and
  satisfied the contract* (or matched its pinned/overridden `Expect`).
- **Faults unify with this** — a fault and a mutation are the same triple
  `{target, perturbation, Expect}`. A mutation's `target` is a **param path**; a fault's `target`
  is a **`Faultable` footprint point**. The current `Faultable []string` field is exactly
  "footprint targets with no declared outcome yet." They differ only in what they perturb (input
  value vs. wire) and thus which capability they require and which oracle judges them: mutations →
  the reject/alternate conformance oracle above; faults → liveness (recover to a `Success` terminal
  or an acceptable `Failure` terminal).

## 6. The differential oracle: reuse the server's own validator registry

§0 assumed the server declares validation nowhere machine-readable — true today, but a
[validator-generator effort](https://github.com/temporalio/temporal/pull/10200) is changing it. It
generates, per request proto, a `<Req>FieldValidators` struct with **one validator func per field**,
a `ValidateAndNormalize(req)` that runs them in order, and a `ValidatorRegistry` keyed by request
type — with a reflective test asserting *every* field has a validator wired (exhaustive). That is
precisely the single, machine-enumerable, per-field source of truth §0 said was missing. Where it
exists, it *upgrades* the pillars rather than replacing them:

- **Enumerate params from the registry, not the descriptor** (sharpens pillar 1). The registry
  distinguishes fields that carry a real rule (`operation_id: maxStringLength`) from pass-throughs
  (`namespace: no-op`), so mutant generation targets only fields that can actually reject.
- **The validator *is* the oracle** (sharpens pillar 2 from a generic contract to a *differential*
  check). For a field with a registered validator, umpire runs the **same `ValidateAndNormalize`
  in-process over the generated mutant** and cross-checks the server:

  ```
  validator(mutant) → error   ⇒ server MUST reject   (and the error is the expected error, for free)
  validator(mutant) → nil     ⇒ server MUST accept
  validator rewrites the field ⇒ Normalize outcome (§2), with Rule = "matches the rewritten value"
  ```

  Generator and oracle share one source of truth: umpire neither reads nor restates the rules, it
  *runs* them as the model. No grounding needed for covered fields.
- **Coverage composes.** The generator's suite guarantees every field *has* a validator; umpire's
  variant coverage guarantees every validator's *reject branch is actually driven and satisfies the
  contract*. Together: no unvalidated field, no undriven rejection.

Scope and fallback — this narrows, but does not remove, §0:

- **`InvalidArgument` / field-validation only.** `ValidateAndNormalize` is a thin *front* layer.
  `NotFound` (unknown id), `FailedPrecondition` / `AlreadyExists` (state- and lookup-dependent) run
  *deeper* and are not field validators — the generic contract, grounding, and the `IDRef` /
  stale-RunID handling (§1, §4) remain their whole story.
- **`local-*` capability.** Calling server code in-process is a grey/white-box realizer
  (`internals` / `directDrive`). In `cicd` / `canary` the differential oracle is unavailable and
  umpire falls back to the generic contract + grounding — the same capability-honest split
  `UMPIRE_SPEC.md` already draws. The registry oracle is the *preferred* path where granted; the
  contract is the *portable* one.
- **Run on a copy.** `ValidateAndNormalize` mutates its argument (it *normalizes*); the in-process
  oracle must validate a clone, and must expect the observed entity to carry the normalized value.
- **Opt-in / partial.** The generator is WiP and covers only some RPCs; for anything unregistered,
  every field still gets type-derived mutants (pillar 1) judged by contract + grounding.

Rule of thumb: **prefer the registry oracle where the field is covered and the environment grants
`internals`; fall back to contract + grounding everywhere else.**

## The one-line version

> Don't restate the server's validation rules — there's nothing to reflect (no `protovalidate`) and
> mirroring them by hand would drift. Instead reflect the proto *descriptor* to mint mutants,
> assert a single generic **rejection contract** (client-error class, no effect, no partial write,
> stable) as the oracle for every field, and *ground-then-pin* the specific code/message. A
> per-field author writes nothing unless the field deviates from the contract.

## Implementation

Built on top of the actions model (`UMPIRE_ACTIONS.md`). The Temporal concretions live in
`tests/umpirev1/action/reject.go`; the abstract schema is in `common/testing/umpire`.

- **E1 — rejection round-trip (done).** `umpire.Action.Reject` + the `RejectSink` seam: `Drive`
  records a Fire outcome on a `Reject` action (error, or nil if accepted) and continues instead of
  aborting. The declared `StartUnknownEndpoint` (well-formed request, non-existent endpoint →
  NotFound) exercises it.
- **E2 — per-field variant enumeration (done).** The `Param` / `Domain` / `Variant` /
  `ValidityClass` schema, plus `reflectStringParams` walking the request descriptor to mint a
  `stringDomain`'s standard mutants per scalar string field. `rpcStartMutated` applies a single
  field mutation to the valid base via protoreflect; `StartFieldVariants` enumerates the negative
  set. Exercised by `TestProbeNexusReflectedVariant` (`operation_id=empty`).
- **E3 — rejection judged by the model (done).** A rejection is now a *fact*, not a domain-side
  check. The gRPC interceptor passes the handler error to `Monitor.RecordRejection`
  (`RejectionRecorder`); the decoder's `ImportRejection` turns a client-error-class
  `StartNexusOperationExecution` failure into a `NexusOperationRejected` fact (gated by
  `fact.RejectionCode`), routed by request id under the namespace. The NexusOperation lifecycle
  gains a `reject` edge to a `rejected` `Failure` terminal, so the invalid action's `reject` Effect
  is confirmed by the ordinary `umpire.Reconcile` — the former `RejectionDrift` is retired.
  Name→id, which a request-only fact lacks, is seeded into the Monitor by the driver (`NewCtx` →
  `SetNamespaceID`). A non-client-error failure produces no fact, so its absent transition surfaces
  as drift rather than a pass. `reject` is the 17th modelled edge and is covered by the exploration.

- **E5 (partial) — non-string domains (done for Duration).** The descriptor reflection generalizes
  past strings: `reflectStartParams` now also emits a `durationDomain` per `google.protobuf.Duration`
  field, and `rpcStartMutated` sets fields by kind (string or Duration message) via `protoValue` /
  `currentValue`. Exercised by `TestProbeNexusReflectedDurationVariant` (`schedule_to_start_timeout=
  negative` → `InvalidArgument`). Enum / int / payload domains remain.

### What remains

- **E4 — blocked on the validator registry.** The differential oracle (§6) runs the server's own
  generated `ValidateAndNormalize` in-process. That generator
  ([temporalio/temporal#10200](https://github.com/temporalio/temporal/pull/10200)) is **not in this
  tree** (no `common/validation`, no `validator_gen.go`), so E4 cannot be built against a real
  registry yet. Until then the generic contract + grounding (E1–E3) is the oracle.
- **E5 (remaining)** — enum / int / payload domains, and variant coverage as a planning goal (drive
  every reflected variant, grounding each outcome) alongside edge coverage.
- **Normalize outcomes** — §2's `Normalize` (e.g. empty `request_id` → UUID) is designed but not yet
  driven; today such a variant would read as an accepted request.
- **Rejection classes as terminals** — E3 models all client-error rejections as one `rejected`
  terminal (the code is kept as the outcome). Splitting into `reject:NotFound` /
  `reject:InvalidArgument` terminals (§3a) is deferred until a rule needs to distinguish them.

## Relationship to the other umpire pieces

- **Actions model** (`UMPIRE_ACTIONS.md`) — this is the divergence layer over it: `Params` and
  `Variants` are new `Action` fields; the happy-path `Effects` is the default `WellFormed` branch.
- **Entity models** (`common/testing/umpire/lifecycle.go`) — gain `reject:*` terminals with
  `Failure` disposition; `Classify` and `Disposition` judge rejections with no new machinery.
- **Reconcile / Drift** (`common/testing/umpire/action.go`) — extended from "declared effect not
  observed" to "declared variant outcome not observed"; the primary error oracle.
- **Monitor decoder** (`UMPIRE_MONITOR.md`, `UMPIRE_TRACING.md`) — teaches wire error-responses to
  become lifecycle facts keyed by `RequestID`; the seam that lets rejections be judged as
  transitions.
- **Planner** (`tests/umpirev1/planner`) — variant coverage becomes a planning goal alongside edge
  coverage.
- **Faults** (`UMPIRE_SPEC.md` mutation-vs-fault split) — the same `{target, perturbation, Expect}`
  shape with a footprint target instead of a param target.
- **Validator registry** ([temporalio/temporal#10200](https://github.com/temporalio/temporal/pull/10200))
  — the server's own per-field `ValidateAndNormalize` becomes umpire's differential oracle and param
  enumeration where granted (§6); the preferred path, with contract + grounding as the portable
  fallback.
```
