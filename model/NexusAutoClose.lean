/-!
# Nexus AutoClose — a design, modelled and machine-checked in Lean

This file is two things at once, and it is written so you can read it knowing neither.

1. **A tutorial.** It teaches enough Lean 4 to read a real proof, introducing each language
   feature at the moment the model first needs it. No prior Lean is assumed.
2. **A load-bearing artifact.** It settles an open question in the Nexus **AutoClose** design
   (`../../spec.md`): when a user-initiated cancellation and a system-initiated one collide,
   which one wins? Three answers were on the table. This file proves two of them wrong.

Read it top to bottom. Every section builds on the previous one.

## How to run it

```
cd temporal/model
mise install                 # pins Lean 4.33.0
make check                   # == mise exec -- lake build
```

A silent build is a passing build: it means Lean's kernel accepted every proof below. If you
break a definition, the build fails and tells you which proof no longer goes through. That is
the whole point — the file cannot drift out of agreement with itself.

Optional, and stricter:

```
mise exec -- lake lint --builtin-only --linters=.all
```

## Part 0 — What Lean is, in five minutes

Lean 4 is a programming language *and* a proof assistant. The two are the same thing, via an
idea called the **Curry–Howard correspondence**:

> A proposition is a type. A proof of that proposition is a value of that type.

So `2 + 2 = 4` is a *type*, and a proof of it is a *value* — just as `Nat` is a type and `7` is
a value. Proving a theorem means constructing a value of the right type, and Lean type-checks
that value the same way a compiler type-checks a program. If it type-checks, the theorem holds.

Three consequences worth internalizing before you read on:

* **The kernel is small and paranoid.** Whatever clever machinery helps you *build* a proof, the
  final artifact is re-checked by a small trusted core. A buggy tactic cannot sneak a false
  theorem past it.
* **Tactics are proof generators, not magic.** When you see `by simp` below, that means "run the
  simplifier to construct the proof term for me". The term still gets kernel-checked.
* **There is exactly one way to cheat**, the `sorry` keyword, which admits a goal without proof.
  This file's build forbids it (`rg '\bsorry\b'` is part of the verification), as it forbids
  `native_decide`, which trusts the compiler rather than the kernel.

Notation you will meet, in order of appearance:

| Syntax | Meaning |
|---|---|
| `inductive` | declare a new type by listing its constructors |
| `def` | define data or a function |
| `theorem` | define a proof (a value whose type is a proposition) |
| `example` | an anonymous checked claim — a compile-time unit test |
| `Prop` | the universe of propositions (statements) |
| `Bool` | the type of `true`/`false` (data) |
| `Option α` | either `none` or `some a` — Lean's "maybe" |
| `by tac` | build the proof term using tactic `tac` |
| `rfl` | "both sides reduce to the same thing" |
| `¬ P` | notation for `P → False` |
| `⟨a, b⟩` | anonymous constructor: builds a pair / structure / existential |
| `{ c with f := v }` | a copy of structure `c` with field `f` replaced |
| `.started` | short for `OpState.started` when Lean already knows the expected type |

## Part 1 — What Nexus is, in five minutes

Temporal runs **workflows**: programs whose execution survives process death, because every step
is journaled to a **history** and replayed on recovery.

**Nexus** lets one workflow call across a team or namespace boundary. Two parties:

* the **caller** — a workflow (or a standalone entity) that starts a Nexus **operation**;
* the **handler** — the service on the other side that actually does the work.

An operation may be *synchronous* (answer returns immediately) or **asynchronous**: the start
request returns a token, the operation sits in state `started`, and the handler completes it
later. Async is where all the interesting failure modes live.

The Nexus protocol has exactly two verbs for an operation: **start** and **cancel**. There is no
"terminate". So the only thing a caller can ever say to a running handler is *please cancel*.

### The problem AutoClose solves

Today, the handler is told to cancel only when someone explicitly asks. If the caller workflow is
**force-closed** — terminated, failed, completed, timed out, continued-as-new — its pending
operations are simply abandoned, and the handler keeps running forever. Nobody told it to stop.

**AutoClose** is an opt-in policy on the operation:

* `ABANDON` — today's behavior, and the default. No regression.
* `REQUEST_CANCEL` — on a forced close, Temporal delivers a cancel to the handler.

### The one rule that makes this subtle: the clamp

A cancel is an RPC, and an RPC needs a timeout. Temporal clamps that timeout to the operation's
*remaining* time. A forced-close cancel fires at or after the operation's schedule-to-close
deadline, so remaining time is ≈ 0 — clamping would starve the request below `MinRequestTimeout`
and it would never be sent at all.

So the design splits the two cases (`../../spec.md`):

* **system-initiated** cancels (from AutoClose) carry a flag and **skip the clamp**;
* **user-initiated** cancels **keep the clamp** — a user cancel with no time left is
  *intentionally* not sent.

Everything in this file follows from that single asymmetry.

### The open question this file answers

An operation holds a cancellation slot. Suppose a **user** already requested cancellation, and
*then* the caller is force-closed so AutoClose wants to cancel too. Two initiators, one slot.
Three candidate resolutions were proposed:

* **skip** — defer to the pending user cancel; create nothing.
* **duplicate** — create a second cancellation alongside it.
* **upgrade** — promote the pending one in place to system-initiated.

They look interchangeable. They are not, and the difference is invisible to testing because the
failure is a *silent non-delivery*. That is what a model is for.

## The argument, in one page

The file has four layers. Each is a Lean section below.

* **Layer 1 — the lifecycle.** The ten states and nine events of a Nexus operation, mirrored from
  the canonical Go declaration. This is scaffolding: it exists so that "reachable configuration"
  in the later layers means something real rather than something assumed.
* **Layer 2 — the configuration.** One operation plus its caller: state, policy, pending
  cancellations, whether the caller is open, and whether any time is left. Plus `delivers`, which
  encodes the clamp rule above.
* **Layer 3 — the hook and the clash.** The force-close hook, parameterised by the three
  resolutions, and four obligations that decide between them.
* **Layer 4 — history and reset.** Temporal rebuilds a run by replaying its history. If the
  initiator is not written to the event, the rebuild invents one — which turns out to be the same
  bug as choosing `skip`, arriving by a different road.

The obligations, and the verdict the file proves:

| | Obligation | `skip` | `duplicate` | `upgrade` |
|---|---|---|---|---|
| **O‑1** | `REQUEST_CANCEL` + forced close + `started` ⇒ the cancel reaches the handler | ✗ | ✓ | ✓ |
| **O‑2** | `ABANDON` ⇒ no system cancellation is ever created | ✓ | ✓ | ✓ |
| **O‑3** | at most one cancel-requested event per operation | ✓ | ✗ | ✓ |
| **O‑4** | a reset rebuild preserves the initiator | only if it is stamped on the event |

Underneath all four sits one root lemma, `initiator_not_derivable`: **no function of the
lifecycle state computes the clamp decision.** That is why a stored flag is *required* rather
than a convenience, and it is the reason the other three obligations are hard.

## What this model does not claim

Honesty about scope is part of the artifact:

* **No link to the Go code.** Nothing connects this file to `chasm/lib/nexusoperation/`. It
  checks the *design*. If the runtime clamp changes and this file does not follow, Lean will
  happily keep proving things about a design that no longer exists.
* **No timing, no liveness.** `slack` is a `Bool`, not a clock. The file shows a clamped cancel
  is *never sent*; it says nothing about retry schedules or delivery before retention expiry.
* **One operation, no concurrency.** No fan-out, no interleaving between the close transaction
  and an in-flight cancel RPC.
* **Hosting collapsed.** Workflow-backed and standalone (SANO) callers become the same
  `callerOpen` coordinate.

## Canonical sources

* Layer 1 mirrors `../../../umpire/temporal/tests/umpire2/internal/model/nexus_operation.go`
  (`NewNexusOperation`).
* Layers 2–4 model `../../spec.md`, whose runtime counterparts in this repo are
  `../chasm/lib/nexusoperation/cancellation.go` (the `auto_close` flag and the clamp),
  `../chasm/lib/workflow/nexus_methods.go` (the hook) and `../chasm/lib/workflow/nexus_events.go`
  (deferred removal).
-/

namespace NexusAutoClose

/-! # Layer 1 — the operation lifecycle

## Lean concept: `inductive`

An `inductive` type is declared by listing its **constructors** — the complete set of ways to
build a value of that type. `OpState` below has ten constructors and no others, so Lean knows the
type is finite, and can therefore check claims about *every* state by exhausting them. That
finiteness is what makes the proofs later in this file possible by brute force.

This is also a modelling decision, not just a Lean one. Mirroring the Go declaration
constructor-for-constructor means a reviewer can diff the two by eye. A state that exists in the
runtime but not here is a hole in the model; a state here that does not exist there is fiction.

`deriving DecidableEq, Repr` asks Lean to auto-generate two instances: `DecidableEq` gives us an
algorithm to compare two states (needed later so `decide` can evaluate claims), and `Repr` gives
us printing (useful when debugging a stuck proof with `#eval`). -/

/-- The lifecycle state of a single Nexus operation. Mirrors `NexusState` in `nexus_operation.go`.

    Six of the ten are terminal; the operation stops moving once it reaches one. -/
inductive OpState where
  /-- The operation does not exist yet — nothing has been scheduled. The initial state. -/
  | unspecified
  /-- Scheduled and being attempted: Temporal is trying to reach the handler. -/
  | scheduled
  /-- A retryable attempt failed; waiting out the backoff timer before the next attempt. -/
  | backingOff
  /-- The handler acknowledged asynchronously and is now doing the work. **This is the state the
      whole AutoClose design is about**: there is a live handler out there, and a token to cancel
      it with. -/
  | started
  /-- Terminal: completed successfully. -/
  | succeeded
  /-- Terminal: completed with a failure. -/
  | failed
  /-- Terminal: cancelled, and the cancellation was accepted. -/
  | canceled
  /-- Terminal: the schedule-to-close deadline elapsed. -/
  | timedOut
  /-- Terminal: forcibly terminated via an external RPC (standalone operations only). -/
  | terminated
  /-- Terminal: the start request was refused synchronously — bad input, unknown endpoint — before
      the operation ever existed. -/
  | rejected
  deriving DecidableEq, Repr

/-- The events that move an operation between states. Mirrors `NexusEvent`.

    Note there is no "force close the caller" event here: closing the *caller* is not an event of
    the *operation's* lifecycle. That separation is exactly what makes the clash in Layer 3
    possible, since the cancellation outlives the thing that created it. -/
inductive OpEvent where
  /-- Schedule an attempt. Fires on creation, and again after each backoff. -/
  | schedule
  /-- A retryable attempt failure; sends the operation into backoff. -/
  | attemptFailed
  /-- The handler acknowledged asynchronously. -/
  | start
  /-- The handler reported success. -/
  | succeed
  /-- The handler reported failure. -/
  | fail
  /-- The cancellation was accepted and the operation settled as cancelled. -/
  | cancel
  /-- The schedule-to-close timer elapsed. -/
  | timeout
  /-- An external terminate RPC. -/
  | terminate
  /-- The start request was refused synchronously. -/
  | reject
  deriving DecidableEq, Repr

/-! ## Lean concept: total functions, pattern matching, and `Option`

Every Lean function is **total**: it must return a value for every input, and the compiler
verifies that the pattern match is exhaustive. There is no "falls off the end" and no null.

But a transition function genuinely has no answer for most (state, event) pairs — there is no
edge from `succeeded` on `start`. The idiomatic way to express "sometimes there is no answer"
without giving up totality is `Option`:

* `none` — no result;
* `some x` — the result is `x`.

So `step` is total as a Lean function while faithfully representing a *partial* transition graph.

**Why `Option` and not a total function into states?** The runtime's oracle (`Lifecycle.Classify`
in the umpire framework) is total: it answers `Advance`, `NoOp`, or `Illegal` for every pair, and
it tolerates observational forward jumps because it must classify whatever it sees on the wire.
This model is deliberately narrower: `none` means *there is no direct edge*, full stop. Narrower
is the right call for a model, because every edge we decline to include is an edge the later
proofs cannot secretly rely on. -/

/-- The direct transition graph. `none` wherever the canonical Go graph has no edge.

    Read the clauses as the edge list they are; each group is annotated with the runtime reasoning
    it mirrors. -/
def step : OpState → OpEvent → Option OpState
  -- `schedule` fires on creation, and again on each retry out of backoff.
  | .unspecified, .schedule      => some .scheduled
  | .backingOff,  .schedule      => some .scheduled
  -- A retryable attempt failure sends the operation into backoff.
  | .scheduled,   .attemptFailed => some .backingOff
  -- The async acknowledgement. It fires from `scheduled` only: a synchronous completion skips
  -- `started` entirely, which is why "started precedes succeeded" is NOT an invariant here.
  | .scheduled,   .start         => some .started
  -- Settlement fires either from a running attempt (`scheduled`) or from an async completion
  -- (`started`). It cannot fire from `backingOff`: the operation leaves that state only via the
  -- backoff timer, so no attempt is in flight to report anything.
  | .scheduled,   .succeed       => some .succeeded
  | .started,     .succeed       => some .succeeded
  | .scheduled,   .fail          => some .failed
  | .started,     .fail          => some .failed
  | .scheduled,   .cancel        => some .canceled
  | .started,     .cancel        => some .canceled
  -- Timeout fires from every active state. The schedule-to-close timer runs independently of the
  -- retry cycle, so it can fire mid-backoff.
  | .scheduled,   .timeout       => some .timedOut
  | .backingOff,  .timeout       => some .timedOut
  | .started,     .timeout       => some .timedOut
  -- Terminate is an external RPC against a standalone operation; it forces any active state to
  -- the terminated terminal.
  | .scheduled,   .terminate     => some .terminated
  | .backingOff,  .terminate     => some .terminated
  | .started,     .terminate     => some .terminated
  -- Rejection happens before the operation exists, so it fires only from `unspecified`.
  | .unspecified, .reject        => some .rejected
  -- Everything else: no edge. This wildcard is doing real work — it is the claim that the list
  -- above is complete.
  | _,            _              => none

/-! ## Lean concept: `Bool` versus `Prop`

Lean distinguishes **data** from **statements**, and beginners trip on this constantly.

* `Bool` is a datatype with two values, `true` and `false`. You can compute with it, branch on
  it, store it in a structure.
* `Prop` is the universe of *propositions* — things that are either true or false as a matter of
  mathematics, and whose "value" is a proof.

`Terminal s = true` is a `Prop` (a claim about a `Bool`). `Terminal s` on its own is a `Bool`.
The two are bridged by `decide`, which turns a decidable proposition into a computation.

We use `Bool` for things the model *computes* (`Terminal`, `delivers`) and `Prop` for the
obligations we *state* (`Honored`, `AtMostOneEvent`, `Reachable`). Rule of thumb: if you want to
`decide` it on a concrete value, make it `Bool`; if it quantifies over everything, make it
`Prop`. -/

/-- Is this a state the operation can never leave? Six of the ten states are terminal.

    Note that terminal-ness is *derived* from the graph rather than declared: the theorem below
    proves that these six are exactly the states with no outgoing edge. -/
def Terminal : OpState → Bool
  | .succeeded | .failed | .canceled | .timedOut | .terminated | .rejected => true
  | _ => false

/-! ## Lean concept: `example` and `rfl` — unit tests the compiler runs

An `example` is a claim with no name. Lean checks it and discards it. That makes it the natural
way to write **tests that are checked at build time**: if the graph changes so that one of these
no longer holds, the build breaks.

Each is proved by `rfl`, short for *reflexivity*. `rfl` proves `a = b` whenever both sides reduce
to literally the same value by computation. Since `step` is a plain pattern match on a finite
type, Lean just evaluates it. No cleverness required — and that is the point: these tests are
free, and they document the graph in the language of the domain.

The tests below are chosen to cover the same behaviors the Go test suite exercises. -/

-- Creation, backoff and retry, async acknowledgement:
example : step .unspecified .schedule = some .scheduled := rfl
example : step .scheduled .attemptFailed = some .backingOff := rfl
example : step .backingOff .schedule = some .scheduled := rfl
example : step .scheduled .start = some .started := rfl

-- Settlement from both settleable states. The second one is the synchronous-completion path,
-- and is the reason "an operation must pass through `started`" is NOT an invariant:
example : step .started .succeed = some .succeeded := rfl
example : step .scheduled .succeed = some .succeeded := rfl
example : step .started .cancel = some .canceled := rfl

-- Timeout and terminate reach every active state, including mid-backoff:
example : step .scheduled .timeout = some .timedOut := rfl
example : step .backingOff .timeout = some .timedOut := rfl
example : step .started .timeout = some .timedOut := rfl
example : step .scheduled .terminate = some .terminated := rfl
example : step .backingOff .terminate = some .terminated := rfl
example : step .started .terminate = some .terminated := rfl

-- Rejection only before the operation exists:
example : step .unspecified .reject = some .rejected := rfl
example : step .scheduled .reject = none := rfl

-- Representative *illegal* pairs. Negative tests matter as much as positive ones: they pin down
-- that the graph is not accidentally permissive. No settlement out of backoff, no restarting a
-- started operation:
example : step .backingOff .succeed = none := rfl
example : step .backingOff .cancel = none := rfl
example : step .started .start = none := rfl
example : step .started .schedule = none := rfl

/-! ## Lean concept: theorems and tactic proofs

An `example` checks one concrete case. A `theorem` states something about *all* cases, and needs
a real proof.

The proof below is written in **tactic mode**, introduced by `by`. Tactics manipulate a *goal*
— the thing left to prove — until nothing remains. Three appear here:

* `cases s` — split into one branch per constructor of `s`. Since `OpState` has ten constructors
  and `OpEvent` nine, `cases s <;> cases e` produces ninety goals.
* `<;>` — "and then, to every goal produced". This is what makes ninety goals tolerable to write.
* `simp_all [Terminal, step]` — simplify everything in sight, unfolding `Terminal` and `step`.

In the ninety goals, two things happen. Where `s` is *not* terminal, the hypothesis `h` reads
`false = true`, which is absurd, so the goal closes by contradiction. Where `s` *is* terminal,
`step s e` evaluates to `none` and the goal closes by computation. Exhaustive brute force —
entirely appropriate, because the type is finite and small. -/

/-- **Terminal states absorb every event.** No direct edge leaves them.

    This is the sanity layer. It is not what the file is *about*, but if it failed, the graph
    would be wrong and nothing built on top of it would mean anything. -/
theorem terminal_step_none (s : OpState) (e : OpEvent) (h : Terminal s = true) :
    step s e = none := by
  cases s <;> cases e <;> simp_all [Terminal, step]

/-! ## Lean concept: inductive *predicates*

This is the most important Lean idea in the file, so it gets its own section.

`inductive` does not only build datatypes; it also builds **relations**, by listing the ways a
fact can be established. `ReachableState` below says: `unspecified` is reachable, and if `s` is
reachable and there is an edge from `s` to `s'`, then `s'` is reachable.

Two things make this powerful:

1. It defines the **smallest** relation closed under those rules. "Reachable" therefore means
   *there exists a finite derivation* — a concrete chain of steps from the start. Nothing is
   reachable by accident or by assumption.
2. Lean automatically generates an **induction principle**. To prove that some property holds of
   every reachable thing, it suffices to check it holds at the start and is preserved by each
   rule. We use exactly that in Layer 3 to prove an invariant.

This is the formal counterpart of `Lifecycle.Reachable` in the umpire framework, which computes
reachability as the legal-edge closure from the initial state. -/

/-- States reachable from `unspecified` by following direct edges. -/
inductive ReachableState : OpState → Prop where
  /-- Every operation begins life in `unspecified`. -/
  | init : ReachableState .unspecified
  /-- Follow one edge of the graph. -/
  | step {s s' : OpState} (e : OpEvent) : ReachableState s → step s e = some s' → ReachableState s'

/-- Terminal absorption, restricted to reachable states — the form the design cares about.

    Note the proof is just the earlier theorem: reachability is irrelevant here, because
    absorption holds of *every* terminal state, reachable or not. Writing the weaker statement
    anyway is worth it because it is the one a reader is looking for; the `_` in the binder
    marks the hypothesis as deliberately unused. -/
theorem reachable_terminal_absorbing {s : OpState} (_ : ReachableState s)
    (h : Terminal s = true) (e : OpEvent) : step s e = none :=
  terminal_step_none s e h

/-! # Layer 2 — the AutoClose configuration

Layer 1 described one operation in isolation. AutoClose is about the *relationship* between an
operation, its caller, and the clock, so we need a richer object.

## The rule this layer encodes

From `../../spec.md`, and it is worth reading twice:

> A forced-close cancel fires at (or after) the op's schedule-to-close deadline, so remaining time
> is ~0. Clamping the cancel RPC to it would starve it below `MinRequestTimeout` and it would
> never reach the handler. System-initiated cancels carry `auto_close=true` and **skip the
> clamp**; user-initiated cancels keep it (a user cancel with no time left is intentionally *not*
> sent).

That paragraph becomes the four-line function `deliverable` below. If the runtime rule ever
changes, that function is what must change with it — and the negative check in the plan
(deliberately break `deliverable`, confirm the proofs fail) exists to prove the proofs actually
depend on it. -/

/-- Who asked for the cancellation. **This is the coordinate the entire file is about.** -/
inductive Initiator where
  /-- A user explicitly requested cancellation. Subject to the clamp. -/
  | user
  /-- Temporal generated the cancellation from the AutoClose policy. Exempt from the clamp. -/
  | system
  deriving DecidableEq, Repr

/-- The per-operation AutoClose policy, captured when the operation is scheduled. -/
inductive Policy where
  /-- Leave the handler running on a forced close. Today's behavior, and the default. -/
  | abandon
  /-- Deliver a cancel to the handler on a forced close. -/
  | requestCancel
  deriving DecidableEq, Repr

/-! ## Lean concept: `structure`

A `structure` is an inductive type with exactly one constructor, presented as named fields. Lean
generates a projection per field (`c.op`, `c.policy`, …) and supports **functional update**
syntax, `{ c with slack := false }`, which builds a copy differing in one field. Nothing is
mutated; `Config` values are immutable, and every "transition" below produces a new one.

### Why `cancels` is a `List` and not an `Option`

This is the single most consequential modelling decision in the file, so it deserves the space.

The obvious encoding of "the operation has at most one pending cancellation" is
`cancels : Option Initiator`. It is also a *trap*: it makes "at most one" true **by
construction**, so the `duplicate` resolution could not even be expressed, and obligation O‑3
could not be stated, let alone violated. The model would then "prove" that all three resolutions
are fine — by having quietly assumed away the thing that distinguishes them.

Using a `List` costs a little convenience and buys the ability to *state* uniqueness as a
property (`AtMostOneEvent`) and then discover which resolutions actually have it. When you model,
prefer the representation that lets the bug exist. -/

/-- One Nexus operation together with everything about its caller that affects delivery.

    Each pending cancellation here corresponds to one `NexusOperationCancelRequested` history
    event in the runtime — which is what makes the list length meaningful in Layer 3. -/
structure Config where
  /-- The operation's lifecycle state, from Layer 1. -/
  op : OpState
  /-- The AutoClose policy the caller opted into. -/
  policy : Policy
  /-- Pending cancellations, in creation order. -/
  cancels : List Initiator
  /-- Is the caller root — the workflow, or the standalone entity — still running? -/
  callerOpen : Bool
  /-- Is there enough time left before schedule-to-close to satisfy `MinRequestTimeout`?
      `true` means a clamped request would still be sent; `false` means it would be starved.

      Modelling time as a `Bool` is a real limitation, stated plainly in the header: it captures
      "the clamp bites" and nothing else. No retry schedule, no backoff, no retention. -/
  slack : Bool
  deriving DecidableEq, Repr

/-- **The clamp.** A user-initiated cancel with no time left is dropped; a system-initiated one is
    sent regardless. Four lines, and every theorem below depends on them. -/
def deliverable : Initiator → Bool → Bool
  | .system, _     => true
  | .user,   slack => slack

/-- Does *anything* actually reach the handler? True when at least one pending cancellation
    survives the clamp.

    `List.any` is the usual "does some element satisfy this predicate". -/
def delivers (c : Config) : Bool := c.cancels.any (fun k => deliverable k c.slack)

/-- Is any pending cancellation system-initiated? Used to state the root lemma: this is the fact
    that the runtime stores in the `auto_close` flag, and the fact we will show cannot be
    recomputed from anything else. -/
def hasSystem (c : Config) : Bool :=
  c.cancels.any (fun k => match k with | .system => true | .user => false)

/-! # Layer 3 — the hook, the clash, and the four obligations

Now the actual design question. The operation holds a cancellation slot. A user cancel may
already be pending when the caller is force-closed and AutoClose wants to cancel too.

Rather than pick a resolution and model it — which would beg the question — we make the hook
**parameterised by the resolution** and let each one earn or fail its verdict. This is a general
technique worth stealing: when a design has an open choice, make the choice a parameter of the
model, and let the theorems discriminate. -/

/-- The three candidate resolutions of the user-vs-system cancellation clash. -/
inductive Resolution where
  /-- Defer to the pending user cancel: create nothing. -/
  | skip
  /-- Create a second cancellation alongside the user's. -/
  | duplicate
  /-- Promote the pending user cancel in place to system-initiated. -/
  | upgrade
  deriving DecidableEq, Repr

/-- How each resolution updates the pending-cancellation list.

    The empty case comes first and is shared: with nothing pending there is no clash, and every
    resolution simply creates the system cancellation. The resolutions differ *only* when
    something is already there. -/
def applyResolution : Resolution → List Initiator → List Initiator
  | _,          []  => [.system]
  | .skip,      ks  => ks
  | .duplicate, ks  => ks ++ [.system]
  | .upgrade,   ks  => ks.map (fun _ => .system)

/-- The force-close hook: case **[B]** (workflow terminate / fail / complete / continue-as-new /
    run- or execution-timeout) and case **[D]** (standalone close).

    Note what it does *not* do: the operation stays in `started`. Only the caller closes. That is
    faithful to the runtime — the operation remains STARTED on the closed run, and the detached
    cancellation is what keeps delivering afterwards. -/
def autoClose (r : Resolution) (c : Config) : Config :=
  if c.policy = .requestCancel ∧ c.op = .started then
    { c with cancels := applyResolution r c.cancels, callerOpen := false }
  else
    { c with callerOpen := false }

/-! ## Reachability, lifted to configurations

Same idea as `ReachableState`, one level up. Two details are worth pausing on.

**Why is `Reachable` parameterised by the resolution?** Because a deployment runs *one*
resolution, and which configurations can arise depends on it. Writing `Reachable r c` keeps the
model honest: a witness used to refute `skip` must be reachable *under* `skip`.

**Why does `advance` keep the pending cancellations?** Look at the constructor: when the
operation transitions, `cancels` is carried along untouched. That is case **[A]** deferred
removal from `nexus_events.go` — a timed-out operation is held resident while a cancellation is
still pending, instead of being removed immediately. Model it wrong and the witness pair for the
root lemma would not exist. -/

/-- Configurations reachable under a chosen resolution `r`.

    Read each constructor as "here is one way the world can move". -/
inductive Reachable (r : Resolution) : Config → Prop where
  /-- A fresh operation under some policy: nothing scheduled, nothing pending, caller running,
      plenty of time. -/
  | init (p : Policy) :
      Reachable r { op := .unspecified, policy := p, cancels := [], callerOpen := true, slack := true }
  /-- The operation follows a direct edge while the caller is running. Pending cancellations
      survive the transition — case **[A]** deferred removal. -/
  | advance {c : Config} (e : OpEvent) {s' : OpState} :
      Reachable r c → c.callerOpen = true → step c.op e = some s' →
      Reachable r { c with op := s' }
  /-- A user requests cancellation of a started operation. Requires the slot to be empty: this is
      the *first* cancellation, and the clash is what happens to it later. -/
  | userCancel {c : Config} :
      Reachable r c → c.op = .started → c.cancels = [] → c.callerOpen = true →
      Reachable r { c with cancels := [.user] }
  /-- Time passes: the schedule-to-close deadline is now too close to clamp against. -/
  | deadline {c : Config} :
      Reachable r c → Reachable r { c with slack := false }
  /-- Case **[A]**: the operation's *own* schedule-to-close fires under `REQUEST_CANCEL` while the
      caller is still running. A system cancellation is created, the operation is held resident in
      `timedOut`, and there is no time left. The same clash resolution applies here as at a
      forced close. -/
  | opDeadline {c : Config} :
      Reachable r c → c.policy = .requestCancel → c.op = .started → c.callerOpen = true →
      Reachable r { c with op := .timedOut, slack := false, cancels := applyResolution r c.cancels }

/-! ## The root lemma: the initiator is not derivable

`../../spec.md` asserts that distinguishing user- from system-initiated cancels is "required and
not derivable from lifecycle state". That is exactly the kind of claim that is easy to assert,
easy to believe, and easy to be wrong about — and if it *were* derivable, the whole `auto_close`
flag (and its reset gap, and this file) would be unnecessary.

So we prove it, by exhibiting two configurations that agree on everything a dispatcher could
observe about the lifecycle, yet require opposite behavior:

* `wSystemAtDeadline` — case **[A]**: the operation's own deadline fired under `REQUEST_CANCEL`,
  so AutoClose created a system cancellation. It must skip the clamp and be sent.
* `wUserAtDeadline` — a user cancelled the started operation, and then the same deadline elapsed.
  It must keep the clamp and *not* be sent.

Both end at `op = timedOut`, `callerOpen = true`, `slack = false`, with exactly one pending
cancellation. Same lifecycle coordinates, opposite required dispatch. Therefore no function of
those coordinates can decide it. -/

/-- Witness `c₁`: case **[A]** — the operation's own schedule-to-close fired under
    `REQUEST_CANCEL`, so the cancellation is system-initiated. -/
def wSystemAtDeadline : Config :=
  { op := .timedOut, policy := .requestCancel, cancels := [.system], callerOpen := true, slack := false }

/-- Witness `c₂`: a user cancelled the started operation, and the same schedule-to-close then
    elapsed. Indistinguishable from `wSystemAtDeadline` on every lifecycle coordinate. -/
def wUserAtDeadline : Config :=
  { op := .timedOut, policy := .requestCancel, cancels := [.user], callerOpen := true, slack := false }

/-! ### Lean concept: building a proof by chaining constructors

The next two proofs are the most concrete in the file, and the best place to see what "there
exists a derivation" really means.

`have h := ...` introduces a named intermediate fact. Each line applies one constructor of
`Reachable` to the previous fact, so the block reads as an execution trace: create, schedule,
start, then the interesting bit. `exact h` finishes by saying "the goal is precisely this".

The `rfl` arguments discharge the side conditions each constructor demands — `c.callerOpen =
true`, `step c.op e = some s'` — and each holds by plain computation on the concrete config.
`(r := r)` supplies a named argument explicitly, because Lean cannot infer the resolution from
`init` alone.

Note these proofs work for *every* `r`: neither witness needs a particular resolution, because
neither one goes through a clash. -/

/-- `wSystemAtDeadline` really can happen: create, schedule, start, then the case-**[A]**
    deadline. -/
theorem wSystemAtDeadline_reachable (r : Resolution) : Reachable r wSystemAtDeadline := by
  have h0 := Reachable.init (r := r) .requestCancel
  have h1 := Reachable.advance .schedule h0 rfl rfl
  have h2 := Reachable.advance .start h1 rfl rfl
  have h3 := Reachable.opDeadline h2 rfl rfl rfl
  exact h3

/-- `wUserAtDeadline` really can happen too: the same prefix, then a user cancel, then the
    operation times out and the clock runs out. -/
theorem wUserAtDeadline_reachable (r : Resolution) : Reachable r wUserAtDeadline := by
  have h0 := Reachable.init (r := r) .requestCancel
  have h1 := Reachable.advance .schedule h0 rfl rfl
  have h2 := Reachable.advance .start h1 rfl rfl
  have h3 := Reachable.userCancel h2 rfl rfl rfl
  have h4 := Reachable.advance .timeout h3 rfl rfl
  have h5 := Reachable.deadline h4
  exact h5

/-! ### Lean concept: proving a negation

`¬ P` is *notation* for `P → False`. So to prove a negation you assume the thing and derive a
contradiction — and to refute a `∀`, you instantiate it at a witness where it fails. That
pattern, "assume the universal, apply it to a concrete counterexample, watch it explode", is the
shape of every impossibility proof in this file.

Tactics new here:

* `rintro ⟨f, hf⟩` — introduce the assumption and destructure it in one move. An `∃` packages a
  value with a proof about it, so this names them `f` and `hf`.
* `simp ... at h1 h2` — simplify hypotheses rather than the goal.
* `rw [h1] at h2` — rewrite using an equation.
* `absurd h (by decide)` — `h` says something; `by decide` computes that its negation holds;
  together they produce `False`. `decide` works here precisely because `Bool` equality is
  computable. -/

/-- **No function of the lifecycle projection computes the clamp decision.**

    Concretely: there is no `f` taking the operation's state and whether time remains, that tells
    you whether a system-initiated cancellation is pending. The two witnesses above agree on both
    inputs and disagree on the output, so any such `f` would have to return two different values
    for the same arguments.

    This is why `auto_close` must be *stored* rather than recomputed — and, in Layer 4, why it
    must be written to the event log rather than reconstructed after a reset. -/
theorem initiator_not_derivable :
    ¬ ∃ f : OpState → Bool → Bool,
        ∀ c, Reachable .upgrade c → f c.op c.slack = hasSystem c := by
  rintro ⟨f, hf⟩
  have h1 := hf wSystemAtDeadline (wSystemAtDeadline_reachable .upgrade)
  have h2 := hf wUserAtDeadline (wUserAtDeadline_reachable .upgrade)
  simp [wSystemAtDeadline, wUserAtDeadline, hasSystem] at h1 h2
  rw [h1] at h2
  exact absurd h2 (by decide)

/-! ## O‑1 — delivery

The obligation the whole feature exists to provide: if the caller opted into `REQUEST_CANCEL` and
was force-closed while the operation was `started`, something must reach the handler. -/

/-- **O‑1.** The delivery obligation, as a proposition about a post-close configuration.

    Written as a chain of implications: *if* the policy was `REQUEST_CANCEL`, *and* the caller is
    closed, *and* the operation is `started`, *then* a cancellation survives the clamp. -/
def Honored (c : Config) : Prop :=
  c.policy = .requestCancel → c.callerOpen = false → c.op = .started → delivers c = true

/-- The clash itself: a user cancellation is already pending on a started operation, the clock has
    run out, and the caller is about to be force-closed. -/
def wClash : Config :=
  { op := .started, policy := .requestCancel, cancels := [.user], callerOpen := true, slack := false }

/-- The clash configuration is reachable under every resolution. -/
theorem wClash_reachable (r : Resolution) : Reachable r wClash := by
  have h0 := Reachable.init (r := r) .requestCancel
  have h1 := Reachable.advance .schedule h0 rfl rfl
  have h2 := Reachable.advance .start h1 rfl rfl
  have h3 := Reachable.userCancel h2 rfl rfl rfl
  have h4 := Reachable.deadline h3
  exact h4

/-- **`skip` is unsound — the headline result.**

    Deferring to the pending user cancellation leaves a cancellation that the clamp will drop. The
    caller opted into `REQUEST_CANCEL`, the caller was force-closed, and *nothing reaches the
    handler*. The policy silently does nothing.

    Note how the proof ends: after `simp` unfolds the definitions at the specialised hypothesis,
    the hypothesis reduces to a false statement, and `simp` closes the goal by contradiction with
    no further tactic. A proof that ends "early" like this is normal, not a mistake.

    This failure is invisible to testing in the worst way — there is no error, no event, no
    retry. Just a handler that runs forever. -/
theorem skip_violates_delivery :
    ¬ ∀ c, Reachable .skip c → Honored (autoClose .skip c) := by
  intro h
  have := h wClash (wClash_reachable .skip)
  simp [Honored, autoClose, applyResolution, delivers, deliverable, wClash] at this

/-! ### Three small lemmas, and why to bother

The next two theorems say the hook leaves the policy and the operation state alone. They look
trivial, and they are — but they let the delivery proofs move a hypothesis about the *post*-close
configuration onto the *pre*-close one, which is what makes those proofs short.

`unfold autoClose; split <;> rfl` reads: expand the definition, split the `if` into its two
branches, and close both by computation.

Extracting small obvious lemmas like this is ordinary proof engineering, and the same instinct as
extracting a helper function: it keeps the interesting proof about the interesting thing. -/

/-- The hook never moves the operation. -/
theorem autoClose_op (r : Resolution) (c : Config) : (autoClose r c).op = c.op := by
  unfold autoClose; split <;> rfl

/-- The hook never rewrites the policy. -/
theorem autoClose_policy (r : Resolution) (c : Config) : (autoClose r c).policy = c.policy := by
  unfold autoClose; split <;> rfl

/-- When the guard holds, the hook takes its interesting branch. Lets later proofs skip the `if`. -/
theorem autoClose_of_guard {r : Resolution} {c : Config}
    (hp : c.policy = .requestCancel) (ho : c.op = .started) :
    autoClose r c = { c with cancels := applyResolution r c.cancels, callerOpen := false } := by
  simp [autoClose, hp, ho]

/-! ### Lean concept: `cases` on a value, and stating the strongest theorem

The two delivery proofs below share a shape:

1. `intro hp _ ho` — `Honored` is a chain of implications, so introducing its three hypotheses
   leaves the conclusion as the goal. The `_` names the middle one nothing, because it is unused.
2. `rw [autoClose_policy] at hp` — move the hypothesis from the post-close config to `c` itself.
3. `rw [autoClose_of_guard hp ho]` — replace the hook with its taken branch.
4. `cases c.cancels with | nil => ... | cons k ks => ...` — the list is either empty or not, and
   the two cases need different reasoning.

Notice these theorems take **no reachability hypothesis**. That was not laziness: delivery holds
for *every* configuration, reachable or not, which is a strictly stronger claim and a simpler
proof. Prove the strongest true statement; derive the weaker one if a reader wants it (as the two
`_reachable` corollaries below do). -/

/-- `duplicate` delivers: appending a system cancellation guarantees one survivor. It pays for
    this in O‑3. -/
theorem duplicate_honors_delivery (c : Config) : Honored (autoClose .duplicate c) := by
  intro hp _ ho
  rw [autoClose_policy] at hp
  rw [autoClose_op] at ho
  rw [autoClose_of_guard hp ho]
  cases c.cancels with
  | nil => simp [delivers, applyResolution, deliverable]
  | cons k ks => simp [delivers, applyResolution, deliverable]

/-- **`upgrade` delivers.** Promoting every pending cancellation to system-initiated makes them
    all clamp-exempt, so the list is non-empty and every element survives. -/
theorem upgrade_honors_delivery (c : Config) : Honored (autoClose .upgrade c) := by
  intro hp _ ho
  rw [autoClose_policy] at hp
  rw [autoClose_op] at ho
  rw [autoClose_of_guard hp ho]
  cases c.cancels with
  | nil => simp [delivers, applyResolution, deliverable]
  | cons k ks => simp [delivers, applyResolution, deliverable]

/-- O‑1 for `duplicate`, restricted to reachable configurations — the form stated in the plan. -/
theorem duplicate_honors_delivery_reachable (c : Config) (_ : Reachable .duplicate c) :
    Honored (autoClose .duplicate c) := duplicate_honors_delivery c

/-- O‑1 for `upgrade`, restricted to reachable configurations. -/
theorem upgrade_honors_delivery_reachable (c : Config) (_ : Reachable .upgrade c) :
    Honored (autoClose .upgrade c) := upgrade_honors_delivery c

/-! ## O‑2 — no regression for the default policy

A new feature must not change behavior for anyone who did not ask for it. `ABANDON` is the
default, so the hook must be inert under it — for all three resolutions. -/

/-- **O‑2.** Under `ABANDON` no resolution ever creates a cancellation; the pending list is
    untouched. Today's behavior is preserved exactly. -/
theorem abandon_creates_no_cancel (r : Resolution) (c : Config) (h : c.policy = .abandon) :
    (autoClose r c).cancels = c.cancels := by
  simp [autoClose, h]

/-! ## O‑3 — event uniqueness, the obligation that decides the design

Both `duplicate` and `upgrade` deliver, so O‑1 does not separate them. Something must, or the
design question stays open — and this is where modelling `cancels` as a list pays off.

In the runtime, each pending cancellation corresponds to one `NexusOperationCancelRequested`
history event. Two events for one operation make a reset rebuild ambiguous: replay has to decide
what two cancel-requests for the same operation mean. That is precisely the concern recorded as
"not sure how to do this so it works with Workflow Reset" in the design discussion.

So: at most one cancellation per operation. `upgrade` keeps it. `duplicate` does not. -/

/-- **O‑3.** At most one pending cancellation, hence at most one history event, per operation. -/
def AtMostOneEvent (c : Config) : Prop := c.cancels.length ≤ 1

/-- Non-duplicating resolutions never lengthen the pending list beyond one.

    `simpa [foo] using h` means "simplify `h` with `foo`, and use the result to close the goal" —
    the one-step form of `simp at h; exact h`. `exact absurd rfl hr` discharges the `duplicate`
    branch, which the hypothesis `hr` has excluded. -/
theorem applyResolution_length_le {r : Resolution} (hr : r ≠ .duplicate) (ks : List Initiator)
    (h : ks.length ≤ 1) : (applyResolution r ks).length ≤ 1 := by
  cases ks with
  | nil => simp [applyResolution]
  | cons k ks' =>
    cases r with
    | skip => simpa [applyResolution] using h
    | duplicate => exact absurd rfl hr
    | upgrade => simpa [applyResolution] using h

/-! ### Lean concept: proving an invariant by induction on a derivation

This is the payoff of defining `Reachable` inductively.

`induction h with | init p => ... | advance e _ _ _ ih => ...` walks the *derivation* of
reachability, giving one case per constructor. In each case, `ih` is the induction hypothesis: the
property already holds of the configuration we came from. So the proof obligation is exactly the
engineer's intuition — *true at the start, and preserved by every step* — and Lean checks that no
step was forgotten. Add a constructor to `Reachable` later and this proof will fail to compile
until you handle it. That is the property you want from a model. -/

/-- Under any non-duplicating resolution, an operation never accumulates more than one pending
    cancellation. Proved by induction over how the configuration was reached. -/
theorem cancels_le_one {r : Resolution} (hr : r ≠ .duplicate) {c : Config}
    (h : Reachable r c) : c.cancels.length ≤ 1 := by
  induction h with
  | init p => simp
  | advance e _ _ _ ih => simpa using ih
  | userCancel _ _ _ _ ih => simp
  | deadline _ ih => simpa using ih
  | opDeadline _ _ _ _ ih => exact applyResolution_length_le hr _ ih

/-- **O‑3 holds for `upgrade`.** Promoting in place preserves the list's length, so one event
    stays one event.

    `by_cases hg : P` splits on whether the guard holds — the propositional counterpart of
    `cases` on a `Bool`. -/
theorem upgrade_preserves_uniqueness (c : Config) (h : Reachable .upgrade c) :
    AtMostOneEvent (autoClose .upgrade c) := by
  have hle := cancels_le_one (by decide) h
  by_cases hg : c.policy = .requestCancel ∧ c.op = .started
  · simp [AtMostOneEvent, autoClose, hg]
    exact applyResolution_length_le (by decide) _ hle
  · simpa [AtMostOneEvent, autoClose, hg] using hle

/-- **`duplicate` breaks event uniqueness — the result that selects `upgrade`.**

    It delivers, but on the clash configuration it emits a second `NexusOperationCancelRequested`
    for a single operation. Same refutation shape as `skip_violates_delivery`: instantiate the
    universal at a reachable witness and let it contradict itself. -/
theorem duplicate_breaks_uniqueness :
    ¬ ∀ c, Reachable .duplicate c → AtMostOneEvent (autoClose .duplicate c) := by
  intro h
  have := h wClash (wClash_reachable .duplicate)
  simp [AtMostOneEvent, autoClose, applyResolution, wClash] at this

/-! # Layer 4 — history, reset, and the stamp

One Temporal mechanism is left to model, and it turns the same bug up a second time.

A **reset** rewinds a workflow to an earlier point and replays its history into a fresh run.
Anything not written to the history does not survive. And today, `auto_close` is set on the
cancellation component *after* the `NexusOperationCancelRequested` event and is not among its
attributes — so a reset that rebuilds a still-pending auto-close cancellation recreates it as
**user**-initiated. The clamp reapplies. The cancel is never sent.

That is `../../spec.md`'s "`auto_close` flag is not event-sourced (reset gap)", and by the root
lemma it cannot be patched over by inspecting the rebuilt state: the initiator is not recoverable
from anything else. It has to be *written down*.

`Stamped` below is deliberately abstract about how. It is satisfied by Candidate A (an explicit
system principal on `HistoryEvent.principal`) and by Candidate B (a dedicated attributes field)
alike. The model constrains only that the initiator reach the event; choosing between the two is
an engineering decision the model does not make. -/

/-- The caller-side history events this model cares about. -/
inductive Event where
  /-- The operation was scheduled, capturing the policy. -/
  | scheduled (p : Policy)
  /-- The handler acknowledged asynchronously. -/
  | started
  /-- A cancellation was requested. `principal = none` is today's event: **the initiator is not
      recorded**. `some k` is the fix, under either candidate. -/
  | cancelRequested (principal : Option Initiator)
  /-- The operation's schedule-to-close deadline elapsed. -/
  | timedOut
  deriving Repr

/-- The state a fresh run starts from before any history is replayed. -/
def initialConfig : Config :=
  { op := .unspecified, policy := .abandon, cancels := [], callerOpen := true, slack := true }

/-- Replay history onto a configuration, one event at a time.

    The `cancelRequested` clause contains the entire reset gap in one expression:
    `p.getD .user` means "use `p` if it is `some`, otherwise default to `.user`". An unstamped
    event therefore rebuilds as user-initiated — not through a bug, but because there is nothing
    else it *could* do. -/
def rebuildFrom (c : Config) : List Event → Config
  | []                        => c
  | .scheduled p :: es        => rebuildFrom { c with op := .scheduled, policy := p } es
  | .started :: es            => rebuildFrom { c with op := .started } es
  | .cancelRequested p :: es  => rebuildFrom { c with cancels := c.cancels ++ [p.getD .user] } es
  | .timedOut :: es           => rebuildFrom { c with op := .timedOut, slack := false } es

/-- A reset replays caller-side history into a fresh run. -/
def rebuild (h : List Event) : Config := rebuildFrom initialConfig h

/-- The caller-side history a configuration would have produced. `stamp = false` is today's
    behavior: the `cancelRequested` events carry no initiator. -/
def emit (stamp : Bool) (c : Config) : List Event :=
  [.scheduled c.policy, .started]
    ++ c.cancels.map (fun k => Event.cancelRequested (if stamp then some k else none))
    ++ (if c.op = .timedOut then [Event.timedOut] else [])

/-- **O‑4.** A rebuild is faithful when it reconstructs the initiators the original run had. -/
def Faithful (c c' : Config) : Prop := c'.cancels = c.cancels

/-! ### Lean concept: induction over lists, and generalising the hypothesis

The next three lemmas are proved by `induction … generalizing c`. The `generalizing` part is the
subtle bit and a classic beginner stumbling block: `rebuildFrom` threads a changing configuration
through the recursion, so the induction hypothesis must hold for *every* starting configuration,
not just the one we began with. Without `generalizing c`, the hypothesis is too weak and the
proof gets stuck. When an induction mysteriously will not close, an over-specific hypothesis is
the first thing to suspect. -/

/-- Replaying `a` then `b` is replaying their concatenation. Lets later proofs decompose `emit`,
    which is built from three appended pieces. -/
theorem rebuildFrom_append (c : Config) (a b : List Event) :
    rebuildFrom c (a ++ b) = rebuildFrom (rebuildFrom c a) b := by
  induction a generalizing c with
  | nil => simp [rebuildFrom]
  | cons e es ih => cases e <;> simp [rebuildFrom, ih]

/-- Replaying stamped cancel-requests appends exactly the initiators they carry. -/
theorem rebuildFrom_stamped_cancels (c : Config) (ks : List Initiator) :
    (rebuildFrom c (ks.map (fun k => Event.cancelRequested (some k)))).cancels
      = c.cancels ++ ks := by
  induction ks generalizing c with
  | nil => simp [rebuildFrom]
  | cons k ks ih => simp [rebuildFrom, ih]

/-- Replaying *unstamped* cancel-requests appends `user` for every one of them, whatever the
    original initiators were. The information is simply gone. -/
theorem rebuildFrom_unstamped_cancels (c : Config) (ks : List Initiator) :
    (rebuildFrom c (ks.map (fun _ => Event.cancelRequested none))).cancels
      = c.cancels ++ ks.map (fun _ => Initiator.user) := by
  induction ks generalizing c with
  | nil => simp [rebuildFrom]
  | cons k ks ih => simp [rebuildFrom, ih]

/-- A trailing `timedOut` event does not touch the pending cancellations.

    The `(P : Prop) [Decidable P]` binders are how Lean says "for any proposition you can actually
    branch on" — needed because `emit` ends with an `if` on a proposition rather than a `Bool`. -/
theorem rebuildFrom_timedOut_cancels (c : Config) (P : Prop) [Decidable P] :
    (rebuildFrom c (if P then [Event.timedOut] else [])).cancels = c.cancels := by
  by_cases h : P <;> simp [h, rebuildFrom]

/-- **With the initiator on the event, a reset is faithful.** Holds for every configuration. -/
theorem rebuild_stamped_is_faithful (c : Config) : Faithful c (rebuild (emit true c)) := by
  simp [Faithful, rebuild, emit, rebuildFrom_append, rebuildFrom, rebuildFrom_timedOut_cancels,
        rebuildFrom_stamped_cancels, initialConfig]

/-! ### Lean concept: proving an existential

`refine ⟨a, b, ?_⟩` supplies the parts of a structure and leaves `?_` as a remaining goal. For an
`∃ c, P c ∧ Q c`, that means "here is the witness, here is the first component, now let me prove
the rest". It is the constructive reading of "there exists": you must produce the thing. -/

/-- **Without the stamp, the initiator is lost.** The case-**[A]** configuration had a
    system-initiated cancellation; its unstamped history rebuilds a user-initiated one. -/
theorem rebuild_unstamped_loses_initiator :
    ∃ c, Reachable .upgrade c ∧ ¬ Faithful c (rebuild (emit false c)) := by
  refine ⟨wSystemAtDeadline, wSystemAtDeadline_reachable .upgrade, ?_⟩
  simp [Faithful, rebuild, emit, wSystemAtDeadline, rebuildFrom, initialConfig]

/-- **The punchline, and why O‑4 is an obligation rather than a nicety.**

    An unstamped rebuild silently converts a cancel that *would* have reached the handler into one
    the clamp drops. That is the same observable failure as choosing `skip` — an orphaned handler
    and no error anywhere — reached by an entirely different road. Two roads to one bug is a
    strong hint that the missing initiator is the real defect, and both O‑1 and O‑4 are symptoms
    of it. -/
theorem unstamped_rebuild_drops_delivery :
    ∃ c, Reachable .upgrade c
       ∧ delivers c = true
       ∧ delivers (rebuild (emit false c)) = false := by
  refine ⟨wSystemAtDeadline, wSystemAtDeadline_reachable .upgrade, by decide, ?_⟩
  simp [delivers, deliverable, rebuild, emit, wSystemAtDeadline, rebuildFrom, initialConfig]

/-- …and with the stamp, the very same rebuild still delivers. The fix works. -/
theorem stamped_rebuild_keeps_delivery :
    delivers (rebuild (emit true wSystemAtDeadline)) = true := by
  simp [delivers, deliverable, rebuild, emit, wSystemAtDeadline, rebuildFrom, initialConfig]

/-! # Verdict

What the kernel accepted, gathered in one place:

| | Obligation | `skip` | `duplicate` | `upgrade` |
|---|---|---|---|---|
| **O‑1** | delivery | ✗ `skip_violates_delivery` | ✓ `duplicate_honors_delivery` | ✓ `upgrade_honors_delivery` |
| **O‑2** | no regression | ✓ | ✓ | ✓ `abandon_creates_no_cancel` |
| **O‑3** | event uniqueness | ✓ | ✗ `duplicate_breaks_uniqueness` | ✓ `upgrade_preserves_uniqueness` |
| **O‑4** | reset faithful | `rebuild_stamped_is_faithful` — only with the initiator on the event |

**Upgrade the pending user cancellation in place, and stamp the initiator on the event.**

One cost the model does not capture, stated here so it is not lost: under `upgrade`, a user cancel
that would have been correctly dropped is now sent, because a forced close happened. That is a
judgement call — defensible, since the caller opted into `REQUEST_CANCEL` — and not a theorem.

## Where to go next

* `../../spec.md` — the design, including the locked decision this file produced.
* `../../lean-model-plan.md` — how the model was scoped, and the negative checks that keep it
  honest. Those are worth understanding: *deliberately break `deliverable` and confirm the proofs
  fail.* A model whose theorems survive a broken definition is proving nothing, and that check is
  the only thing standing between this file and comfortable fiction.
-/

end NexusAutoClose
