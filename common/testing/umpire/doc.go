// Package umpire is a generic, Temporal-agnostic framework for property-based test
// monitoring: it observes a running system, builds an executable model of its entities from a
// stream of facts, and judges that model with rules. The Temporal-specific entities, facts,
// and rules live in tests/umpirev1; the whole-system pitch — the Monitor / Driver / Planner
// parts and the environment/capability model — lives in the repo-root UMPIRE_SPEC.md and its
// siblings.
//
// # Naming convention
//
// A *Registry holds what is declared before a run ([RuleRegistry], and the planned
// CoverpointRegistry); *State and logs hold what accumulates during one ([ModelState],
// [FactLog]). One wrinkle: [ModelState] is the runtime model but also carries entity/fact
// registration ([ModelState.RegisterEntity] / [ModelState.RegisterFact]) — the declared
// "entity registry" role and the runtime "model state" role still share one type.
//
// # Model
//
//   - [Fact] — a normalized unit of observation addressed to one entity; inert data
//     ([Fact.Name], [Fact.TargetEntity]). A [BroadcastFact] fans out to every entity of a type.
//   - [Entity] — one piece of the model: a state machine that interprets facts via
//     [Entity.OnFact]. Addressed by [EntityType] / [EntityID] / [EntityPath]; created by an
//     [EntityFactory].
//   - [Lifecycle] — the reusable FSM primitive entities are built on: per-state entry times,
//     derived terminal states ([Lifecycle.IsTerminal]), a MustProgress set, and the transition
//     graph ([Lifecycle.Reachable] / [Lifecycle.Cells] / [Lifecycle.Validate]).
//   - [Lifecycle.Classify] — the total transition function: every (state, event) maps to
//     [Advance], [NoOp], or [Illegal] (an [Outcome]). Being total is the source of "no vacuous
//     pass"; [Lifecycle.Fire] is defined over it.
//   - [Flag] — a named boolean an entity sets/clears on transitions; a small observable rules
//     and debugging can read.
//   - [ModelState] — declared role: [ModelState.RegisterEntity] / [ModelState.RegisterFact]. Runtime
//     role: [ModelState.RouteFacts] routes each fact to its entity and stamps a generation;
//     [ModelState.QueryEntities] returns only entities changed since a watermark.
//   - [FactLog] — an append-only, queryable record of every fact ([FactLog.QueryByType],
//     [FactLog.QueryByID]), independent of the FSMs.
//
// # Judging
//
//   - [SafetyRule] — must hold at every observation; a failed check is an immediate [Violation].
//   - [LivenessRule] — must eventually hold; tracked via [LivenessContext.Pending] /
//     [LivenessContext.Resolve], with anything unresolved at teardown becoming a [Violation].
//   - [SafetyContext] / [LivenessContext] — what a rule reads: changed entities (as
//     [EntityResult] values) plus [SafetyContext.Eval] / [SafetyContext.Pass].
//   - [RuleRegistry] — the name-validated rule registry that initialises and runs rules.
//   - [Violation] — {Rule, Message, Tags}; the framework's only output.
//
// # Coverage (planned — not yet in code)
//
//   - Coverpoint — a named condition worth reaching at least once, plus a Detect predicate.
//   - CoverpointRegistry — the declared registry of coverpoints (mirrors [RuleRegistry]).
//   - Coverage — the runtime tally of coverpoint hits; Unmet() is the reward signal for the
//     active side.
//
// # Observation and injection seams
//
//   - [FactRecorder] / [ResponseRecorder] — hooks that turn a gRPC request (and its response
//     or error) into facts, wired via [NewUnaryServerInterceptor].
//   - [FaultInjector] — the active hook ([FaultInjector.Inject]); wired but no-op today, the
//     seam the Driver's fault actions will use.
//   - [Instrument] / [RecordFact] — emit observations from inside the server as OTEL spans.
package umpire
