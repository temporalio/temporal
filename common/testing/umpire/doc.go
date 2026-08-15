// Package umpire is a generic, Temporal-agnostic framework for model-based testing. It plans and
// realizes semantic actions, observes a running system as facts, builds an executable model, and
// evaluates that model with rules. The canonical Temporal protocol lives in tests/umpire2. See the
// repository-root UMPIRE.md for the system architecture.
//
// # Model
//
// A [Fact] is a normalized observation addressed by [EntityPath]. [ModelState] routes facts to
// [Entity] instances and tracks their generations. [Lifecycle] supplies the reusable transition
// function and classifies every observed event as [Advance], [NoOp], or [Illegal]. [RelationStore]
// holds typed cross-entity relationships, while [FactLog] retains the observation ledger.
//
// # Judging
//
// [RuleRegistry] evaluates [SafetyRule] invariants at runtime checkpoints and tracks
// [LivenessRule] obligations through [LivenessContext.Pending] and [LivenessContext.Resolve].
// Failures are reported as [Violation] values.
//
// # Planning and coverage
//
// [Action] declares semantic preconditions and effects. [Drive] executes actions through domain
// realizers and reconciles their observed effects. [PlanTo] and [Explore] search lifecycles under
// explicit [Constraints]. [Coverage] compares declared semantic obligations with observed points,
// and [GeneratePairwise] produces deterministic constrained combinations.
//
// # Observation and injection seams
//
// [FactRecorder], [ResponseRecorder], and [RejectionRecorder] expose observation contracts.
// [FaultInjector] provides the corresponding active fault seam. Concrete gRPC and OpenTelemetry
// adapters live in dedicated subpackages so the model-testing core remains transport-neutral.
package umpire
