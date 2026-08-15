package action

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// ---- Declared actions (Phase 1: the standalone completion path) ----

func nexusOp(v string, fresh bool) umpire.Ref {
	return umpire.Ref{Type: model.NexusOperationType, Var: v, Fresh: fresh}
}

var (
	StartStandalone = umpire.Action{
		Name: "StartNexusOperationExecution", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
		Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusSchedule}},
		Entry:   []string{"StartNexusOperationExecution"},
		// The internal calls a standalone start triggers: the CHASM operation task and the outbound
		// Nexus HTTP invocation to the handler (service/operation of the mock endpoint). Learned via
		// LearnFootprint; declared here so ReconcileFootprint catches wire-level drift.
		Footprint: []string{"StartNexusOperation", "HTTP POST /service/operation"},
		Realize:   rpcStartStandalone{},
	}
	HandlerAsyncAck = umpire.Action{
		Name: "handler:AsyncAck", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusStart}},
		Realize:  handlerAsync{},
	}
	CallbackSucceed = CompleteWith(nil, model.NexusSucceed)

	// ScheduleEmbedded creates the operation inside a caller workflow (embedded hosting),
	// realized by the real kitchensink interpreter (see kitchensink.go).
	ScheduleEmbedded = umpire.Action{
		Name: "cmd:ScheduleNexusOperation", Kind: umpire.WorkerCommand, Hosting: umpire.Embedded,
		Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusSchedule}},
		Realize: kitchensink{},
	}
)

// CompleteWith delivers an async completion (opErr nil = success) to a started operation,
// firing `event` (succeed/fail/cancel). Used by both hostings.
func CompleteWith(opErr *nexus.OperationError, event string) umpire.Action {
	return umpire.Action{
		Name: "callback:Complete", Kind: umpire.CompletionCallback,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusStarted}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: event}},
		Realize:  completion{opErr: opErr},
	}
}

// EmbeddedSucceed / EmbeddedFail / EmbeddedCancel are the embedded async-completion plans:
// schedule the operation via a caller workflow, async-ack the start, then complete it —
// started --> {succeeded, failed, canceled}. Computed by the planner.
func EmbeddedSucceed() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusSucceed, umpire.Embedded)
}
func EmbeddedFail() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusFail, umpire.Embedded)
}
func EmbeddedCancel() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusCancel, umpire.Embedded)
}

// HandlerSyncOk / HandlerOpFailed settle the operation directly from the start attempt.
var (
	HandlerSyncOk = umpire.Action{
		Name: "handler:SyncOk", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusSucceed}},
		Realize:  handlerSyncOk{},
	}
	HandlerOpFailed = umpire.Action{
		Name: "handler:OpFailed", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusFail}},
		Realize:  handlerOpFailed{},
	}
	HandlerOpCanceled = umpire.Action{
		Name: "handler:OpCanceled", Kind: umpire.HandlerResponse,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: model.NexusScheduled}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusCancel}},
		Realize:  handlerOpCanceled{},
	}
)

// TimerForceTimeout fires the schedule-to-close timeout from `from` (a
// testhooks.NexusForceTimeoutFrom* value: scheduled or backing_off) — timed_out.
func TimerForceTimeout(from string) umpire.Action {
	return umpire.Action{
		Name: "timer:ForceTimeout(" + from + ")", Kind: umpire.Timer,
		Effects: []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusTimeout}},
		Realize: timerForceTimeout{from: from},
	}
}

// EmbeddedSyncSuccess / EmbeddedOpFailure / EmbeddedScheduledCancel settle the operation from
// the start attempt: scheduled --> {succeeded, failed, canceled}. Computed by the planner.
func EmbeddedSyncSuccess() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusSucceed, umpire.Embedded)
}
func EmbeddedOpFailure() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusFail, umpire.Embedded)
}
func EmbeddedScheduledCancel() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusCancel, umpire.Embedded)
}

// EmbeddedTimeoutScheduled forces the timeout on the first attempt (scheduled --> timed_out);
// EmbeddedTimeoutBackingOff first fails retryably into backoff, then times out from there
// (backing_off --> timed_out). Computed by the planner.
func EmbeddedTimeoutScheduled() []umpire.Action {
	return mustPlan(model.NexusScheduled, model.NexusTimeout, umpire.Embedded)
}
func EmbeddedTimeoutBackingOff() []umpire.Action {
	return mustPlan(model.NexusBackingOff, model.NexusTimeout, umpire.Embedded)
}

// StandaloneCompletion is the Phase-1 plan: create a standalone operation, acknowledge the
// start asynchronously, then complete it — unspecified→scheduled→started→succeeded.
func StandaloneCompletion() []umpire.Action {
	return mustPlan(model.NexusStarted, model.NexusSucceed, umpire.Standalone)
}

// HandlerBlock holds the start attempt so the operation stays scheduled.
var HandlerBlock = umpire.Action{
	Name: "handler:Block", Kind: umpire.HandlerResponse,
	Realize: handlerBlock{},
}

// HandlerRetryable sends the operation into backoff: scheduled→backing_off.
var HandlerRetryable = umpire.Action{
	Name: "handler:RetryableError", Kind: umpire.HandlerResponse,
	Effects: []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusAttemptFailed}},
	Realize: handlerRetryable{},
}

// TerminateFrom is the terminate action gated on the operation being in `state` — the
// precondition is what pins which edge (state --terminate--> terminated) the plan exercises.
// It is Standalone-only (an embedded operation has no terminate RPC).
func TerminateFrom(state string) umpire.Action {
	return umpire.Action{
		Name: "TerminateNexusOperationExecution", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
		Requires: []umpire.Pre{{Ref: nexusOp("op", false), State: state}},
		Effects:  []umpire.Effect{{Ref: nexusOp("op", false), Event: model.NexusTerminate}},
		Entry:    []string{"TerminateNexusOperationExecution"},
		Realize:  rpcTerminate{},
	}
}

// StandaloneTerminate is the plan that reaches state --terminate--> terminated. The route to
// `state` needs a handler that holds the operation there; the planner's actionFor picks the
// attempt outcome (async→started, retryable→backing_off), but "hold in scheduled" has no
// outcome event, so that one case is completed with HandlerBlock here.
func StandaloneTerminate(state string) []umpire.Action {
	if state == model.NexusScheduled {
		return []umpire.Action{StartStandalone, HandlerBlock, TerminateFrom(state)}
	}
	return mustPlan(state, model.NexusTerminate, umpire.Standalone)
}
