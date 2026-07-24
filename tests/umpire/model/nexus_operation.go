package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

const NexusOperationType = fact.NexusOperationType

var _ umpire.Entity = (*NexusOperation)(nil)
var _ umpire.Lifecycled = (*NexusOperation)(nil)

// NexusOperation mirrors the Nexus-operation HSM
// (components/nexusoperations/statemachine.go) as a Lifecycled entity. Its ID is
// "<callerWorkflowID>:<scheduledEventID>" and it is rooted under the caller
// Workflow (see UMPIRE_NEXUS.md).
type NexusOperation struct {
	ScheduledEventID string
	WorkflowID       string
	Outcome          string // set on a terminal transition, from the span's nexus.outcome
	FSM              *umpire.Lifecycle
}

func NewNexusOperation() *NexusOperation {
	op := &NexusOperation{}
	op.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "unspecified",
		Transitions: []umpire.Transition{
			// schedule fires on init and again on each retry out of backing_off.
			{Event: "schedule", From: []string{"unspecified", "backing_off"}, To: "scheduled"},
			// attempt_failed: a retryable attempt failure sends it into backoff.
			{Event: "attempt_failed", From: []string{"scheduled"}, To: "backing_off"},
			// start: the async handler acknowledged (sync completion skips this).
			{Event: "start", From: []string{"scheduled", "backing_off"}, To: "started"},
			// Terminal transitions may fire from scheduled/backing_off/started;
			// "started precedes succeeded" is NOT an invariant (sync completes direct).
			{Event: "succeed", From: []string{"scheduled", "backing_off", "started"}, To: "succeeded"},
			{Event: "fail", From: []string{"scheduled", "backing_off", "started"}, To: "failed"},
			{Event: "cancel", From: []string{"scheduled", "backing_off", "started"}, To: "canceled"},
			{Event: "timeout", From: []string{"scheduled", "backing_off", "started"}, To: "timed_out"},
		},
		// A scheduled/backing_off/started operation must eventually settle;
		// terminal states derive automatically.
		MustProgress: []string{"scheduled", "backing_off", "started"},
	})
	return op
}

func (op *NexusOperation) Type() umpire.EntityType { return NexusOperationType }

// Lifecycle exposes the operation's state machine to generic lifecycle rules.
func (op *NexusOperation) Lifecycle() *umpire.Lifecycle { return op.FSM }

// The *At accessors are derived from the lifecycle's per-state entry times, so
// "state reached ⇔ timestamp set" holds by construction.
func (op *NexusOperation) ScheduledAt() time.Time { t, _ := op.FSM.EnteredAt("scheduled"); return t }
func (op *NexusOperation) StartedAt() time.Time   { t, _ := op.FSM.EnteredAt("started"); return t }

// SettledAt returns when the operation reached a terminal state, and whether it has.
func (op *NexusOperation) SettledAt() (time.Time, bool) {
	for _, s := range []string{"succeeded", "failed", "canceled", "timed_out"} {
		if t, ok := op.FSM.EnteredAt(s); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func (op *NexusOperation) OnFact(ctx context.Context, ident *umpire.EntityPath, events iter.Seq[umpire.Fact]) error {
	if op.WorkflowID == "" && ident != nil {
		if parent := ident.Parent(); parent != nil && parent.EntityID.Type == WorkflowType {
			op.WorkflowID = parent.EntityID.ID
		}
	}

	for ev := range events {
		switch e := ev.(type) {
		case *fact.NexusOperationScheduled:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			op.FSM.Fire(ctx, "schedule")
		case *fact.NexusOperationAttemptFailed:
			op.FSM.Fire(ctx, "attempt_failed")
		case *fact.NexusOperationStarted:
			op.FSM.Fire(ctx, "start")
		case *fact.NexusOperationSucceeded:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, "succeed") {
				op.Outcome = e.Outcome
			}
		case *fact.NexusOperationFailed:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, "fail") {
				op.Outcome = e.Outcome
			}
		case *fact.NexusOperationCanceled:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, "cancel") {
				op.Outcome = e.Outcome
			}
		case *fact.NexusOperationTimedOut:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, "timeout") {
				op.Outcome = e.Outcome
			}
		}
	}
	return nil
}

func (op *NexusOperation) capture(scheduledEventID, workflowID string) {
	if op.ScheduledEventID == "" {
		op.ScheduledEventID = scheduledEventID
	}
	if op.WorkflowID == "" {
		op.WorkflowID = workflowID
	}
}

func (op *NexusOperation) String() string {
	return fmt.Sprintf("NexusOperation{workflowID=%s, scheduledEventID=%s, state=%s}",
		op.WorkflowID, op.ScheduledEventID, op.FSM.Current())
}
