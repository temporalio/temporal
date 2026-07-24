package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

const WorkflowUpdateType = fact.WorkflowUpdateType

type WorkflowUpdateID struct {
	parent WorkflowID
	id     string
}

func (u WorkflowUpdateID) String() string {
	return u.parent.String() + "/update:" + u.id
}

var _ umpire.Entity = (*WorkflowUpdate)(nil)
var _ umpire.Lifecycled = (*WorkflowUpdate)(nil)

// WorkflowUpdate represents a workflow update entity with live Markers.
type WorkflowUpdate struct {
	UpdateID    string
	WorkflowID  string
	HandlerName string
	FSM         *umpire.Lifecycle

	IsSpeculative bool
	Outcome       string

	RequestCount int
	FirstSeenAt  time.Time
	LastSeenAt   time.Time
}

func NewWorkflowUpdate() *WorkflowUpdate {
	wu := &WorkflowUpdate{}
	wu.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "unspecified",
		Transitions: []umpire.Transition{
			{Event: "admit", From: []string{"unspecified"}, To: "admitted"},
			{Event: "accept", From: []string{"admitted"}, To: "accepted"},
			// complete may fire directly from admitted when a worker accepts and
			// completes an update in the same workflow task (no separate accepted event).
			{Event: "complete", From: []string{"admitted", "accepted"}, To: "completed"},
			{Event: "reject", From: []string{"unspecified", "admitted", "accepted"}, To: "rejected"},
			// abort: update aborted by server (e.g., workflow closed, registry cleared).
			{Event: "abort", From: []string{"unspecified", "admitted", "accepted"}, To: "aborted"},
		},
		// Once admitted or accepted, an update must progress to a terminal state;
		// "unspecified" (requested but not admitted) is an acceptable resting point.
		MustProgress: []string{"admitted", "accepted"},
	})
	return wu
}

func (wu *WorkflowUpdate) Type() umpire.EntityType {
	return WorkflowUpdateType
}

// Lifecycle exposes the update's state machine to generic lifecycle rules.
func (wu *WorkflowUpdate) Lifecycle() *umpire.Lifecycle {
	return wu.FSM
}

// The *At accessors are derived from the lifecycle's per-state entry times, so
// "state reached ⇔ timestamp set" holds by construction (no separate fields to
// drift — this is why there is no state/timestamp-consistency rule).
func (wu *WorkflowUpdate) AdmittedAt() time.Time  { t, _ := wu.FSM.EnteredAt("admitted"); return t }
func (wu *WorkflowUpdate) AcceptedAt() time.Time  { t, _ := wu.FSM.EnteredAt("accepted"); return t }
func (wu *WorkflowUpdate) CompletedAt() time.Time { t, _ := wu.FSM.EnteredAt("completed"); return t }
func (wu *WorkflowUpdate) RejectedAt() time.Time  { t, _ := wu.FSM.EnteredAt("rejected"); return t }

func (wu *WorkflowUpdate) OnFact(ctx context.Context, ident *umpire.EntityPath, events iter.Seq[umpire.Fact]) error {
	if wu.WorkflowID == "" && ident != nil {
		if parent := ident.Parent(); parent != nil && parent.EntityID.Type == WorkflowType {
			wu.WorkflowID = parent.EntityID.ID
		}
	}

	for ev := range events {
		switch e := ev.(type) {
		case *fact.WorkflowUpdateRequested:
			if wu.UpdateID == "" {
				wu.UpdateID = e.UpdateID()
				wu.WorkflowID = e.WorkflowID()
				wu.HandlerName = e.HandlerName()
				wu.FirstSeenAt = time.Now()
			}
			wu.RequestCount++
			wu.LastSeenAt = time.Now()
		case *fact.WorkflowUpdateAdmitted:
			if wu.UpdateID == "" {
				wu.UpdateID = e.UpdateID
			}
			if wu.FSM.Fire(ctx, "admit") {
			}
		case *fact.WorkflowUpdateAccepted:
			if wu.FSM.Fire(ctx, "accept") {
			}
		case *fact.WorkflowUpdateCompleted:
			if wu.UpdateID == "" {
				wu.UpdateID = e.UpdateID
			}
			if wu.FSM.Fire(ctx, "complete") {
				if e.IsSuccess() {
					wu.Outcome = "success"
				} else {
					wu.Outcome = "failure"
				}
			}
		case *fact.WorkflowUpdateAborted:
			wu.FSM.Fire(ctx, "abort")
		case *fact.WorkflowUpdateRejected:
			if wu.UpdateID == "" {
				wu.UpdateID = e.UpdateID
			}
			if wu.FSM.Fire(ctx, "reject") {
			}
		}
	}
	return nil
}

func (wu *WorkflowUpdate) String() string {
	return fmt.Sprintf("WorkflowUpdate{updateID=%s, workflowID=%s, state=%s, handler=%s}",
		wu.UpdateID, wu.WorkflowID, wu.FSM.Current(), wu.HandlerName)
}
