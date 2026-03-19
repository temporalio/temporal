package entity

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/looplab/fsm"
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

// WorkflowUpdate represents a workflow update entity with live Markers.
type WorkflowUpdate struct {
	UpdateID    string
	WorkflowID  string
	HandlerName string
	FSM         *fsm.FSM

	AdmittedAt    time.Time
	AcceptedAt    time.Time
	CompletedAt   time.Time
	RejectedAt    time.Time
	IsSpeculative bool
	Outcome       string

	RequestCount int
	FirstSeenAt  time.Time
	LastSeenAt   time.Time

	// Live Flags — set by FSM transitions.
	Admitted  *umpire.Flag
	Accepted  *umpire.Flag
	Completed *umpire.Flag
	Rejected  *umpire.Flag
}

func NewWorkflowUpdate() *WorkflowUpdate {
	wu := &WorkflowUpdate{
		Admitted:  umpire.NewFlag("WorkflowUpdate:Admitted"),
		Accepted:  umpire.NewFlag("WorkflowUpdate:Accepted"),
		Completed: umpire.NewFlag("WorkflowUpdate:Completed"),
		Rejected:  umpire.NewFlag("WorkflowUpdate:Rejected"),
	}
	wu.FSM = fsm.NewFSM(
		"unspecified",
		fsm.Events{
			{Name: "admit", Src: []string{"unspecified"}, Dst: "admitted"},
			{Name: "accept", Src: []string{"admitted"}, Dst: "accepted"},
			{Name: "complete", Src: []string{"accepted"}, Dst: "completed"},
			{Name: "reject", Src: []string{"unspecified", "admitted", "accepted"}, Dst: "rejected"},
			// abort: update aborted by server (e.g., workflow closed, registry cleared).
			{Name: "abort", Src: []string{"unspecified", "admitted", "accepted"}, Dst: "aborted"},
		},
		fsm.Callbacks{},
	)
	return wu
}

func (wu *WorkflowUpdate) Type() umpire.EntityType {
	return WorkflowUpdateType
}

func (wu *WorkflowUpdate) OnFact(ctx context.Context, ident *umpire.Identity, events iter.Seq[umpire.Fact]) error {
	if wu.WorkflowID == "" && ident != nil && ident.ParentID != nil &&
		ident.ParentID.Type == WorkflowType {
		wu.WorkflowID = ident.ParentID.ID
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
				wu.UpdateID = e.UpdateID()
				wu.HandlerName = e.HandlerName()
			}
			if wu.FSM.Can("admit") {
				_ = wu.FSM.Event(ctx, "admit")
				wu.AdmittedAt = time.Now()
				wu.Admitted.Set()
			}
		case *fact.WorkflowUpdateAccepted:
			if wu.FSM.Can("accept") {
				_ = wu.FSM.Event(ctx, "accept")
				wu.AcceptedAt = time.Now()
				wu.Accepted.Set()
			}
		case *fact.WorkflowUpdateCompleted:
			if wu.UpdateID == "" {
				wu.UpdateID = e.UpdateID()
			}
			if wu.FSM.Can("complete") {
				_ = wu.FSM.Event(ctx, "complete")
				wu.CompletedAt = time.Now()
				wu.Completed.Set()
				if e.IsSuccess() {
					wu.Outcome = "success"
				} else {
					wu.Outcome = "failure"
				}
			}
		case *fact.WorkflowUpdateAborted:
			if wu.FSM.Can("abort") {
				_ = wu.FSM.Event(ctx, "abort")
			}
		case *fact.WorkflowUpdateRejected:
			if wu.UpdateID == "" {
				wu.UpdateID = e.UpdateID()
			}
			if wu.FSM.Can("reject") {
				_ = wu.FSM.Event(ctx, "reject")
				wu.RejectedAt = time.Now()
				wu.Rejected.Set()
			}
		}
	}
	return nil
}

func (wu *WorkflowUpdate) String() string {
	return fmt.Sprintf("WorkflowUpdate{updateID=%s, workflowID=%s, state=%s, handler=%s}",
		wu.UpdateID, wu.WorkflowID, wu.FSM.Current(), wu.HandlerName)
}
