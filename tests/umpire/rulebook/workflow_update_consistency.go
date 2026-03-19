package rulebook

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateStateConsistencyRule ensures FSM state matches timestamp fields.
type WorkflowUpdateStateConsistencyRule struct{}

func (m *WorkflowUpdateStateConsistencyRule) Name() string {
	return "WorkflowUpdateStateConsistencyRule"
}

func (m *WorkflowUpdateStateConsistencyRule) CheckSafety(c *umpire.SafetyContext) {
	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.UpdateID == "" {
			continue
		}
		state := wu.FSM.Current()
		violated := false

		if !wu.AcceptedAt.IsZero() &&
			state != "accepted" && state != "completed" && state != "rejected" && state != "aborted" {
			violated = true
			c.Eval(r.Key+":accept-mismatch", false, umpire.Violation{
				Message: "update has AcceptedAt timestamp but FSM is not in expected state",
				Tags: map[string]string{
					"workflowID":   wu.WorkflowID,
					"updateID":     wu.UpdateID,
					"currentState": state,
					"acceptedAt":   wu.AcceptedAt.Format(time.RFC3339),
				},
			})
		}

		if !wu.CompletedAt.IsZero() && state != "completed" {
			violated = true
			c.Eval(r.Key+":complete-mismatch", false, umpire.Violation{
				Message: "update has CompletedAt timestamp but FSM is not in completed state",
				Tags: map[string]string{
					"workflowID":   wu.WorkflowID,
					"updateID":     wu.UpdateID,
					"currentState": state,
					"completedAt":  wu.CompletedAt.Format(time.RFC3339),
				},
			})
		}

		if !wu.RejectedAt.IsZero() && state != "rejected" {
			violated = true
			c.Eval(r.Key+":reject-mismatch", false, umpire.Violation{
				Message: "update has RejectedAt timestamp but FSM is not in rejected state",
				Tags: map[string]string{
					"workflowID":   wu.WorkflowID,
					"updateID":     wu.UpdateID,
					"currentState": state,
					"rejectedAt":   wu.RejectedAt.Format(time.RFC3339),
				},
			})
		}

		if state == "completed" && wu.CompletedAt.IsZero() {
			violated = true
			c.Eval(r.Key+":missing-completed-at", false, umpire.Violation{
				Message: "update FSM is in completed state but CompletedAt is not set",
				Tags: map[string]string{
					"workflowID": wu.WorkflowID,
					"updateID":   wu.UpdateID,
				},
			})
		}

		if state == "accepted" && wu.AcceptedAt.IsZero() {
			violated = true
			c.Eval(r.Key+":missing-accepted-at", false, umpire.Violation{
				Message: "update FSM is in accepted state but AcceptedAt is not set",
				Tags: map[string]string{
					"workflowID": wu.WorkflowID,
					"updateID":   wu.UpdateID,
				},
			})
		}

		if !violated {
			c.Pass(r.Key)
		}
	}
}
