package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/umpire1/action"
	"go.temporal.io/server/tests/umpire1/model"
)

// TestWorkflowPlanEdge proves the entity-agnostic planner serves a second entity: the Workflow
// completion edge routes through start, so the plan is [StartWorkflowExecution, Signal(finish)] —
// computed by the same planEdge core NexusOperation uses.
func TestWorkflowPlanEdge(t *testing.T) {
	seq, err := action.WorkflowPlanEdge(model.WorkflowStarted, model.WorkflowComplete)
	require.NoError(t, err)
	require.Equal(t, []string{"StartWorkflowExecution", "SignalWorkflowExecution(finish)"}, names(seq))
}

// TestWorkflowAutoCoverPlans checks the coverage list is computed from the Workflow model: one
// settling edge (started→completed), one plan — computed by the same settlingEdgesFor core that
// serves NexusOperation.
func TestWorkflowAutoCoverPlans(t *testing.T) {
	plans := action.WorkflowAutoCoverPlans()
	require.Len(t, plans, 1, "the Workflow model has one settling edge")
	require.Equal(t, []string{"StartWorkflowExecution", "SignalWorkflowExecution(finish)"}, names(plans[0]))
}
