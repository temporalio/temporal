package tests

import (
	"testing"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type AdminForceDeleteSuite struct {
	parallelsuite.Suite[*AdminForceDeleteSuite]
}

func TestAdminForceDeleteSuite(t *testing.T) {
	parallelsuite.Run(t, &AdminForceDeleteSuite{})
}

// TestForceDeleteClearsCachedMutableState force-deletes a closed workflow that history has cached,
// then describes it without a reload, which is how force replication verifies the target cluster.
func (s *AdminForceDeleteSuite) TestForceDeleteClearsCachedMutableState() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	ctx := s.Context()

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: startResp.RunId}

	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: execution,
		Reason:            "close before force delete",
	})
	s.NoError(err)

	// Loads the closed workflow into the history cache.
	_, err = env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
	})
	s.NoError(err)

	_, err = env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)

	_, err = env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace:       env.Namespace().String(),
		Execution:       execution,
		SkipForceReload: true,
	})
	s.ErrorAs(err, new(*serviceerror.NotFound))
}
