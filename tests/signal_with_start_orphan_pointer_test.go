package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type SignalWithStartOrphanPointerSuite struct {
	parallelsuite.Suite[*SignalWithStartOrphanPointerSuite]
}

func TestSignalWithStartOrphanPointerSuite(t *testing.T) {
	parallelsuite.Run(t, &SignalWithStartOrphanPointerSuite{})
}

// TestSignalWithStartAfterOrphanedCompletedPointer starts a workflow, completes it, then
// deletes mutable state and history while leaving current_executions in place. That is the
// corruption SignalWithStart used to livelock on. Closing the shard drops the cached copy
// so the next request has to load from persistence.
func (s *SignalWithStartOrphanPointerSuite) TestSignalWithStartAfterOrphanedCompletedPointer() {
	env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster())
	tv := testvars.New(s.T())
	ctx := s.Context()

	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		tv.WorkflowID(),
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	execMgr := env.GetTestCluster().ExecutionManager()
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	s.Require().NoError(err)
	orphanedRunID := startResp.RunId

	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			}},
		}, nil
	})
	s.Require().NoError(err)

	branchToken := captureCurrentBranchToken(ctx, env, tv.WorkflowID(), orphanedRunID)

	s.Require().NoError(execMgr.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		RunID:       orphanedRunID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}))
	s.Require().NoError(execMgr.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
	}))
	waitForMutableStateGone(ctx, env, shardID, execMgr, tv.WorkflowID(), orphanedRunID)

	current, err := execMgr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.Require().NoError(err)
	s.Require().Equal(orphanedRunID, current.RunID)
	s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, current.Status)

	env.CloseShard(env.NamespaceID().String(), tv.WorkflowID())

	swsCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	swsResp, err := env.FrontendClient().SignalWithStartWorkflowExecution(swsCtx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:             uuid.NewString(),
		Namespace:             env.Namespace().String(),
		WorkflowId:            tv.WorkflowID(),
		WorkflowType:          tv.WorkflowType(),
		TaskQueue:             tv.TaskQueue(),
		Identity:              "orphan-pointer-test",
		SignalName:            "repro",
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:    durationpb.New(30 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(5 * time.Second),
	})
	s.Require().NoError(err)
	s.Require().True(swsResp.Started)
	s.Require().NotEmpty(swsResp.RunId)
	s.Require().NotEqual(orphanedRunID, swsResp.RunId)

	replaced, err := execMgr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.Require().NoError(err)
	s.Require().Equal(swsResp.RunId, replaced.RunID)
}
