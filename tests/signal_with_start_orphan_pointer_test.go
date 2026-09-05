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
)

type SignalWithStartOrphanPointerSuite struct {
	parallelsuite.Suite[*SignalWithStartOrphanPointerSuite]
}

func TestSignalWithStartOrphanPointerSuite(t *testing.T) {
	parallelsuite.Run(t, &SignalWithStartOrphanPointerSuite{})
}

// Pointer exists, mutable state doesn't. Used to hang until the client deadline.
func (s *SignalWithStartOrphanPointerSuite) TestSignalWithStartAfterOrphanedCompletedPointer() {
	if !testcore.UseCassandraPersistence() {
		s.T().Skip("Cassandra only: SQL surfaces this as a unique constraint, not a hang")
	}

	env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster()) // CloseShard is cluster-wide
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
	env.NoError(err)
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
	env.NoError(err)

	// Mutable state + history go away; current_executions stays. That's the bug shape.
	env.NoError(execMgr.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		RunID:       orphanedRunID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}))
	waitForMutableStateGone(ctx, env, shardID, execMgr, tv.WorkflowID(), orphanedRunID)

	current, err := execMgr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	env.NoError(err)
	env.Equal(orphanedRunID, current.RunID)
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, current.Status)

	env.CloseShard(env.NamespaceID().String(), tv.WorkflowID()) // otherwise cache still has the completed MS

	swsCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	swsResp, err := env.FrontendClient().SignalWithStartWorkflowExecution(swsCtx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:             uuid.NewString(),
		Namespace:             env.Namespace().String(),
		WorkflowId:            tv.WorkflowID(),
		WorkflowType:          tv.WorkflowType(),
		TaskQueue:             tv.TaskQueue(),
		Identity:              tv.WorkerIdentity(),
		SignalName:            tv.SignalName(),
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	})
	env.NoError(err)
	env.True(swsResp.Started)
	env.NotEqual(orphanedRunID, swsResp.RunId)
	env.Equal(swsResp.RunId, swsResp.FirstExecutionRunId)

	replaced, err := execMgr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	env.NoError(err)
	env.Equal(swsResp.RunId, replaced.RunID)
}
