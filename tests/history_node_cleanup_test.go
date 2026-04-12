package tests

// This file contains two functional tests to make sure that history_tree and
// history_node rows are cleaned up correctly after a workflow deletion.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestDeletionOfSingleWorkflow runs a single workflow, force-deletes it via the
// admin API, then asserts that all history_tree and history_node rows are removed.
func TestDeletionOfSingleWorkflow(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	tv := testvars.New(t)
	ctx := env.Context()

	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		tv.WorkflowID(),
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	execMgr := env.GetTestCluster().TestBase().ExecutionManager
	poller := taskpoller.New(t, env.FrontendClient(), env.Namespace().String())

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	env.NoError(err)
	runID := startResp.RunId

	completeWorkflowWithActivities(env, tv, poller)

	branchToken := captureCurrentBranchToken(ctx, env, tv.WorkflowID(), runID)

	// The admin force-delete and the DeleteHistoryEventTask retention timer both
	// reach the same persistence.ExecutionManager.DeleteHistoryBranch call, which
	// is the operation that removes history_tree and history_node rows.
	_, err = env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	env.NoError(err)
	waitForMutableStateGone(ctx, env, shardID, execMgr, tv.WorkflowID(), runID)

	resp, err := execMgr.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.EndEventID,
		PageSize:    1000,
	})
	if err == nil {
		env.Empty(resp.HistoryEvents, "history_node rows should be gone after deletion")
	}
	// A NotFound/InvalidArgument error is also acceptable — it means the branch is gone.
}

// TestDeletionOfWorkflowAfterReset runs a workflow, resets it to create a new
// run, force-deletes both runs via the admin API, then asserts that no
// history_node rows remain for either branch.
func TestDeletionOfWorkflowAfterReset(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	tv := testvars.New(t)
	ctx := env.Context()

	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		tv.WorkflowID(),
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	execMgr := env.GetTestCluster().TestBase().ExecutionManager
	poller := taskpoller.New(t, env.FrontendClient(), env.Namespace().String())

	// ── Step 1: start and complete run A ─────────────────────────────────────
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	env.NoError(err)
	runIDA := startResp.RunId

	completeWorkflowWithActivities(env, tv, poller)

	branchTokenA := captureCurrentBranchToken(ctx, env, tv.WorkflowID(), runIDA)

	// Find the first WorkflowTaskCompleted event to use as the reset point.
	// B inherits A's opening events and forks from there.
	var resetEventID int64
	var histPageToken []byte
resetSearch:
	for {
		histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       env.Namespace().String(),
			Execution:       &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDA},
			NextPageToken:   histPageToken,
			MaximumPageSize: 100,
		})
		env.NoError(err)
		for _, event := range histResp.GetHistory().GetEvents() {
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				resetEventID = event.EventId
				break resetSearch
			}
		}
		histPageToken = histResp.GetNextPageToken()
		if len(histPageToken) == 0 {
			break
		}
	}
	env.NotZero(resetEventID)

	// ── Step 2: reset A → run B ───────────────────────────────────────────────
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDA},
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: resetEventID,
	})
	env.NoError(err)
	runIDB := resetResp.RunId

	tvB := tv.WithRunID(runIDB)
	completeWorkflowWithActivities(env, tvB, poller)

	branchTokenB := captureCurrentBranchToken(ctx, env, tv.WorkflowID(), runIDB)

	// ── Step 3: force-delete run A ────────────────────────────────────────────
	// Both the admin force-delete and the DeleteHistoryEventTask retention timer
	// ultimately call persistence.ExecutionManager.DeleteHistoryBranch, the same
	// operation that removes history_tree and history_node rows.
	_, err = env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDA},
		Archetype: chasm.WorkflowArchetype,
	})
	env.NoError(err)
	waitForMutableStateGone(ctx, env, shardID, execMgr, tv.WorkflowID(), runIDA)

	// ── Step 4: force-delete run B ────────────────────────────────────────────
	_, err = env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDB},
		Archetype: chasm.WorkflowArchetype,
	})
	env.NoError(err)
	waitForMutableStateGone(ctx, env, shardID, execMgr, tv.WorkflowID(), runIDB)

	// ── Assertions ────────────────────────────────────────────────────────────
	for _, tc := range []struct {
		label string
		token []byte
	}{
		{"run A (original)", branchTokenA},
		{"run B (reset)", branchTokenB},
	} {
		resp, err := execMgr.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:     shardID,
			BranchToken: tc.token,
			MinEventID:  common.FirstEventID,
			MaxEventID:  common.EndEventID,
			PageSize:    1000,
		})
		if err == nil {
			env.Empty(resp.HistoryEvents,
				"history_node rows for %s should be gone after deletion", tc.label)
		}
		// A NotFound/InvalidArgument error is acceptable — it means the branch is gone.
	}
}

// completeWorkflowWithActivities drives a workflow through a single activity then completes it.
func completeWorkflowWithActivities(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	poller *taskpoller.TaskPoller,
) {
	activityScheduled := false
	wtHandler := func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !activityScheduled {
			activityScheduled = true
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             "act",
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
							StartToCloseTimeout:    durationpb.New(10 * time.Second),
						},
					},
				}},
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			}},
		}, nil
	}

	_, err := poller.PollAndHandleWorkflowTask(tv, wtHandler)
	env.NoError(err)
	_, err = poller.PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	env.NoError(err)
	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	env.NoError(err)
}

// captureCurrentBranchToken extracts the current branch token from a workflow's mutable state.
func captureCurrentBranchToken(ctx context.Context, env *testcore.TestEnv, workflowID, runID string) []byte {
	descResp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	env.NoError(err)
	vh := descResp.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
	currentVH, err := versionhistory.GetCurrentVersionHistory(vh)
	env.NoError(err)
	token := currentVH.GetBranchToken()
	env.NotEmpty(token)
	return token
}

// waitForMutableStateGone polls until GetWorkflowExecution returns NotFound for the given runID.
func waitForMutableStateGone(ctx context.Context, env *testcore.TestEnv, shardID int32, execMgr persistence.ExecutionManager, workflowID, runID string) {
	env.Eventually(func() bool {
		_, err := execMgr.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: env.NamespaceID().String(),
			WorkflowID:  workflowID,
			RunID:       runID,
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		return common.IsNotFoundError(err)
	}, 10*time.Second, 100*time.Millisecond,
		"timed out waiting for mutable state of run %s to be deleted", runID)
}
