package xdc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testhooks"
)

const (
	// syncWorkflowStateDelay is how long the active cluster stalls before answering the standby's
	// SyncWorkflowState request. It is deliberately longer than every deadline that used to bound
	// this code path:
	//
	//	taskTimeout                    3s   standby transfer task (transfer_queue_task_executor_base.go)
	//	client/admin.DefaultTimeout    10s  cross-cluster SyncWorkflowState leg
	//	client/history.DefaultTimeout  30s  child shard -> parent shard verify RPC
	//
	// so a parent pull that completes at all proves the verify call now runs on its own context
	// governed by history.verifyChildWorkflowCompletionRecordedTimeout, and that both client-side
	// ceilings were lifted for the two RPCs on this path.
	syncWorkflowStateDelay = 4 * time.Minute

	// stateSyncTimeout is the value given to history.ReplicationTaskApplyTimeout, the setting
	// under test. It must exceed syncWorkflowStateDelay for the resend to succeed.
	stateSyncTimeout = 5 * time.Minute

	// localParentVerificationWindow is shortened from its 5m default so the child stops trying to
	// verify against local (never-arriving) parent state and asks for a parent resend quickly.
	localParentVerificationWindow = 30 * time.Second
)

// verifyChildCompletionParentResendSuite drives the standby child -> parent verification path:
// the child workflow closes on the standby cluster while the parent workflow has never replicated
// there, so the only way the child's CloseExecutionTask can verify that the parent recorded its
// completion is to pull the parent's state from the active cluster.
type verifyChildCompletionParentResendSuite struct {
	xdcBaseSuite

	// parentWorkflowID identifies the workflow whose replication tasks are dropped on the standby.
	parentWorkflowID atomic.Pointer[string]
	// droppedParentTasks counts replication tasks withheld from the standby, proving the parent
	// really never replicated by the normal path.
	droppedParentTasks atomic.Int64
	// syncStateCalls counts SyncWorkflowState requests served by the active cluster, proving the
	// child actually triggered a parent pull.
	syncStateCalls atomic.Int64
}

func TestVerifyChildCompletionParentResendSuite(t *testing.T) {
	t.Parallel()
	s := &verifyChildCompletionParentResendSuite{}
	// resendParent is only set when transition history is enabled, which is what makes the
	// SyncWorkflowState-based parent pull available at all.
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *verifyChildCompletionParentResendSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():       true,
		dynamicconfig.EnableReplicationTaskBatching.Key(): true,

		// The setting under test: give the verify RPC (and the parent pull it performs) a budget
		// that comfortably covers syncWorkflowStateDelay.
		dynamicconfig.ReplicationTaskApplyTimeout.Key(): stateSyncTimeout,
		// The resend must run in the background; otherwise the standby task's own short deadline
		// bounds it and the stall below can never complete.
		dynamicconfig.EnableAsyncParentWorkflowResend.Key(): true,
		// Trigger the parent resend quickly instead of after the 5m default.
		dynamicconfig.MaxLocalParentWorkflowVerificationDuration.Key(): localParentVerificationWindow,
	}
	s.logger = log.NewTestLogger()
	s.setupSuite()

	// Standby cluster: withhold the parent workflow's replication tasks so the parent never
	// arrives by the normal replication path.
	s.clusters[1].InjectHook(s.T(), testhooks.NewHook(
		testhooks.HistoryReplicationTaskInterceptor,
		s.dropParentReplicationTask,
	), testhooks.GlobalScope)

	// Active cluster: stall the state pull so it cannot possibly fit inside any of the old
	// deadlines on this path.
	s.clusters[0].InjectHook(s.T(), testhooks.NewHook(
		testhooks.HistorySyncWorkflowStateInterceptor,
		s.delaySyncWorkflowState,
	), testhooks.GlobalScope)
}

func (s *verifyChildCompletionParentResendSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *verifyChildCompletionParentResendSuite) SetupTest() {
	s.setupTest()
}

// dropParentReplicationTask acknowledges, without applying, every inbound replication task that
// belongs to the parent workflow. Tasks for the child workflow (and everything else) are applied
// normally, so the child still closes on the standby cluster.
func (s *verifyChildCompletionParentResendSuite) dropParentReplicationTask(
	task *replicationspb.ReplicationTask,
	execute func() error,
) error {
	parentID := s.parentWorkflowID.Load()
	if parentID == nil {
		return execute()
	}

	var workflowID string
	switch {
	case task.GetHistoryTaskAttributes() != nil:
		workflowID = task.GetHistoryTaskAttributes().GetWorkflowId()
	case task.GetSyncVersionedTransitionTaskAttributes() != nil:
		workflowID = task.GetSyncVersionedTransitionTaskAttributes().GetWorkflowId()
	case task.GetVerifyVersionedTransitionTaskAttributes() != nil:
		workflowID = task.GetVerifyVersionedTransitionTaskAttributes().GetWorkflowId()
	case task.GetSyncWorkflowStateTaskAttributes() != nil:
		workflowID = task.GetSyncWorkflowStateTaskAttributes().GetWorkflowState().GetExecutionInfo().GetWorkflowId()
	case task.GetBackfillHistoryTaskAttributes() != nil:
		workflowID = task.GetBackfillHistoryTaskAttributes().GetWorkflowId()
	}

	if workflowID != *parentID {
		return execute()
	}

	dropped := s.droppedParentTasks.Add(1)
	s.logger.Info("TEST: dropping parent replication task on standby",
		tag.NewStringTag("parent-workflow-id", workflowID),
		tag.NewStringTag("replication-task-type", task.GetTaskType().String()),
		tag.NewInt64("dropped-so-far", dropped),
	)
	return nil
}

// delaySyncWorkflowState stalls the active cluster's answer to the standby's parent pull.
func (s *verifyChildCompletionParentResendSuite) delaySyncWorkflowState(
	ctx context.Context,
	request *historyservice.SyncWorkflowStateRequest,
	execute func() error,
) error {
	calls := s.syncStateCalls.Add(1)
	deadline := "none"
	if d, ok := ctx.Deadline(); ok {
		deadline = time.Until(d).String()
	}
	s.logger.Info("TEST: active cluster received SyncWorkflowState, stalling",
		tag.NewStringTag("workflow-id", request.GetExecution().GetWorkflowId()),
		tag.NewInt64("sync-state-calls", calls),
		tag.NewStringTag("incoming-ctx-remaining", deadline),
		tag.NewDurationTag("stall-for", syncWorkflowStateDelay),
	)

	select {
	case <-time.After(syncWorkflowStateDelay):
	case <-ctx.Done():
		// The caller's deadline expired while we stalled. Under the old hard-coded deadlines this
		// is the branch that would always be taken.
		s.logger.Error("TEST: SyncWorkflowState caller gave up while stalling",
			tag.NewStringTag("workflow-id", request.GetExecution().GetWorkflowId()),
			tag.Error(ctx.Err()),
		)
		return ctx.Err()
	}

	s.logger.Info("TEST: stall complete, serving SyncWorkflowState",
		tag.NewStringTag("workflow-id", request.GetExecution().GetWorkflowId()),
	)
	return execute()
}

// TestChildPullsParentWhenParentReplicationIsWithheld asserts that a standby child workflow can
// complete its parent-completion verification by pulling the parent from the active cluster, even
// when that pull takes far longer than the standby transfer task's own timeout and longer than the
// default history/admin client timeouts.
func (s *verifyChildCompletionParentResendSuite) TestChildPullsParentWhenParentReplicationIsWithheld() {
	// Budget: 30s to trigger the resend + a 4m stall + replication/verification slack. The
	// per-assertion Eventually budgets below can add up to ~9.5m in the worst case, so keep this
	// comfortably above that.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()

	// The resend, and the guard that suppresses duplicates, run on the parent's shard, which is on
	// the standby cluster.
	capture := s.clusters[1].Host().CaptureMetricsHandler().StartCapture()
	defer s.clusters[1].Host().CaptureMetricsHandler().StopCapture(capture)

	parentWorkflowID := "parent-" + uuid.NewString()
	childWorkflowID := "child-" + uuid.NewString()
	taskQueue := "tq-" + uuid.NewString()
	s.parentWorkflowID.Store(&parentWorkflowID)

	childWorkflow := func(workflow.Context) (string, error) {
		return "child-done", nil
	}
	// The parent records the child's completion and then stays running, so that the standby has a
	// running parent to verify against (a closed parent short-circuits verification).
	parentWorkflow := func(ctx workflow.Context) error {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: childWorkflowID,
		})
		var result string
		if err := workflow.ExecuteChildWorkflow(childCtx, childWorkflow).Get(childCtx, &result); err != nil {
			return err
		}
		return workflow.Await(ctx, func() bool { return false })
	}

	sdkClient, worker := s.newClientAndWorker(
		s.clusters[0].Host().FrontendGRPCAddress(), ns, taskQueue, "parent-resend-worker")
	defer sdkClient.Close()
	worker.RegisterWorkflow(parentWorkflow)
	worker.RegisterWorkflow(childWorkflow)
	s.NoError(worker.Start())
	defer worker.Stop()

	// Run the parent on the active cluster and wait until the child has closed there.
	_, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        parentWorkflowID,
		TaskQueue: taskQueue,
	}, parentWorkflow)
	s.NoError(err)

	s.logger.Info("TEST: waiting for child to close on the active cluster",
		tag.NewStringTag("child-workflow-id", childWorkflowID))
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.clusters[0].FrontendClient().DescribeWorkflowExecution(ctx,
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{WorkflowId: childWorkflowID},
			})
		require.NoError(t, err)
		require.NotNil(t, resp.GetWorkflowExecutionInfo().GetCloseTime(),
			"child should be closed on the active cluster")
	}, 60*time.Second, time.Second)

	// The child must replicate to the standby (its tasks are not dropped) so that its
	// CloseExecutionTask runs there and starts verifying against the parent.
	s.logger.Info("TEST: waiting for child to appear on the standby cluster")
	s.EventuallyWithT(func(t *assert.CollectT) {
		_, err := s.clusters[1].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: childWorkflowID},
		})
		require.NoError(t, err)
	}, 60*time.Second, time.Second)

	// The parent must NOT be on the standby: its replication tasks were dropped. This is what
	// forces the child's verification down the resend path.
	_, err = s.clusters[1].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: parentWorkflowID},
	})
	s.Error(err, "parent must not have replicated to the standby by the normal path")
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
	s.Greater(s.droppedParentTasks.Load(), int64(0), "expected parent replication tasks to be dropped")
	s.logger.Info("TEST: confirmed parent absent from standby",
		tag.NewInt64("dropped-parent-tasks", s.droppedParentTasks.Load()))

	// Now the real assertion. After localParentVerificationWindow the child asks the parent's
	// standby shard to resend the parent; that pull is stalled for syncWorkflowStateDelay by the
	// hook on the active cluster. If the verify call is still bounded by the 3s standby task
	// timeout (or the 30s history-client / 10s admin-client ceilings) the pull can never finish and
	// the parent will never materialize here.
	s.logger.Info("TEST: waiting for the child to pull the parent onto the standby",
		tag.NewDurationTag("local-verification-window", localParentVerificationWindow),
		tag.NewDurationTag("sync-stall", syncWorkflowStateDelay),
		tag.NewDurationTag("verify-timeout", stateSyncTimeout))

	s.EventuallyWithT(func(t *assert.CollectT) {
		_, err := s.clusters[1].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: parentWorkflowID},
		})
		require.NoError(t, err, "parent should have been pulled from the active cluster")
	}, localParentVerificationWindow+syncWorkflowStateDelay+3*time.Minute, 5*time.Second)

	s.Greater(s.syncStateCalls.Load(), int64(0),
		"the child should have triggered at least one SyncWorkflowState against the active cluster")

	// The child retried while the resend was stalled. Prove those retries were turned away by the
	// per-parent guard rather than each starting their own pull: the parent's shard lives on the
	// standby cluster, so that is where the metric is recorded.
	skipped := capture.Snapshot()["parent_workflow_resend_skipped"]
	s.NotEmpty(skipped, "expected the per-parent guard to reject at least one retry")
	s.logger.Info("TEST: parent successfully pulled onto standby",
		tag.NewInt64("sync-state-calls", s.syncStateCalls.Load()),
		tag.NewInt64("dropped-parent-tasks", s.droppedParentTasks.Load()))
}
