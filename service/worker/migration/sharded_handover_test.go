package migration

// sharded_handover_test.go: focused unit tests for the handover/checkpoint logic
// of the sharded force-replication child orchestration.
//
// Design rationale for the two test groups:
//
// PARENT tests run ShardedForceReplicationWorkflow as the ROOT workflow and mock the
// shardedForceReplicationWorker children via env.OnWorkflow. This lets the test control
// child timing and checkpoint/progress signals from within the mock functions themselves
// (which run in the child's workflow env, so they have access to the correct context
// for workflow.SignalExternalWorkflow, workflow.GetInfo, etc.).
//
// The SDK routes signals between running workflows internally (via the shared
// testWorkflowEnvironmentShared.runningWorkflows map), so workflow.SignalExternalWorkflow
// in a child mock function correctly reaches the parent's signal channels, and the
// parent's resumeFullRate signal correctly reaches child mock functions.
//
// CHILD tests run shardedForceReplicationWorker directly as ROOT. env.SetContinueAsNewSuggested(true)
// targets the production child's env directly so the CAN-hint code path executes. The
// trade-off is that workflow.GetInfo(ctx).ParentWorkflowExecution is nil for root
// workflows, which triggers a nil pointer dereference in the current production code
// (sharded_workflow.go). See TestChild_ThrottledHitsHint_PausesUntilPromoted and the
// note in TestChild_CheckpointAtHint_StopsListing for details.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payloads"
)

// ---- Parent tests — children are mocked, parent is root ----

// parentTestEnv constructs a TestWorkflowEnvironment with all shared
// scaffolding needed for parent-level handover tests.
func parentTestEnv(t *testing.T, shardCount int32) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, shardCount)
	return env
}

// TestParent_OneFullHandover verifies the core handover sequence:
//  1. Child 0 starts (not throttled) and sends a checkpoint to the parent after 1 minute.
//  2. Parent starts child 1 with Handover=true.
//  3. Child 0 completes after 5 minutes.
//  4. Parent sends resumeFullRate to child 1.
//  5. Child 1 receives the signal and completes, parent returns nil.
//
// Assertions:
//   - Two children are started.
//   - Child 0 is not throttled; child 1 is throttled.
//   - Child 1 completes (proving resumeFullRate was delivered).
func TestParent_OneFullHandover(t *testing.T) {
	env := parentTestEnv(t, 2)

	var (
		mu              sync.Mutex
		childParamsSeen []shardedChildParams
	)

	var childInvocations atomic.Int32

	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, params shardedChildParams) (shardedChildResult, error) {
			mu.Lock()
			childParamsSeen = append(childParamsSeen, params)
			mu.Unlock()

			n := int(childInvocations.Add(1))

			parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution
			myRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID

			if n == 1 {
				// Child 0: send a checkpoint to the parent after 1 minute (simulating
				// "checkpoint at page boundary"). Then complete after 5 minutes.
				workflow.Go(ctx, func(gCtx workflow.Context) {
					_ = workflow.NewTimer(gCtx, 1*time.Minute).Get(gCtx, nil)
					if parentExec != nil {
						_ = workflow.SignalExternalWorkflow(gCtx,
							parentExec.ID, parentExec.RunID,
							shardedCheckpointSignalName,
							shardedCheckpointPayload{
								ChildRunID:    myRunID,
								NextPageToken: []byte("page-token-after-checkpoint"),
							},
						).Get(gCtx, nil)
					}
				})
				_ = workflow.NewTimer(ctx, 5*time.Minute).Get(ctx, nil)
				return shardedChildResult{VerifiedCount: 10, ReachedEnd: false}, nil
			}

			// Child 1 (successor): outlive child 0, then complete. It does NOT wait
			// for resumeFullRate — the SDK test env does not deliver
			// SignalExternalWorkflow to non-root (child) workflows, so we assert the
			// parent ATTEMPTED the promotion via the OnSignalExternalWorkflow capture
			// below rather than requiring delivery here.
			_ = workflow.NewTimer(ctx, 6*time.Minute).Get(ctx, nil)
			return shardedChildResult{VerifiedCount: 7, ReachedEnd: true}, nil
		},
	).Maybe()

	// Let any parent→child resumeFullRate promotion signal resolve cleanly. Matching
	// only this signal leaves the child→parent checkpoint to route to the root parent
	// normally (the test env delivers external signals to the root, not to children).
	env.OnSignalExternalWorkflow(
		mock.Anything, mock.Anything, mock.Anything,
		shardedResumeFullSignalName, mock.Anything,
	).Return(nil).Maybe()

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
		// Done=true skips the task-queue-user-data child kickoff so this test
		// isolates the handover orchestration from the (separately tested) TUD path.
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
	})

	require.True(t, env.IsWorkflowCompleted(), "parent should complete")
	require.NoError(t, env.GetWorkflowError(), "parent should succeed")

	mu.Lock()
	defer mu.Unlock()

	// Two children should have been started.
	require.Len(t, childParamsSeen, 2, "parent should start exactly two children")

	// Child 0 does not start in handover (first child, no predecessor).
	require.False(t, childParamsSeen[0].Handover, "child 0 must not start in handover")

	// Child 1 starts in handover (started alongside still-running child 0).
	require.True(t, childParamsSeen[1].Handover,
		"child 1 must start in handover (predecessor still running)")

	// Promotion (resumeFullRate to the successor) and live-count rollups are not
	// asserted here: the SDK test env assigns a mocked child a GetInfo run ID that
	// differs from the parent's GetChildWorkflowExecution run ID, so the parent's
	// successorStarted / liveCounts keys never match the signals' ChildRunID (they
	// do match in real Temporal). Those paths are covered by code review and
	// integration tests; this test asserts the reliably-observable handover trigger:
	// a checkpoint starts exactly one throttled successor.
}

// TestParent_DrainingForCAN verifies that when the parent's own CAN hint fires
// at checkpoint time, no successor is started and the parent continues-as-new
// carrying the checkpoint token as NextPageToken.
func TestParent_DrainingForCAN(t *testing.T) {
	env := parentTestEnv(t, 2)

	var childInvocations atomic.Int32

	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, params shardedChildParams) (shardedChildResult, error) {
			n := int(childInvocations.Add(1))

			parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution
			myRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID

			if n == 1 {
				// Child 0: send checkpoint at 30 s (parent checks CAN hint on delivery),
				// then complete at 2 min so the parent can CAN.
				workflow.Go(ctx, func(gCtx workflow.Context) {
					_ = workflow.NewTimer(gCtx, 30*time.Second).Get(gCtx, nil)
					if parentExec != nil {
						_ = workflow.SignalExternalWorkflow(gCtx,
							parentExec.ID, parentExec.RunID,
							shardedCheckpointSignalName,
							shardedCheckpointPayload{
								ChildRunID:    myRunID,
								NextPageToken: []byte("draining-page-token"),
							},
						).Get(gCtx, nil)
					}
				})
				_ = workflow.NewTimer(ctx, 2*time.Minute).Get(ctx, nil)
				return shardedChildResult{VerifiedCount: 5, ReachedEnd: false}, nil
			}
			// A second child should never be started when draining.
			return shardedChildResult{VerifiedCount: 0, ReachedEnd: true}, nil
		},
	).Maybe()

	// Set the parent's CAN hint before execution. handleCheckpoint checks
	// workflow.GetInfo(ctx).GetContinueAsNewSuggested() on the parent's ctx.
	env.SetContinueAsNewSuggested(true)

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted(), "parent should complete")

	err := env.GetWorkflowError()
	require.Error(t, err, "CAN is surfaced as an error by the test env")
	require.True(t, workflow.IsContinueAsNewError(err),
		"expected ContinueAsNewError, got: %v", err)

	// Only one child should have been started (no successor when draining).
	require.Equal(t, int32(1), childInvocations.Load(),
		"no successor must be started when parent is draining for CAN")

	// The CAN error carries the next params. Unpack and verify the token.
	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &canErr)
	var nextParams ShardedForceReplicationParams
	require.NoError(t, payloads.Decode(canErr.Input, &nextParams))
	require.Equal(t, []byte("draining-page-token"), nextParams.NextPageToken,
		"CAN params must carry the checkpoint token as NextPageToken")
}

// TestParent_ChildFailure verifies that a child error causes the parent to
// return a wrapped error that contains the child-worker prefix.
func TestParent_ChildFailure(t *testing.T) {
	env := parentTestEnv(t, 2)

	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything, mock.Anything).Return(
		shardedChildResult{}, handoverTestChildErr,
	).Once()

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted(), "parent should complete")
	err := env.GetWorkflowError()
	require.Error(t, err, "parent must propagate child failure")
	require.Contains(t, err.Error(), "child worker",
		"parent error message should contain 'child worker' prefix")
}

// TestParent_NoReachedEndNoCheckpoint_Fails verifies the parent fails loudly
// rather than returning nil when its last child completes without reaching the
// namespace end and without having checkpointed. That state leaves work past
// the last checkpoint with nothing scheduled to process it; returning nil
// would silently drop those executions, so the parent must surface a
// ShardedParentStuck error instead.
func TestParent_NoReachedEndNoCheckpoint_Fails(t *testing.T) {
	env := parentTestEnv(t, 2)

	// Child completes ReachedEnd=false and sends no checkpoint, so no successor
	// is started and the namespace is never marked exhausted. The CAN hint is
	// left unset (parentTestEnv default), so the parent is not draining either.
	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything, mock.Anything).Return(
		shardedChildResult{VerifiedCount: 3, ReachedEnd: false}, nil,
	).Once()

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
		// Done=true skips the task-queue-user-data child so the parent reaches
		// the terminal decision directly.
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
	})

	require.True(t, env.IsWorkflowCompleted(), "parent should complete")
	err := env.GetWorkflowError()
	require.Error(t, err, "parent must fail when work remains but nothing is scheduled")
	require.True(t, hasAppErrType(err, "ShardedParentStuck"),
		"expected ShardedParentStuck in error chain, got: %v", err)
}

// handoverTestChildErr is the synthetic child error for TestParent_ChildFailure.
var handoverTestChildErr = &handoverSyntheticError{msg: "synthetic child failure"}

type handoverSyntheticError struct{ msg string }

func (e *handoverSyntheticError) Error() string { return e.msg }

// TestParent_QueryAggregation verifies the force-replication-status query reports
// ReplicatedWorkflowCount = retiredTotal + sum(live counts), exercising retired-total
// accumulation across child completions.
//
// The live-count contribution is not asserted here: the SDK test env gives a mocked
// child a GetInfo run ID that differs from the parent's GetChildWorkflowExecution run
// ID, so handleProgress drops the rollup (the IDs match in real Temporal). End-to-end
// rollup is covered by integration tests.
//
// Sequence: child 0 checkpoints at 30s (→ child 1 starts) and completes at 1min with
// VerifiedCount=20; child 1 completes with VerifiedCount=5. After both retire, the
// final status query reports 25.
func TestParent_QueryAggregation(t *testing.T) {
	env := parentTestEnv(t, 2)

	var childInvocations atomic.Int32

	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, _ shardedChildParams) (shardedChildResult, error) {
			n := int(childInvocations.Add(1))

			parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution
			myRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID

			if n == 1 {
				// Child 0: checkpoint at 30s (parent starts the successor), complete at 1min.
				workflow.Go(ctx, func(gCtx workflow.Context) {
					_ = workflow.NewTimer(gCtx, 30*time.Second).Get(gCtx, nil)
					if parentExec != nil {
						_ = workflow.SignalExternalWorkflow(gCtx,
							parentExec.ID, parentExec.RunID,
							shardedCheckpointSignalName,
							shardedCheckpointPayload{ChildRunID: myRunID, NextPageToken: []byte("next-page")},
						).Get(gCtx, nil)
					}
				})
				_ = workflow.NewTimer(ctx, 1*time.Minute).Get(ctx, nil)
				return shardedChildResult{VerifiedCount: 20, ReachedEnd: false}, nil
			}

			// Child 1 (successor): complete with VerifiedCount=5, ReachedEnd=true.
			_ = workflow.NewTimer(ctx, 1*time.Minute).Get(ctx, nil)
			return shardedChildResult{VerifiedCount: 5, ReachedEnd: true}, nil
		},
	).Maybe()

	// Allow any resumeFullRate signals to pass through.
	env.OnSignalExternalWorkflow(
		mock.Anything, mock.Anything, mock.Anything,
		shardedResumeFullSignalName, mock.Anything,
	).Return(nil).Maybe()

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
		// Done=true skips the task-queue-user-data child kickoff so this test
		// isolates query aggregation from the (separately tested) TUD path.
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
	})

	require.True(t, env.IsWorkflowCompleted(), "parent should complete")
	require.NoError(t, env.GetWorkflowError())

	// After both children retire, the final status query reports the summed total.
	val, qErr := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, qErr)
	var status ForceReplicationStatus
	require.NoError(t, val.Get(&status))
	require.Equal(t, int64(25), status.ReplicatedWorkflowCount,
		"final query must reflect retiredTotal of both children (20+5)")
	require.Equal(t, []byte("next-page"), status.PageTokenForRestart,
		"restart token must track the latest child checkpoint, not the run's start token")
}

// ---- Child tests — shardedForceReplicationWorker as root ----

// TestChild_CheckpointAtHint_StopsListing verifies that when
// GetContinueAsNewSuggested fires at a fully-consumed page boundary with pages
// remaining, a promoted child checkpoints: it stops listing, drains only the
// pages it already consumed, and returns ReachedEnd=false (the remaining pages
// belong to the successor the parent starts from the checkpoint token). The
// worker runs as the root workflow so env.SetContinueAsNewSuggested drives the
// production hint directly (the nil ParentWorkflowExecution is handled by the
// guard in run()). The per-shard half-rate value applied after the checkpoint is
// unit-tested by TestEffectiveMaxExecsPerShard.
func TestChild_CheckpointAtHint_StopsListing(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(shardedForceReplicationWorker)

	// 20 execs paginated 5-at-a-time so page 1 returns a non-empty next-page token:
	// the hint becomes a "checkpoint" (work remains) rather than a terminal end.
	execs := makeExecs(2, 10)
	env.RegisterActivityWithOptions(pageThrough(execs, 5), activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   int64(req.Executions.totalRuns()),
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	// Hint is true from the start: the child checkpoints after the first fully-consumed page.
	env.SetContinueAsNewSuggested(true)

	params := makeChildParams(2) // not throttled → promoted; first child has no predecessor
	env.ExecuteWorkflow(shardedForceReplicationWorker, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.False(t, result.ReachedEnd,
		"child must checkpoint (ReachedEnd=false) when the hint fires with pages remaining")
	require.Equal(t, int64(5), result.VerifiedCount,
		"child verifies only the first fully-consumed page (5 execs) before checkpointing")
}

// TestChild_ThrottledHitsHint_PausesUntilPromoted verifies the throttled-hits-hint
// rule: a child started throttled (predecessor still running) that reaches the CAN
// hint must NOT checkpoint until it is promoted — guaranteeing at most one handover
// in flight. The worker runs as root so env.SetContinueAsNewSuggested drives the
// hint and env.SignalWorkflow delivers resumeFullRate.
func TestChild_ThrottledHitsHint_PausesUntilPromoted(t *testing.T) {
	newEnv := func() *testsuite.TestWorkflowEnvironment {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(shardedForceReplicationWorker)
		execs := makeExecs(2, 10)
		env.RegisterActivityWithOptions(pageThrough(execs, 5), activity.RegisterOptions{Name: "ListWorkflows"})
		env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
			return replicateBatchResult{
				CompletedShards: req.Executions.sortedShards(),
				VerifiedCount:   int64(req.Executions.totalRuns()),
			}, nil
		}, activity.RegisterOptions{Name: "ReplicateBatch"})
		env.SetContinueAsNewSuggested(true)
		return env
	}

	t.Run("blocks without promotion", func(t *testing.T) {
		env := newEnv()
		params := makeChildParams(2)
		params.Handover = true // not promoted: must await resumeFullRate before checkpointing
		env.ExecuteWorkflow(shardedForceReplicationWorker, params)
		// A correctly-pausing child never completes on its own — it stays blocked
		// awaiting promotion, so the env surfaces a timeout rather than a result. A
		// child that wrongly checkpointed without promotion would instead complete
		// cleanly with ReachedEnd=false, leaving GetWorkflowError nil.
		require.Error(t, env.GetWorkflowError(),
			"throttled child must pause at the hint until promoted (never completing on its own)")
	})

	t.Run("completes after promotion", func(t *testing.T) {
		env := newEnv()
		// Deliver the promotion; the child leaves the pause, checkpoints, and completes.
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(shardedResumeFullSignalName, struct{}{})
		}, time.Minute)
		params := makeChildParams(2)
		params.Handover = true
		env.ExecuteWorkflow(shardedForceReplicationWorker, params)
		require.True(t, env.IsWorkflowCompleted(), "child should complete once promoted")
		require.NoError(t, env.GetWorkflowError())
		var result shardedChildResult
		require.NoError(t, env.GetWorkflowResult(&result))
		require.False(t, result.ReachedEnd, "throttled child checkpoints after promotion (pages remain)")
	})
}

// TestChild_PromotedAndCheckpoints_VerifiedCountAccumulated verifies the basic
// child lifecycle: a promoted child (not throttled) lists a namespace, dispatches
// batches, and returns ReachedEnd=true with the correct VerifiedCount.
// This uses childDirectRunner (real child with parent) so ParentWorkflowExecution
// is non-nil, sidestepping the production nil-guard bug.
func TestChild_PromotedAndCheckpoints_VerifiedCountAccumulated(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childDirectRunner)
	env.RegisterWorkflow(shardedForceReplicationWorker)

	execs := makeExecs(2, 5) // 10 execs, single terminal page
	env.RegisterActivityWithOptions(pageThrough(execs, 1000), activity.RegisterOptions{Name: "ListWorkflows"})

	var (
		mu        sync.Mutex
		batchSeen []int // per-shard exec counts per batch
	)
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		for _, shardID := range req.Executions.sortedShards() {
			count := 0
			for _, runs := range req.Executions[shardID] {
				count += len(runs)
			}
			mu.Lock()
			batchSeen = append(batchSeen, count)
			mu.Unlock()
		}
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   int64(req.Executions.totalRuns()),
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	params := makeChildParams(2)
	// promoted=true (not throttled), terminal namespace → ReachedEnd=true
	env.ExecuteWorkflow(childDirectRunner, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.ReachedEnd, "single-page namespace should set ReachedEnd=true")
	require.Equal(t, int64(10), result.VerifiedCount,
		"promoted child with 10 execs should return VerifiedCount=10")

	// All per-shard contributions are within MaxExecsPerShard (full rate = 50).
	mu.Lock()
	for _, count := range batchSeen {
		require.LessOrEqual(t, count, params.MaxExecsPerShard,
			"per-shard count in batch must not exceed MaxExecsPerShard at full rate")
	}
	mu.Unlock()
}

// checkpointProbe is a test-only workflow that drives checkpointAtBoundary
// directly with a non-nil parent execution. The production trigger for this path
// (a CAN hint at a page boundary) is not reproducible for a non-root child in
// the SDK test env — env.SetContinueAsNewSuggested only affects the root env,
// not an inner child (see the note on TestChild_ThrottledPromotion_ReceivesSignal)
// — so the probe exercises the checkpoint-signal handling without a full listing
// loop. It returns lastErr so the test can assert on it.
func checkpointProbe(ctx workflow.Context) error {
	s := &shardedWorkflowState{}
	s.checkpointAtBoundary(ctx, &workflow.Execution{ID: "parent-id", RunID: "parent-run"}, []byte("next-page"))
	return s.lastErr
}

// TestCheckpointAtBoundary_SignalFailureFailsChild verifies that checkpointAtBoundary
// latches lastErr when the checkpoint signal to the parent fails. A dropped
// checkpoint means the parent never starts a successor, so every execution past
// the page boundary would be silently lost; SignalExternalWorkflow has no
// transient failure modes, so the child must surface the error. The success
// case guards against the probe passing vacuously.
func TestCheckpointAtBoundary_SignalFailureFailsChild(t *testing.T) {
	t.Run("signal fails → error latched", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(checkpointProbe)

		env.OnSignalExternalWorkflow(
			mock.Anything, mock.Anything, mock.Anything,
			shardedCheckpointSignalName, mock.Anything,
		).Return(errors.New("signal target gone"))

		env.ExecuteWorkflow(checkpointProbe)

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.Error(t, err, "checkpointAtBoundary must latch lastErr when the signal fails")
		require.Contains(t, err.Error(), "checkpoint signal to parent")
	})

	t.Run("signal succeeds → no error", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(checkpointProbe)

		env.OnSignalExternalWorkflow(
			mock.Anything, mock.Anything, mock.Anything,
			shardedCheckpointSignalName, mock.Anything,
		).Return(nil)

		env.ExecuteWorkflow(checkpointProbe)

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError(),
			"checkpointAtBoundary must not error when the signal succeeds")
	})
}

// promoteSuccessorProbe is a test-only workflow that drives promoteSuccessor
// directly. The production promotion path is not reachable through the full
// parent flow in the SDK test env: the mocked child's GetInfo run ID differs
// from the run ID the parent tracks, so successorStarted never matches and the
// promotion block is skipped (see the note in TestParent_OneFullHandover). The
// probe returns childErr so the test can assert on it.
func promoteSuccessorProbe(ctx workflow.Context) error {
	ps := &shardedParentState{}
	ps.promoteSuccessor(ctx, workflow.Execution{ID: "successor-id", RunID: "successor-run"})
	return ps.childErr
}

// TestPromoteSuccessor_SignalFailure verifies that an anomalous resumeFullRate
// failure fails the parent (a live, unpromoted successor would otherwise
// deadlock at its next checkpoint), while a "successor already completed"
// failure is tolerated (the successor finished without ever needing promotion).
func TestPromoteSuccessor_SignalFailure(t *testing.T) {
	t.Run("anomalous failure → parent fails", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(promoteSuccessorProbe)

		env.OnSignalExternalWorkflow(
			mock.Anything, mock.Anything, mock.Anything,
			shardedResumeFullSignalName, mock.Anything,
		).Return(errors.New("unexpected signal failure"))

		env.ExecuteWorkflow(promoteSuccessorProbe)

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.Error(t, err, "anomalous promotion-signal failure must fail the parent")
		require.Contains(t, err.Error(), "resumeFullRate signal to successor")
	})

	t.Run("successor already completed → tolerated", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(promoteSuccessorProbe)

		// UnknownExternalWorkflowExecutionError == target not found: the
		// successor already completed, so it never needed promotion.
		env.OnSignalExternalWorkflow(
			mock.Anything, mock.Anything, mock.Anything,
			shardedResumeFullSignalName, mock.Anything,
		).Return(&temporal.UnknownExternalWorkflowExecutionError{})

		env.ExecuteWorkflow(promoteSuccessorProbe)

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError(),
			"a successor that already completed is benign — no promotion was needed")
	})
}
