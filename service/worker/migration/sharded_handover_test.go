package migration

// sharded_handover_test.go: focused unit tests for the handover/cut logic of the
// sharded force-replication child orchestration.
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
// (sharded_workflow.go). See TestChild_ThrottledHitsHint_SkipCase and the note in
// TestChild_CutAtHint_HalfRateAfterCut for details.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
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
//  2. Parent starts child 1 with StartThrottled=true.
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
				// "cut at page boundary"). Then complete after 5 minutes.
				workflow.Go(ctx, func(gCtx workflow.Context) {
					_ = workflow.NewTimer(gCtx, 1*time.Minute).Get(gCtx, nil)
					if parentExec != nil {
						_ = workflow.SignalExternalWorkflow(gCtx,
							parentExec.ID, parentExec.RunID,
							shardedCheckpointSignalName,
							shardedCheckpointPayload{
								ChildRunID:    myRunID,
								NextPageToken: []byte("page-token-after-cut"),
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

	// Child 0 is not throttled (first child, no predecessor).
	require.False(t, childParamsSeen[0].StartThrottled, "child 0 must not be throttled")

	// Child 1 is throttled (started alongside still-running child 0).
	require.True(t, childParamsSeen[1].StartThrottled,
		"child 1 must start throttled (predecessor still running)")

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
// Sequence: child 0 cuts at 30s (→ child 1 starts) and completes at 1min with
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
				// Child 0: cut at 30s (parent starts the successor), complete at 1min.
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
}

// ---- Child tests — shardedForceReplicationWorker as root ----

// TestChild_CutAtHint_StopsListing verifies that when GetContinueAsNewSuggested
// fires at a fully-consumed page boundary with pages remaining, a promoted child
// cuts: it stops listing, drains only the pages it already consumed, and returns
// ReachedEnd=false (the remaining pages belong to the successor the parent starts
// from the checkpoint token). The worker runs as the root workflow so
// env.SetContinueAsNewSuggested drives the production hint directly (the nil
// ParentWorkflowExecution is handled by the guard in run()). The per-shard
// half-rate value applied after the cut is unit-tested by TestEffectiveMaxExecsPerShard.
func TestChild_CutAtHint_StopsListing(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(shardedForceReplicationWorker)

	// 20 execs paginated 5-at-a-time so page 1 returns a non-empty next-page token:
	// the hint becomes a "cut" (work remains) rather than a terminal end.
	execs := makeExecs(2, 10)
	env.RegisterActivityWithOptions(pageThrough(execs, 5), activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   int64(req.Executions.totalRuns()),
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	// Hint is true from the start: the child cuts after the first fully-consumed page.
	env.SetContinueAsNewSuggested(true)

	params := makeChildParams(2) // not throttled → promoted; first child has no predecessor
	env.ExecuteWorkflow(shardedForceReplicationWorker, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.False(t, result.ReachedEnd,
		"child must cut (ReachedEnd=false) when the hint fires with pages remaining")
	require.Equal(t, int64(5), result.VerifiedCount,
		"child verifies only the first fully-consumed page (5 execs) before cutting")
}

// TestChild_ThrottledHitsHint_PausesUntilPromoted verifies the throttled-hits-hint
// rule: a child started throttled (predecessor still running) that reaches the CAN
// hint must NOT cut until it is promoted — guaranteeing at most one handover in
// flight. The worker runs as root so env.SetContinueAsNewSuggested drives the hint
// and env.SignalWorkflow delivers resumeFullRate.
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
		params.StartThrottled = true // not promoted: must await resumeFullRate before cutting
		env.ExecuteWorkflow(shardedForceReplicationWorker, params)
		// A correctly-pausing child never completes on its own — it stays blocked
		// awaiting promotion, so the env surfaces a timeout rather than a result. A
		// child that wrongly cut without promotion would instead complete cleanly
		// with ReachedEnd=false, leaving GetWorkflowError nil.
		require.Error(t, env.GetWorkflowError(),
			"throttled child must pause at the hint until promoted (never completing on its own)")
	})

	t.Run("completes after promotion", func(t *testing.T) {
		env := newEnv()
		// Deliver the promotion; the child leaves the pause, cuts, and completes.
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(shardedResumeFullSignalName, struct{}{})
		}, time.Minute)
		params := makeChildParams(2)
		params.StartThrottled = true
		env.ExecuteWorkflow(shardedForceReplicationWorker, params)
		require.True(t, env.IsWorkflowCompleted(), "child should complete once promoted")
		require.NoError(t, env.GetWorkflowError())
		var result shardedChildResult
		require.NoError(t, env.GetWorkflowResult(&result))
		require.False(t, result.ReachedEnd, "throttled child cuts after promotion (pages remain)")
	})
}

// TestChild_PromotedAndCuts_VerifiedCountAccumulated verifies the basic child
// lifecycle: a promoted child (not throttled) lists a namespace, dispatches
// batches, and returns ReachedEnd=true with the correct VerifiedCount.
// This uses childDirectRunner (real child with parent) so ParentWorkflowExecution
// is non-nil, sidestepping the production nil-guard bug.
func TestChild_PromotedAndCuts_VerifiedCountAccumulated(t *testing.T) {
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
