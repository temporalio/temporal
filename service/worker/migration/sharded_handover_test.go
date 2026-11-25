package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payloads"
)

func parentTestEnv(t *testing.T, shardCount int32) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerShardedScaffolding(env, shardCount)
	return env
}

func TestShardedParentSequentialChildren(t *testing.T) {
	t.Parallel()
	for _, delayed := range []bool{false, true} {
		name := "immediate completion"
		if delayed {
			name = "draining child"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := parentTestEnv(t, 4)
			env.OnWorkflow(shardedForceReplicationWorker, mock.Anything,
				mock.MatchedBy(func(p shardedChildParams) bool { return string(p.StartPageToken) == "start" })).Return(
				func(ctx workflow.Context, _ shardedChildParams) (shardedChildResult, error) {
					if delayed {
						if err := workflow.Sleep(ctx, time.Minute); err != nil {
							return shardedChildResult{}, err
						}
					}
					return shardedChildResult{VerifiedCount: 12, NextPageToken: []byte("next")}, nil
				}).Once()
			env.OnWorkflow(shardedForceReplicationWorker, mock.Anything,
				mock.MatchedBy(func(p shardedChildParams) bool { return string(p.StartPageToken) == "next" })).Return(
				shardedChildResult{VerifiedCount: 3}, nil).Once()
			var duringDrain ForceReplicationStatus
			var queryErr error
			if delayed {
				env.RegisterDelayedCallback(func() {
					value, err := env.QueryWorkflow(forceReplicationStatusQueryType)
					queryErr = err
					if err == nil {
						queryErr = value.Get(&duringDrain)
					}
				}, 30*time.Second)
			}
			env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
				Namespace: "test-ns", TargetClusterName: "remote_cluster", NextPageToken: []byte("start"),
				TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
			})
			require.NoError(t, env.GetWorkflowError())
			if delayed {
				require.NoError(t, queryErr)
				require.Equal(t, []byte("start"), duringDrain.PageTokenForRestart)
				require.Zero(t, duringDrain.ReplicatedWorkflowCount)
			}
			value, err := env.QueryWorkflow(forceReplicationStatusQueryType)
			require.NoError(t, err)
			var status ForceReplicationStatus
			require.NoError(t, value.Get(&status))
			require.Equal(t, int64(15), status.ReplicatedWorkflowCount)
			require.Equal(t, []byte("next"), status.PageTokenForRestart)
			env.AssertExpectations(t)
		})
	}
}

func TestShardedParentFailureKeepsSafeRestartToken(t *testing.T) {
	t.Parallel()
	env := parentTestEnv(t, 4)
	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything,
		mock.MatchedBy(func(p shardedChildParams) bool { return len(p.StartPageToken) == 0 })).Return(
		shardedChildResult{VerifiedCount: 10, NextPageToken: []byte("safe")}, nil).Once()
	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything,
		mock.MatchedBy(func(p shardedChildParams) bool { return string(p.StartPageToken) == "safe" })).Return(
		shardedChildResult{}, temporal.NewNonRetryableApplicationError("drain failed", "DrainFailed", nil)).Once()
	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace: "test-ns", TargetClusterName: "remote_cluster",
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
	})
	require.ErrorContains(t, env.GetWorkflowError(), "drain failed")
	value, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)
	var status ForceReplicationStatus
	require.NoError(t, value.Get(&status))
	require.Equal(t, []byte("safe"), status.PageTokenForRestart)
	require.Equal(t, int64(10), status.ReplicatedWorkflowCount)
	env.AssertExpectations(t)
}

func TestShardedParentContinueAsNewAfterChildDrain(t *testing.T) {
	t.Parallel()
	env := parentTestEnv(t, 4)
	env.SetContinueAsNewSuggested(true)
	env.OnWorkflow(shardedForceReplicationWorker, mock.Anything, mock.Anything).Return(
		shardedChildResult{VerifiedCount: 7, NextPageToken: []byte("safe")}, nil).Once()
	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace: "test-ns", TargetClusterName: "remote_cluster", ReplicatedWorkflowCount: 11,
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
	})
	canErr, ok := errors.AsType[*workflow.ContinueAsNewError](env.GetWorkflowError())
	require.True(t, ok)
	var next ShardedForceReplicationParams
	require.NoError(t, payloads.Decode(canErr.Input, &next))
	require.Equal(t, []byte("safe"), next.NextPageToken)
	require.Equal(t, int64(18), next.ReplicatedWorkflowCount)
	require.Equal(t, 1, next.ContinuedAsNewCount)
	env.AssertExpectations(t)
}

func TestShardedChildDrainsCheckpointRange(t *testing.T) {
	t.Parallel()
	for _, failDrain := range []bool{false, true} {
		name := "success"
		if failDrain {
			name = "drain failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetContinueAsNewSuggested(true)
			execs := makeExecs(1, 13)
			env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
				return &listWorkflowsResponse{Executions: execs, NextPageToken: []byte("next-range")}, nil
			}, activity.RegisterOptions{Name: shardedListWorkflowsActivityName})
			// One shard and one activity slot leave eight executions buffered
			// when listing reaches the checkpoint boundary.
			env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
				if failDrain && req.BatchID == 2 {
					return replicateBatchResult{}, temporal.NewNonRetryableApplicationError("drain failed", "DrainFailed", nil)
				}
				return replicateBatchResult{VerifiedCount: int64(req.Executions.totalRuns())}, nil
			}, activity.RegisterOptions{Name: shardedReplicateBatchActivityName})
			params := makeChildParams(1)
			params.MaxExecsPerShard = 5
			params.BatchSize = 5
			env.ExecuteWorkflow(shardedForceReplicationWorker, params)
			if failDrain {
				require.ErrorContains(t, env.GetWorkflowError(), "drain failed")
				return
			}
			require.NoError(t, env.GetWorkflowError())
			var result shardedChildResult
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, shardedChildResult{VerifiedCount: 13, NextPageToken: []byte("next-range")}, result)
		})
	}
}

func TestShardedParentCancellation(t *testing.T) {
	t.Parallel()
	env := parentTestEnv(t, 4)
	env.RegisterActivityWithOptions(func(ctx context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, activity.RegisterOptions{Name: shardedListWorkflowsActivityName})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace: "test-ns", TargetClusterName: "remote_cluster",
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
	})
	require.True(t, temporal.IsCanceledError(env.GetWorkflowError()), "%v", env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestShardedChildBatchFailureDoesNotWaitForOtherBatch(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(pageThrough(makeExecs(2, 1), 100), activity.RegisterOptions{Name: shardedListWorkflowsActivityName})
	env.RegisterActivityWithOptions(func(context.Context, *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{}, nil
	}, activity.RegisterOptions{Name: shardedReplicateBatchActivityName})
	env.OnActivity(shardedReplicateBatchActivityName, mock.Anything,
		mock.MatchedBy(func(req *shardedBatchReq) bool { return req.BatchID == 1 })).
		After(time.Hour).Return(replicateBatchResult{VerifiedCount: 1}, nil).Once()
	env.OnActivity(shardedReplicateBatchActivityName, mock.Anything,
		mock.MatchedBy(func(req *shardedBatchReq) bool { return req.BatchID == 2 })).
		Return(replicateBatchResult{}, temporal.NewNonRetryableApplicationError("batch failed", "BatchFailed", nil)).Once()
	otherCompleted := false
	env.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, err error) {
		if info.ActivityType.Name == shardedReplicateBatchActivityName && err == nil {
			otherCompleted = true
		}
	})
	params := makeChildParams(2)
	params.BatchSize = 1
	params.MaxExecsPerShard = 1
	params.ConcurrentBatchCount = 2
	env.ExecuteWorkflow(shardedForceReplicationWorker, params)
	require.ErrorContains(t, env.GetWorkflowError(), "batch failed")
	require.False(t, otherCompleted, "a failed batch must interrupt the final drain wait")
	env.AssertExpectations(t)
}
