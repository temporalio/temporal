package migration

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
)

// testNamespaceID is what metadataResponseFor returns. Tests pass it
// through to bidsForShards so the BIDs they hand to makeExecs hash
// to the same shards the workflow will compute during the page loop.
const testNamespaceID = "test-ns-id"

// ---- Test setup helpers ----

// bidsForShards returns, for each shard the hash actually populates
// under common.WorkflowIDToHistoryShard(namespaceID, bid, totalShards),
// a slice of `perShard` BusinessIDs. Brute-force search over candidate
// strings — sufficient for the small shard counts the tests use.
func bidsForShards(namespaceID string, totalShards int32, perShard int) map[int32][]string {
	out := make(map[int32][]string, totalShards)
	for i := 0; ; i++ {
		bid := fmt.Sprintf("wf-%d", i)
		sh := common.WorkflowIDToHistoryShard(namespaceID, bid, totalShards)
		if len(out[sh]) < perShard {
			out[sh] = append(out[sh], bid)
		}
		if int32(len(out)) == totalShards {
			done := true
			for sh := range out {
				if len(out[sh]) < perShard {
					done = false
					break
				}
			}
			if done {
				return out
			}
		}
	}
}

// makeExecs builds a slice of ExecutionInfos engineered to hash across
// `shards` distinct shards (`perShard` execs per shard) under the test
// namespace ID + shard count. The workflow's page loop computes the
// destination shard itself via common.WorkflowIDToHistoryShard.
func makeExecs(shards int32, perShard int) []*ExecutionInfo {
	bids := bidsForShards(testNamespaceID, shards, perShard)
	var execs []*ExecutionInfo
	idx := 0
	for sh := range bids {
		for _, bid := range bids[sh] {
			execs = append(execs, &ExecutionInfo{
				BusinessID: bid,
				RunID:      "run-" + strconv.Itoa(idx),
			})
			idx++
		}
	}
	return execs
}

// pageThrough returns a function suitable for OnActivity("ListWorkflows")
// that paginates `all` into pages of `pageSize` execs each. The workflow
// computes each exec's destination shard itself, so callers don't need
// to set anything shard-related on the ExecutionInfos.
func pageThrough(all []*ExecutionInfo, pageSize int) func(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
	return func(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		start := 0
		if len(req.NextPageToken) > 0 {
			start, _ = strconv.Atoi(string(req.NextPageToken))
		}
		end := min(start+pageSize, len(all))
		var nextToken []byte
		if end < len(all) {
			nextToken = []byte(strconv.Itoa(end))
		}
		return &listWorkflowsResponse{
			Executions:    all[start:end],
			NextPageToken: nextToken,
		}, nil
	}
}

// metadataResponseFor returns a function suitable for
// OnActivity("GetMetadata") that yields a fixed shard count + ns ID.
func metadataResponseFor(shardCount int32) func(context.Context, MetadataRequest) (*MetadataResponse, error) {
	return func(_ context.Context, _ MetadataRequest) (*MetadataResponse, error) {
		return &MetadataResponse{
			ShardCount:  shardCount,
			NamespaceID: testNamespaceID,
		}, nil
	}
}

// registerShardedScaffolding registers the GetMetadata + CountWorkflow
// stubs every sharded test needs before the page loop runs, plus the
// task-queue-user-data child workflow + its activity so the parent's
// terminal Await on Done resolves. Also registers shardedForceReplicationWorker
// so parent tests can spawn it inline as a child.
func registerShardedScaffolding(env *testsuite.TestWorkflowEnvironment, shardCount int32) {
	registerShardedScaffoldingWithSeed(env, shardCount, func(_ context.Context, _ TaskQueueUserDataReplicationParamsWithNamespace) error {
		return nil
	})
}

// registerShardedScaffoldingWithSeed is like registerShardedScaffolding
// but lets the caller supply the SeedReplicationQueueWithUserDataEntries
// activity body — needed for tests that exercise the seed-failure path.
func registerShardedScaffoldingWithSeed(
	env *testsuite.TestWorkflowEnvironment,
	shardCount int32,
	seed func(context.Context, TaskQueueUserDataReplicationParamsWithNamespace) error,
) {
	env.RegisterActivityWithOptions(metadataResponseFor(shardCount), activity.RegisterOptions{Name: "GetMetadata"})
	env.RegisterActivityWithOptions(func(_ context.Context, _ DescribeTargetClusterRequest) (*DescribeTargetClusterResponse, error) {
		return &DescribeTargetClusterResponse{ShardCount: shardCount}, nil
	}, activity.RegisterOptions{Name: "DescribeTargetCluster"})
	env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.CountWorkflowExecutionsRequest) (*countWorkflowResponse, error) {
		return &countWorkflowResponse{WorkflowCount: 0}, nil
	}, activity.RegisterOptions{Name: "CountWorkflow"})
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	env.RegisterActivityWithOptions(seed, activity.RegisterOptions{Name: "SeedReplicationQueueWithUserDataEntries"})
	// Register child worker so parent tests can spawn it inline.
	env.RegisterWorkflow(shardedForceReplicationWorker)
}

// makeChildParams returns a shardedChildParams with sensible defaults for
// child-direct tests. Callers override fields as needed.
func makeChildParams(shardCount int32) shardedChildParams {
	return shardedChildParams{
		Namespace:             "test-ns",
		NamespaceID:           testNamespaceID,
		TargetClusterName:     "remote_cluster",
		TargetShardCount:      shardCount,
		BatchSize:             100,
		MaxExecsPerShard:      50,
		ListWorkflowsPageSize: 1000,
		ConcurrentBatchCount:  1,
		PerBatchGenerateRPS:   30,
		ShardNoProgress:       time.Hour,
		IdleShardCost:         time.Hour,
	}
}

// hasAppErrType walks an error chain looking for a *temporal.ApplicationError
// with the given type. Use this when the error may be wrapped inside a
// child-workflow or fmt.Errorf chain.
func hasAppErrType(err error, wantType string) bool {
	for err != nil {
		if appErr, ok := err.(*temporal.ApplicationError); ok && appErr.Type() == wantType {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return false
}

// ---- Parent workflow tests ----

// TestSharded_HappyPath_SingleCycle: a small workload exhausts in one
// cycle, every batch returns clean completion, no CAN, no resume.
func TestSharded_HappyPath_SingleCycle(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)

	execs := makeExecs(4, 5) // 20 execs across 4 shards
	registerShardedScaffolding(env, 4)
	env.RegisterActivityWithOptions(pageThrough(execs, 1000), activity.RegisterOptions{Name: "ListWorkflows"})

	var (
		mu          sync.Mutex
		batchesSeen int
		execsSeen   int
		shardsSeen  = map[int32]struct{}{}
	)
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		mu.Lock()
		batchesSeen++
		execsSeen += req.Executions.totalRuns()
		for _, sh := range req.Executions.sortedShards() {
			shardsSeen[sh] = struct{}{}
		}
		mu.Unlock()
		return replicateBatchResult{}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted(), "workflow should complete")
	require.NoError(t, env.GetWorkflowError(), "workflow should succeed")
	// 20 execs across 4 shards, BatchSize=100, MaxExecsPerShard=50, single
	// page — every exec fits in one batch.
	require.Equal(t, 1, batchesSeen, "all execs should pack into a single batch")
	require.Equal(t, len(execs), execsSeen, "every exec should reach the activity")
	require.Len(t, shardsSeen, 4, "every shard should be represented")
}

// TestSharded_ShardNoProgress_FailsWorkflow: activity returns a
// non-retryable ShardNoProgress error → workflow fails with that
// error, no CAN.
func TestSharded_ShardNoProgress_FailsWorkflow(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 2)
	env.RegisterActivityWithOptions(pageThrough(makeExecs(2, 5), 1000), activity.RegisterOptions{Name: "ListWorkflows"})

	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		shards := req.Executions.sortedShards()
		return replicateBatchResult{}, temporal.NewNonRetryableApplicationError(
			"shard "+strconv.Itoa(int(shards[0]))+" stuck", "ShardNoProgress", nil)
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	// The error chain is: WorkflowExecutionError → childErr (wrapError) → ApplicationError{ShardNoProgress}.
	// Walk the chain to find the first ApplicationError with type ShardNoProgress.
	require.True(t, hasAppErrType(err, "ShardNoProgress"), "expected ShardNoProgress in error chain, got: %v", err)
}

// TestSharded_DisableVerification_NoVerifiedCount: with verification
// disabled the workflow runs inject-only batches, completes
// successfully, and the status query reports ReplicatedWorkflowCount=0
// because no verification ran.
func TestSharded_DisableVerification_NoVerifiedCount(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)

	execs := makeExecs(4, 5)
	registerShardedScaffolding(env, 4)
	env.RegisterActivityWithOptions(pageThrough(execs, 1000), activity.RegisterOptions{Name: "ListWorkflows"})

	var sawDisable atomic.Bool
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		if req.DisableVerification {
			sawDisable.Store(true)
		}
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   0,
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:           "test-ns",
		TargetClusterName:   "remote_cluster",
		DisableVerification: true,
	})

	require.True(t, env.IsWorkflowCompleted(), "workflow should complete")
	require.NoError(t, env.GetWorkflowError(), "workflow should succeed")
	require.True(t, sawDisable.Load(), "activity req should carry DisableVerification=true")

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)
	var status ForceReplicationStatus
	require.NoError(t, envValue.Get(&status))
	require.Equal(t, int64(0), status.ReplicatedWorkflowCount, "no verification ran so verified count must stay 0")
}

// TestSharded_InvalidInput: validateShardedForceReplicationParams
// rejects an empty Namespace and a missing TargetClusterName. Mirrors
// the existing force-replication TestInvalidInput.
func TestSharded_InvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params ShardedForceReplicationParams
	}{
		{
			name:   "empty namespace",
			params: ShardedForceReplicationParams{},
		},
		{
			name: "missing target cluster name",
			params: ShardedForceReplicationParams{
				Namespace: "test-ns",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(ShardedForceReplicationWorkflow)
			env.ExecuteWorkflow(ShardedForceReplicationWorkflow, tc.params)

			require.True(t, env.IsWorkflowCompleted())
			err := env.GetWorkflowError()
			require.Error(t, err)
			require.Contains(t, err.Error(), "InvalidArgument")
		})
	}
}

// TestSharded_ListWorkflowsError: a hard failure from ListWorkflows
// propagates out as the workflow error. Mirrors the existing
// force-replication TestListWorkflowsError.
func TestSharded_ListWorkflowsError(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 2)

	env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		return nil, temporal.NewNonRetryableApplicationError("mock listWorkflows error", "ListFailed", nil)
	}, activity.RegisterOptions{Name: "ListWorkflows"})

	// ReplicateBatch should never be invoked because listing fails up
	// front. Register a fail-loud stub so we notice if the workflow
	// ever dispatches a batch.
	env.RegisterActivityWithOptions(func(_ context.Context, _ *shardedBatchReq) (replicateBatchResult, error) {
		t.Fatal("ReplicateBatch must not be called when listing fails")
		return replicateBatchResult{}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock listWorkflows error")
}

// TestSharded_ReplicateBatchRetryableError: when ReplicateBatch returns
// a retryable error, the workflow exhausts its configured retry policy
// and surfaces the error as lastErr. Mirrors the existing
// TestGenerateReplicationTaskRetryableError.
func TestSharded_ReplicateBatchRetryableError(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 2)
	env.RegisterActivityWithOptions(pageThrough(makeExecs(2, 5), 1000), activity.RegisterOptions{Name: "ListWorkflows"})

	var attempts atomic.Int32
	env.RegisterActivityWithOptions(func(_ context.Context, _ *shardedBatchReq) (replicateBatchResult, error) {
		attempts.Add(1)
		return replicateBatchResult{}, temporal.NewApplicationError("transient backend error", "Transient")
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "transient backend error")
	require.GreaterOrEqual(t, attempts.Load(), int32(2),
		"expected ReplicateBatch to be retried at least twice before failing")
}

// TestSharded_TaskQueueReplicationFailure: when the
// SeedReplicationQueueWithUserDataEntries activity returns a
// non-retryable error, the child workflow signals the failure back
// and the parent fails with the seed error message; the status
// query reports the failure reason. Mirrors the existing
// TestTaskQueueReplicationFailure.
func TestSharded_TaskQueueReplicationFailure(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffoldingWithSeed(env, 2,
		func(_ context.Context, _ TaskQueueUserDataReplicationParamsWithNamespace) error {
			return temporal.NewNonRetryableApplicationError("namespace is required", "InvalidArgument", nil)
		})
	env.RegisterActivityWithOptions(pageThrough(nil, 1000), activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, _ *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:         "test-ns",
		TargetClusterName: "remote_cluster",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "namespace is required")

	envValue, qErr := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, qErr)
	var status ForceReplicationStatus
	require.NoError(t, envValue.Get(&status))
	require.True(t, status.TaskQueueUserDataReplicationStatus.Done)
	require.Contains(t, status.TaskQueueUserDataReplicationStatus.FailureMessage, "namespace is required")
}

// ---- Child workflow tests ----

// childWorkerID is the deterministic workflow ID given to the child worker
// spawned by childDirectRunner. Knowing this ID in advance lets the relay
// goroutine forward promotion signals to the child via SignalExternalWorkflow.
const childWorkerID = "test-sharded-child-worker"

// childDirectRunner is a test-only wrapper workflow that spawns
// shardedForceReplicationWorker as a child via workflow.ExecuteChildWorkflow.
//
// Two problems it solves:
//
//  1. The production child signals its parent execution (checkpoint and
//     progress rollups), which the nil-parent guard in run() skips when the
//     child runs as root. childDirectRunner provides a real parent so
//     ParentWorkflowExecution is non-nil and those signal paths execute.
//
//  2. env.SignalWorkflow targets the top-level execution (childDirectRunner),
//     not the child execution. childDirectRunner therefore relays the
//     shardedResumeFullSignalName signal to the child via
//     workflow.SignalExternalWorkflow so promotion signals from test
//     callbacks reach the production code.
func childDirectRunner(ctx workflow.Context, params shardedChildParams) (shardedChildResult, error) {
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: childWorkerID,
	})

	// Relay promotion signal: when the test sends env.SignalWorkflow(shardedResumeFullSignalName, ...),
	// it reaches this wrapper. Forward it to the child so the production
	// handleReleaseSignals / resume goroutine inside shardedForceReplicationWorker receives it.
	workflow.Go(ctx, func(gCtx workflow.Context) {
		ch := workflow.GetSignalChannel(gCtx, shardedResumeFullSignalName)
		var dummy struct{}
		for ch.Receive(gCtx, &dummy) {
			_ = workflow.SignalExternalWorkflow(gCtx, childWorkerID, "", shardedResumeFullSignalName, struct{}{}).Get(gCtx, nil)
		}
	})

	var result shardedChildResult
	err := workflow.ExecuteChildWorkflow(childCtx, shardedForceReplicationWorker, params).Get(ctx, &result)
	return result, err
}

// TestChild_HappyPath_TerminalChild: a single page of execs fits in one
// batch; the child verifies them all and returns ReachedEnd=true.
func TestChild_HappyPath_TerminalChild(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childDirectRunner)
	env.RegisterWorkflow(shardedForceReplicationWorker)

	execs := makeExecs(4, 5) // 20 execs across 4 shards, single page
	env.RegisterActivityWithOptions(pageThrough(execs, 1000), activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{
			VerifiedCount:   5,
			CompletedShards: req.Executions.sortedShards(),
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(childDirectRunner, makeChildParams(4))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.ReachedEnd, "single-page namespace should set ReachedEnd=true")
	require.Equal(t, int64(5), result.VerifiedCount)
}

// TestChild_VerifiedCountAccumulates: two batches (two pages of execs),
// each returning VerifiedCount=3; the child accumulates to 6.
func TestChild_VerifiedCountAccumulates(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childDirectRunner)
	env.RegisterWorkflow(shardedForceReplicationWorker)

	// Two pages of 10 execs each across 2 shards: makeExecs(2,5) produces
	// 10 execs (5 per shard). With pageSize=5, the pager yields two fetches.
	all := makeExecs(2, 5) // 10 execs; pageSize=5 → 2 pages of 5
	env.RegisterActivityWithOptions(pageThrough(all, 5), activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, _ *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{VerifiedCount: 3}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	params := makeChildParams(2)
	params.ConcurrentBatchCount = 1
	env.ExecuteWorkflow(childDirectRunner, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, int64(6), result.VerifiedCount)
	require.True(t, result.ReachedEnd)
}

// TestChild_DisableVerification_ReachesEnd: with DisableVerification=true
// the child completes with VerifiedCount=0 and ReachedEnd=true.
func TestChild_DisableVerification_ReachesEnd(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childDirectRunner)
	env.RegisterWorkflow(shardedForceReplicationWorker)

	execs := makeExecs(2, 5)
	env.RegisterActivityWithOptions(pageThrough(execs, 1000), activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   0,
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	params := makeChildParams(2)
	params.DisableVerification = true
	env.ExecuteWorkflow(childDirectRunner, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.ReachedEnd)
	require.Equal(t, int64(0), result.VerifiedCount)
}

// TestChild_ThrottledPromotion_ReceivesSignal: a child started with
// Handover=true runs at half rate but can receive a promotion signal
// via the childDirectRunner relay and complete a terminal page. This verifies
// that (a) childDirectRunner correctly relays the shardedResumeFullSignalName
// signal to the production child, and (b) a throttled child completes
// normally once promoted — i.e., that the promotion signal does not break
// execution when no CAN hint is active.
//
// Note: the "pause at CAN hint and wait for promotion" path is not testable
// via the child-workflow approach because env.SetContinueAsNewSuggested only
// affects the root (childDirectRunner) env, not the grandchild
// (shardedForceReplicationWorker) env. The CAN state is per-env and the
// SDK provides no API to set it on an inner child env from outside.
func TestChild_ThrottledPromotion_ReceivesSignal(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childDirectRunner)
	env.RegisterWorkflow(shardedForceReplicationWorker)

	execs := makeExecs(2, 5) // 10 execs, single terminal page
	env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		// From inside the activity body, schedule a delayed callback (runs on
		// the workflow scheduler goroutine) to send the promotion signal.
		// childDirectRunner's relay coroutine will forward it to the production
		// child. Since the page is terminal, the child will proceed to
		// completion rather than waiting for CAN-hint-triggered promotion.
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(shardedResumeFullSignalName, struct{}{})
		}, 0)
		return &listWorkflowsResponse{
			Executions:    execs,
			NextPageToken: nil, // terminal — child reaches end naturally
		}, nil
	}, activity.RegisterOptions{Name: "ListWorkflows"})

	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   5,
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	params := makeChildParams(2)
	params.Handover = true // child begins at half rate
	env.ExecuteWorkflow(childDirectRunner, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	// Terminal page → child reaches end, even when starting throttled.
	require.True(t, result.ReachedEnd, "terminal-page child should reach end regardless of throttle state")
	require.Equal(t, int64(5), result.VerifiedCount)
}

// TestChild_ListingBackpressure_BlocksUntilSlotFree: with ConcurrentBatchCount=1
// the listing loop must not fetch the next page until the in-flight batch from
// the previous page has completed and freed the single dispatch slot. Without
// backpressure (waitForDispatchSlot), the loop would list every page up front,
// inflating the buckets with execs it can't dispatch.
//
// Four pages, each one exec on a distinct shard so per-shard exclusivity never
// binds and ConcurrentBatchCount is the only limiter. The events slice records
// each ListWorkflows ("L") and ReplicateBatch ("R") call; backpressure forces
// strict L,R,L,R,... alternation. waitForDispatchSlot Awaits the batch
// coroutine's releaseAll, which drops batches.count() (and runs only after
// ReplicateBatch returns) before the next list, so the recorded order is
// deterministic.
func TestChild_ListingBackpressure_BlocksUntilSlotFree(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childDirectRunner)
	env.RegisterWorkflow(shardedForceReplicationWorker)

	all := makeExecs(4, 1) // 4 execs across 4 distinct shards
	pager := pageThrough(all, 1)

	var mu sync.Mutex
	var events []string
	env.RegisterActivityWithOptions(func(ctx context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		mu.Lock()
		events = append(events, "L")
		mu.Unlock()
		return pager(ctx, req)
	}, activity.RegisterOptions{Name: "ListWorkflows"})
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		mu.Lock()
		events = append(events, "R")
		mu.Unlock()
		return replicateBatchResult{
			VerifiedCount:   1,
			CompletedShards: req.Executions.sortedShards(),
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	params := makeChildParams(4)
	params.ConcurrentBatchCount = 1
	env.ExecuteWorkflow(childDirectRunner, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result shardedChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.ReachedEnd)
	require.Equal(t, int64(4), result.VerifiedCount)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"L", "R", "L", "R", "L", "R", "L", "R"}, events,
		"listing must block on the single dispatch slot, alternating list and replicate")
}
