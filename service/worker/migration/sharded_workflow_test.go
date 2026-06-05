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
	"go.temporal.io/sdk/converter"
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
// terminal Await on Done resolves.
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
	env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.CountWorkflowExecutionsRequest) (*countWorkflowResponse, error) {
		return &countWorkflowResponse{WorkflowCount: 0}, nil
	}, activity.RegisterOptions{Name: "CountWorkflow"})
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	env.RegisterActivityWithOptions(seed, activity.RegisterOptions{Name: "SeedReplicationQueueWithUserDataEntries"})
}

// ---- Tests ----

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
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 4,
	})

	require.True(t, env.IsWorkflowCompleted(), "workflow should complete")
	require.NoError(t, env.GetWorkflowError(), "workflow should succeed")
	// 20 execs across 4 shards, BatchSize=100, MaxExecsPerShard=50, single
	// page — every exec fits in one batch.
	require.Equal(t, 1, batchesSeen, "all execs should pack into a single batch")
	require.Equal(t, len(execs), execsSeen, "every exec should reach the activity")
	require.Len(t, shardsSeen, 4, "every shard should be represented")
}

// TestSharded_ResumeShards_Packed: a non-empty ResumeShards in params
// gets packed into multi-shard batches up to BatchSize. Asserts that
// no resume batch exceeds BatchSize and the per-shard contributions
// match the input.
func TestSharded_ResumeShards_Packed(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 8)
	env.RegisterActivityWithOptions(pageThrough(nil, 1000), activity.RegisterOptions{Name: "ListWorkflows"})

	// 8 shards, 15 unverified execs each = 120 total. With BatchSize=100,
	// we expect 2 batches: e.g., [0..5] (90 execs) + [5+, 6, 7] (30) or
	// similar — depends on greedy packing.
	resumeShards := make([]ResumeShard, 8)
	for s := range 8 {
		resumeShards[s] = ResumeShard{
			Shard: int32(s),
			Execs: makeExecsForShard(int32(s), 15),
		}
	}

	var (
		mu      sync.Mutex
		batches [][]int32
	)
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		require.True(t, req.Resume, "all resume-dispatched batches must have Resume=true")
		require.LessOrEqual(t, req.Executions.totalRuns(), 100, "batch must not exceed BatchSize")
		mu.Lock()
		batches = append(batches, req.Executions.sortedShards())
		mu.Unlock()
		return replicateBatchResult{}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 8,
		ResumeShards:            resumeShards,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Confirm every shard was covered exactly once across all batches.
	covered := map[int32]int{}
	for _, b := range batches {
		for _, sh := range b {
			covered[sh]++
		}
	}
	for s := range int32(8) {
		require.Equal(t, 1, covered[s], "shard %d should be covered exactly once", s)
	}
	require.GreaterOrEqual(t, len(batches), 2, "should pack into at least 2 batches given 120 execs / BatchSize=100")
}

// TestSharded_ReleaseShards_FreesShardForReuse: an activity sends a
// ReleaseShards signal mid-flight. The workflow must clear the shard
// from shardInFlight so the packer can dispatch a fresh batch
// targeting that shard *while the original activity is still running*
// — the slot in ConcurrentBatchCount is still claimed by batch 1, so
// batch 2 can only dispatch if signal-release worked.
//
// ConcurrentBatchCount=2 is explicit: defaultConcurrentBatchCount(2)=1
// would gate batch 2 on the dispatch slot regardless of shard state, so
// the test couldn't distinguish "release-from-flight" from
// "batch 1 returned and freed the slot".
func TestSharded_ReleaseShards_FreesShardForReuse(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 2)

	// Two pages of execs on the same shards. The second batch can dispatch
	// only when batch 1 releases its shards.
	phase1 := makeExecs(2, 10)
	phase2 := makeExecs(2, 10)
	all := append(append([]*ExecutionInfo{}, phase1...), phase2...)
	env.RegisterActivityWithOptions(pageThrough(all, 20), activity.RegisterOptions{Name: "ListWorkflows"})

	var (
		mu              sync.Mutex
		batches         []*shardedBatchReq
		secondStarted   = make(chan struct{})
		secondOnce      sync.Once
		releaseObserved atomic.Bool
	)
	env.RegisterActivityWithOptions(func(_ context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		mu.Lock()
		batches = append(batches, req)
		idx := len(batches)
		mu.Unlock()

		switch idx {
		case 1:
			// Signal-release, then block here until batch 2 actually
			// dispatches. If signal-release wires through, the workflow
			// dispatches batch 2 concurrently; if it doesn't, batch 2
			// can't run until this activity returns (the safety timeout
			// below).
			env.SignalWorkflow("ReleaseShards", releaseShardsPayload{
				BatchID: req.BatchID,
				Shards:  req.Executions.sortedShards(),
			})
			select {
			case <-secondStarted:
				releaseObserved.Store(true)
			case <-time.After(30 * time.Second):
				// Safety release so the test fails the assertion rather
				// than hanging indefinitely. Generous bound because CI
				// can be slow; the happy path returns immediately.
			}
		case 2:
			secondOnce.Do(func() { close(secondStarted) })
		default:
		}
		return replicateBatchResult{}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 2,
		ConcurrentBatchCount:    2,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, batches, 2, "expected two batches across the two pages")
	require.True(t, releaseObserved.Load(),
		"signal-release should let batch 2 dispatch while batch 1 still holds its dispatch slot")
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
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "ShardNoProgress", appErr.Type())
}

// TestSharded_DrainResult_FromActivityResult_FeedsCANCarryover: a
// non-empty InFlight in the activity's replicateBatchResult must
// populate drainPayload and end up as ResumeShards in the CAN
// carry-over.
//
// Tests the workflow plumbing only. In production, an activity
// returns InFlight after entering drain mode and grace-expiring; here
// we exercise the same code path by returning InFlight from a
// non-cancelled run, because the testsuite delivers cancellation as
// a CanceledError without preserving the activity's returned result.
// The dispatch coroutine's err == nil branch is what we're testing —
// it doesn't care whether the activity was cancelled or not, only
// whether the returned result has InFlight to fold into drainPayload.
func TestSharded_DrainResult_FromActivityResult_FeedsCANCarryover(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 10)

	// Page 1 returns a multi-shard population so the streaming packer
	// has something to dispatch under the pinned BatchSize /
	// MaxExecsPerShard the test sets below. The activity flips
	// CAN-suggested from inside its body before returning, so by the
	// time it has handed back its InFlight the workflow is committed
	// to CAN — but without going through cancel, which the testsuite
	// delivers as a CanceledError regardless of any result the
	// activity returned.
	pageExecs := makeExecs(10, 10)
	// Drained exec mirrors a real input row so the simulated drain
	// payload would be a valid response from a real activity. drainedBID
	// is the first exec; drainedShard is the shard the workflow will
	// hash it to.
	drainedBID := pageExecs[0].BusinessID
	const drainedRunID = "run-drained"
	drainedShard := common.WorkflowIDToHistoryShard(testNamespaceID, drainedBID, 10)
	env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		return &listWorkflowsResponse{
			Executions:    pageExecs,
			NextPageToken: []byte("more"),
		}, nil
	}, activity.RegisterOptions{Name: "ListWorkflows"})

	env.RegisterActivityWithOptions(func(_ context.Context, _ *shardedBatchReq) (replicateBatchResult, error) {
		env.SetContinueAsNewSuggested(true)
		return replicateBatchResult{
			InFlight: []ResumeShard{{
				Shard:              drainedShard,
				Execs:              map[string][]RunEntry{drainedBID: {{RunID: drainedRunID}}},
				NoProgressDuration: 42 * time.Second,
			}},
		}, nil
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 10,
		BatchSize:               100,
		MaxExecsPerShard:        10,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err, "workflow should CAN, not return success")

	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &canErr, "error should be ContinueAsNewError")

	var nextParams ShardedForceReplicationParams
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &nextParams))
	require.NotEmpty(t, nextParams.ResumeShards, "InFlight from a returned activity should appear in resume payload")
	require.Equal(t, drainedShard, nextParams.ResumeShards[0].Shard)
	runs, ok := nextParams.ResumeShards[0].Execs[drainedBID]
	require.True(t, ok, "drained business ID should appear in nested resume payload")
	require.Equal(t, []RunEntry{{RunID: drainedRunID}}, runs)
	require.Equal(t, 42*time.Second, nextParams.ResumeShards[0].NoProgressDuration)
}

// TestSharded_CancelBeforeStart_NoLostExecs pins down recovery when
// an activity is dispatched and the workflow CANs before the activity
// body runs: the activity returns CanceledError with no result, and
// the recovery path re-buckets the input execs into RecoveredBuckets
// so the next cycle dispatches them as fresh inject+verify batches.
func TestSharded_CancelBeforeStart_NoLostExecs(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ShardedForceReplicationWorkflow)
	registerShardedScaffolding(env, 4)

	execs := makeExecs(4, 5) // 20 execs across 4 shards
	pageServed := false
	env.RegisterActivityWithOptions(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		if pageServed {
			return &listWorkflowsResponse{}, nil
		}
		pageServed = true
		// Trigger CAN as soon as this page is served so drainForCAN
		// runs before any dispatched batches can complete.
		env.SetContinueAsNewSuggested(true)
		return &listWorkflowsResponse{
			Executions:    execs,
			NextPageToken: []byte("more"),
		}, nil
	}, activity.RegisterOptions{Name: "ListWorkflows"})

	// Activity that responds to ctx cancellation by returning a
	// CanceledError with no result — simulating the
	// "cancelled before any work done" path.
	var (
		batchCount   atomic.Int32
		cancelledIDs []int64
		muIDs        sync.Mutex
	)
	env.RegisterActivityWithOptions(func(ctx context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
		batchCount.Add(1)
		<-ctx.Done()
		muIDs.Lock()
		cancelledIDs = append(cancelledIDs, req.BatchID)
		muIDs.Unlock()
		return replicateBatchResult{}, temporal.NewCanceledError()
	}, activity.RegisterOptions{Name: "ReplicateBatch"})

	env.ExecuteWorkflow(ShardedForceReplicationWorkflow, ShardedForceReplicationParams{
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 4,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &canErr, "expected CAN, got %v", err)

	var nextParams ShardedForceReplicationParams
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &nextParams))

	// Count execs the next cycle would re-dispatch. The activity
	// body never ran in this race, so execs land in RecoveredBuckets
	// (fresh inject+verify) rather than ResumeShards (verify-only).
	recovered := nextParams.RecoveredBuckets.totalRuns()

	t.Logf("dispatched %d batches, cancelled %d, recovered execs %d (expected %d)",
		batchCount.Load(), len(cancelledIDs), recovered, len(execs))

	require.Equal(t, len(execs), recovered, "every dispatched exec should land in RecoveredBuckets when its activity is cancelled before it can run")
	require.Empty(t, nextParams.ResumeShards, "no ResumeShards — activity never ran, never injected, so no resume work")
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
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 4,
		DisableVerification:     true,
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
// rejects an empty Namespace and a missing TargetClusterEndpoint /
// TargetClusterName when verification is enabled. Mirrors the existing
// force-replication TestInvalidInput.
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
			name: "missing target with verification on",
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
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock listWorkflows error")
}

// TestSharded_ReplicateBatchRetryableError: when ReplicateBatch returns
// a retryable error, the workflow exhausts its 3-attempt retry policy
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
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "transient backend error")
	// MaximumAttempts: 3 in spawnBatch's activity options — assert at
	// least 2 retries actually happened so a future change that drops
	// the retry policy fails this test.
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
		Namespace:               "test-ns",
		TargetClusterName:       "remote_cluster",
		TargetClusterShardCount: 2,
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

// ---- internal helpers ----

// makeExecsForShard produces `count` runs for the named shard's
// ResumeShard.Execs payload. Each run gets a distinct businessID so
// the resulting map has `count` entries with one run each — the
// simplest shape for tests that don't care about BID-reuse.
func makeExecsForShard(shard int32, count int) map[string][]RunEntry {
	out := map[string][]RunEntry{}
	for i := range count {
		bid := "wf-" + strconv.Itoa(int(shard)) + "-" + strconv.Itoa(i)
		out[bid] = []RunEntry{{RunID: "run-" + strconv.Itoa(int(shard)*1000+i)}}
	}
	return out
}
