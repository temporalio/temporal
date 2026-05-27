package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type ForceReplicationWorkflowV3TestSuite struct {
	suite.Suite
}

func TestForceReplicationWorkflowV3TestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ForceReplicationWorkflowV3TestSuite{})
}

// TestHappyPath drives V3 through three pages of ListWorkflows. Each batch
// runs InjectBatch then VerifyBatch (both succeed, no pending), so no
// retries fire and the workflow completes cleanly.
func (s *ForceReplicationWorkflowV3TestSuite) TestHappyPath() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 3}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		if pageCount < 3 {
			return &listWorkflowsResponse{
				Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
				NextPageToken: []byte("next"),
			}, nil
		}
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
			NextPageToken: nil,
		}, nil
	}).Times(3)

	// One inject + one verify per batch. Each verify returns clean (no
	// pending) so the controller doesn't even need to retry.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(3)
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).
		Return(&adaptiveVerifyBatchResponse{Verified: 1, Pending: nil}, nil).
		Times(3)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   10,
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	s.Equal(int64(3), status.ReplicatedWorkflowCount)
	s.Equal(0, status.QuarantinedShardCount)
}

// TestInjectOnlyMode runs V3 with EnableVerification=false. VerifyBatch
// must not be invoked; the workflow completes once all pages have been
// injected, and the status query reports the injected count as
// ReplicatedWorkflowCount (since inject-success is the only progress
// signal available in this mode).
func (s *ForceReplicationWorkflowV3TestSuite) TestInjectOnlyMode() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 3}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		if pageCount < 3 {
			return &listWorkflowsResponse{
				Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
				NextPageToken: []byte("next"),
			}, nil
		}
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
			NextPageToken: nil,
		}, nil
	}).Times(3)

	// InjectBatch runs once per page; VerifyBatch must not be called.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(3)
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(nil, nil).Times(0)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   10,
		EnableVerification:      false,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	// Inject-only counts toward ReplicatedWorkflowCount via inject success.
	s.Equal(int64(3), status.ReplicatedWorkflowCount)
	s.Equal(0, status.QuarantinedShardCount)
}

// TestShardQuarantineRecovers drives V3 against a one-shard topology with
// two distinct WF IDs that both pend once, tripping shard quarantine, then
// both verify cleanly in the slow lane and the per-batch shard counter
// decay brings the count back below the release threshold → quarantine
// releases.
func (s *ForceReplicationWorkflowV3TestSuite) TestShardQuarantineRecovers() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 2}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 1, NamespaceID: namespaceID}, nil)

	execA := &ExecutionInfo{BusinessID: "wf-a", RunID: "run-1"}
	execB := &ExecutionInfo{BusinessID: "wf-b", RunID: "run-1"}
	listed := false
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		if listed {
			return &listWorkflowsResponse{Executions: nil, NextPageToken: nil}, nil
		}
		listed = true
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{execA, execB},
			NextPageToken: nil,
		}, nil
	})

	// InjectBatch is called once — initial dispatch. Retry rounds use
	// dispatchVerifyOnly, never re-injecting.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(1)
	// Verify outcomes by call:
	//   1: both pending (initial fast batch) → shardPending = 2 → shard quarantined
	//   2: both clean (drainRetries round 0, slow lane) → shardPending decays
	//      to 1 → 1 <= releaseAt(1) → quarantine releases
	verifyCalls := 0
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		verifyCalls++
		if verifyCalls == 1 {
			return &adaptiveVerifyBatchResponse{Verified: 0, Pending: req.Executions}, nil
		}
		return &adaptiveVerifyBatchResponse{Verified: int64(len(req.Executions)), Pending: nil}, nil
	}).Times(2)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		Query:                    "",
		ConcurrentActivityCount:  2,
		OverallRps:               10,
		ListWorkflowsPageSize:    2,
		PageCountPerExecution:    10,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		ShardQuarantineThreshold: 2,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	s.Equal(0, status.QuarantinedShardCount)
}

func TestPendingListMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	t.Run("empty roundtrips to {}", func(t *testing.T) {
		var pl pendingList
		b, err := json.Marshal(pl)
		require.NoError(t, err)
		require.Equal(t, "{}", string(b))

		var out pendingList
		require.NoError(t, json.Unmarshal(b, &out))
		require.Empty(t, out)
	})

	t.Run("groups by BusinessID with array tuples", func(t *testing.T) {
		pl := pendingList{
			{BusinessID: "wf-a", RunID: "r1", ArchetypeID: 7},
			{BusinessID: "wf-a", RunID: "r2", ArchetypeID: 7},
			{BusinessID: "wf-b", RunID: "r3", ArchetypeID: 9},
		}
		b, err := json.Marshal(pl)
		require.NoError(t, err)
		require.JSONEq(t, `{"wf-a":[["r1",7],["r2",7]],"wf-b":[["r3",9]]}`, string(b))
	})

	t.Run("roundtrip preserves all fields", func(t *testing.T) {
		pl := pendingList{
			{BusinessID: "wf-a", RunID: "r1", ArchetypeID: 7},
			{BusinessID: "wf-b", RunID: "r2", ArchetypeID: 0},
			{BusinessID: "wf-a", RunID: "r3", ArchetypeID: 7},
		}
		b, err := json.Marshal(pl)
		require.NoError(t, err)

		var out pendingList
		require.NoError(t, json.Unmarshal(b, &out))
		require.Len(t, out, 3)

		seen := make(map[string]ExecutionInfo, 3)
		for _, e := range out {
			seen[e.BusinessID+"/"+e.RunID] = *e
		}
		require.Equal(t, ExecutionInfo{BusinessID: "wf-a", RunID: "r1", ArchetypeID: 7}, seen["wf-a/r1"])
		require.Equal(t, ExecutionInfo{BusinessID: "wf-a", RunID: "r3", ArchetypeID: 7}, seen["wf-a/r3"])
		require.Equal(t, ExecutionInfo{BusinessID: "wf-b", RunID: "r2", ArchetypeID: 0}, seen["wf-b/r2"])
	})

	t.Run("unmarshal order is deterministic", func(t *testing.T) {
		// Same encoded bytes must produce the same slice order every
		// time, even though map iteration during marshal is random —
		// encoding/json sorts keys lexically, and UnmarshalJSON sorts
		// before flattening. Without that, workflow replay would
		// dispatch retries in a different order.
		encoded := `{"wf-c":[["r3",1]],"wf-a":[["r1",1],["r2",1]],"wf-b":[["r4",1]]}`
		var first pendingList
		require.NoError(t, json.Unmarshal([]byte(encoded), &first))

		for range 20 {
			var again pendingList
			require.NoError(t, json.Unmarshal([]byte(encoded), &again))
			require.Len(t, again, len(first))
			for j := range first {
				require.Equal(t, *first[j], *again[j], "ordering must be stable across unmarshals")
			}
		}
		// Expected order: sorted BIDs (wf-a, wf-b, wf-c), runs in JSON
		// array order within each.
		require.Equal(t, "wf-a", first[0].BusinessID)
		require.Equal(t, "r1", first[0].RunID)
		require.Equal(t, "wf-a", first[1].BusinessID)
		require.Equal(t, "r2", first[1].RunID)
		require.Equal(t, "wf-b", first[2].BusinessID)
		require.Equal(t, "wf-c", first[3].BusinessID)
	})

	t.Run("malformed tuple errors", func(t *testing.T) {
		var pl pendingList
		err := json.Unmarshal([]byte(`{"wf-a":[["r1"]]}`), &pl)
		require.Error(t, err)
		require.Contains(t, err.Error(), "2-element")
	})
}

// TestEstimateCANPayloadBytes locks down the upper-bound invariant:
// estimateCANPayloadBytes must not underestimate the actual marshalled
// size of the input-growing CAN fields. If the constants drift below the
// real per-entry overhead, the early-CAN / hard-cap triggers stop
// firing in time and the SDK's 512KB blob warn becomes the de-facto
// limit. This test catches that regression.
func TestEstimateCANPayloadBytes(t *testing.T) {
	t.Parallel()

	// Build a state that exercises every term in the estimator. Mix
	// long and short BIDs, and repeat some BIDs to confirm pending
	// entries over-count vs the BID-grouped wire form.
	s := &adaptiveWorkflowState{
		params: &AdaptiveForceReplicationParams{NamespaceID: "ns-1", HistoryShardCount: 4},
	}
	for i := range 50 {
		// Two runs per BID to exercise the over-count path.
		bid := fmt.Sprintf("wf-fast-%d", i%25)
		s.fastPending = append(s.fastPending, &ExecutionInfo{
			BusinessID:  bid,
			RunID:       fmt.Sprintf("run-%d", i),
			ArchetypeID: uint32(i),
		})
	}
	for i := range 20 {
		s.slowPending = append(s.slowPending, &ExecutionInfo{
			BusinessID: fmt.Sprintf("wf-slow-%d", i),
			RunID:      fmt.Sprintf("run-%d", i),
		})
	}

	// Marshal each CAN-serialized field independently and sum. The
	// estimator covers exactly these two; shard-keyed state is bounded
	// by shard count and intentionally excluded.
	fpBytes, err := json.Marshal(pendingList(s.fastPending))
	require.NoError(t, err)
	spBytes, err := json.Marshal(pendingList(s.slowPending))
	require.NoError(t, err)

	actual := len(fpBytes) + len(spBytes)
	estimate := s.estimateCANPayloadBytes()
	require.GreaterOrEqual(t, estimate, actual,
		"estimate must be a non-strict upper bound on actual marshalled size (estimate=%d, actual=%d)",
		estimate, actual)
}

// TestReclassifyPendingForRetry covers the two-direction routing in
// reclassifyPendingForRetry by shard quarantine: fast→slow when a BID's
// hosting shard is now quarantined, slow→fast when it has been released.
func TestReclassifyPendingForRetry(t *testing.T) {
	t.Parallel()

	newState := func(quarantinedShards []int32) *adaptiveWorkflowState {
		return &adaptiveWorkflowState{
			params: &AdaptiveForceReplicationParams{
				NamespaceID:       "ns-1",
				HistoryShardCount: 4,
			},
			quarantinedShard: sliceToInt32Set(quarantinedShards),
		}
	}

	t.Run("slow entries move to fast when shard quarantine released", func(t *testing.T) {
		// Shard quarantine has been released since these entries were
		// originally routed to slow, so they should move back to fast.
		s := newState(nil)
		s.slowPending = []*ExecutionInfo{
			{BusinessID: "wf-a", RunID: "r1"},
			{BusinessID: "wf-b", RunID: "r2"},
		}
		reFast, reSlow := s.reclassifyPendingForRetry()
		require.Len(t, reFast, 2, "released shard should let entries return to fast")
		require.Empty(t, reSlow)
		require.Empty(t, s.fastPending, "consumed into reFast/reSlow")
		require.Empty(t, s.slowPending)
	})

	t.Run("fast entry on quarantined shard moves to slow", func(t *testing.T) {
		// Compute the shard wf-x hashes onto, then quarantine that shard.
		s := newState(nil)
		shard := s.shardOf("wf-x")
		s.quarantinedShard[shard] = struct{}{}
		s.fastPending = []*ExecutionInfo{{BusinessID: "wf-x", RunID: "r1"}}
		reFast, reSlow := s.reclassifyPendingForRetry()
		require.Empty(t, reFast)
		require.Len(t, reSlow, 1)
	})

	t.Run("slow entry on still-quarantined shard stays slow", func(t *testing.T) {
		s := newState(nil)
		shard := s.shardOf("wf-x")
		s.quarantinedShard[shard] = struct{}{}
		s.slowPending = []*ExecutionInfo{{BusinessID: "wf-x", RunID: "r1"}}
		reFast, reSlow := s.reclassifyPendingForRetry()
		require.Empty(t, reFast)
		require.Len(t, reSlow, 1)
	})
}

// TestDecrementCleanCountersGatesOnPendingInBatch pins the drown-out
// invariant: a shard's counter must NOT decay on batches where any of
// its BIDs were pending, even if other BIDs on the same shard verified
// cleanly in the same batch. Without this gate, a burst of cold
// neighbours on a shard with one hot BID would erase the hot BID's
// signal: shardPending hovers near zero and shard quarantine never
// engages.
func TestDecrementCleanCountersGatesOnPendingInBatch(t *testing.T) {
	t.Parallel()
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	type result struct {
		ShardPending     int
		QuarantinedShard bool
	}

	runWorkflow := func(ctx workflow.Context) (result, error) {
		state := &adaptiveWorkflowState{
			params: &AdaptiveForceReplicationParams{
				Namespace:                "test-ns",
				NamespaceID:              "test-ns-id",
				HistoryShardCount:        1, // every BID hashes to the single shard
				ShardQuarantineThreshold: 4, // releaseAt = max(4/2, 1) = 2
			},
			shardPending:     map[int32]int{},
			quarantinedShard: map[int32]struct{}{},
		}
		shard := state.shardOf("hot-a")
		state.shardPending[shard] = 4
		state.quarantinedShard[shard] = struct{}{}

		dispatched := []*ExecutionInfo{
			{BusinessID: "cold-a", RunID: "r1"},
			{BusinessID: "hot-a", RunID: "r2"},
		}
		pendingBIDs := map[string]struct{}{"hot-a": {}}
		pendingShards := map[int32]struct{}{shard: {}}
		state.decrementCleanCounters(ctx, dispatched, pendingBIDs, pendingShards)
		_, quarantined := state.quarantinedShard[shard]
		return result{
			ShardPending:     state.shardPending[shard],
			QuarantinedShard: quarantined,
		}, nil
	}

	env.RegisterWorkflow(runWorkflow)
	env.ExecuteWorkflow(runWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got result
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, 4, got.ShardPending,
		"counter must not decay on a batch that also produced pending on this shard")
	require.True(t, got.QuarantinedShard, "shard quarantine must persist")
}

// TestDecrementCleanCountersDecaysOnFullyCleanBatch is the inverse of
// the drown-out test: when a quarantined shard sees only clean traffic
// in a batch (zero pending BIDs on it), the counter decays by exactly
// one and quarantine releases once the counter crosses the
// threshold/2 hysteresis floor.
func TestDecrementCleanCountersDecaysOnFullyCleanBatch(t *testing.T) {
	t.Parallel()
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	type result struct {
		ShardPending     int
		QuarantinedShard bool
	}

	runWorkflow := func(ctx workflow.Context) (result, error) {
		state := &adaptiveWorkflowState{
			params: &AdaptiveForceReplicationParams{
				Namespace:                "test-ns",
				NamespaceID:              "test-ns-id",
				HistoryShardCount:        1,
				ShardQuarantineThreshold: 4, // releaseAt = max(4/2, 1) = 2
			},
			shardPending:     map[int32]int{},
			quarantinedShard: map[int32]struct{}{},
		}
		shard := state.shardOf("wf-x")
		// Pre-load to (releaseAt + 1) so a single decay tips into release.
		state.shardPending[shard] = 3
		state.quarantinedShard[shard] = struct{}{}

		dispatched := []*ExecutionInfo{
			{BusinessID: "wf-x", RunID: "r1"},
			{BusinessID: "wf-y", RunID: "r2"},
		}
		state.decrementCleanCounters(ctx, dispatched, map[string]struct{}{}, map[int32]struct{}{})
		_, quarantined := state.quarantinedShard[shard]
		return result{
			ShardPending:     state.shardPending[shard],
			QuarantinedShard: quarantined,
		}, nil
	}

	env.RegisterWorkflow(runWorkflow)
	env.ExecuteWorkflow(runWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got result
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, 2, got.ShardPending, "counter must decay by exactly one per fully-clean batch")
	require.False(t, got.QuarantinedShard,
		"quarantine must release once counter hits the threshold/2 hysteresis floor")
}

// continueAsNewParams extracts the CAN input from the workflow's error or
// fails the test.
func continueAsNewParams(t require.TestingT, err error) AdaptiveForceReplicationParams {
	require.Error(t, err)
	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &canErr)
	payloads := canErr.Input.GetPayloads()
	require.Len(t, payloads, 1)
	var params AdaptiveForceReplicationParams
	require.NoError(t, json.Unmarshal(payloads[0].GetData(), &params))
	return params
}

// TestPendingCarriesAcrossCAN drives one cycle where every dispatched
// batch returns pending, then asserts that CAN happened and the
// pending list is serialized into the CAN params. Without the carry,
// the page cursor would advance past these executions and they'd
// never be retried.
func (s *ForceReplicationWorkflowV3TestSuite) TestPendingCarriesAcrossCAN() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 100}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	// Single page with two execs, NextPageToken non-nil so the workflow
	// CANs rather than running drainRetries on the final cycle.
	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		return &listWorkflowsResponse{
			Executions: []*ExecutionInfo{
				{BusinessID: "wf-a", RunID: "run-1"},
				{BusinessID: "wf-b", RunID: "run-1"},
			},
			NextPageToken: []byte("next"),
		}, nil
	}).Once()

	// One page → one InjectBatch + one VerifyBatch → CAN. Times(1)
	// guards against accidental re-dispatch.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(1)
	// Both execs come back pending — they should land in params.FastPending
	// (shard quarantine doesn't fire since the threshold is well above the
	// two-BID observation).
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		return &adaptiveVerifyBatchResponse{Verified: 0, Pending: req.Executions}, nil
	}).Times(1)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		Query:                    "",
		ConcurrentActivityCount:  1,
		OverallRps:               10,
		ListWorkflowsPageSize:    2,
		PageCountPerExecution:    1,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		ShardQuarantineThreshold: 999,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	canParams := continueAsNewParams(s.T(), env.GetWorkflowError())
	s.Len(canParams.FastPending, 2, "both pending execs should carry across CAN")
	s.Empty(canParams.SlowPending, "neither exec is quarantined so nothing in slow")
	s.Equal(int64(0), canParams.ReplicatedWorkflowCount)
	// lastProgressAt must persist across CAN so the no-progress detector
	// spans cycles. Zero here would reset the detector on every CAN.
	s.False(canParams.LastProgressAt.IsZero(), "lastProgressAt should be snapshotted into CAN params")
}

// TestEarlyCANOnPendingPressure verifies that runOnePagedCycle breaks
// the page loop early once combined pending exceeds
// earlyCANPendingThreshold instead of grinding through all
// PageCountPerExecution pages. The trigger is observable as ListWorkflows
// being called fewer times than the page-count cap.
func (s *ForceReplicationWorkflowV3TestSuite) TestEarlyCANOnPendingPressure() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 100000}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	// Page size sized so a handful of batches trip the early-CAN
	// threshold and the post-break overshoot stays under the hard cap
	// (otherwise drainRetries fires and the workflow doesn't take the
	// CAN path the test is exercising).
	pageSize := earlyCANPendingThreshold / 3
	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		execs := make([]*ExecutionInfo, pageSize)
		for i := range execs {
			execs[i] = &ExecutionInfo{
				BusinessID: fmt.Sprintf("wf-%d-%d", pageCount, i),
				RunID:      "run-1",
			}
		}
		return &listWorkflowsResponse{
			Executions:    execs,
			NextPageToken: []byte("next"),
		}, nil
	})

	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil)
	// Every batch returns its full input as pending so the threshold
	// trips as fast as possible.
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		return &adaptiveVerifyBatchResponse{Verified: 0, Pending: req.Executions}, nil
	})

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	const pageCountCap = 100
	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 1,
		OverallRps:              10,
		ListWorkflowsPageSize:   pageSize,
		PageCountPerExecution:   pageCountCap,
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
		// Quarantine thresholds far above any per-shard / per-WF
		// pending count we'll accumulate — keeps everything in the
		// fast lane so we can observe early-CAN there specifically.
		ShardQuarantineThreshold: 1_000_000,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	canParams := continueAsNewParams(s.T(), env.GetWorkflowError())

	s.Less(pageCount, pageCountCap, "page loop should break before exhausting PageCountPerExecution")
	s.NotEmpty(canParams.FastPending, "pending should carry into CAN params")
	// Hard cap is enforced before snapshot; carried pending stays under
	// the maxPendingCarryAcrossCAN budget.
	s.LessOrEqual(len(canParams.FastPending)+len(canParams.SlowPending), maxPendingCarryAcrossCAN,
		"hard cap should bound carried pending")
}

// TestHardCapDrainsBeforeCAN feeds the workflow more carry-over pending
// than maxPendingCarryAcrossCAN and verifies the workflow runs
// drainRetries inline before CAN so the snapshot stays bounded. The
// drain mocks VerifyBatch as clean so retries empty the pending list
// entirely.
func (s *ForceReplicationWorkflowV3TestSuite) TestHardCapDrainsBeforeCAN() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 100000}, nil).Maybe()
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	// ListWorkflows returns empty execs but a non-nil NextPageToken so
	// the workflow stays on the CAN path (not the final-cycle drainRetries
	// path). The carry-over by itself trips both the early-CAN break
	// (immediately) and the workflow-level hard cap.
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(&listWorkflowsResponse{
		Executions:    nil,
		NextPageToken: []byte("next"),
	}, nil).Once()

	// VerifyBatch during drainRetries returns clean — drainRetries
	// should clear the pending list entirely so the CAN snapshot is
	// empty.
	verifyCalls := 0
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		verifyCalls++
		return &adaptiveVerifyBatchResponse{Verified: int64(len(req.Executions)), Pending: nil}, nil
	})

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Build carry-over above the hard cap.
	carryCount := maxPendingCarryAcrossCAN + 1000
	carry := make([]*ExecutionInfo, carryCount)
	for i := range carry {
		carry[i] = &ExecutionInfo{BusinessID: fmt.Sprintf("wf-%d", i), RunID: "run-1"}
	}

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		Query:                    "",
		ConcurrentActivityCount:  1,
		OverallRps:               10,
		ListWorkflowsPageSize:    1000,
		PageCountPerExecution:    100,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		ShardQuarantineThreshold: 999,
		NoProgressTimeoutSeconds: 3600,
		FastPending:              carry,
		ContinuedAsNewCount:      1,
	})

	s.True(env.IsWorkflowCompleted())
	canParams := continueAsNewParams(s.T(), env.GetWorkflowError())

	s.Positive(verifyCalls, "drainRetries should have invoked VerifyBatch")
	s.Empty(canParams.FastPending, "drainRetries should have cleared fastPending before CAN")
	s.Empty(canParams.SlowPending)
	// The carry-over executions verify cleanly during the drain, so
	// totalVerified picks them all up.
	s.Equal(int64(carryCount), canParams.ReplicatedWorkflowCount)
}

// TestDrainOnlySkipsListing verifies that a workflow started with
// DrainOnly=true skips ListWorkflows entirely and goes straight to
// draining the carried pending list. This is the recovery path after
// a prior cycle's drainRetries bailed on GetContinueAsNewSuggested:
// the workflow CAN'd with DrainOnly=true and the carried pending, and
// the resumed cycle should pick up the drain without re-listing
// already-processed pages.
func (s *ForceReplicationWorkflowV3TestSuite) TestDrainOnlySkipsListing() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 100}, nil).Maybe()
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	// Intentionally do not mock ListWorkflows. If the DrainOnly guard in
	// runOnePagedCycle doesn't fire, the unmocked activity errors out
	// and the workflow fails — making this a strict "ListWorkflows is
	// never called" assertion.

	// DrainOnly path → one drainRetries round → one VerifyBatch with all
	// carry execs. InjectBatch must never be called (drain re-verifies
	// already-injected work); the missing mock means an unexpected call
	// would fail the test.
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		return &adaptiveVerifyBatchResponse{Verified: int64(len(req.Executions)), Pending: nil}, nil
	}).Times(1)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	carry := []*ExecutionInfo{
		{BusinessID: "wf-1", RunID: "r1"},
		{BusinessID: "wf-2", RunID: "r2"},
		{BusinessID: "wf-3", RunID: "r3"},
	}

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		ConcurrentActivityCount:  1,
		OverallRps:               10,
		ListWorkflowsPageSize:    1000,
		PageCountPerExecution:    10,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		ShardQuarantineThreshold: 999,
		NoProgressTimeoutSeconds: 3600,
		DrainOnly:                true,
		FastPending:              carry,
		ContinuedAsNewCount:      1,
		// Mark TQ user-data replication done so the post-drain Await
		// returns immediately; otherwise the workflow blocks waiting
		// for a signal the test never sends.
		TaskQueueUserDataReplicationStatus: TaskQueueUserDataReplicationStatus{Done: true},
		TotalForceReplicateWorkflowCount:   100,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	s.Equal(int64(len(carry)), status.ReplicatedWorkflowCount)
}

// TestAIMDIncreasesOnClean drives the workflow through several clean
// batches and verifies the fast-lane RPS additively ramped up via the
// status query.
//
// Initial fast RPS = OverallRps / ConcurrentActivityCount = 10.
// Step = AIMDIncreaseStep × initial = 0.10 × 10 = 1.0.
// Three clean batches → three +1 adjustments → final RPS = 13.
func (s *ForceReplicationWorkflowV3TestSuite) TestAIMDIncreasesOnClean() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 3}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		var nextPage []byte
		if pageCount < 3 {
			nextPage = []byte("next")
		}
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{{BusinessID: fmt.Sprintf("wf-%d", pageCount), RunID: "run-1"}},
			NextPageToken: nextPage,
		}, nil
	}).Times(3)

	// Three pages → three inject + three verify pairs. Times(3) on both
	// asserts no retry rounds (final cycle drainRetries returns
	// immediately with empty pending).
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(3)
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		return &adaptiveVerifyBatchResponse{Verified: int64(len(req.Executions)), Pending: nil}, nil
	}).Times(3)
	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		ConcurrentActivityCount:  1,
		OverallRps:               10,
		ListWorkflowsPageSize:    1,
		PageCountPerExecution:    10,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		ShardQuarantineThreshold: 999,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	// Initial = 10, +1 per clean batch × 3 batches = 13.
	s.InDelta(13.0, status.CurrentFastRPS, 1e-9, "three clean batches should ramp RPS by 3 × step")
}

// TestAIMDDecreasesOnPending verifies multiplicative back-off when a
// batch returns pending. The pending exec also re-runs in drainRetries
// (final-cycle path) which runs VerifyBatch again — that retry is also
// pending, so RPS is halved twice. Initial 10 → 5 (first cut) → 2.5
// (second cut from drainRetries).
func (s *ForceReplicationWorkflowV3TestSuite) TestAIMDDecreasesOnPending() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 1}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	// One page, final cycle. AIMD sees two pending events (initial verify
	// + drainRetries round 0) before the third verify returns clean.
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(&listWorkflowsResponse{
		Executions:    []*ExecutionInfo{{BusinessID: "stuck-wf", RunID: "run-1"}},
		NextPageToken: nil,
	}, nil).Once()

	// One InjectBatch on initial dispatch; drainRetries verify-only after.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(1)
	verifyCalls := 0
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		verifyCalls++
		// First two verifies (initial + drainRetries round 0) return
		// pending; round 1 returns clean to terminate the drain loop.
		if verifyCalls <= 2 {
			return &adaptiveVerifyBatchResponse{Verified: 0, Pending: req.Executions}, nil
		}
		return &adaptiveVerifyBatchResponse{Verified: int64(len(req.Executions)), Pending: nil}, nil
	}).Times(3)
	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		ConcurrentActivityCount:  1,
		OverallRps:               10,
		ListWorkflowsPageSize:    1,
		PageCountPerExecution:    10,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		ShardQuarantineThreshold: 999,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	// Two pending events (one from initial batch, one from drainRetries
	// round 0), then clean. AIMD halves twice, then adds step on the
	// clean recovery: 10 × 0.5 = 5, × 0.5 = 2.5, + 1 = 3.5.
	s.InDelta(3.5, status.CurrentFastRPS, 1e-9, "pending → halve, halve, then +step on clean recovery")
	s.Less(status.CurrentFastRPS, 10.0, "RPS must end below initial after pending")
}

// TestCheckProgressFiresWhenStale exercises the no-progress detector
// directly. checkProgress trips when workflow time has advanced past
// NoProgressTimeoutSeconds since lastProgressAt with no growth in
// totalVerified.
//
// We drive it via a thin test workflow that constructs an
// adaptiveWorkflowState with a stale lastProgressAt (set to two seconds
// before workflow.Now), then calls checkProgress. In production the
// same condition arises naturally when long-running VerifyBatch
// activities return without verifying anything — activity wall-time
// advances workflow time, lastProgressAt stays put, the detector fires.
func (s *ForceReplicationWorkflowV3TestSuite) TestCheckProgressFiresWhenStale() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		state := &adaptiveWorkflowState{
			params: &AdaptiveForceReplicationParams{
				Namespace:                "test-ns",
				NoProgressTimeoutSeconds: 1,
				EnableVerification:       true,
			},
			lastProgressAt: workflow.Now(ctx).Add(-2 * time.Second),
		}
		return state.checkProgress(ctx)
	})

	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	s.Require().Error(err)
	var appErr *temporal.ApplicationError
	s.Require().ErrorAs(err, &appErr)
	s.Equal("AdaptiveForceReplicationNoProgress", appErr.Type())
	s.True(appErr.NonRetryable(), "no-progress timeout should be non-retryable")
}

// TestCheckProgressBypassedInInjectOnlyMode verifies the detector
// returns nil even with stale lastProgressAt when EnableVerification
// is false — inject-only has no arrival signal so totalVerified is
// just the injected count and would fire spuriously.
func (s *ForceReplicationWorkflowV3TestSuite) TestCheckProgressBypassedInInjectOnlyMode() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		state := &adaptiveWorkflowState{
			params: &AdaptiveForceReplicationParams{
				Namespace:                "test-ns",
				NoProgressTimeoutSeconds: 1,
				EnableVerification:       false,
			},
			lastProgressAt: workflow.Now(ctx).Add(-2 * time.Second),
		}
		return state.checkProgress(ctx)
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())
}

// TestCheckProgressResetsOnProgress verifies the inverse: when
// totalVerified has advanced past lastVerified, checkProgress updates
// lastProgressAt and returns nil even if wall-time has elapsed past
// the threshold.
func (s *ForceReplicationWorkflowV3TestSuite) TestCheckProgressResetsOnProgress() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		state := &adaptiveWorkflowState{
			params: &AdaptiveForceReplicationParams{
				Namespace:                "test-ns",
				NoProgressTimeoutSeconds: 1,
				EnableVerification:       true,
			},
			lastProgressAt: workflow.Now(ctx).Add(-2 * time.Second),
			totalVerified:  5,
			lastVerified:   0,
		}
		return state.checkProgress(ctx)
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())
}
