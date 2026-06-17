package migration

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

const (
	// shardedForceReplicationWorkflowName is the registered workflow name for
	// the parent. Distinct from the legacy ForceReplicationWorkflow so both
	// variants coexist in the same worker — pick at workflow start time by
	// setting the workflow type.
	shardedForceReplicationWorkflowName = "force-replication-sharded"

	// shardedForceReplicationWorkerName is the registered workflow name for
	// the child worker workflows spawned by the parent. Registered on the
	// same sharded worker component and task queue as the parent.
	shardedForceReplicationWorkerName = "force-replication-sharded-worker"

	// releaseShardsSignalName carries mid-flight ReleaseShards signals
	// from active replicate-batch activities back to their parent
	// workflow (the child). Drain-mode shard completions are gone in
	// the new design, so this signal only fires while the activity is
	// still running normally and the child needs to free the shard early
	// so a successor can pack against it.
	releaseShardsSignalName = "ReleaseShards"

	// shardedCheckpointSignalName is sent by a child to the parent at
	// its cut point — when GetContinueAsNewSuggested fires at a page
	// boundary. Payload: shardedCheckpointPayload.
	shardedCheckpointSignalName = "force-replication-sharded-checkpoint"

	// shardedProgressSignalName carries a periodic (60 s) cumulative
	// verified-count rollup from each live child to the parent.
	// Payload: shardedProgressPayload.
	shardedProgressSignalName = "force-replication-sharded-progress"

	// shardedResumeFullSignalName is sent by the parent to a child to
	// promote it to full-rate operation once its predecessor has
	// completed. Payload: empty struct — presence is the signal.
	shardedResumeFullSignalName = "force-replication-sharded-resume-full"

	// defaultShardedListPageSize is the ListWorkflows page size when
	// the sharded workflow's params.ListWorkflowsPageSize is unset.
	defaultShardedListPageSize = 1000

	// defaultBatchSize bounds the total executions in any single
	// ReplicateBatch activity. Activity-payload sizing knob, not a
	// shard-fill threshold.
	defaultBatchSize = 100

	// defaultMaxExecsPerShard bounds the executions any single shard
	// can contribute to a batch — i.e. the per-shard inject blast
	// radius before that shard's apply queue has to absorb a burst.
	// 50 keeps a hot shard's contribution under half a default-sized
	// batch (BatchSize=100), so a batch still spans ≥2 shards.
	defaultMaxExecsPerShard = 50

	// defaultShardNoProgress is the per-shard cumulative no-progress
	// backstop. While a shard's pending exec count is non-zero and
	// no exec on that shard has produced a verified outcome for this
	// long, the activity fails non-retryably naming the stuck shard.
	// A shard awaiting its very first verification gets double this
	// window so the server has time to clear any backlog predating our
	// task submission; it reverts to this value once the shard's first
	// exec verifies.
	defaultShardNoProgress = 5 * time.Minute

	// defaultIdleShardCost is the cumulative idle-time threshold
	// (the "shard-seconds" unit: 30 s with 1 idle shard equals
	// 3.3 s with 9 idle) at which the activity signal-releases its
	// completed-but-not-yet-released shards mid-flight.
	defaultIdleShardCost = 30 * time.Second

	// defaultPerBatchGenerateRPS is the per-batch inject-phase target.
	// Sharded dispatches many concurrent batches and each builds its
	// own limiter, so this caps the per-batch generate-replication-task
	// rate; the workflow does not normalise against a global cap the
	// way the existing migration's OverallRps does.
	defaultPerBatchGenerateRPS = 30.0

	// defaultConcurrentBatchCap is the ceiling applied to the derived
	// default of targetShardCount/4. Keeps the in-flight batch count
	// safely inside per-worker concurrent-activity budgets and bounds
	// the cluster blast radius of a single force-rep run.
	defaultConcurrentBatchCap = 500
)

// RunEntry is the per-run leaf in the nested batch payload. Carries the
// RunID plus an optional ArchetypeID, serialised as a JSON tuple:
// `["runID"]` when ArchetypeID is zero, `["runID", N]` when set.
//
// Why a tuple, not an object: a tuple omits the JSON field names
// (`"r":`, `"a":`) that would otherwise repeat on every run, which is
// the main lever behind the nested payload's byte savings. With many
// runs per BusinessID (the heavy-reuse case), this collapses the
// per-run encoding overhead from ~47 bytes (flat ExecutionInfo) to
// ~10 bytes per run.
type RunEntry struct {
	RunID       string
	ArchetypeID uint32
}

// MarshalJSON serialises RunEntry as a heterogeneous JSON tuple. Custom
// because Go's default JSON can't express a heterogeneous tuple, and
// archetype-omission needs to happen by changing the tuple length
// rather than emitting an explicit zero. UnmarshalJSON below is the
// inverse.
func (r RunEntry) MarshalJSON() ([]byte, error) {
	if r.ArchetypeID == 0 {
		return fmt.Appendf(nil, `[%q]`, r.RunID), nil
	}
	return fmt.Appendf(nil, `[%q,%d]`, r.RunID, r.ArchetypeID), nil
}

func (r *RunEntry) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("RunEntry: %w", err)
	}
	if len(raw) < 1 || len(raw) > 2 {
		return fmt.Errorf("RunEntry: expected [runID] or [runID,archetypeID], got %d-element array", len(raw))
	}
	if err := json.Unmarshal(raw[0], &r.RunID); err != nil {
		return fmt.Errorf("RunEntry runID: %w", err)
	}
	r.ArchetypeID = 0
	if len(raw) == 2 {
		if err := json.Unmarshal(raw[1], &r.ArchetypeID); err != nil {
			return fmt.Errorf("RunEntry archetypeID: %w", err)
		}
	}
	return nil
}

// BatchPayload groups runs by (shard, businessID) so a single businessID
// with many runs costs one BID-string-worth of bytes instead of one per
// run. The wire shape behind shardedBatchReq.Executions.
//
// On-wire form:
//
//	{"shardID": {"businessID": [["runID"], ["runID", archetypeID], ...], ...}, ...}
//
// Top-level keys are shard IDs; inner-map keys are businessIDs; values
// are RunEntry tuples.
type BatchPayload map[int32]map[string][]RunEntry

// totalRuns counts runs across all (shard, BID) groups.
func (p BatchPayload) totalRuns() int {
	n := 0
	//workflowcheck:ignore (summation is order-independent)
	for _, byBID := range p {
		//workflowcheck:ignore (summation is order-independent)
		for _, runs := range byBID {
			n += len(runs)
		}
	}
	return n
}

// sortedShards returns shard IDs in ascending order. Used to give the
// activity-side flatten a deterministic iteration order for replays.
func (p BatchPayload) sortedShards() []int32 {
	out := make([]int32, 0, len(p))
	//workflowcheck:ignore (output is sorted before any observable use)
	for sh := range p {
		out = append(out, sh)
	}
	slices.Sort(out)
	return out
}

// shardedExecutionInfo pairs an upstream ExecutionInfo with the destination
// history shard the sharded design routes by. Shard is kept out of the
// upstream ExecutionInfo struct because no other workflow needs it; the
// wire format (BatchPayload) carries shard as the outer map key.
type shardedExecutionInfo struct {
	*ExecutionInfo
	Shard int32
}

// flatten produces a deterministically-ordered slice of execs paired with
// their destination shard: shards ascending, BIDs alphabetical within
// shard, runs in input order. Lets the inner verify loop stay index-based
// even though the wire shape is nested.
func (p BatchPayload) flatten() []*shardedExecutionInfo {
	n := p.totalRuns()
	if n == 0 {
		return nil
	}
	out := make([]*shardedExecutionInfo, 0, n)
	for _, sh := range p.sortedShards() {
		byBID := p[sh]
		bids := make([]string, 0, len(byBID))
		//workflowcheck:ignore (bids is sorted before use)
		for bid := range byBID {
			bids = append(bids, bid)
		}
		slices.Sort(bids)
		for _, bid := range bids {
			for _, r := range byBID[bid] {
				out = append(out, &shardedExecutionInfo{
					ExecutionInfo: &ExecutionInfo{
						BusinessID:  bid,
						RunID:       r.RunID,
						ArchetypeID: r.ArchetypeID,
					},
					Shard: sh,
				})
			}
		}
	}
	return out
}

// ShardedForceReplicationParams is the parent workflow input. Configuration
// fields are read-only across CAN cycles; the carry-over block at the
// bottom is mutated each cycle.
type ShardedForceReplicationParams struct {
	// ---- Configuration ----
	Namespace             string
	Query                 string
	BatchSize             int
	MaxExecsPerShard      int
	ListWorkflowsPageSize int
	TargetClusterName     string
	DisableVerification   bool

	ShardNoProgress time.Duration
	IdleShardCost   time.Duration

	TaskQueueUserDataReplicationParams TaskQueueUserDataReplicationParams

	// PerBatchGenerateRPS is the inject-phase rate-limiter target inside
	// each ReplicateBatch activity. See defaultPerBatchGenerateRPS for
	// the rationale; defaults to that value.
	PerBatchGenerateRPS float64

	// ConcurrentBatchCount is the absolute ceiling on in-flight
	// ReplicateBatch activities. Per-shard exclusivity already bounds
	// concurrency to the target shard count, but at large cluster sizes
	// that's well past the worker's concurrent-activity budget. This
	// cap keeps the workflow inside that budget and limits the cluster
	// blast radius of a single force-rep run. Defaults to
	// min(targetShardCount/4, defaultConcurrentBatchCap).
	ConcurrentBatchCount int

	// EstimationMultiplier sizes the QPSQueue's initial slice capacity
	// (multiplier × ConcurrentBatchCount + 1). Pure allocation hint —
	// the sliding window's logical max stays ConcurrentBatchCount + 1.
	// Defaults to 2.
	EstimationMultiplier int

	// ---- Continue-as-new carry-over ----
	NextPageToken                    []byte
	ContinuedAsNewCount              int
	TotalForceReplicateWorkflowCount int64
	ReplicatedWorkflowCount          int64
	ReplicatedWorkflowCountPerSecond float64

	// QPSQueue carries the sliding-window samples across CAN so the
	// per-second rate doesn't drop to zero on every cycle boundary.
	QPSQueue QPSQueue

	TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus
}

// shardedChildParams is the input to each child worker workflow
// (shardedForceReplicationWorker). It carries the configuration subset
// needed by the child and the handover state for the child's listing range.
type shardedChildParams struct {
	// Configuration subset passed down from the parent.
	Namespace, Query, NamespaceID, TargetClusterName                         string
	TargetShardCount                                                         int32
	BatchSize, MaxExecsPerShard, ListWorkflowsPageSize, ConcurrentBatchCount int
	DisableVerification                                                      bool
	ShardNoProgress                                                          time.Duration
	IdleShardCost                                                            time.Duration
	PerBatchGenerateRPS                                                      float64

	// StartPageToken is the ListWorkflows continuation token from which
	// this child begins listing. Nil for the very first child.
	StartPageToken []byte

	// StartThrottled, when true, means the child begins at half
	// MaxExecsPerShard until the parent signals resumeFullRate. Used
	// during handover overlap so a new child and its not-yet-drained
	// predecessor together stay ≤ MaxExecsPerShard per shard.
	StartThrottled bool
}

// shardedChildResult is the return value of each child worker workflow.
type shardedChildResult struct {
	// VerifiedCount is the total number of executions verified by this
	// child during its lifetime. Folded into the parent's retiredTotal
	// on child completion.
	VerifiedCount int64

	// ReachedEnd is true when this child exhausted the namespace
	// (ListWorkflows returned an empty next-page token) — meaning there
	// is no more work for a successor. The parent uses this to skip
	// starting a successor and to proceed toward terminal completion.
	ReachedEnd bool
}

// shardedCheckpointPayload is the body of the shardedCheckpointSignalName
// signal that a child sends to the parent when it reaches a
// GetContinueAsNewSuggested hint at a page boundary. The parent starts a
// successor from NextPageToken and, when this child completes, promotes the
// successor to full rate.
type shardedCheckpointPayload struct {
	// ChildRunID identifies the sending child so the parent can track
	// which child's successor has already been started.
	ChildRunID string
	// NextPageToken is the continuation token for the next page range.
	// The successor starts listing from here.
	NextPageToken []byte
}

// shardedProgressPayload is the body of the shardedProgressSignalName
// signal that each live child sends to the parent every 60 seconds.
// The parent aggregates these to answer the force-replication-status query.
type shardedProgressPayload struct {
	// ChildRunID identifies the sending child within the parent's liveCounts map.
	ChildRunID string
	// VerifiedCount is the child's cumulative verified-execution count
	// at the time of this rollup signal.
	VerifiedCount int64
}

// shardedBatchReq is the per-batch activity input. Executions is the
// per-shard, per-BID nested payload — the workflow has marked every
// shard appearing as a top-level key in shardInFlight before dispatch,
// and the activity is responsible for either signal-releasing each shard
// mid-flight or listing it in the return value's CompletedShards set.
type shardedBatchReq struct {
	BatchID     int64
	Namespace   string
	NamespaceID string
	Executions  BatchPayload

	TargetClusterName string

	DisableVerification bool

	PerBatchGenerateRPS float64

	ShardNoProgress time.Duration
	IdleShardCost   time.Duration
}

// replicateBatchResult is the activity's return payload.
//
// CompletedShards is informational (the dispatch coroutine's defer clears
// heldByBatch + shardInFlight regardless), but keeping it in the result
// gives metrics a clean handle on "which shards this batch finished".
type replicateBatchResult struct {
	CompletedShards []int32

	// VerifiedCount is the number of executions this activity invocation
	// finished verifying (including retention/zombie skips that resolve
	// as verified). The child workflow accumulates this into its running
	// verifiedCount.
	VerifiedCount int64
}

type replicateBatchHeartbeat struct {
	// NextInjectIdx is the index of the next exec to inject on retry.
	NextInjectIdx int
	// InjectDone marks the inject phase as complete; retries skip inject.
	InjectDone bool
}

// releaseShardsPayload is the body of the mid-flight ReleaseShards signal
// an activity sends to its parent workflow when the cumulative idle cost
// across its completed-but-not-yet-released shards crosses IdleShardCost.
// The workflow handler clears these shards from shardInFlight +
// heldByBatch[BatchID] so the packer can immediately dispatch new work
// against them while the activity stays running on its still-pending
// shards.
type releaseShardsPayload struct {
	BatchID int64
	Shards  []int32
}
