package migration

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

const (
	// shardedForceReplicationWorkflowName is the registered workflow name.
	// Distinct from the legacy ForceReplicationWorkflow so both variants
	// coexist in the same worker — pick which to use at workflow start
	// time by setting the start request's workflow type.
	shardedForceReplicationWorkflowName = "force-replication-sharded"

	// releaseShardsSignalName carries mid-flight ReleaseShards signals
	// from active replicate-batch activities back to their parent
	// workflow. Drain-mode shard completions ride the activity return
	// value instead, so this signal only fires while the activity is
	// still running normally.
	releaseShardsSignalName = "ReleaseShards"

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
	// long (carried across CAN via the resume payload), the activity
	// fails non-retryably naming the stuck shard.
	defaultShardNoProgress = 5 * time.Minute

	// defaultDrainGrace is the wall-budget the activity gets after
	// the workflow cancels it (on lastErr or cycle drain timeout).
	// Continues verifying until either the grace expires, the
	// idle-cost trigger fires, or every exec verifies.
	defaultDrainGrace = 15 * time.Second

	// defaultIdleShardCost is the cumulative idle-time threshold
	// (the "shard-seconds" unit: 30 s with 1 idle shard equals
	// 3.3 s with 9 idle) at which the activity signal-releases its
	// completed-but-not-yet-released shards mid-flight.
	defaultIdleShardCost = 30 * time.Second

	// defaultCycleDrainTimeout bounds the wall-clock the workflow
	// will spend draining buckets + awaiting in-flight activities
	// after the page loop stops. GetContinueAsNewSuggested trips at
	// ~8% of the hard history cap so we have ~92% of the budget
	// remaining when the page loop breaks; 10 minutes is generously
	// inside that. Catches the "many shards making slow-but-real
	// progress" case; a single stuck shard is already bounded by
	// ShardNoProgress on the activity side.
	defaultCycleDrainTimeout = 10 * time.Minute

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
// run. The wire shape behind shardedBatchReq.Executions, ResumeShard.Execs
// (the per-shard inner map), and ShardedForceReplicationParams.RecoveredBuckets.
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
	for _, byBID := range p {
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

// merge folds src into p.
func (p BatchPayload) merge(src BatchPayload) {
	for sh, byBID := range src {
		if p[sh] == nil {
			p[sh] = map[string][]RunEntry{}
		}
		for bid, runs := range byBID {
			p[sh][bid] = append(p[sh][bid], runs...)
		}
	}
}

// ShardedForceReplicationParams is the workflow input. Configuration
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
	DrainGrace      time.Duration
	IdleShardCost   time.Duration

	// CycleDrainTimeout caps how long the workflow will spend after
	// the page loop stops, draining buckets and waiting for in-flight
	// activities to complete naturally. On expiry the workflow falls
	// into drainForCAN — cancels in-flight batches, collects their
	// drain payload, and CANs with the recovered state. Defaults to
	// defaultCycleDrainTimeout.
	CycleDrainTimeout time.Duration

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

	// ResumeShards carries unverified execs from drained activities in
	// the prior CAN cycle. The new run dispatches resume activities for
	// these before the page loop runs, so their shards are claimed in
	// shardInFlight from the start and the packer treats them as busy.
	ResumeShards []ResumeShard

	// RecoveredBuckets carries execs whose dispatching activity returned
	// a cancellation without returning a result — i.e., the activity
	// body never ran, because cancellation (from lastErr or cycle drain
	// timeout) reached it before the worker picked it up. They were
	// dispatched but never injected, so the new cycle restores them
	// into the streaming buckets to be dispatched as fresh inject+verify
	// batches.
	RecoveredBuckets BatchPayload

	TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus
}

// ResumeShard carries one shard's worth of unverified execs from a drained
// activity across a CAN boundary to the resume activity that picks them
// up. NoProgressDuration is the cumulative time the shard went without a
// verified outcome at drain time; the resume activity initialises its own
// per-shard last-progress clock to (now - NoProgressDuration) so the
// backstop check sees the full elapsed no-progress window, not just the
// current activity's slice.
//
// Execs is keyed by businessID: each entry is a list of RunEntry tuples
// for that BID. Grouping by BID at the wire level lets a hot BID (with
// many runs) collapse to one BID-string + N tuples rather than N copies
// of the BID; see BatchPayload's docstring.
type ResumeShard struct {
	Shard              int32
	Execs              map[string][]RunEntry
	NoProgressDuration time.Duration
}

// shardedBatchReq is the per-batch activity input. Executions is the
// per-shard, per-BID nested payload — the workflow has marked every
// shard appearing as a top-level key in shardInFlight before dispatch,
// and the activity is responsible for either signal-releasing each shard
// mid-flight or listing it in the return value's CompletedShards / InFlight
// set.
//
// Resume=true skips the inject phase: the execs were already injected by
// some earlier activity that was cancelled at drain time and returned its
// unverified execs in its result. NoProgressByShard carries the cumulative
// pre-resume no-progress duration so the per-shard backstop stays
// meaningful across resume cycles.
type shardedBatchReq struct {
	BatchID     int64
	Namespace   string
	NamespaceID string
	Executions  BatchPayload

	TargetClusterName string

	Resume              bool
	DisableVerification bool
	NoProgressByShard   map[int32]time.Duration

	PerBatchGenerateRPS float64

	ShardNoProgress time.Duration
	DrainGrace      time.Duration
	IdleShardCost   time.Duration
}

// replicateBatchResult is the activity's return payload. The activity is
// the source of truth for which execs verified vs. are still outstanding
// when it returns — only it has the per-exec verify state — so the drain
// payload rides the return value rather than a signal. The workflow's
// dispatch coroutine reads InFlight into drainPayload on nil-error return.
//
// CompletedShards is informational (the dispatch coroutine's defer clears
// heldByBatch + shardInFlight regardless), but keeping it in the result
// gives metrics a clean handle on "which shards this batch finished".
type replicateBatchResult struct {
	CompletedShards []int32
	InFlight        []ResumeShard

	// VerifiedCount is the number of executions this activity invocation
	// finished verifying (including retention/zombie skips that resolve
	// as verified). The workflow accumulates this into its running
	// ReplicatedWorkflowCount and emits the per-batch delta as the
	// replicated_workflow_count counter.
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
// shards. Only fires in normal mode — once the activity enters drain mode
// it returns its remaining state via the activity result instead.
type releaseShardsPayload struct {
	BatchID int64
	Shards  []int32
}
