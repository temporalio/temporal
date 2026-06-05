package migration

import (
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
)

// ShardedForceReplicationWorkflow runs the sharded design for one CAN
// cycle: dispatch any resume activities carried over from the prior
// cycle, then page through ListWorkflows until either the namespace
// exhausts or workflow.GetContinueAsNewSuggested(ctx) trips, bucketing
// each execution by destination history shard and dispatching a
// paired inject+verify activity once a bucket reaches packing
// eligibility. At cycle end, every remaining bucket flushes as
// packed activities.
//
// If there are more pages, in-flight activities are cancelled (giving
// them DrainGrace to drain), their drain payload arrives via the
// activity return value, and the workflow CANs with NextPageToken +
// ResumeShards in the carry-over. Otherwise it waits for activities
// to finish naturally and returns nil.
func ShardedForceReplicationWorkflow(ctx workflow.Context, params ShardedForceReplicationParams) error {
	// Page token at workflow entry — returned by the status query as
	// PageTokenForRestart so an operator can resume from this run's
	// starting position rather than its current (in-flight) position.
	startPageToken := params.NextPageToken

	// Register the status query under the same name upstream uses
	// (forceReplicationStatusQueryType = "force-replication-status")
	// so tooling that polls force-rep progress works across both
	// workflow variants.
	if err := workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		return ForceReplicationStatus{
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
			ReplicatedWorkflowCount:            params.ReplicatedWorkflowCount,
			ReplicatedWorkflowCountPerSecond:   params.ReplicatedWorkflowCountPerSecond,
			PageTokenForRestart:                startPageToken,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
		}, nil
	}); err != nil {
		return err
	}

	if err := validateShardedForceReplicationParams(&params); err != nil {
		return err
	}

	state, err := newShardedWorkflowState(ctx, &params)
	if err != nil {
		return err
	}
	// Defaults are now applied; reject configurations the packer
	// can't honour. MaxExecsPerShard > BatchSize is meaningless —
	// each batch caps at BatchSize total, so the per-shard cap
	// can't exceed the whole-batch cap.
	if params.MaxExecsPerShard > params.BatchSize {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("MaxExecsPerShard (%d) must be <= BatchSize (%d)", params.MaxExecsPerShard, params.BatchSize),
			"InvalidConfiguration", nil)
	}

	// On the first cycle, populate TotalForceReplicateWorkflowCount
	// via the same CountWorkflow activity upstream uses. Skipped on
	// subsequent CAN cycles — the count carries across via params.
	if params.TotalForceReplicateWorkflowCount == 0 {
		wfCount, err := shardedCountWorkflowsForReplication(ctx, &params)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	if !params.TaskQueueUserDataReplicationStatus.Done {
		if err := maybeKickoffShardedTaskQueueUserDataReplication(ctx, &params, func(failureReason string) {
			params.TaskQueueUserDataReplicationStatus.FailureMessage = failureReason
			params.TaskQueueUserDataReplicationStatus.Done = true
		}); err != nil {
			return err
		}
	}

	if err := state.run(ctx); err != nil {
		return err
	}

	// state.run returned nil only on the terminal cycle (no more pages,
	// no errors). On CAN cycles it returns the CAN error, so we never
	// reach here mid-replication.
	if err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done }); err != nil {
		return err
	}
	if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
		return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
	}
	return nil
}

func validateShardedForceReplicationParams(params *ShardedForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: Namespace is required", "InvalidArgument", nil)
	}
	if len(params.TargetClusterName) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: TargetClusterName is required", "InvalidArgument", nil)
	}
	return nil
}

func maybeKickoffShardedTaskQueueUserDataReplication(ctx workflow.Context, params *ShardedForceReplicationParams, onDone func(failureReason string)) error {
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, taskQueueUserDataReplicationDoneSignalType)
		var errStr string
		_ = ch.Receive(ctx, &errStr)
		onDone(errStr)
	})

	if params.ContinuedAsNewCount > 0 {
		return nil
	}

	options := workflow.ChildWorkflowOptions{
		WorkflowID:        fmt.Sprintf("%s-task-queue-user-data-replicator", workflow.GetInfo(ctx).WorkflowExecution.ID),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
	childCtx := workflow.WithChildOptions(ctx, options)
	input := TaskQueueUserDataReplicationParamsWithNamespace{
		TaskQueueUserDataReplicationParams: params.TaskQueueUserDataReplicationParams,
		Namespace:                          params.Namespace,
	}
	child := workflow.ExecuteChildWorkflow(childCtx, ForceTaskQueueUserDataReplicationWorkflow, input)
	var childExecution workflow.Execution
	return child.GetChildWorkflowExecution().Get(ctx, &childExecution)
}

// shardedCountWorkflowsForReplication asks the frontend how many
// workflows match the namespace's force-rep query. Used once at
// workflow start to seed TotalForceReplicateWorkflowCount for the
// status query's progress reporting.
func shardedCountWorkflowsForReplication(ctx workflow.Context, params *ShardedForceReplicationParams) (int64, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}
	var a *activities
	var output countWorkflowResponse
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, ao),
		a.CountWorkflow,
		&workflowservice.CountWorkflowExecutionsRequest{
			Namespace: params.Namespace,
			Query:     params.Query,
		}).Get(ctx, &output); err != nil {
		return 0, err
	}
	return output.WorkflowCount, nil
}

// shardedWorkflowState holds the workflow's per-run state. Workflow
// coroutines yield only at SDK calls, so plain maps + ints are safe
// without mutexes — workflow.Await re-evaluates its predicate after
// each yield, which is what makes the shard-in-flight bookkeeping
// drive each dispatch coroutine's wait.
type shardedWorkflowState struct {
	params *ShardedForceReplicationParams

	namespaceID string

	// targetShardCount is the target cluster's history shard count,
	// fetched once via DescribeTargetCluster at state construction.
	// Drives the per-exec shard hash (so packing groups execs by their
	// destination shard) and the default ConcurrentBatchCount.
	targetShardCount int32

	// buckets accumulate execs that have been listed but not yet
	// dispatched. Nested by destination shard then businessID, so a
	// hot BID's many runs share one BID-string-worth of bytes when
	// the bucket is shipped over the wire.
	buckets BatchPayload

	// bucketCounts mirrors len of all runs across BIDs for each
	// shard. Kept as a sidecar so the packer's per-shard ordering
	// decisions are O(1) rather than O(#BIDs in shard); it's
	// consulted many times per cycle.
	bucketCounts map[int32]int

	// shardInFlight is the per-shard exclusivity set: a shard's
	// entry is set when it's part of any in-flight batch and
	// cleared when that batch returns (either fully or via mid-flight
	// signal-release). Concurrent batches are limited only by this
	// set — there is no global slot cap.
	shardInFlight map[int32]bool

	// heldByBatch tracks per-batch shard ownership. spawnBatch
	// populates it with the batch's claimed shards; the signal
	// handler removes entries as shards are released mid-flight;
	// the dispatch coroutine's defer clears whatever's left after
	// the activity returns. Required because a signal-released
	// shard may have been re-claimed by a subsequent batch — the
	// returning original batch must only clear its own remaining
	// claims, not stomp on the new claimant.
	heldByBatch map[int64]map[int32]bool

	// activityCtx is a cancellable child of run()'s ctx; every
	// dispatched batch activity is derived from it. cancelActivities
	// is the matching cancel func — drainForCAN calls it once to
	// cancel every in-flight batch at once. Cancelling activityCtx
	// leaves the workflow's main ctx alive so the drain loop's
	// Await keeps running.
	activityCtx      workflow.Context
	cancelActivities workflow.CancelFunc

	// batchExecs tracks the input payload of each in-flight batch.
	// Cleared on any nil-error return (drained execs are folded
	// into drainPayload from the activity result; cleanly completed
	// batches return an empty InFlight). Anything left at CAN time
	// corresponds to a batch whose activity returned CanceledError
	// with no result — i.e. the activity body never ran. Those
	// execs are recovered into the next cycle's streaming buckets.
	batchExecs map[int64]BatchPayload

	// pendingDispatches counts spawned dispatch coroutines that
	// have not yet returned. The main coroutine waits on this
	// dropping to zero before issuing CAN or returning.
	pendingDispatches int

	// drainPayload accumulates ResumeShard entries from drained
	// activities (via the activity result on nil-error return).
	// Fed into the CAN-carry-over params at the end.
	drainPayload []ResumeShard

	// lastErr stops further dispatch once an activity errors out
	// (e.g. ShardNoProgress). Without it the workflow would keep
	// paging through ListWorkflows after a broken namespace surfaces,
	// burning the rest of the population before returning the
	// failure.
	lastErr error

	nextBatchID int64

	// metricsHandler is tagged with the workflow's fixed scope +
	// namespace once at state construction; recordVerified reuses it on
	// every batch return.
	metricsHandler sdkclient.MetricsHandler
}

func newShardedWorkflowState(ctx workflow.Context, params *ShardedForceReplicationParams) (*shardedWorkflowState, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}
	metaCtx := workflow.WithActivityOptions(ctx, ao)
	var a *activities
	var md MetadataResponse
	if err := workflow.ExecuteActivity(metaCtx, a.GetMetadata, MetadataRequest{Namespace: params.Namespace}).Get(ctx, &md); err != nil {
		return nil, err
	}
	var targetMd DescribeTargetClusterResponse
	if err := workflow.ExecuteActivity(metaCtx, a.DescribeTargetCluster, DescribeTargetClusterRequest{
		TargetClusterName: params.TargetClusterName,
	}).Get(ctx, &targetMd); err != nil {
		return nil, err
	}
	if params.BatchSize <= 0 {
		params.BatchSize = defaultBatchSize
	}
	if params.MaxExecsPerShard <= 0 {
		params.MaxExecsPerShard = defaultMaxExecsPerShard
	}
	if params.ShardNoProgress <= 0 {
		params.ShardNoProgress = defaultShardNoProgress
	}
	if params.DrainGrace <= 0 {
		params.DrainGrace = defaultDrainGrace
	}
	if params.IdleShardCost <= 0 {
		params.IdleShardCost = defaultIdleShardCost
	}
	if params.ListWorkflowsPageSize <= 0 {
		params.ListWorkflowsPageSize = defaultShardedListPageSize
	}
	if params.PerBatchGenerateRPS <= 0 {
		params.PerBatchGenerateRPS = defaultPerBatchGenerateRPS
	}
	if params.ConcurrentBatchCount <= 0 {
		params.ConcurrentBatchCount = defaultConcurrentBatchCount(targetMd.ShardCount)
	}
	if params.EstimationMultiplier <= 0 {
		params.EstimationMultiplier = 2
	}
	// QPSQueue is sized off ConcurrentBatchCount (one sample slot per
	// expected in-flight batch + one for the starting count). Seeded
	// with the current ReplicatedWorkflowCount so the very first
	// post-CAN batch return has a baseline to compute the rate against.
	if params.QPSQueue.Data == nil {
		params.QPSQueue = NewQPSQueue(params.ConcurrentBatchCount, params.EstimationMultiplier)
		params.QPSQueue.Enqueue(ctx, params.ReplicatedWorkflowCount)
	}
	s := &shardedWorkflowState{
		params:           params,
		namespaceID:      md.NamespaceID,
		targetShardCount: targetMd.ShardCount,
		buckets:          BatchPayload{},
		bucketCounts:     map[int32]int{},
		shardInFlight:    map[int32]bool{},
		heldByBatch:      map[int64]map[int32]bool{},
		batchExecs:       map[int64]BatchPayload{},
		metricsHandler: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{
			metrics.OperationTagName: metrics.MigrationWorkflowScope,
			NamespaceTagName:         params.Namespace,
		}),
	}
	// Restore execs recovered from cancel-before-start batches in
	// the prior cycle so the streaming packer picks them up
	// alongside any new pages.
	s.buckets.merge(params.RecoveredBuckets)
	for sh, byBID := range params.RecoveredBuckets {
		for _, runs := range byBID {
			s.bucketCounts[sh] += len(runs)
		}
	}
	params.RecoveredBuckets = nil
	return s, nil
}

func (s *shardedWorkflowState) run(ctx workflow.Context) error {
	// Cancellable child ctx for all dispatched batch activities.
	// drainForCAN cancels it once to drain in-flight batches without
	// touching the workflow's main ctx (which the drain loop's
	// Await still rides on).
	actCtx, cancelAll := workflow.WithCancel(ctx)
	defer cancelAll()
	s.activityCtx = actCtx
	s.cancelActivities = cancelAll

	// Start the signal handler coroutine first so any signal
	// arriving during resume dispatch or page-loop drains is
	// processed promptly.
	workflow.Go(ctx, s.handleReleaseSignals)

	// Dispatch resume activities carried over from the prior cycle.
	// Done before the page loop so their shards are claimed in
	// shardInFlight before any new pages arrive — keeps the packer
	// from racing to dispatch against them with fresh execs.
	s.dispatchResumeBatches(ctx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    3,
		},
	}
	listCtx := workflow.WithActivityOptions(ctx, ao)

	// Drive ListWorkflows until either we exhaust the namespace or
	// the SDK signals that history is large enough to CAN.
	for !workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
		if s.lastErr != nil {
			break
		}
		listReq := &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     s.params.Namespace,
			Query:         s.params.Query,
			PageSize:      int32(s.params.ListWorkflowsPageSize),
			NextPageToken: s.params.NextPageToken,
		}
		var a *activities
		var listResp listWorkflowsResponse
		if err := workflow.ExecuteActivity(listCtx, a.ListWorkflows, listReq).Get(ctx, &listResp); err != nil {
			return err
		}
		for _, ex := range listResp.Executions {
			sh := common.WorkflowIDToHistoryShard(s.namespaceID, ex.BusinessID, s.targetShardCount)
			s.addToBucket(sh, ex.BusinessID, RunEntry{
				RunID:       ex.RunID,
				ArchetypeID: ex.ArchetypeID,
			})
		}
		s.params.NextPageToken = listResp.NextPageToken

		for s.tryPackStreaming(ctx, false) { //nolint:revive // intentional empty body
		}

		if len(listResp.NextPageToken) == 0 {
			break
		}
	}

	// Drain remaining buckets — dispatch every leftover exec. The
	// dispatched batches may themselves get cancelled by drainForCAN
	// below if we're going to CAN; that's fine, each cancelled
	// batch returns its drain payload in its result.
	if s.lastErr == nil {
		s.drainBuckets(ctx)
	}

	// If there are no more pages we're done: wait for activities to
	// finish naturally and return. Slow shards hold their claims
	// until their per-shard no-progress backstop trips.
	if len(s.params.NextPageToken) == 0 || s.lastErr != nil {
		_ = workflow.Await(ctx, func() bool {
			return s.pendingDispatches == 0 || s.lastErr != nil
		})
		if s.lastErr != nil {
			return s.lastErr
		}
		return nil
	}

	// More pages → CAN. Cancel in-flight batches, wait for them to
	// drain, then harvest any leftover batchExecs entries (batches
	// whose activity was cancelled before its body ran — no result
	// returned) into RecoveredBuckets so they get re-dispatched as
	// fresh inject+verify activities next cycle.
	s.drainForCAN(ctx)

	if s.lastErr != nil {
		return s.lastErr
	}

	next := *s.params
	next.ContinuedAsNewCount++
	// Defensive copy so we don't alias s.drainPayload into the
	// carry-over params. drainForCAN guarantees every spawnBatch
	// coroutine has finished appending before we get here (the
	// append happens before the defer that decrements
	// pendingDispatches), so this is style — but cheap insurance
	// against future code paths that append post-drain.
	next.ResumeShards = append([]ResumeShard(nil), s.drainPayload...)
	next.RecoveredBuckets = collectRecoveredBuckets(s.batchExecs)
	return workflow.NewContinueAsNewError(ctx, ShardedForceReplicationWorkflow, next)
}

// recordVerified accumulates one batch's verified-exec delta into the
// workflow's running count, emits the per-batch counter delta, and
// updates the sliding-window RPS gauge. No-op when verified == 0 so a
// batch that ran entirely as drain-no-progress doesn't poison the
// QPSQueue with a zero-delta sample.
func (s *shardedWorkflowState) recordVerified(ctx workflow.Context, verified int64) {
	if verified <= 0 {
		return
	}
	s.params.ReplicatedWorkflowCount += verified

	s.metricsHandler.Counter(metrics.ReplicatedWorkflowCount.Name()).Inc(verified)

	s.params.QPSQueue.Enqueue(ctx, s.params.ReplicatedWorkflowCount)
	s.params.ReplicatedWorkflowCountPerSecond = s.params.QPSQueue.CalculateQPS()
	s.metricsHandler.Gauge(ForceReplicationRpsTagName).Update(s.params.ReplicatedWorkflowCountPerSecond)
}

// defaultConcurrentBatchCount derives the in-flight-batch ceiling
// from the target cluster's shard count: a quarter of the shards,
// capped at defaultConcurrentBatchCap. The 1/4 fraction leaves worker
// slots free for unrelated activities; the absolute cap bounds the
// cluster blast radius regardless of cluster size. Returns at least 1.
func defaultConcurrentBatchCount(shards int32) int {
	return max(min(int(shards)/4, defaultConcurrentBatchCap), 1)
}

// collectRecoveredBuckets re-buckets any execs from batches whose
// dispatching activity returned CanceledError without returning a
// result — i.e. the activity body never ran, so its execs were
// never injected. They go back into the next cycle's streaming
// buckets to be dispatched as fresh inject+verify batches. The
// shard is the top-level map key on each batch's payload, so no
// re-hashing here — collectRecoveredBuckets just merges.
func collectRecoveredBuckets(batchExecs map[int64]BatchPayload) BatchPayload {
	if len(batchExecs) == 0 {
		return nil
	}
	out := BatchPayload{}
	for _, bp := range batchExecs {
		out.merge(bp)
	}
	return out
}

// addToBucket appends one run to the (shard, BID) bucket and bumps
// the sidecar count.
func (s *shardedWorkflowState) addToBucket(shard int32, businessID string, run RunEntry) {
	if s.buckets[shard] == nil {
		s.buckets[shard] = map[string][]RunEntry{}
	}
	s.buckets[shard][businessID] = append(s.buckets[shard][businessID], run)
	s.bucketCounts[shard]++
}

// takeFromBucket consumes up to n runs from the given shard and
// returns them grouped by BID. Walks BIDs in alphabetical order so
// the resulting payload is deterministic across replays; takes whole
// per-BID runs only as needed to reach n. Empties the shard from
// s.buckets / s.bucketCounts when nothing remains.
func (s *shardedWorkflowState) takeFromBucket(shard int32, n int) map[string][]RunEntry {
	if n <= 0 {
		return nil
	}
	byBID := s.buckets[shard]
	if len(byBID) == 0 {
		return nil
	}
	bids := make([]string, 0, len(byBID))
	for bid := range byBID {
		bids = append(bids, bid)
	}
	slices.Sort(bids)

	out := map[string][]RunEntry{}
	taken := 0
	for _, bid := range bids {
		if taken >= n {
			break
		}
		runs := byBID[bid]
		take := min(len(runs), n-taken)
		// append([]RunEntry(nil), ...) gives the output its own
		// backing array — keeps the workflow's leftover slice
		// (byBID[bid][take:]) and the activity's input independent
		// in case either side appends later.
		out[bid] = append([]RunEntry(nil), runs[:take]...)
		if take == len(runs) {
			delete(byBID, bid)
		} else {
			byBID[bid] = runs[take:]
		}
		taken += take
	}
	s.bucketCounts[shard] -= taken
	if s.bucketCounts[shard] <= 0 {
		delete(s.bucketCounts, shard)
		delete(s.buckets, shard)
	}
	return out
}

// handleReleaseSignals runs as a long-lived workflow coroutine,
// consuming ReleaseShards signals from in-flight activities. Each
// signal lists shards the activity considers complete; the handler
// clears them from heldByBatch[BatchID] (so the dispatch coroutine's
// defer won't double-release) and shardInFlight (so the packer can
// dispatch new work against them while the activity stays running on
// its still-pending shards).
//
// DO NOT add workflow yields (ExecuteActivity, Sleep, Await, etc.)
// between Receive and the next Receive. drainForCAN relies on
// ch.Len() == 0 implying "every delivered signal has been processed";
// a yield mid-handler would invalidate that, leaving shardInFlight
// stale after a CAN.
func (s *shardedWorkflowState) handleReleaseSignals(ctx workflow.Context) {
	ch := workflow.GetSignalChannel(ctx, releaseShardsSignalName)
	for ctx.Err() == nil {
		var payload releaseShardsPayload
		if !ch.Receive(ctx, &payload) {
			return
		}
		held, ok := s.heldByBatch[payload.BatchID]
		if !ok {
			continue
		}
		for _, sh := range payload.Shards {
			if held[sh] {
				delete(held, sh)
				delete(s.shardInFlight, sh)
			}
		}
	}
}

// dispatchSlotAvailable returns true when the workflow is below the
// in-flight batch ceiling and is free to spawn another batch. Callers
// that can defer dispatch (the streaming packer) consult this and
// bail out; callers that must dispatch (resume payloads) pair it
// with waitForDispatchSlot. ConcurrentBatchCount is normalised to
// >= 1 at state construction, so no zero-disable path is needed.
func (s *shardedWorkflowState) dispatchSlotAvailable() bool {
	return s.pendingDispatches < s.params.ConcurrentBatchCount
}

// waitForDispatchSlot blocks the calling workflow coroutine until a
// dispatch slot frees up or lastErr trips.
func (s *shardedWorkflowState) waitForDispatchSlot(ctx workflow.Context) {
	_ = workflow.Await(ctx, func() bool {
		return s.lastErr != nil || s.pendingDispatches < s.params.ConcurrentBatchCount
	})
}

// dispatchResumeBatches turns the prior cycle's drain payload into a
// fresh round of resume activities, packed across shards up to
// BatchSize per batch. Each shard appears at most once across the
// payload (shardInFlight enforces that only one batch holds a shard
// at a time, and a shard only lands in a drain return while its
// owning batch still has unverified execs on it), so per-shard
// contributions are taken whole and no MaxExecsPerShard cap applies —
// resume carries no inject load so the per-shard blast-radius the
// streaming packer guards against doesn't exist here.
func (s *shardedWorkflowState) dispatchResumeBatches(ctx workflow.Context) {
	if len(s.params.ResumeShards) == 0 {
		return
	}
	entries := make([]ResumeShard, 0, len(s.params.ResumeShards))
	for _, rs := range s.params.ResumeShards {
		if runCount(rs.Execs) == 0 {
			continue
		}
		entries = append(entries, rs)
	}
	slices.SortFunc(entries, func(a, b ResumeShard) int {
		return int(a.Shard - b.Shard)
	})

	payload := BatchPayload{}
	packed := 0
	packNoProgress := map[int32]time.Duration{}
	flush := func() {
		if packed == 0 {
			return
		}
		// Block until a dispatch slot is free so resume payloads
		// can't overshoot ConcurrentBatchCount on cycles that
		// carried many shards across CAN.
		s.waitForDispatchSlot(ctx)
		if s.lastErr != nil {
			return
		}
		for sh := range payload {
			s.shardInFlight[sh] = true
		}
		s.spawnBatch(ctx, payload, true, packNoProgress)
		payload = BatchPayload{}
		packed = 0
		packNoProgress = map[int32]time.Duration{}
	}

	for _, rs := range entries {
		if s.lastErr != nil {
			return
		}
		rsCount := runCount(rs.Execs)
		// Flush before this shard if it would push us over the
		// batch cap, so each batch stays within BatchSize. A
		// single shard's contribution is taken whole — we don't
		// split a shard across batches because shardInFlight only
		// admits one batch per shard at a time.
		if packed+rsCount > s.params.BatchSize && packed > 0 {
			flush()
		}
		payload[rs.Shard] = rs.Execs
		packNoProgress[rs.Shard] = rs.NoProgressDuration
		packed += rsCount
		if packed >= s.params.BatchSize {
			flush()
		}
	}
	flush()
}

// runCount sums runs across BIDs in a single shard's payload entry.
func runCount(byBID map[string][]RunEntry) int {
	n := 0
	for _, runs := range byBID {
		n += len(runs)
	}
	return n
}

// drainBuckets blocks until buckets are empty (success) or lastErr
// trips (failure). Each pass packs everything currently dispatchable,
// then awaits any change in pendingDispatches + shardInFlight so the
// next pass can attempt shards just freed by signal-release.
func (s *shardedWorkflowState) drainBuckets(ctx workflow.Context) {
	for {
		if s.lastErr != nil {
			return
		}
		for s.tryPackStreaming(ctx, true) { //nolint:revive
		}
		if s.bucketsEmpty() || s.lastErr != nil {
			return
		}
		currentPending := s.pendingDispatches
		if currentPending == 0 {
			s.failDrainBucketsStuck()
			return
		}
		_ = workflow.Await(ctx, s.drainBucketsAwaitPredicate(currentPending))
	}
}

// failDrainBucketsStuck sets lastErr when buckets are non-empty but no
// batches are in flight — the shard-claim bookkeeping is corrupted, and
// returning silently would proceed to CAN with execs that were never
// dispatched (silent data loss). Failing forces lastErr to propagate
// through run().
func (s *shardedWorkflowState) failDrainBucketsStuck() {
	remaining := 0
	for _, n := range s.bucketCounts {
		remaining += n
	}
	s.lastErr = temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("drainBuckets: %d execs in buckets but no batches in flight (shard-claim bookkeeping corrupted)", remaining),
		"DrainBucketsStuck", nil)
}

// drainBucketsAwaitPredicate returns true when the drainBuckets loop
// should wake up: lastErr tripped, a dispatch slot just freed, or a
// new free shard is ready to pack. A "free shard" wake-up only counts
// when there's also a dispatch slot to use it, otherwise the outer
// loop would busy-spin on tryPackStreaming returning false against the
// in-flight cap.
func (s *shardedWorkflowState) drainBucketsAwaitPredicate(currentPending int) func() bool {
	return func() bool {
		if s.lastErr != nil {
			return true
		}
		if s.pendingDispatches < currentPending {
			return true
		}
		if !s.dispatchSlotAvailable() {
			return false
		}
		for sh, n := range s.bucketCounts {
			if n > 0 && !s.shardInFlight[sh] {
				return true
			}
		}
		return false
	}
}

// drainForCAN cancels every in-flight batch and waits for them to
// return AND for the ReleaseShards signal channel to be drained.
// Activities honour cancellation by entering drain mode and returning
// a result whose InFlight carries their still-unverified execs;
// spawnBatch appends those entries to s.drainPayload. The signal
// channel drain is so a final ReleaseShards fired by an activity just
// before it returns doesn't get stranded mid-flight, which would
// leave shardInFlight set for shards the activity already considers
// complete.
//
// Channel.Len() is safe here because handleReleaseSignals has no
// yield points between Receive and the next blocking Receive, so
// Len() == 0 observed across an Await re-evaluation means every
// delivered signal has been processed.
//
// No explicit time bound: a well-behaved activity returns within
// req.DrainGrace (15s default) plus a small idle-cost slack; a
// misbehaved one is bounded by the activity's HeartbeatTimeout (1m).
// In practice this Await unblocks well under a minute.
func (s *shardedWorkflowState) drainForCAN(ctx workflow.Context) {
	if s.pendingDispatches == 0 {
		return
	}
	s.cancelActivities()
	releaseCh := workflow.GetSignalChannel(ctx, releaseShardsSignalName)
	_ = workflow.Await(ctx, func() bool {
		if s.lastErr != nil {
			return true
		}
		return s.pendingDispatches == 0 && releaseCh.Len() == 0
	})
}

// spawnBatch dispatches one batch on a new workflow.Go coroutine.
// Callers must have already marked every shard appearing as a
// top-level key in payload as shardInFlight (the "claim") so the
// packer can see them as busy while picking subsequent batches. The
// activity is run on s.activityCtx so drainForCAN can cancel every
// in-flight batch with a single call.
func (s *shardedWorkflowState) spawnBatch(
	ctx workflow.Context,
	payload BatchPayload,
	resume bool,
	noProgressByShard map[int32]time.Duration,
) {
	if payload.totalRuns() == 0 {
		return
	}
	s.nextBatchID++
	batchID := s.nextBatchID

	req := &shardedBatchReq{
		BatchID:             batchID,
		Namespace:           s.params.Namespace,
		NamespaceID:         s.namespaceID,
		Executions:          payload,
		TargetClusterName:   s.params.TargetClusterName,
		Resume:              resume,
		DisableVerification: s.params.DisableVerification,
		NoProgressByShard:   noProgressByShard,
		PerBatchGenerateRPS: s.params.PerBatchGenerateRPS,
		ShardNoProgress:     s.params.ShardNoProgress,
		DrainGrace:          s.params.DrainGrace,
		IdleShardCost:       s.params.IdleShardCost,
	}

	held := make(map[int32]bool, len(payload))
	for sh := range payload {
		held[sh] = true
	}
	s.heldByBatch[batchID] = held
	s.batchExecs[batchID] = payload

	s.pendingDispatches++
	workflow.Go(ctx, func(coroCtx workflow.Context) {
		defer func() {
			s.pendingDispatches--
			// Clear any shards we still hold — signal-released
			// shards have already been cleared from shardInFlight
			// by handleReleaseSignals and may by now belong to a
			// subsequent batch's claim.
			for sh := range s.heldByBatch[batchID] {
				delete(s.shardInFlight, sh)
			}
			delete(s.heldByBatch, batchID)
		}()
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 24 * time.Hour,
			HeartbeatTimeout:    time.Minute,
			// 3 attempts: per-exec backoff still owns the per-exec
			// retry, but a transient activity failure can recover
			// via heartbeat-resume without losing inject progress.
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 3,
			},
			// WaitForCancellation: the cancelled activity needs to
			// run its drain logic and return its drain result
			// before the dispatch coroutine's defer fires.
			WaitForCancellation: true,
		}
		actx := workflow.WithActivityOptions(s.activityCtx, ao)
		var result replicateBatchResult
		err := workflow.ExecuteActivity(actx, shardedBatchActivityName, req).Get(coroCtx, &result)
		if err == nil {
			// Activity body ran and returned cleanly — either a
			// clean completion (empty InFlight) or a drained
			// CAN-cancel (InFlight carries the still-unverified
			// execs). CompletedShards is informational; the
			// defer above clears heldByBatch + shardInFlight
			// either way.
			if len(result.InFlight) > 0 {
				s.drainPayload = append(s.drainPayload, result.InFlight...)
			}
			s.recordVerified(coroCtx, result.VerifiedCount)
			delete(s.batchExecs, batchID)
			return
		}
		if temporal.IsCanceledError(err) {
			// Cancel-before-start: the activity body never ran,
			// so no result is available. Leaving batchExecs[batchID]
			// intact lets the CAN-end recovery path re-bucket the
			// execs as fresh inject+verify work next cycle.
			return
		}
		s.lastErr = err
	})
}

// tryPackStreaming attempts to pack and dispatch one batch from
// s.buckets. Returns true if a batch was dispatched.
//
// No per-shard or total-bucket threshold: as soon as any free shard
// has any execs and a dispatch slot is open, a batch fires. Safety
// is enforced outside the packer — MaxExecsPerShard caps a single
// shard's contribution to a batch (so one hot shard can't dominate),
// ConcurrentBatchCount caps in-flight batches, and PerBatchGenerateRPS
// caps the per-batch source RPS. Within those bounds the packer's
// sole job is to make progress every chance it gets.
//
// See shardIDsByPackPriority for the relax-mode ordering rationale.
func (s *shardedWorkflowState) tryPackStreaming(ctx workflow.Context, relax bool) bool {
	if s.lastErr != nil || s.params.BatchSize <= 0 || s.params.MaxExecsPerShard <= 0 {
		return false
	}
	if !s.dispatchSlotAvailable() {
		return false
	}
	shardIDs := s.shardIDsByPackPriority(relax)
	if len(shardIDs) == 0 {
		return false
	}

	payload := BatchPayload{}
	packed := 0
	for _, sh := range shardIDs {
		room := s.params.BatchSize - packed
		if room <= 0 {
			break
		}
		take := min(s.params.MaxExecsPerShard, s.bucketCounts[sh], room)
		if take == 0 {
			continue
		}
		payload[sh] = s.takeFromBucket(sh, take)
		packed += take
		s.shardInFlight[sh] = true
	}
	if packed == 0 {
		return false
	}
	s.spawnBatch(ctx, payload, false, nil)
	return true
}

// bucketsEmpty reports whether every shard's bucket is empty. Reads
// from the sidecar count map so it's O(#shards), not O(#runs).
func (s *shardedWorkflowState) bucketsEmpty() bool {
	for _, n := range s.bucketCounts {
		if n > 0 {
			return false
		}
	}
	return true
}

// shardIDsByPackPriority returns free, non-empty shard IDs in the
// order the packer should consider them. Deterministic across
// replays: ordering is derived from workflow state (bucketCounts)
// with shard ID as a stable tiebreaker.
//
// relax=false (streaming): fullest first. Packer naturally produces
// large, predictable batches when work is plentiful and small ones
// when it isn't — either way it ships rather than waiting.
//
// relax=true (drain): hot shards (count > MaxExecsPerShard) first,
// fullest within hot. These need >1 round trip to drain, so total
// drain wall-clock is bounded by the heaviest shard; starting their
// pipelines first is the dominant lever. After all hot shards are
// claimed, remaining batch capacity fills from smallest cold buckets
// (ascending count) so light shards clear out quickly — nothing is
// arriving in drain, so waiting for cold buckets to grow is wasted
// wall-clock.
func (s *shardedWorkflowState) shardIDsByPackPriority(relax bool) []int32 {
	out := make([]int32, 0, len(s.bucketCounts))
	for sh, n := range s.bucketCounts {
		if n == 0 || s.shardInFlight[sh] {
			continue
		}
		out = append(out, sh)
	}
	if relax {
		maxPerShard := s.params.MaxExecsPerShard
		slices.SortFunc(out, func(a, b int32) int {
			aHot, bHot := s.bucketCounts[a] > maxPerShard, s.bucketCounts[b] > maxPerShard
			switch {
			case aHot && !bHot:
				return -1
			case !aHot && bHot:
				return 1
			case aHot && bHot:
				if d := s.bucketCounts[b] - s.bucketCounts[a]; d != 0 {
					return d
				}
			default:
				if d := s.bucketCounts[a] - s.bucketCounts[b]; d != 0 {
					return d
				}
			}
			return int(a - b)
		})
		return out
	}
	slices.SortFunc(out, func(a, b int32) int {
		if d := s.bucketCounts[b] - s.bucketCounts[a]; d != 0 {
			return d
		}
		return int(a - b)
	})
	return out
}
