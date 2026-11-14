package migration

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
)

// shardedForceReplicationWorker lists and replicates a range of executions.
// It stops listing at a page boundary when continue-as-new is suggested, drains
// every buffered and in-flight execution, then returns the next page token.
func shardedForceReplicationWorker(ctx workflow.Context, params shardedChildParams) (shardedChildResult, error) {
	s := &shardedWorkflowState{
		params:           &params,
		namespaceID:      params.NamespaceID,
		targetShardCount: params.TargetShardCount,
		metricsHandler: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{
			metrics.OperationTagName: metrics.MigrationWorkflowScope,
			NamespaceTagName:         params.Namespace,
		}),
	}
	return s.run(ctx)
}

// shardedWorkflowState holds the child workflow's per-run state. Workflow
// coroutines yield only at SDK calls, so plain maps + ints are safe
// without mutexes — workflow.Await re-evaluates its predicate after each
// yield, which is what makes the shard-in-flight bookkeeping drive each
// dispatch coroutine's wait.
type shardedWorkflowState struct {
	params *shardedChildParams

	namespaceID string

	// targetShardCount is the target cluster's history shard count.
	// Drives the per-exec shard hash and ConcurrentBatchCount derivation.
	targetShardCount int32

	// buckets buffer execs that have been listed but not yet dispatched,
	// grouped by destination shard then businessID, with a sidecar
	// per-shard count for O(1) packer ordering. See shardBuckets.
	buckets shardBuckets

	// batches tracks the shards claimed by in-flight ReplicateBatch
	// activities and enforces per-shard exclusivity (each shard is in at
	// most one in-flight batch at a time). See inFlightBatches.
	batches inFlightBatches

	// lastErr latches the first activity error. Once set, further dispatch
	// stops (tryPackStreaming and drainBuckets bail out) and the child
	// returns the error.
	lastErr error

	// verifiedCount accumulates verified executions for the final child
	// result and the 60 s progress rollup signal to the parent.
	verifiedCount int64

	// metricsHandler is tagged with the workflow's fixed scope + namespace
	// once at construction; recordVerified reuses it on every batch return.
	metricsHandler sdkclient.MetricsHandler
}

// run is the child workflow body. See shardedForceReplicationWorker for
// the full lifecycle description.
func (s *shardedWorkflowState) run(ctx workflow.Context) (shardedChildResult, error) {
	// Intra-child signal handler: activities signal mid-flight shard releases.
	workflow.Go(ctx, s.handleReleaseSignals)

	parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution
	s.startBackgroundCoroutines(ctx, parentExec)

	nextPageToken := s.listUntilCheckpointOrEnd(ctx)
	if s.lastErr != nil {
		return shardedChildResult{}, s.lastErr
	}

	s.drainBuckets(ctx)
	if s.lastErr != nil {
		return shardedChildResult{}, s.lastErr
	}

	// Await all in-flight batches. No cancellation — children always run
	// to natural completion so their verified counts are exact.
	if err := workflow.Await(ctx, func() bool { return s.batches.count() == 0 || s.lastErr != nil }); err != nil {
		return shardedChildResult{}, err
	}
	if s.lastErr != nil {
		return shardedChildResult{}, s.lastErr
	}

	return shardedChildResult{
		VerifiedCount: s.verifiedCount,
		NextPageToken: nextPageToken,
	}, nil
}

// startBackgroundCoroutines reports progress every 60 seconds when the worker
// has a parent to signal.
func (s *shardedWorkflowState) startBackgroundCoroutines(ctx workflow.Context, parentExec *workflow.Execution) {
	if parentExec == nil {
		return
	}
	// 60 s progress rollup: send cumulative verifiedCount to the parent so the
	// force-replication-status query stays current even for long-running
	// children. Stops when ctx is cancelled at workflow exit (timer Get errors).
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			if err := workflow.NewTimer(gCtx, 60*time.Second).Get(gCtx, nil); err != nil {
				return
			}
			// The parent is alive for the whole child lifetime — it awaits all
			// children before completing or continuing-as-new, and a child is
			// terminated with its parent (ParentClosePolicy TERMINATE). So a
			// progress signal can only fail for a permanent, anomalous reason
			// (SignalExternalWorkflow has no transient failure modes). Latch it
			// and stop: continuing would mean operating against a parent we can
			// no longer reach, with unpredictable results.
			err := workflow.SignalExternalWorkflow(gCtx,
				parentExec.ID, parentExec.RunID,
				shardedProgressSignalName,
				shardedProgressPayload{
					ChildRunID:    workflow.GetInfo(gCtx).WorkflowExecution.RunID,
					VerifiedCount: s.verifiedCount,
				}).Get(gCtx, nil)
			if err != nil {
				s.setLastErr(fmt.Errorf("progress signal to parent: %w", err))
				return
			}
		}
	})
}

// listUntilCheckpointOrEnd returns the next range's token at a page boundary,
// or nil when listing is exhausted. Errors are latched in lastErr.
func (s *shardedWorkflowState) listUntilCheckpointOrEnd(ctx workflow.Context) []byte {
	currentPageToken := s.params.StartPageToken
	for s.lastErr == nil {
		executions, nextPageToken, err := s.listWorkflowPageWithToken(ctx, currentPageToken)
		if err != nil {
			s.lastErr = err
			return nil
		}

		for _, ex := range executions {
			sh := common.WorkflowIDToHistoryShard(s.namespaceID, ex.BusinessID, s.targetShardCount)
			s.buckets.add(sh, ex.BusinessID, RunEntry{
				RunID:       ex.RunID,
				ArchetypeID: ex.ArchetypeID,
			})
		}

		// Ship whatever this page made packable in streaming priority
		// (relax=false: fullest shards first) so dispatch slots stay busy
		// while listing continues; the drain pass handles the remainder.
		for s.tryPackStreaming(ctx, false) { //nolint:revive // intentional empty body
		}

		if len(nextPageToken) == 0 {
			return nil
		}
		currentPageToken = nextPageToken

		// Check the CAN hint at this fully-consumed page boundary, so
		// currentPageToken is exact and safe to hand to a successor.
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			return currentPageToken
		}

		// Backpressure: don't fetch the next page until a dispatch slot
		// is free. With every slot full, tryPackStreaming can't dispatch,
		// so listing further would only inflate the buckets (and replay
		// history) with execs we can't act on. Wakes on a freed slot or
		// lastErr; the loop's lastErr guard then re-checks.
		s.waitForDispatchSlot(ctx)
	}
	return nil
}

var (
	shardedListWorkflowsRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumAttempts:    3,
	}
	shardedListWorkflowsActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		RetryPolicy:         shardedListWorkflowsRetryPolicy,
	}

	// Per-exec backoff owns the per-exec retry; MaximumAttempts lets a
	// transient activity failure (or a worker lost to a deploy) recover via
	// heartbeat-resume without losing inject or verify progress.
	//
	// 10 attempts is sized for fleet rollouts: a rolling deploy of the
	// activity workers can burn several attempts per batch (a lost worker is
	// detected once its heartbeats lapse and the attempt is retried
	// elsewhere). 3 was tight enough that two unlucky deploys could exhaust
	// the budget.
	shardedReplicateBatchRetryPolicy = &temporal.RetryPolicy{
		MaximumAttempts: 10,
	}
	shardedReplicateBatchActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		// 30s rather than the usual minute: a worker lost to a deploy or crash
		// is only detected once its heartbeats lapse, and until then the shards
		// this batch holds are stalled. The verify loop heartbeats per exec, so
		// 30s sits comfortably above the real inter-heartbeat interval and won't
		// false-positive on a slow pass.
		HeartbeatTimeout: 30 * time.Second,
		RetryPolicy:      shardedReplicateBatchRetryPolicy,
	}
)

// listWorkflowPageWithToken fetches one page of ListWorkflows results
// starting from the given continuation token (nil for the first page).
func (s *shardedWorkflowState) listWorkflowPageWithToken(ctx workflow.Context, pageToken []byte) ([]*ExecutionInfo, []byte, error) {
	listCtx := workflow.WithActivityOptions(ctx, shardedListWorkflowsActivityOptions)
	listReq := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     s.params.Namespace,
		Query:         s.params.Query,
		PageSize:      int32(s.params.ListWorkflowsPageSize),
		NextPageToken: pageToken,
	}
	var listResp listWorkflowsResponse
	if err := workflow.ExecuteActivity(listCtx, shardedListWorkflowsActivityName, listReq).Get(ctx, &listResp); err != nil {
		return nil, nil, err
	}
	return listResp.Executions, listResp.NextPageToken, nil
}

func (s *shardedWorkflowState) replicateBatch(ctx workflow.Context, req *shardedBatchReq) (replicateBatchResult, error) {
	actx := workflow.WithActivityOptions(ctx, shardedReplicateBatchActivityOptions)
	var result replicateBatchResult
	if err := workflow.ExecuteActivity(actx, shardedReplicateBatchActivityName, req).Get(ctx, &result); err != nil {
		return replicateBatchResult{}, err
	}
	return result, nil
}

// handleReleaseSignals runs as a long-lived workflow coroutine,
// consuming ReleaseShards signals from in-flight activities. Each
// signal lists shards the activity considers complete; the handler
// hands them to batches.releaseShards, which clears them from the
// batch's held set (so the dispatch coroutine's defer won't
// double-release) and from the in-flight set (so the packer can
// dispatch new work against them while the activity stays running on
// its still-pending shards).
//
// DO NOT add workflow yields (ExecuteActivity, Sleep, Await, etc.)
// between Receive and the next Receive. The handler relies on
// ch.Len() == 0 being a reliable "every delivered signal has been
// processed" indicator; a yield mid-handler would invalidate that.
func (s *shardedWorkflowState) handleReleaseSignals(ctx workflow.Context) {
	ch := workflow.GetSignalChannel(ctx, releaseShardsSignalName)
	for ctx.Err() == nil {
		var payload releaseShardsPayload
		if !ch.Receive(ctx, &payload) {
			return
		}
		s.batches.releaseShards(payload.BatchID, payload.Shards)
	}
}

// setLastErr latches the first error encountered. Subsequent errors are
// dropped so the root cause is preserved for the child's returned failure.
func (s *shardedWorkflowState) setLastErr(err error) {
	if s.lastErr == nil {
		s.lastErr = err
	}
}

// dispatchSlotAvailable returns true when the workflow is below the
// in-flight batch ceiling and is free to spawn another batch.
func (s *shardedWorkflowState) dispatchSlotAvailable() bool {
	return s.batches.count() < s.params.ConcurrentBatchCount
}

// waitForDispatchSlot blocks until a dispatch slot frees up or lastErr trips.
func (s *shardedWorkflowState) waitForDispatchSlot(ctx workflow.Context) {
	err := workflow.Await(ctx, func() bool {
		return s.lastErr != nil || s.batches.count() < s.params.ConcurrentBatchCount
	})
	if err != nil {
		s.setLastErr(err)
	}
}

// recordVerified accumulates one batch's verified-exec delta into the
// child's running count and emits the per-batch counter metric.
// No-op when verified <= 0 so batches with only inject (DisableVerification)
// don't add zeros to the count.
func (s *shardedWorkflowState) recordVerified(verified int64) {
	if verified <= 0 {
		return
	}
	s.verifiedCount += verified
	s.metricsHandler.Counter(metrics.ReplicatedWorkflowCount.Name()).Inc(verified)
}

// spawnBatch dispatches one batch on a new workflow.Go coroutine.
// Callers must have already verified that every shard in payload is free
// (batches.isInFlight false); claim marks them in-flight here.
func (s *shardedWorkflowState) spawnBatch(ctx workflow.Context, payload BatchPayload) {
	if payload.totalRuns() == 0 {
		return
	}
	batchID := s.batches.claim(payload)

	req := &shardedBatchReq{
		BatchID:             batchID,
		Namespace:           s.params.Namespace,
		NamespaceID:         s.namespaceID,
		Executions:          payload,
		TargetClusterName:   s.params.TargetClusterName,
		DisableVerification: s.params.DisableVerification,
		PerBatchGenerateRPS: s.params.PerBatchGenerateRPS,
		ShardNoProgress:     s.params.ShardNoProgress,
		IdleShardCost:       s.params.IdleShardCost,
	}

	workflow.Go(ctx, func(coroCtx workflow.Context) {
		// releaseAll drops the batch (so batches.count falls) and clears
		// whatever shards it still holds. Shards signal-released mid-flight
		// are already gone (and may now belong to a later batch), so it only
		// touches our remaining claims.
		defer s.batches.releaseAll(batchID)
		result, err := s.replicateBatch(coroCtx, req)
		if err != nil {
			s.setLastErr(err)
			return
		}
		s.recordVerified(result.VerifiedCount)
	})
}

// tryPackStreaming attempts to pack and dispatch one batch from s.buckets.
// Returns true if a batch was dispatched. Uses MaxExecsPerShard
// as the per-shard cap.
//
// No per-shard or total-bucket threshold: as soon as any free shard has
// any execs and a dispatch slot is open, a batch fires. Safety is enforced
// outside the packer — MaxExecsPerShard caps a single shard's
// contribution, ConcurrentBatchCount caps in-flight batches, and
// PerBatchGenerateRPS caps the per-batch source RPS.
func (s *shardedWorkflowState) tryPackStreaming(ctx workflow.Context, relax bool) bool {
	if s.lastErr != nil || !s.dispatchSlotAvailable() {
		return false
	}
	shardIDs := s.shardIDsByPackPriority(relax)
	if len(shardIDs) == 0 {
		return false
	}

	effectiveCap := s.params.MaxExecsPerShard
	payload := BatchPayload{}
	packed := 0
	for _, sh := range shardIDs {
		room := s.params.BatchSize - packed
		if room <= 0 {
			break
		}
		take := min(effectiveCap, s.buckets.count(sh), room)
		if take == 0 {
			continue
		}
		payload[sh] = s.buckets.take(sh, take)
		packed += take
	}
	if packed == 0 {
		return false
	}
	s.spawnBatch(ctx, payload)
	return true
}

// shardIDsByPackPriority returns free, non-empty shard IDs in the order
// the packer should consider them. Deterministic across replays: the
// candidate set comes from buckets.shards() (sorted) and ordering is
// derived from per-shard counts with shard ID as a stable tiebreaker.
//
// relax=false (streaming): fullest first. Packer naturally produces large,
// predictable batches when work is plentiful and small ones when it isn't
// — either way it ships rather than waiting.
//
// relax=true (drain): hot shards (count > MaxExecsPerShard) first,
// fullest within hot. These need >1 round trip to drain, so total drain
// wall-clock is bounded by the heaviest shard; starting their pipelines
// first is the dominant lever. After all hot shards are claimed, remaining
// batch capacity fills from smallest cold buckets (ascending count) so
// light shards clear out quickly — nothing is arriving in drain, so
// waiting for cold buckets to grow is wasted wall-clock.
func (s *shardedWorkflowState) shardIDsByPackPriority(relax bool) []int32 {
	candidates := s.buckets.shards()
	out := make([]int32, 0, len(candidates))
	for _, sh := range candidates {
		if !s.batches.isInFlight(sh) {
			out = append(out, sh)
		}
	}
	if relax {
		maxPerShard := s.params.MaxExecsPerShard
		slices.SortFunc(out, func(a, b int32) int {
			ca, cb := s.buckets.count(a), s.buckets.count(b)
			aHot, bHot := ca > maxPerShard, cb > maxPerShard
			switch {
			case aHot && !bHot:
				return -1
			case !aHot && bHot:
				return 1
			case aHot && bHot:
				if d := cb - ca; d != 0 {
					return d
				}
			default:
				if d := ca - cb; d != 0 {
					return d
				}
			}
			return int(a - b)
		})
		return out
	}
	slices.SortFunc(out, func(a, b int32) int {
		if d := s.buckets.count(b) - s.buckets.count(a); d != 0 {
			return d
		}
		return int(a - b)
	})
	return out
}

// drainBuckets blocks until buckets are empty (success) or lastErr trips
// (failure). Each pass packs everything currently dispatchable, then
// awaits any change in the in-flight batch count or shard claims so the next
// pass can attempt shards just freed by signal-release.
func (s *shardedWorkflowState) drainBuckets(ctx workflow.Context) {
	for {
		if s.lastErr != nil {
			return
		}
		for s.tryPackStreaming(ctx, true) { //nolint:revive
		}
		if s.buckets.empty() || s.lastErr != nil {
			return
		}
		currentPending := s.batches.count()
		if currentPending == 0 {
			s.failDrainBucketsStuck()
			return
		}
		if err := workflow.Await(ctx, s.drainBucketsAwaitPredicate(currentPending)); err != nil {
			s.setLastErr(err)
			return
		}
	}
}

// failDrainBucketsStuck sets lastErr when buckets are non-empty but no
// batches are in flight — the shard-claim bookkeeping is corrupted, and
// returning silently would proceed with execs that were never dispatched
// (silent data loss). Failing forces the child to return the error.
func (s *shardedWorkflowState) failDrainBucketsStuck() {
	remaining := s.buckets.totalRemaining()
	s.setLastErr(temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("drainBuckets: %d execs in buckets but no batches in flight (shard-claim bookkeeping corrupted)", remaining),
		"DrainBucketsStuck", nil))
}

// drainBucketsAwaitPredicate returns true when the drainBuckets loop
// should wake up: lastErr tripped, a dispatch slot just freed, or a new
// free shard is ready to pack. A "free shard" wake-up only counts when
// there's also a dispatch slot to use it, otherwise the outer loop would
// busy-spin on tryPackStreaming returning false against the in-flight cap.
func (s *shardedWorkflowState) drainBucketsAwaitPredicate(currentPending int) func() bool {
	return func() bool {
		if s.lastErr != nil {
			return true
		}
		if s.batches.count() < currentPending {
			return true
		}
		if !s.dispatchSlotAvailable() {
			return false
		}
		for _, sh := range s.buckets.shards() {
			if !s.batches.isInFlight(sh) {
				return true
			}
		}
		return false
	}
}

// shardBuckets accumulates listed-but-not-yet-dispatched execs, grouped by
// destination shard then businessID, with a sidecar per-shard count so the
// packer's ordering decisions stay O(1).
//
// Invariant: counts[sh] always equals the total runs under byShard[sh], and a
// shard key is present in both maps or in neither (a shard is dropped from
// both the moment its last run is taken). All mutation goes through add/take,
// so the invariant can't be violated from outside the type. The zero value is
// usable — add lazily initialises the maps on first use.
type shardBuckets struct {
	// byShard holds the execs, nested shard → businessID → runs. This is the
	// BatchPayload wire shape, reused so take can hand a shard's per-BID map
	// straight into a batch payload.
	byShard BatchPayload
	// counts mirrors len of all runs across BIDs for each shard. Consulted
	// many times per pack cycle, so it's kept as an O(1) sidecar rather than
	// recomputed by walking byShard.
	counts map[int32]int
}

// add appends one run to the (shard, BID) bucket and bumps the sidecar count.
func (b *shardBuckets) add(shard int32, businessID string, run RunEntry) {
	if b.byShard == nil {
		b.byShard = BatchPayload{}
		b.counts = map[int32]int{}
	}
	if b.byShard[shard] == nil {
		b.byShard[shard] = map[string][]RunEntry{}
	}
	b.byShard[shard][businessID] = append(b.byShard[shard][businessID], run)
	b.counts[shard]++
}

// take consumes up to n runs from the given shard and returns them grouped by
// BID. Walks BIDs in alphabetical order so the result is deterministic across
// replays; takes whole per-BID runs only as needed to reach n. Drops the shard
// from both maps when nothing remains.
func (b *shardBuckets) take(shard int32, n int) map[string][]RunEntry {
	if n <= 0 {
		return nil
	}
	byBID := b.byShard[shard]
	if len(byBID) == 0 {
		return nil
	}
	bids := make([]string, 0, len(byBID))
	//workflowcheck:ignore (bids is sorted before use)
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
		// append([]RunEntry(nil), ...) gives the output its own backing
		// array — keeps the workflow's leftover slice (byBID[bid][take:])
		// and the activity's input independent in case either side appends.
		out[bid] = append([]RunEntry(nil), runs[:take]...)
		if take == len(runs) {
			delete(byBID, bid)
		} else {
			byBID[bid] = runs[take:]
		}
		taken += take
	}
	b.counts[shard] -= taken
	if b.counts[shard] <= 0 {
		delete(b.counts, shard)
		delete(b.byShard, shard)
	}
	return out
}

// count returns the number of runs buffered for a shard (0 if none).
func (b *shardBuckets) count(shard int32) int {
	return b.counts[shard]
}

// empty reports whether every shard's bucket is empty. Reads the sidecar
// count map so it's O(#shards), not O(#runs).
func (b *shardBuckets) empty() bool {
	//workflowcheck:ignore (existence check; iteration order does not affect result)
	for _, n := range b.counts {
		if n > 0 {
			return false
		}
	}
	return true
}

// totalRemaining sums runs across all shards.
func (b *shardBuckets) totalRemaining() int {
	remaining := 0
	//workflowcheck:ignore (summation is order-independent)
	for _, n := range b.counts {
		remaining += n
	}
	return remaining
}

// shards returns every shard ID with a non-empty bucket, in ascending order.
// Sorted so callers can iterate deterministically across replays.
func (b *shardBuckets) shards() []int32 {
	out := make([]int32, 0, len(b.counts))
	//workflowcheck:ignore (output is sorted below before any observable use)
	for sh, n := range b.counts {
		if n > 0 {
			out = append(out, sh)
		}
	}
	slices.Sort(out)
	return out
}

// inFlightBatches tracks which shards are claimed by in-flight ReplicateBatch
// activities, enforcing per-shard exclusivity: a shard is claimed by at most
// one batch at a time.
//
// Invariant: inFlight is the union of every batch's held set, so a shard is in
// inFlight iff exactly one batch holds it. All mutation goes through
// claim/releaseShards/releaseAll. The zero value is usable — claim lazily
// initialises the maps on first use.
//
// The per-batch held sets (rather than a single shared in-flight set) exist
// for the signal-vs-defer interplay: an activity may signal-release some of
// its shards mid-flight (releaseShards), after which a later batch can
// re-claim them; when the original activity finally returns, releaseAll must
// clear only the shards it still holds, not stomp the new claimant. Because
// releaseShards has already pruned held[batchID], releaseAll touching just
// held[batchID] makes that automatic.
type inFlightBatches struct {
	// inFlight is the per-shard exclusivity set: the union of all held sets.
	inFlight map[int32]bool
	// held maps batchID → the shards that batch still owns.
	held map[int64]map[int32]bool
	// nextID hands out monotonic batch IDs.
	nextID int64
}

// claim marks every shard in payload as in-flight under a fresh batch ID and
// returns that ID. Callers must have already confirmed each shard is free
// (isInFlight false) via the packer.
func (b *inFlightBatches) claim(payload BatchPayload) int64 {
	if b.inFlight == nil {
		b.inFlight = map[int32]bool{}
		b.held = map[int64]map[int32]bool{}
	}
	b.nextID++
	batchID := b.nextID
	held := make(map[int32]bool, len(payload))
	//workflowcheck:ignore (building a set of flags is order-independent)
	for sh := range payload {
		b.inFlight[sh] = true
		held[sh] = true
	}
	b.held[batchID] = held
	return batchID
}

// isInFlight reports whether a shard is currently claimed by any batch.
func (b *inFlightBatches) isInFlight(shard int32) bool {
	return b.inFlight[shard]
}

// count returns the number of batches currently in flight: claimed but not
// yet releaseAll'd. This equals the number of live dispatch coroutines,
// because claim/releaseAll bracket each coroutine's lifetime. A batch that
// has signal-released all its shards still counts until its coroutine returns
// — releaseShards empties the batch's held set but leaves the batch entry, so
// the still-running activity remains counted.
func (b *inFlightBatches) count() int {
	return len(b.held)
}

// releaseShards clears the named shards from the batch's held set and from the
// in-flight set, so the packer can dispatch new work against them while the
// activity keeps running on its still-pending shards. Shards not held by this
// batch (already released, or re-claimed by another) are skipped.
func (b *inFlightBatches) releaseShards(batchID int64, shards []int32) {
	held, ok := b.held[batchID]
	if !ok {
		return
	}
	for _, sh := range shards {
		if held[sh] {
			delete(held, sh)
			delete(b.inFlight, sh)
		}
	}
}

// releaseAll clears whatever shards the batch still holds and drops the batch.
// Signal-released shards are already gone from held (and may now belong to a
// later batch), so this only touches shards still owned here.
func (b *inFlightBatches) releaseAll(batchID int64) {
	//workflowcheck:ignore (deletes are commutative; order-independent)
	for sh := range b.held[batchID] {
		delete(b.inFlight, sh)
	}
	delete(b.held, batchID)
}
