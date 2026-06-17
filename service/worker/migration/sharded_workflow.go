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

// shardedForceReplicationWorker is the child workflow spawned by
// ShardedForceReplicationWorkflow for each listing range. It pages through
// ListWorkflows starting from StartPageToken, routes each execution to a
// destination history shard bucket, and dispatches packed inject+verify
// activities (ReplicateBatch) as buckets fill.
//
// Lifecycle:
//  1. List pages at effectiveMaxExecsPerShard rate until either the
//     namespace is exhausted (ReachedEnd=true, no checkpoint) or
//     GetContinueAsNewSuggested fires at a page boundary (send checkpoint
//     signal to parent, set cut flag, stop listing).
//  2. Drain remaining bucket execs via drainBuckets.
//  3. Await pendingDispatches == 0.
//  4. Return shardedChildResult{VerifiedCount, ReachedEnd}.
//
// A 60 s timer coroutine sends cumulative verifiedCount rollup signals to
// the parent so the force-replication-status query stays up to date.
//
// Throttled-hits-hint: a child started with StartThrottled=true (not yet
// promoted) pauses at a CAN hint and awaits the parent's resumeFullRate
// signal before cutting. This ensures at most one handover is in flight at
// any time and at most two children run concurrently.
func shardedForceReplicationWorker(ctx workflow.Context, params shardedChildParams) (shardedChildResult, error) {
	s := &shardedWorkflowState{
		params:           &params,
		namespaceID:      params.NamespaceID,
		targetShardCount: params.TargetShardCount,
		buckets:          BatchPayload{},
		bucketCounts:     map[int32]int{},
		shardInFlight:    map[int32]bool{},
		heldByBatch:      map[int64]map[int32]bool{},
		// StartThrottled=true → wait for parent promotion before going full.
		// StartThrottled=false → first child, already at full rate.
		promoted: !params.StartThrottled,
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

	// buckets accumulate execs that have been listed but not yet
	// dispatched. Nested by destination shard then businessID.
	buckets BatchPayload

	// bucketCounts mirrors len of all runs across BIDs for each shard.
	// Kept as a sidecar so the packer's per-shard ordering decisions are
	// O(1); consulted many times per cycle.
	bucketCounts map[int32]int

	// shardInFlight is the per-shard exclusivity set: a shard's entry is
	// set when it's part of any in-flight batch and cleared when that
	// batch returns (either fully or via mid-flight signal-release). The
	// packer uses this to ensure each shard is in at most one in-flight
	// batch at a time.
	shardInFlight map[int32]bool

	// heldByBatch tracks per-batch shard ownership. spawnBatch populates
	// it with the batch's claimed shards; the signal handler removes
	// entries as shards are released mid-flight; the dispatch coroutine's
	// defer clears whatever remains after the activity returns. Required
	// because a signal-released shard may have been re-claimed by a
	// subsequent batch — the returning original batch must only clear its
	// own remaining claims, not stomp on the new claimant.
	heldByBatch map[int64]map[int32]bool

	// pendingDispatches counts spawned dispatch coroutines that have not
	// yet returned. The main coroutine awaits this dropping to zero before
	// returning the child result.
	pendingDispatches int

	// lastErr latches the first activity error. Once set, further dispatch
	// stops (tryPackStreaming and drainBuckets bail out) and the child
	// returns the error.
	lastErr error

	// promoted is set when the parent sends the resumeFullRate signal:
	// the predecessor child has completed and this child may run at the
	// full MaxExecsPerShard rate. A child started with StartThrottled=false
	// is promoted from the start (no predecessor).
	promoted bool

	// cut is set when this child has sent its checkpoint signal to the
	// parent and stopped listing new pages. After cut, effectiveMaxExecsPerShard
	// drops to half so the successor can run at half alongside us without
	// the combined per-shard in-flight count exceeding MaxExecsPerShard.
	cut bool

	// verifiedCount accumulates verified executions for the final child
	// result and the 60 s progress rollup signal to the parent.
	verifiedCount int64

	nextBatchID int64

	// metricsHandler is tagged with the workflow's fixed scope + namespace
	// once at construction; recordVerified reuses it on every batch return.
	metricsHandler sdkclient.MetricsHandler
}

// effectiveMaxExecsPerShard returns MaxExecsPerShard when this child is
// promoted (predecessor done) and not yet cut (checkpoint not yet sent):
// i.e., the normal full-rate steady state. In all other cases it returns
// max(MaxExecsPerShard/2, 1).
//
// Half-rate applies in two distinct situations:
//   - Before promotion: this child was started alongside a still-running
//     predecessor; together they must stay ≤ MaxExecsPerShard per shard.
//   - After cut: a successor has been started at half rate alongside us;
//     same combined-load constraint.
//
// Minimum of 1 ensures at least one exec can always be packed regardless
// of BatchSize or MaxExecsPerShard settings.
func (s *shardedWorkflowState) effectiveMaxExecsPerShard() int {
	if s.promoted && !s.cut {
		return s.params.MaxExecsPerShard
	}
	return max(s.params.MaxExecsPerShard/2, 1)
}

// run is the child workflow body. See shardedForceReplicationWorker for
// the full lifecycle description.
func (s *shardedWorkflowState) run(ctx workflow.Context) (shardedChildResult, error) {
	// Intra-child signal handler: activities signal mid-flight shard releases.
	workflow.Go(ctx, s.handleReleaseSignals)

	parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution
	s.startBackgroundCoroutines(ctx, parentExec)

	reachedEnd := s.listUntilCutOrEnd(ctx, parentExec)
	if s.lastErr != nil {
		return shardedChildResult{}, s.lastErr
	}

	// Drain remaining buckets (at effectiveMaxExecsPerShard, which is
	// half after cut so the successor can run alongside).
	s.drainBuckets(ctx)
	if s.lastErr != nil {
		return shardedChildResult{}, s.lastErr
	}

	// Await all in-flight batches. No cancellation — children always run
	// to natural completion so their verified counts are exact.
	_ = workflow.Await(ctx, func() bool { return s.pendingDispatches == 0 })
	if s.lastErr != nil {
		return shardedChildResult{}, s.lastErr
	}

	return shardedChildResult{
		VerifiedCount: s.verifiedCount,
		ReachedEnd:    reachedEnd,
	}, nil
}

// startBackgroundCoroutines launches the promotion-signal handler and the
// 60 s progress rollup. The rollup runs only when the worker has a parent to
// signal (skipped when the worker runs standalone, e.g. in unit tests).
func (s *shardedWorkflowState) startBackgroundCoroutines(ctx workflow.Context, parentExec *workflow.Execution) {
	// Parent promotion handler: a one-shot signal that sets promoted=true,
	// allowing the child to advance to full rate. Runs until the signal
	// arrives (or ctx is cancelled at workflow end).
	workflow.Go(ctx, func(gCtx workflow.Context) {
		ch := workflow.GetSignalChannel(gCtx, shardedResumeFullSignalName)
		var dummy struct{}
		if ch.Receive(gCtx, &dummy) {
			s.promoted = true
		}
	})

	if parentExec == nil {
		return
	}
	// 60 s progress rollup: send cumulative verifiedCount to the parent so the
	// force-replication-status query stays current even for long-running
	// children. Stops when ctx is cancelled at workflow exit (timer Get errors).
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			if err := workflow.NewTimer(gCtx, 60*time.Second).Get(gCtx, nil); err != nil {
				return // ctx cancelled — workflow is completing
			}
			// Best-effort: signal failure is not fatal for the child.
			_ = workflow.SignalExternalWorkflow(gCtx,
				parentExec.ID, parentExec.RunID,
				shardedProgressSignalName,
				shardedProgressPayload{
					ChildRunID:    workflow.GetInfo(gCtx).WorkflowExecution.RunID,
					VerifiedCount: s.verifiedCount,
				}).Get(gCtx, nil)
		}
	})
}

// listUntilCutOrEnd drives the listing loop: page through ListWorkflows,
// bucketing and opportunistically packing each page, until either the
// namespace is exhausted (returns true) or a CAN hint at a fully-consumed page
// boundary triggers a cut (returns false). Latches s.lastErr on listing failure.
func (s *shardedWorkflowState) listUntilCutOrEnd(ctx workflow.Context, parentExec *workflow.Execution) bool {
	currentPageToken := s.params.StartPageToken
	for s.lastErr == nil {
		executions, nextPageToken, err := s.listWorkflowPageWithToken(ctx, currentPageToken)
		if err != nil {
			s.lastErr = err
			return false
		}

		for _, ex := range executions {
			sh := common.WorkflowIDToHistoryShard(s.namespaceID, ex.BusinessID, s.targetShardCount)
			s.addToBucket(sh, ex.BusinessID, RunEntry{
				RunID:       ex.RunID,
				ArchetypeID: ex.ArchetypeID,
			})
		}

		// Opportunistically pack and dispatch batches from the buckets.
		for s.tryPackStreaming(ctx, false) { //nolint:revive // intentional empty body
		}

		if len(nextPageToken) == 0 {
			return true // namespace exhausted — terminal child, no successor needed
		}
		currentPageToken = nextPageToken

		// Check the CAN hint at this fully-consumed page boundary, so
		// currentPageToken is exact and safe to hand to a successor.
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			s.cutAtBoundary(ctx, parentExec, currentPageToken)
			return false
		}
	}
	return false
}

// cutAtBoundary performs the handover cut. If still throttled it first awaits
// promotion (so at most one handover is ever in flight), then sets cut (dropping
// to half rate) and signals the parent the next page token; the parent starts a
// successor from there and promotes it once this child completes.
func (s *shardedWorkflowState) cutAtBoundary(ctx workflow.Context, parentExec *workflow.Execution, pageToken []byte) {
	if !s.promoted {
		// Throttled-hits-hint: the predecessor is still running. Pausing here
		// bounds our history at ~hint size and guarantees at most one handover
		// is in flight at a time. Await promotion (predecessor completion), then
		// cut immediately.
		_ = workflow.Await(ctx, func() bool { return s.promoted || s.lastErr != nil })
		if s.lastErr != nil {
			return
		}
	}
	s.cut = true
	if parentExec == nil {
		return
	}
	_ = workflow.SignalExternalWorkflow(ctx,
		parentExec.ID, parentExec.RunID,
		shardedCheckpointSignalName,
		shardedCheckpointPayload{
			ChildRunID:    workflow.GetInfo(ctx).WorkflowExecution.RunID,
			NextPageToken: pageToken,
		}).Get(ctx, nil)
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
	// transient activity failure recover via heartbeat-resume without
	// losing inject progress.
	//
	// 10 attempts is sized for fleet rollouts: a rolling deploy of the
	// activity workers can burn several attempts per batch (each shutdown
	// surfaces as a retryable WorkerShutdown error). 3 was tight enough
	// that two unlucky deploys could exhaust the budget.
	shardedReplicateBatchRetryPolicy = &temporal.RetryPolicy{
		MaximumAttempts: 10,
	}
	shardedReplicateBatchActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         shardedReplicateBatchRetryPolicy,
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
	var a *activities
	var listResp listWorkflowsResponse
	if err := workflow.ExecuteActivity(listCtx, a.ListWorkflows, listReq).Get(ctx, &listResp); err != nil {
		return nil, nil, err
	}
	return listResp.Executions, listResp.NextPageToken, nil
}

func (s *shardedWorkflowState) replicateBatch(ctx workflow.Context, req *shardedBatchReq) (replicateBatchResult, error) {
	actx := workflow.WithActivityOptions(ctx, shardedReplicateBatchActivityOptions)
	var a *activities
	var result replicateBatchResult
	if err := workflow.ExecuteActivity(actx, a.ReplicateBatch, req).Get(ctx, &result); err != nil {
		return replicateBatchResult{}, err
	}
	return result, nil
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
	return s.pendingDispatches < s.params.ConcurrentBatchCount
}

// waitForDispatchSlot blocks until a dispatch slot frees up or lastErr trips.
func (s *shardedWorkflowState) waitForDispatchSlot(ctx workflow.Context) {
	_ = workflow.Await(ctx, func() bool {
		return s.lastErr != nil || s.pendingDispatches < s.params.ConcurrentBatchCount
	})
}

// recordVerified accumulates one batch's verified-exec delta into the
// child's running count and emits the per-batch counter metric.
// No-op when verified == 0 so batches with only inject (DisableVerification)
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
// (not in shardInFlight) — spawnBatch marks them in-flight here.
func (s *shardedWorkflowState) spawnBatch(ctx workflow.Context, payload BatchPayload) {
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
		DisableVerification: s.params.DisableVerification,
		PerBatchGenerateRPS: s.params.PerBatchGenerateRPS,
		ShardNoProgress:     s.params.ShardNoProgress,
		IdleShardCost:       s.params.IdleShardCost,
	}

	held := make(map[int32]bool, len(payload))
	//workflowcheck:ignore (building a set of flags is order-independent)
	for sh := range payload {
		s.shardInFlight[sh] = true
		held[sh] = true
	}
	s.heldByBatch[batchID] = held

	s.pendingDispatches++
	workflow.Go(ctx, func(coroCtx workflow.Context) {
		defer func() {
			s.pendingDispatches--
			// Clear any shards we still hold. Signal-released shards have
			// already been cleared from shardInFlight by handleReleaseSignals
			// and may by now belong to a subsequent batch's claim.
			//workflowcheck:ignore (deletes are commutative; order-independent)
			for sh := range s.heldByBatch[batchID] {
				delete(s.shardInFlight, sh)
			}
			delete(s.heldByBatch, batchID)
		}()
		result, err := s.replicateBatch(coroCtx, req)
		if err != nil {
			s.setLastErr(err)
			return
		}
		s.recordVerified(result.VerifiedCount)
	})
}

// tryPackStreaming attempts to pack and dispatch one batch from s.buckets.
// Returns true if a batch was dispatched. Uses effectiveMaxExecsPerShard
// as the per-shard cap so the half-rate constraint propagates into every
// batch packed while the child is throttled or cut.
//
// No per-shard or total-bucket threshold: as soon as any free shard has
// any execs and a dispatch slot is open, a batch fires. Safety is enforced
// outside the packer — effectiveMaxExecsPerShard caps a single shard's
// contribution, ConcurrentBatchCount caps in-flight batches, and
// PerBatchGenerateRPS caps the per-batch source RPS.
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

	effectiveCap := s.effectiveMaxExecsPerShard()
	payload := BatchPayload{}
	packed := 0
	for _, sh := range shardIDs {
		room := s.params.BatchSize - packed
		if room <= 0 {
			break
		}
		take := min(effectiveCap, s.bucketCounts[sh], room)
		if take == 0 {
			continue
		}
		payload[sh] = s.takeFromBucket(sh, take)
		packed += take
	}
	if packed == 0 {
		return false
	}
	s.spawnBatch(ctx, payload)
	return true
}

// bucketsEmpty reports whether every shard's bucket is empty. Reads from
// the sidecar count map so it's O(#shards), not O(#runs).
func (s *shardedWorkflowState) bucketsEmpty() bool {
	//workflowcheck:ignore (existence check; iteration order does not affect result)
	for _, n := range s.bucketCounts {
		if n > 0 {
			return false
		}
	}
	return true
}

// shardIDsByPackPriority returns free, non-empty shard IDs in the order
// the packer should consider them. Deterministic across replays: ordering
// is derived from workflow state (bucketCounts) with shard ID as a stable
// tiebreaker.
//
// relax=false (streaming): fullest first. Packer naturally produces large,
// predictable batches when work is plentiful and small ones when it isn't
// — either way it ships rather than waiting.
//
// relax=true (drain): hot shards (count > effectiveMaxExecsPerShard) first,
// fullest within hot. These need >1 round trip to drain, so total drain
// wall-clock is bounded by the heaviest shard; starting their pipelines
// first is the dominant lever. After all hot shards are claimed, remaining
// batch capacity fills from smallest cold buckets (ascending count) so
// light shards clear out quickly — nothing is arriving in drain, so
// waiting for cold buckets to grow is wasted wall-clock.
func (s *shardedWorkflowState) shardIDsByPackPriority(relax bool) []int32 {
	out := make([]int32, 0, len(s.bucketCounts))
	//workflowcheck:ignore (output is sorted below before any observable use)
	for sh, n := range s.bucketCounts {
		if n == 0 || s.shardInFlight[sh] {
			continue
		}
		out = append(out, sh)
	}
	if relax {
		maxPerShard := s.effectiveMaxExecsPerShard()
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

// addToBucket appends one run to the (shard, BID) bucket and bumps
// the sidecar count.
func (s *shardedWorkflowState) addToBucket(shard int32, businessID string, run RunEntry) {
	if s.buckets[shard] == nil {
		s.buckets[shard] = map[string][]RunEntry{}
	}
	s.buckets[shard][businessID] = append(s.buckets[shard][businessID], run)
	s.bucketCounts[shard]++
}

// takeFromBucket consumes up to n runs from the given shard and returns
// them grouped by BID. Walks BIDs in alphabetical order so the resulting
// payload is deterministic across replays; takes whole per-BID runs only
// as needed to reach n. Empties the shard from s.buckets / s.bucketCounts
// when nothing remains.
func (s *shardedWorkflowState) takeFromBucket(shard int32, n int) map[string][]RunEntry {
	if n <= 0 {
		return nil
	}
	byBID := s.buckets[shard]
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
	s.bucketCounts[shard] -= taken
	if s.bucketCounts[shard] <= 0 {
		delete(s.bucketCounts, shard)
		delete(s.buckets, shard)
	}
	return out
}

// drainBuckets blocks until buckets are empty (success) or lastErr trips
// (failure). Each pass packs everything currently dispatchable, then
// awaits any change in pendingDispatches + shardInFlight so the next pass
// can attempt shards just freed by signal-release.
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
// returning silently would proceed with execs that were never dispatched
// (silent data loss). Failing forces the child to return the error.
func (s *shardedWorkflowState) failDrainBucketsStuck() {
	remaining := 0
	//workflowcheck:ignore (summation is order-independent)
	for _, n := range s.bucketCounts {
		remaining += n
	}
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
		if s.pendingDispatches < currentPending {
			return true
		}
		if !s.dispatchSlotAvailable() {
			return false
		}
		//workflowcheck:ignore (existence check; iteration order does not affect result)
		for sh, n := range s.bucketCounts {
			if n > 0 && !s.shardInFlight[sh] {
				return true
			}
		}
		return false
	}
}
