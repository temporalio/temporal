package migration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
)

// ReplicateBatch is the per-batch activity body for the sharded force
// replication workflow. Runs inject (skipped on Resume) then verify,
// signal-releasing completed shards mid-flight as their cumulative
// idle cost crosses IdleShardCost. On workflow-initiated cancellation
// it enters drain mode and returns a replicateBatchResult carrying
// any still-unverified execs. Drain-mode signal traffic is suppressed:
// once we know we're about to return, there's no point racing a
// signal against the return value.
func (a *activities) ReplicateBatch(ctx context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
	// Flatten once so per-exec bookkeeping (verified[], attempts[],
	// nextRetryAt[]) can stay index-based.
	execs := req.Executions.flatten()
	of := len(execs)
	if of == 0 {
		return replicateBatchResult{}, nil
	}

	remoteAdminClient := a.clientFactory.NewRemoteAdminClientWithTimeout(
		req.TargetClusterEndpoint, admin.DefaultTimeout, admin.DefaultLargeTimeout)

	var hb replicateBatchHeartbeat
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &hb)
	}

	// ---- Inject phase ----
	if !req.Resume && !hb.InjectDone {
		startIdx := hb.NextInjectIdx
		if err := a.runInjectPhase(ctx, req, execs, startIdx); err != nil {
			return replicateBatchResult{}, err
		}
		activity.RecordHeartbeat(ctx, replicateBatchHeartbeat{InjectDone: true})
	}

	// inject-only path: skip verify and let the workflow accounting
	// release shards on activity return.
	if req.DisableVerification {
		return replicateBatchResult{
			CompletedShards: req.Executions.sortedShards(),
			VerifiedCount:   0,
		}, nil
	}

	// Namespace lookup feeds the verify phase's retention/zombie skip
	// check (checkSkipWorkflowExecution needs ns.Retention()). Snapshotted
	// once per activity; we don't track config changes mid-batch.
	ns, err := a.NamespaceRegistry.GetNamespaceByID(namespace.ID(req.NamespaceID))
	if err != nil {
		return replicateBatchResult{}, fmt.Errorf("look up namespace %s: %w", req.NamespaceID, err)
	}

	return a.runVerifyPhase(ctx, req, execs, of, remoteAdminClient, ns)
}

// runVerifyPhase is the verify-phase loop body of ReplicateBatch. It
// owns per-exec bookkeeping, the drain-transition handoff, and the
// per-iteration completion / stuck-shard / signal-release decisions
// — extracted from ReplicateBatch to keep its cognitive complexity
// under the linter cap.
func (a *activities) runVerifyPhase(
	ctx context.Context,
	req *shardedBatchReq,
	execs []*shardedExecutionInfo,
	of int,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
) (replicateBatchResult, error) {
	verified := make([]bool, of)
	attempts := make([]int, of)
	nextRetryAt := make([]time.Time, of)
	doneCount := 0

	shards := newShardVerifyTracker(execs, req.Resume, req.NoProgressByShard)

	var draining bool
	var drainStartAt time.Time

	// callCtx is what attemptVerifyExec uses for DescribeMutableState.
	// In drain mode we swap to a detached context: the parent ctx is
	// already dead (that's what triggered the transition), so reusing
	// it would make every drain-mode RPC fail instantly.
	callCtx := ctx
	// Pre-create the drain context up front and defer cancel right away
	// so go vet's lostcancel pass sees the canonical pattern.
	drainCtx, drainCancel := context.WithCancel(context.Background())
	defer drainCancel()

	for {
		// Worker shutdown short-circuits drain mode entirely. The SDK
		// closes WorkerStopChannel WorkerStopTimeout before forcibly
		// returning; burning that window on DMS calls that can't drive
		// their results back is worse than returning current state and
		// letting ResumeShards / RecoveredBuckets recover next cycle.
		// Re-checked each iteration because shutdown can fire after
		// we've already entered drain — the detached ctx wouldn't
		// notice on its own.
		select {
		case <-activity.GetWorkerStopChannel(ctx):
			return replicateBatchResult{
				CompletedShards: shards.allCompleted(),
				InFlight:        a.buildInFlight(execs, verified, shards, time.Now()),
				VerifiedCount:   int64(doneCount),
			}, nil
		default:
		}

		// Workflow-initiated activity cancellation (drainForCAN)
		// transitions us into drain mode with the full DrainGrace window.
		// WaitForCancellation=true on the activity options guarantees the
		// workflow blocks for us, so the grace window is genuinely
		// available — swap callCtx onto a detached deadline so
		// DescribeMutableState keeps working after the parent ctx died.
		if !draining && ctx.Err() != nil {
			draining = true
			drainStartAt = time.Now()
			// Start the drain budget timer here rather than at activity
			// entry so the grace window measures from drain transition,
			// not from activity start.
			time.AfterFunc(req.DrainGrace, drainCancel)
			callCtx = drainCtx
		}

		passDelta, minNextRetry, ctxAborted, vErr := a.runVerifyPass(
			callCtx, remoteAdminClient, ns, req, execs, verified, attempts, nextRetryAt, shards)
		if vErr != nil {
			return replicateBatchResult{}, vErr
		}
		doneCount += passDelta

		activity.RecordHeartbeat(ctx, replicateBatchHeartbeat{InjectDone: true})

		if done, result, err := a.evaluateVerifyIteration(
			ctx, req, execs, verified, shards, doneCount, of, draining, drainStartAt); err != nil {
			return replicateBatchResult{}, err
		} else if done {
			return result, nil
		}

		// If the inner loop aborted because callCtx died, skip the sleep
		// entirely so the outer-loop top sees the new state promptly
		// (normal → drain transition, or drain → exit).
		if ctxAborted {
			continue
		}

		a.waitNextTick(ctx, callCtx, minNextRetry, draining, drainStartAt, req.DrainGrace)
	}
}

// evaluateVerifyIteration runs the post-pass checks (clean completion,
// stuck-shard backstop, drain-exit decision, mid-flight signal release)
// for a single verify-loop iteration. Returns done=true with the result
// when the loop should exit; otherwise (false, _, nil) means continue.
func (a *activities) evaluateVerifyIteration(
	ctx context.Context,
	req *shardedBatchReq,
	execs []*shardedExecutionInfo,
	verified []bool,
	shards shardVerifyTracker,
	doneCount, of int,
	draining bool,
	drainStartAt time.Time,
) (bool, replicateBatchResult, error) {
	// Clean completion — every exec verified.
	if doneCount >= of {
		return true, replicateBatchResult{
			CompletedShards: shards.allCompleted(),
			VerifiedCount:   int64(doneCount),
		}, nil
	}

	// Per-shard cumulative no-progress backstop.
	if sErr := a.checkStuckShard(req, shards, execs, verified, doneCount, of); sErr != nil {
		return false, replicateBatchResult{}, sErr
	}

	if draining {
		// Drain-mode exit checks. No signals here — the return value
		// carries everything the workflow needs (completed shards +
		// unverified execs grouped by shard with their cumulative
		// no-progress duration).
		if a.shouldExitDrain(req, shards, drainStartAt) {
			return true, replicateBatchResult{
				CompletedShards: shards.allCompleted(),
				InFlight:        a.buildInFlight(execs, verified, shards, time.Now()),
				VerifiedCount:   int64(doneCount),
			}, nil
		}
		return false, replicateBatchResult{}, nil
	}

	if err := a.maybeSignalRelease(ctx, req, shards); err != nil {
		return false, replicateBatchResult{}, err
	}
	return false, replicateBatchResult{}, nil
}

// runInjectPhase walks execs in flattened order, generating one
// replication task per exec under a per-batch RPS limiter. Cancellation
// mid-loop returns a CanceledError so spawnBatch's IsCanceledError
// check fires and the batch's batchExecs entry is preserved for
// RecoveredBuckets re-injection next cycle. Already-injected execs
// are re-injected harmlessly — replication dedupes per (namespace,
// wf, run).
func (a *activities) runInjectPhase(ctx context.Context, req *shardedBatchReq, execs []*shardedExecutionInfo, startIdx int) error {
	rateLimiter := quotas.NewRateLimiter(req.PerBatchGenerateRPS, int(math.Ceil(req.PerBatchGenerateRPS)))
	for i := startIdx; i < len(execs); i++ {
		ex := execs[i]
		if ctx.Err() != nil {
			return temporal.NewCanceledError("inject phase cancelled")
		}
		if err := a.generateReplicationTaskForExec(ctx, rateLimiter, req, ex); err != nil {
			if ctx.Err() != nil {
				return temporal.NewCanceledError("inject phase cancelled")
			}
			if !common.IsNotFoundError(err) {
				return err
			}
			a.Logger.Warn("force-replication-sharded ignore replication task due to NotFoundServiceError",
				tag.WorkflowNamespaceID(req.NamespaceID),
				tag.WorkflowID(ex.BusinessID),
				tag.WorkflowRunID(ex.RunID),
				tag.Error(err))
		}
		activity.RecordHeartbeat(ctx, replicateBatchHeartbeat{NextInjectIdx: i + 1})
	}
	return nil
}

// runVerifyPass runs one pass over every unverified exec, attempting a
// verify on those whose backoff timer has expired. Returns the count of
// execs newly verified this pass, the earliest pending retry deadline
// (for sleep scheduling), and whether callCtx died mid-pass — in which
// case the outer loop's top reassesses (drain transition or exit).
// Returns a non-nil error only for hard errors from the verify path;
// ctx-derived errors set ctxAborted instead so the outer loop owns
// the decision about what to do next.
func (a *activities) runVerifyPass(
	callCtx context.Context,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	req *shardedBatchReq,
	execs []*shardedExecutionInfo,
	verified []bool,
	attempts []int,
	nextRetryAt []time.Time,
	shards shardVerifyTracker,
) (int, time.Time, bool, error) {
	now := time.Now()
	var minNextRetry time.Time
	verifiedDelta := 0
	for i, ex := range execs {
		if verified[i] {
			continue
		}
		if !nextRetryAt[i].IsZero() && nextRetryAt[i].After(now) {
			minNextRetry = earliest(minNextRetry, nextRetryAt[i])
			continue
		}

		ok, err := a.attemptVerifyExec(callCtx, remoteAdminClient, ns, req, ex)
		if err != nil {
			if callCtx.Err() != nil {
				// callCtx is dead. Two cases: (1) normal-mode parent ctx
				// was just cancelled mid-call — the outer-loop top will
				// promote to drain on the next iteration; (2) drain-mode
				// detached ctx expired — the drain exit check fires.
				return verifiedDelta, minNextRetry, true, nil
			}
			return verifiedDelta, minNextRetry, false, err
		}

		if ok {
			verified[i] = true
			verifiedDelta++
			shards.recordVerified(ex.Shard, time.Now())
			continue
		}

		attempts[i]++
		nextRetryAt[i] = time.Now().Add(backoffDelay(attempts[i]))
		minNextRetry = earliest(minNextRetry, nextRetryAt[i])
	}
	return verifiedDelta, minNextRetry, false, nil
}

// earliest returns the earlier of cur (which may be zero) and candidate.
// Used to track the next-due retry deadline across the verify pass.
func earliest(cur, candidate time.Time) time.Time {
	if cur.IsZero() || candidate.Before(cur) {
		return candidate
	}
	return cur
}

// checkStuckShard fails non-retryably if any shard has gone longer than
// req.ShardNoProgress without a verified outcome. Duration is cumulative
// across CAN cycles via tracker seeding.
func (a *activities) checkStuckShard(
	req *shardedBatchReq,
	shards shardVerifyTracker,
	execs []*shardedExecutionInfo,
	verified []bool,
	doneCount, total int,
) error {
	stuckShard, stuckDur, ok := shards.pickStuck(time.Now(), req.ShardNoProgress)
	if !ok {
		return nil
	}
	msg := fmt.Sprintf("shard %d no progress for %v", stuckShard, stuckDur)
	if stuckIdx, found := a.firstUnverifiedOnShard(execs, verified, stuckShard); found {
		stuck := execs[stuckIdx]
		msg = fmt.Sprintf("shard %d no progress for %v on %s/%s (%d/%d done)",
			stuckShard, stuckDur, stuck.BusinessID, stuck.RunID, doneCount, total)
	}
	return temporal.NewNonRetryableApplicationError(msg, "ShardNoProgress", nil)
}

// shouldExitDrain reports whether the drain-mode exit conditions are
// met: either the grace window has expired, or the cumulative idle cost
// across completed-but-unsignaled shards crossed the threshold.
func (a *activities) shouldExitDrain(req *shardedBatchReq, shards shardVerifyTracker, drainStartAt time.Time) bool {
	if time.Since(drainStartAt) >= req.DrainGrace {
		return true
	}
	return shards.totalIdleCost(time.Now()) >= req.IdleShardCost
}

// maybeSignalRelease signals the workflow to release any
// completed-but-unsignaled shards if their cumulative idle cost crossed
// the threshold. Only fires in normal mode — drain mode rides the
// activity result instead.
func (a *activities) maybeSignalRelease(ctx context.Context, req *shardedBatchReq, shards shardVerifyTracker) error {
	if shards.totalIdleCost(time.Now()) < req.IdleShardCost {
		return nil
	}
	releaseList := shards.awaitingRelease()
	if len(releaseList) == 0 {
		return nil
	}
	if err := a.signalReleaseShards(ctx, req, releaseList); err != nil {
		return err
	}
	shards.markReleased(releaseList)
	return nil
}

// waitNextTick sleeps until the next exec is due for retry, capped by
// DrainGrace remaining when in drain mode. In drain mode the parent ctx
// is already dead, so we wake on the detached drain ctx instead — using
// the parent ctx would tight-loop on its Done channel.
func (a *activities) waitNextTick(
	ctx, callCtx context.Context,
	minNextRetry time.Time,
	draining bool,
	drainStartAt time.Time,
	drainGrace time.Duration,
) {
	sleepDur := 50 * time.Millisecond
	if !minNextRetry.IsZero() {
		if delta := time.Until(minNextRetry); delta > sleepDur {
			sleepDur = delta
		}
	}
	if draining {
		if remaining := drainGrace - time.Since(drainStartAt); remaining > 0 && remaining < sleepDur {
			sleepDur = remaining
		}
		select {
		case <-time.After(sleepDur):
		case <-callCtx.Done():
		}
		return
	}
	select {
	case <-time.After(sleepDur):
	case <-ctx.Done():
		// ctx cancel just sets draining on the next iteration; don't
		// unwind here.
	}
}

// generateReplicationTaskForExec is the per-exec inject wrapper around
// generateWorkflowReplicationTask; supplies the sharded-only
// single-target-clusters slice and the generateViaFrontend flag.
func (a *activities) generateReplicationTaskForExec(
	ctx context.Context,
	rateLimiter quotas.RateLimiter,
	req *shardedBatchReq,
	ex *shardedExecutionInfo,
) error {
	start := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(req.Namespace)).
			Timer(metrics.GenerateReplicationTaskLatency.Name()).Record(time.Since(start))
	}()
	return a.generateWorkflowReplicationTask(
		ctx,
		rateLimiter,
		req.Namespace,
		req.NamespaceID,
		ex.ExecutionInfo,
		[]string{req.TargetClusterName},
		a.generateMigrationTaskViaFrontend(),
	)
}

// attemptVerifyExec runs the source-describe + target-applied check for
// a single execution and returns whether it's now verified.
//
// Why this isn't a delegation to verifySingleReplicationTask: that
// helper folds BUSY_WORKFLOW into the generic notVerified result. We
// inline the DMS call here to keep busy-workflow as a distinct metric
// counter, preserving the "passive cluster apply is in progress"
// signal.
func (a *activities) attemptVerifyExec(
	ctx context.Context,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	req *shardedBatchReq,
	ex *shardedExecutionInfo,
) (bool, error) {
	attemptStart := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(req.Namespace)).
			Timer(metrics.VerifyReplicationTaskLatency.Name()).Record(time.Since(attemptStart))
	}()

	archetype, err := a.archetypeIDToName(ctx, ex.ArchetypeID)
	if err != nil {
		return false, err
	}

	vreq := &verifyReplicationTasksRequest{
		Namespace:             req.Namespace,
		NamespaceID:           req.NamespaceID,
		TargetClusterEndpoint: req.TargetClusterEndpoint,
		TargetClusterName:     req.TargetClusterName,
	}

	describeStart := time.Now()
	mu, err := remoteAdminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: req.Namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: ex.BusinessID,
			RunId:      ex.RunID,
		},
		Archetype:       archetype,
		ArchetypeId:     ex.ArchetypeID,
		SkipForceReload: true,
	})
	a.forceReplicationMetricsHandler.Timer(metrics.VerifyDescribeMutableStateLatency.Name()).Record(time.Since(describeStart))

	nsTag := metrics.NamespaceTag(req.Namespace)

	if err == nil {
		result, vErr := a.workflowVerifier(ctx, vreq, remoteAdminClient, a.adminClient, ns, ex.ExecutionInfo, mu)
		if vErr != nil {
			return false, vErr
		}
		if result.isVerified() {
			a.forceReplicationMetricsHandler.WithTags(nsTag).Counter(metrics.VerifyReplicationTaskSuccess.Name()).Record(1)
			return true, nil
		}
		a.forceReplicationMetricsHandler.WithTags(nsTag).Counter(metrics.VerifyReplicationTaskPending.Name()).Record(1)
		return false, nil
	}

	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		a.forceReplicationMetricsHandler.WithTags(nsTag).Counter(metrics.VerifyReplicationTaskNotFound.Name()).Record(1)
		// Retention/zombie path: a not-found execution may already be
		// deleted on source (zombie or past retention), in which case it
		// never needs to replicate — treat that as verified so the
		// shard's completion accounting moves forward.
		result, sErr := a.checkSkipWorkflowExecution(ctx, vreq, ex.ExecutionInfo, ns)
		if sErr != nil {
			return false, sErr
		}
		return result.isVerified(), nil
	}

	if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
		return false, temporal.NewNonRetryableApplicationError(
			"failed to describe workflow from the remote cluster", "NamespaceNotFound", err)
	}

	if resExhausted, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok && resExhausted.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
		// Passive cluster holds the workflow cache lock while applying
		// history during SyncWorkflowStateTask. Counted separately from
		// pending so the "apply is in progress" signal stays visible,
		// but the workflow-side treatment matches pending — per-exec
		// backoff applies and the per-shard last-progress timer does
		// not move (it only updates on verified outcomes).
		a.forceReplicationMetricsHandler.WithTags(nsTag).Counter(metrics.VerifyReplicationTaskBusy.Name()).Record(1)
		return false, nil
	}

	a.forceReplicationMetricsHandler.WithTags(nsTag, metrics.ServiceErrorTypeTag(err)).
		Counter(metrics.VerifyReplicationTaskFailed.Name()).Record(1)
	return false, fmt.Errorf("describe workflow on remote cluster: %w", err)
}

// signalReleaseShards sends the mid-flight ReleaseShards signal to the
// parent workflow, freeing the listed shards in the workflow's
// shardInFlight set so the packer can dispatch fresh batches against them
// while this activity stays running on its still-pending shards.
//
// No retry wrapping: a transient failure propagates up so the activity
// fails, the workflow records it via lastErr, and the in-flight batch is
// recovered into the next CAN's RecoveredBuckets — preferable to silently
// swallowing the error here and stranding completed shards.
func (a *activities) signalReleaseShards(ctx context.Context, req *shardedBatchReq, shards []int32) error {
	info := activity.GetInfo(ctx)
	return a.sdkClientFactory.GetSystemClient().SignalWorkflow(ctx, info.WorkflowExecution.ID, info.WorkflowExecution.RunID, releaseShardsSignalName, releaseShardsPayload{
		BatchID: req.BatchID,
		Shards:  shards,
	})
}

// shardVerify holds per-shard verify-phase state for one batch.
type shardVerify struct {
	pending      int
	doneAt       time.Time // set when pending first hits zero; cleared on signal release
	released     bool      // ReleaseShards signal already sent
	lastProgress time.Time // wall time of the most recent verified outcome
}

type shardVerifyTracker map[int32]shardVerify

func newShardVerifyTracker(
	execs []*shardedExecutionInfo,
	resume bool,
	noProgressByShard map[int32]time.Duration,
) shardVerifyTracker {
	t := shardVerifyTracker{}
	for _, ex := range execs {
		sv := t[ex.Shard]
		sv.pending++
		t[ex.Shard] = sv
	}
	nowSeed := time.Now()
	for sh, sv := range t {
		if resume {
			sv.lastProgress = nowSeed.Add(-noProgressByShard[sh])
		} else {
			sv.lastProgress = nowSeed
		}
		t[sh] = sv
	}
	return t
}

func (t shardVerifyTracker) recordVerified(sh int32, now time.Time) {
	sv := t[sh]
	sv.pending--
	sv.lastProgress = now
	if sv.pending == 0 {
		sv.doneAt = now
	}
	t[sh] = sv
}

func (t shardVerifyTracker) markReleased(shards []int32) {
	for _, sh := range shards {
		sv := t[sh]
		sv.released = true
		sv.doneAt = time.Time{}
		t[sh] = sv
	}
}

// totalIdleCost sums idle time across shards that are completed but not
// yet released — the "shard-seconds" unit the IdleShardCost threshold is
// denominated in.
func (t shardVerifyTracker) totalIdleCost(now time.Time) time.Duration {
	var total time.Duration
	for _, sv := range t {
		if !sv.doneAt.IsZero() {
			total += now.Sub(sv.doneAt)
		}
	}
	return total
}

// awaitingRelease returns completed-but-not-yet-signaled shard IDs in
// ascending order so the signal payload is deterministic across replays.
func (t shardVerifyTracker) awaitingRelease() []int32 {
	var out []int32
	for sh, sv := range t {
		if !sv.doneAt.IsZero() {
			out = append(out, sh)
		}
	}
	slices.Sort(out)
	return out
}

// allCompleted returns every shard that finished during this activity
// run — both signal-released and still awaiting release at return.
func (t shardVerifyTracker) allCompleted() []int32 {
	var out []int32
	for sh, sv := range t {
		if sv.released || !sv.doneAt.IsZero() {
			out = append(out, sh)
		}
	}
	slices.Sort(out)
	return out
}

// pickStuck returns (shard, age, true) for the lowest-numbered shard
// whose cumulative no-progress duration meets or exceeds threshold.
func (t shardVerifyTracker) pickStuck(now time.Time, threshold time.Duration) (int32, time.Duration, bool) {
	var (
		minShard int32
		minAge   time.Duration
		found    bool
	)
	for sh, sv := range t {
		if sv.pending <= 0 {
			continue
		}
		age := now.Sub(sv.lastProgress)
		if age < threshold {
			continue
		}
		if !found || sh < minShard {
			minShard = sh
			minAge = age
			found = true
		}
	}
	return minShard, minAge, found
}

// buildInFlight groups unverified execs by shard then businessID and
// attaches the cumulative no-progress duration per shard, for the
// drain-mode activity return. Shards with zero unverified execs are
// reported via CompletedShards instead.
func (a *activities) buildInFlight(
	execs []*shardedExecutionInfo,
	verified []bool,
	shards shardVerifyTracker,
	now time.Time,
) []ResumeShard {
	byShard := map[int32]map[string][]RunEntry{}
	for i, ex := range execs {
		if verified[i] {
			continue
		}
		if byShard[ex.Shard] == nil {
			byShard[ex.Shard] = map[string][]RunEntry{}
		}
		byShard[ex.Shard][ex.BusinessID] = append(byShard[ex.Shard][ex.BusinessID], RunEntry{
			RunID:       ex.RunID,
			ArchetypeID: ex.ArchetypeID,
		})
	}
	if len(byShard) == 0 {
		return nil
	}
	shardIDs := make([]int32, 0, len(byShard))
	for sh := range byShard {
		shardIDs = append(shardIDs, sh)
	}
	slices.Sort(shardIDs)
	out := make([]ResumeShard, 0, len(shardIDs))
	for _, sh := range shardIDs {
		out = append(out, ResumeShard{
			Shard:              sh,
			Execs:              byShard[sh],
			NoProgressDuration: now.Sub(shards[sh].lastProgress),
		})
	}
	return out
}

// firstUnverifiedOnShard returns the index of the first execution in the
// flattened execs slice that targets the given shard and hasn't verified
// yet, and a found flag. Callers should only invoke this for shards
// with at least one pending exec; the found=false return is a defensive
// fallback so a tracker / verified-slice drift can't crash the activity.
func (a *activities) firstUnverifiedOnShard(execs []*shardedExecutionInfo, verified []bool, shard int32) (int, bool) {
	for i, ex := range execs {
		if verified[i] {
			continue
		}
		if ex.Shard == shard {
			return i, true
		}
	}
	return 0, false
}

// backoffDelay returns the per-exec retry delay after `attempt`
// consecutive failed verify attempts: 100ms × 2^(attempt-1), capped at
// 5s. The cap bounds how long after the apply pipeline recovers we'd
// take to notice; the per-shard no-progress timer fires after enough of
// these capped retries to distinguish "actively checking" from "lazy
// polling gave up".
func backoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		return 5 * time.Second
	}
	return 100 * time.Millisecond * (1 << (attempt - 1))
}
