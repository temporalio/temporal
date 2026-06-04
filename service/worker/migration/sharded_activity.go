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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
)

// ReplicateBatch is the per-batch activity body for the sharded force
// replication workflow. It runs inject (skip on Resume) followed by
// verify, signal-releasing completed shards mid-flight once their
// cumulative idle cost crosses IdleShardCost. On workflow-initiated
// cancellation it enters drain mode: continues verifying for up to
// DrainGrace, then returns a replicateBatchResult carrying any
// still-unverified execs grouped by shard. Drain-mode signal traffic
// is intentionally suppressed — once we know we're about to return,
// there's no point racing a signal against the return value.
func (a *activities) ReplicateBatch(ctx context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
	// Flatten the nested wire shape once on entry so the per-exec
	// bookkeeping (verified[], attempts[], nextRetryAt[]) can stay
	// index-based. flatten() walks shards ascending then BIDs
	// alphabetical, so the order is deterministic — replays of the same
	// payload produce the same slice.
	execs := req.Executions.flatten()
	of := len(execs)
	if of == 0 {
		return replicateBatchResult{}, nil
	}

	remoteAdminClient := a.clientFactory.NewRemoteAdminClientWithTimeout(
		req.TargetClusterEndpoint, admin.DefaultTimeout, admin.DefaultLargeTimeout)

	// Namespace lookup feeds the verify phase's retention/zombie skip
	// check (checkSkipWorkflowExecution needs ns.Retention()). Looked up
	// once per activity, since the namespace registry is local and
	// immutable across the run.
	ns, err := a.NamespaceRegistry.GetNamespaceByID(namespace.ID(req.NamespaceID))
	if err != nil {
		return replicateBatchResult{}, fmt.Errorf("look up namespace %s: %w", req.NamespaceID, err)
	}

	// ---- Inject phase (skipped on Resume: the execs have already been
	// injected by an earlier activity that was drained for CAN).
	if !req.Resume {
		// One limiter per activity invocation, sized for RPS — the
		// post-call ReserveN reservation pulls extra tokens proportional
		// to history size, so a single limiter shared across this
		// batch's execs is what enforces the per-batch RPS budget
		// end-to-end.
		rateLimiter := quotas.NewRateLimiter(req.PerBatchGenerateRPS, int(math.Ceil(req.PerBatchGenerateRPS)))
		for _, ex := range execs {
			if err := ctx.Err(); err != nil {
				// Worker shutdown or workflow drain mid-inject. Return a
				// recognizable CanceledError so spawnBatch's
				// IsCanceledError check fires and the batch's batchExecs
				// entry is preserved for RecoveredBuckets re-injection
				// next cycle. Any execs already injected here are
				// re-injected harmlessly — replication dedupes per
				// (namespace, wf, run).
				return replicateBatchResult{}, temporal.NewCanceledError("inject phase cancelled")
			}
			if err := a.generateReplicationTaskForExec(ctx, rateLimiter, req, ex); err != nil {
				if ctx.Err() != nil {
					return replicateBatchResult{}, temporal.NewCanceledError("inject phase cancelled")
				}
				return replicateBatchResult{}, err
			}
		}
	}

	// ---- Verify phase ----
	verified := make([]bool, of)
	attempts := make([]int, of)
	nextRetryAt := make([]time.Time, of)
	doneCount := 0

	shards := newShardVerifyTracker(execs, req.Resume, req.NoProgressByShard)

	var draining bool
	var drainStartAt time.Time

	// callCtx is what attemptVerifyExec uses for DescribeMutableState. In
	// normal mode it's the activity's parent ctx; when drain mode kicks
	// in, it swaps to a fresh detached context with a DrainGrace timeout
	// so DMS calls keep working long enough for nearly-verified execs to
	// land. The parent ctx is already dead by the time we transition
	// (that's what triggers the transition), so using it for drain-mode
	// RPCs would defeat the grace window — every call would fail
	// instantly.
	callCtx := ctx
	// Pre-create the drain context up front and defer cancel right away
	// so go vet's lostcancel pass sees the canonical pattern. The drain
	// timer (started below on the cancel transition) plus the unconditional
	// defer cancel make the lifetime obvious; the context only matters
	// once draining is set, so creating it early costs nothing.
	drainCtx, drainCancel := context.WithCancel(context.Background())
	defer drainCancel()

	for {
		// Worker shutdown short-circuits drain mode entirely. The SDK
		// closes activity.GetWorkerStopChannel a fixed WorkerStopTimeout
		// (10s default) before forcibly returning; burning that window
		// on DescribeMutableState calls that won't get to drive their
		// results back is worse than returning current state and letting
		// the next cycle's ResumeShards / RecoveredBuckets paths recover.
		//
		// Checked at the top of each outer iteration (not just on initial
		// drain transition) because worker shutdown can fire after we've
		// already entered workflow-initiated drain — e.g. CAN cancel
		// arrives, drain starts under detached ctx, then a deploy hits
		// mid-window. The detached ctx wouldn't notice on its own.
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
		if !draining {
			if err := ctx.Err(); err != nil {
				draining = true
				drainStartAt = time.Now()
				// Start the drain budget timer here rather than at activity
				// entry so the grace window measures from drain transition,
				// not from activity start.
				time.AfterFunc(req.DrainGrace, drainCancel)
				callCtx = drainCtx
			}
		}

		now := time.Now()
		var minNextRetry time.Time
		ctxAborted := false
		for i := range of {
			if verified[i] {
				continue
			}
			if !nextRetryAt[i].IsZero() && nextRetryAt[i].After(now) {
				if minNextRetry.IsZero() || nextRetryAt[i].Before(minNextRetry) {
					minNextRetry = nextRetryAt[i]
				}
				continue
			}

			ex := execs[i]
			shard := ex.Shard
			ok, err := a.attemptVerifyExec(callCtx, remoteAdminClient, ns, req, ex)
			if err != nil {
				if callCtx.Err() != nil {
					// callCtx is dead. Two cases: (1) normal-mode parent
					// ctx was just cancelled mid-call — the outer-loop top
					// will promote to drain on the next iteration;
					// (2) drain-mode detached ctx expired — the drain
					// exit check below will fire. Either way, drop out of
					// the inner loop now.
					ctxAborted = true
					break
				}
				return replicateBatchResult{}, err
			}

			if ok {
				verified[i] = true
				doneCount++
				shards.recordVerified(shard, time.Now())
				continue
			}

			attempts[i]++
			nextRetryAt[i] = time.Now().Add(backoffDelay(attempts[i]))
			if minNextRetry.IsZero() || nextRetryAt[i].Before(minNextRetry) {
				minNextRetry = nextRetryAt[i]
			}
		}

		activity.RecordHeartbeat(ctx, doneCount)

		// Clean completion — every exec verified.
		if doneCount >= of {
			return replicateBatchResult{
				CompletedShards: shards.allCompleted(),
				VerifiedCount:   int64(doneCount),
			}, nil
		}

		// Per-shard cumulative no-progress backstop. Trips on any shard
		// still holding pending execs whose last verified outcome
		// (carried across CAN via tracker seeding) is older than
		// ShardNoProgress.
		if stuckShard, stuckDur, ok := shards.pickStuck(time.Now(), req.ShardNoProgress); ok {
			stuckIdx := a.firstUnverifiedOnShard(execs, verified, stuckShard)
			stuck := execs[stuckIdx]
			return replicateBatchResult{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("shard %d no progress for %v on %s/%s (%d/%d done)",
					stuckShard, stuckDur, stuck.BusinessID, stuck.RunID, doneCount, of),
				"ShardNoProgress", nil)
		}

		// Drain-mode exit checks. Drain mode is entered when the workflow
		// has cancelled the activity for CAN. No signals here — the
		// return value carries everything the workflow needs (completed
		// shards + unverified execs grouped by shard with their cumulative
		// no-progress duration).
		if draining {
			elapsedDrain := time.Since(drainStartAt)
			if elapsedDrain >= req.DrainGrace || shards.totalIdleCost(time.Now()) >= req.IdleShardCost {
				return replicateBatchResult{
					CompletedShards: shards.allCompleted(),
					InFlight:        a.buildInFlight(execs, verified, shards, time.Now()),
					VerifiedCount:   int64(doneCount),
				}, nil
			}
		} else {
			// Normal mode: if the cumulative idle cost across
			// completed-but-not-yet-signaled shards crosses the threshold,
			// signal-release them so the workflow can dispatch new batches
			// against those shards while this activity keeps draining its
			// still-pending ones.
			if shards.totalIdleCost(time.Now()) >= req.IdleShardCost {
				releaseList := shards.awaitingRelease()
				if len(releaseList) > 0 {
					if err := a.signalReleaseShards(ctx, req, releaseList); err != nil {
						return replicateBatchResult{}, err
					}
					shards.markReleased(releaseList)
				}
			}
		}

		// If the inner loop aborted because callCtx died, skip the sleep
		// entirely so the outer-loop top sees the new state promptly
		// (normal → drain transition, or drain → exit).
		if ctxAborted {
			continue
		}

		// Sleep until the next exec is due for retry. Drain mode honours
		// the same scheduling so we don't burn the grace window
		// busy-spinning when every remaining exec is in backoff.
		sleepDur := 50 * time.Millisecond
		if !minNextRetry.IsZero() {
			if delta := time.Until(minNextRetry); delta > sleepDur {
				sleepDur = delta
			}
		}
		if draining {
			remaining := req.DrainGrace - time.Since(drainStartAt)
			if remaining > 0 && remaining < sleepDur {
				sleepDur = remaining
			}
			// Parent ctx is already dead in drain mode, so the
			// select-on-ctx.Done() in normal mode would tight-loop here.
			// Use the detached drain ctx (and a pure wall-clock fallback)
			// instead.
			select {
			case <-time.After(sleepDur):
			case <-callCtx.Done():
			}
		} else {
			select {
			case <-time.After(sleepDur):
			case <-ctx.Done():
				// ctx cancel just sets draining on the next iteration;
				// don't unwind here.
			}
		}
	}
}

// generateReplicationTaskForExec injects one execution into the
// replication queue. Delegates to generateWorkflowReplicationTask so the
// rateLimiter wait, frontend-vs-history RPC choice, archetype lookup,
// and history-size-proportional token reservation stay in one place —
// sharded only wraps it to supply a single-element TargetClusters slice
// and to thread the dynamic-config-driven generateViaFrontend flag
// through.
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
// a single execution. Mirrors verifySingleReplicationTask's
// classification — DescribeMutableState on the remote followed by
// workflowVerifier content comparison on success or
// checkSkipWorkflowExecution on NotFound — and returns whether the exec
// is now verified.
//
// Per-classification metric counters are emitted inline (success,
// pending, not-found, busy-workflow, failed). The caller only needs the
// verified bit; not-verified outcomes (missing/busy) drive per-exec
// backoff identically but stay distinct in metrics so a "passive
// cluster apply is in progress" signal stays visible.
//
// We do the DescribeMutableState call inline rather than delegating
// straight to verifySingleReplicationTask so the busy-workflow branch
// keeps its distinct counter. The existing helper folds BUSY_WORKFLOW
// into the generic notVerified result, losing the signal that the
// passive cluster is making progress.
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

// shardVerifyTracker is keyed by history shard ID.
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
// drain-mode activity return. Shards with zero unverified execs are not
// included — fully verified shards are reported via CompletedShards (and
// any that completed mid-flight have already been signal-released so the
// workflow's packer could reuse them).
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
// yet. Used to name a concrete (BusinessID, RunID) in the ShardNoProgress
// failure event.
//
// Panics if no such execution exists — the only caller is the stuck-shard
// backstop, which by construction only fires for shards with at least one
// pending exec. A silent fallback to index 0 would hide a future
// invariant violation behind a misleading error message.
func (a *activities) firstUnverifiedOnShard(execs []*shardedExecutionInfo, verified []bool, shard int32) int {
	for i, ex := range execs {
		if verified[i] {
			continue
		}
		if ex.Shard == shard {
			return i
		}
	}
	panic(fmt.Sprintf("firstUnverifiedOnShard: no unverified exec on shard %d", shard))
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
