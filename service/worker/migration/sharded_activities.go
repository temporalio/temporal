package migration

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
)

type (
	DescribeTargetClusterRequest struct {
		TargetClusterName string
	}

	DescribeTargetClusterResponse struct {
		ShardCount int32
	}
)

// DescribeTargetCluster returns the remote cluster's history shard count
// from the locally-cached cluster_metadata table. The remote is registered
// via AdminService.AddOrUpdateRemoteCluster (which fetches HistoryShardCount
// from the remote at registration time and stores it) — a prerequisite for
// force replication, since the source generates replication tasks against
// it. ClusterMetadata refreshes the cache roughly every minute by default,
// so the value can be slightly stale; shard count never changes for a live
// cluster so this is fine.
func (a *activities) DescribeTargetCluster(_ context.Context, req DescribeTargetClusterRequest) (*DescribeTargetClusterResponse, error) {
	info, ok := a.clusterMetadata.GetAllClusterInfo()[req.TargetClusterName]
	if !ok {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("target cluster %q not registered in cluster metadata", req.TargetClusterName),
			"TargetClusterNotRegistered", nil)
	}
	if info.ShardCount <= 0 {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("target cluster %q has non-positive ShardCount (%d) in cluster metadata", req.TargetClusterName, info.ShardCount),
			"InvalidTargetShardCount", nil)
	}
	return &DescribeTargetClusterResponse{ShardCount: info.ShardCount}, nil
}

// ReplicateBatch is the per-batch activity body for the sharded force
// replication workflow. Runs inject then verify, signal-releasing completed
// shards mid-flight as their cumulative idle cost crosses IdleShardCost.
// Returns {CompletedShards, VerifiedCount} on success. Both phases are
// heartbeat-resumable — inject via NextInjectIdx/InjectDone, verify via the
// ReleasedShards/VerifiedExecs progress snapshot — so a retry (e.g. a worker
// lost to a deploy and detected via the heartbeat timeout) resumes in place
// rather than re-injecting or re-verifying completed work.
func (a *activities) ReplicateBatch(ctx context.Context, req *shardedBatchReq) (replicateBatchResult, error) {
	// Flatten once so per-exec bookkeeping (verified[], attempts[],
	// nextRetryAt[]) can stay index-based.
	execs := req.Executions.flatten()
	execCount := len(execs)
	if execCount == 0 {
		return replicateBatchResult{}, nil
	}

	var hb replicateBatchHeartbeat
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &hb)
	}

	// ---- Inject phase ----
	if !hb.InjectDone {
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

	remoteAdminClient, err := a.clientBean.GetRemoteAdminClient(req.TargetClusterName)
	if err != nil {
		return replicateBatchResult{}, fmt.Errorf("get remote admin client for %s: %w", req.TargetClusterName, err)
	}

	return a.runVerifyPhase(ctx, req, execs, execCount, remoteAdminClient, ns, hb)
}

// runVerifyPhase is the verify-phase loop body of ReplicateBatch. It
// owns per-exec bookkeeping, the per-iteration completion / stuck-shard /
// signal-release decisions — extracted from ReplicateBatch to keep its
// cognitive complexity under the linter cap.
func (a *activities) runVerifyPhase(
	ctx context.Context,
	req *shardedBatchReq,
	execs []*shardedExecutionInfo,
	execCount int,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	hb replicateBatchHeartbeat,
) (replicateBatchResult, error) {
	// Seed from the resumed heartbeat so a retried attempt (e.g. a worker lost
	// to a deploy, detected via the heartbeat timeout) picks up where the last
	// left off rather than re-verifying every exec and re-releasing shards the
	// workflow has already freed. A fresh attempt starts from a zero hb.
	verified, doneCount, shards := seedVerifyState(execs, hb)
	attempts := make([]int, execCount)
	nextRetryAt := make([]time.Time, execCount)

	for {
		passDelta, minNextRetry, vErr := a.runVerifyPass(
			ctx, remoteAdminClient, ns, req, execs, verified, attempts, nextRetryAt, shards)
		doneCount += passDelta
		if vErr != nil {
			return replicateBatchResult{}, vErr
		}

		activity.RecordHeartbeat(ctx, verifyHeartbeat(execs, verified, shards))

		if done, result, err := a.evaluateVerifyIteration(
			ctx, req, execs, verified, shards, doneCount, execCount); err != nil {
			return replicateBatchResult{}, err
		} else if done {
			return result, nil
		}

		waitNextTick(ctx, minNextRetry)
	}
}

// seedVerifyState reconstructs verify-phase bookkeeping from a resumed
// heartbeat. Execs on already-released shards are marked verified (they were
// all applied before the shard was released) and the shards re-flagged
// released, so the resumed attempt neither re-verifies them nor re-signals a
// release the workflow already acted on; remaining verified execs are replayed
// from hb.VerifiedExecs. The tracker's lastProgress is seeded at now (via
// newShardVerifyTracker), so the no-progress backstop measures from this
// attempt's start — resumed work gets the benefit of the doubt.
func seedVerifyState(execs []*shardedExecutionInfo, hb replicateBatchHeartbeat) ([]bool, int, shardVerifyTracker) {
	verified := make([]bool, len(execs))
	shards := newShardVerifyTracker(execs)
	now := time.Now()
	doneCount := 0

	released := make(map[int32]bool, len(hb.ReleasedShards))
	for _, sh := range hb.ReleasedShards {
		released[sh] = true
	}
	for i, ex := range execs {
		if released[ex.Shard] {
			verified[i] = true
			doneCount++
			shards.recordVerified(ex.Shard, now)
		}
	}
	for _, i := range hb.VerifiedExecs {
		if i < 0 || i >= len(execs) || verified[i] {
			continue
		}
		verified[i] = true
		doneCount++
		shards.recordVerified(execs[i].Shard, now)
	}
	// markReleased after recording so the released shards land with
	// pending==0, released==true, and doneAt cleared (no idle-cost accrual).
	shards.markReleased(hb.ReleasedShards)
	return verified, doneCount, shards
}

// verifyHeartbeat snapshots verify progress for the activity heartbeat so a
// retry can resume via seedVerifyState. Released shards are recorded by ID;
// their execs are implicitly verified and omitted from VerifiedExecs to keep
// the payload compact.
func verifyHeartbeat(execs []*shardedExecutionInfo, verified []bool, shards shardVerifyTracker) replicateBatchHeartbeat {
	released := shards.releasedShards()
	releasedSet := make(map[int32]bool, len(released))
	for _, sh := range released {
		releasedSet[sh] = true
	}
	var verifiedExecs []int
	for i, ex := range execs {
		if verified[i] && !releasedSet[ex.Shard] {
			verifiedExecs = append(verifiedExecs, i)
		}
	}
	return replicateBatchHeartbeat{
		InjectDone:     true,
		ReleasedShards: released,
		VerifiedExecs:  verifiedExecs,
	}
}

// evaluateVerifyIteration runs the post-pass checks (clean completion,
// stuck-shard backstop, mid-flight signal release) for a single
// verify-loop iteration. Returns done=true with the result when the loop
// should exit; otherwise (false, _, nil) means continue.
func (a *activities) evaluateVerifyIteration(
	ctx context.Context,
	req *shardedBatchReq,
	execs []*shardedExecutionInfo,
	verified []bool,
	shards shardVerifyTracker,
	doneCount, execCount int,
) (bool, replicateBatchResult, error) {
	if doneCount >= execCount {
		return true, replicateBatchResult{
			CompletedShards: shards.allCompleted(),
			VerifiedCount:   int64(doneCount),
		}, nil
	}

	if sErr := a.checkStuckShard(req, shards, execs, verified, doneCount, execCount); sErr != nil {
		return false, replicateBatchResult{}, sErr
	}

	if err := a.maybeSignalRelease(ctx, req, shards); err != nil {
		return false, replicateBatchResult{}, err
	}
	return false, replicateBatchResult{}, nil
}

// runInjectPhase walks execs in flattened order, generating one
// replication task per exec under a per-batch RPS limiter. Cancellation
// mid-loop returns a CanceledError so the caller can propagate it.
// Already-injected execs are re-injected harmlessly — replication dedupes
// per (namespace, wf, run).
func (a *activities) runInjectPhase(ctx context.Context, req *shardedBatchReq, execs []*shardedExecutionInfo, startIdx int) error {
	rateLimiter := quotas.NewRateLimiter(req.PerBatchGenerateRPS, int(math.Ceil(req.PerBatchGenerateRPS)))
	for i := startIdx; i < len(execs); i++ {
		ex := execs[i]
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
// (for sleep scheduling), and a non-nil error only for hard errors from
// the verify path.
//
// ctx is the activity ctx, used for both the DMS call and heartbeating —
// a single pass over a large batch can outlast HeartbeatTimeout if we
// only heartbeat once at the end, so we tick per attempted exec.
func (a *activities) runVerifyPass(
	ctx context.Context,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	req *shardedBatchReq,
	execs []*shardedExecutionInfo,
	verified []bool,
	attempts []int,
	nextRetryAt []time.Time,
	shards shardVerifyTracker,
) (int, time.Time, error) {
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

		ok, err := a.attemptVerifyExec(ctx, remoteAdminClient, ns, req, ex)
		if err != nil {
			return verifiedDelta, minNextRetry, err
		}

		if ok {
			verified[i] = true
			verifiedDelta++
			shards.recordVerified(ex.Shard, time.Now())
		} else {
			attempts[i]++
			nextRetryAt[i] = time.Now().Add(backoffDelay(attempts[i]))
			minNextRetry = earliest(minNextRetry, nextRetryAt[i])
		}

		activity.RecordHeartbeat(ctx, verifyHeartbeat(execs, verified, shards))
	}
	return verifiedDelta, minNextRetry, nil
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
// req.ShardNoProgress without a verified outcome (double that for a shard
// still awaiting its first verification — see pickStuck). Duration is
// cumulative from activity start (seeded at time.Now() on first activity
// attempt).
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
	if stuckIdx, found := firstUnverifiedOnShard(execs, verified, stuckShard); found {
		stuck := execs[stuckIdx]
		msg = fmt.Sprintf("shard %d no progress for %v on %s/%s (%d/%d done)",
			stuckShard, stuckDur, stuck.BusinessID, stuck.RunID, doneCount, total)
	}
	return temporal.NewNonRetryableApplicationError(msg, "ShardNoProgress", nil)
}

// maybeSignalRelease signals the workflow to release any
// completed-but-unsignaled shards if their cumulative idle cost crossed
// the threshold.
//
// Ctx-canceled errors from signalReleaseShards are suppressed: a
// workflow-initiated cancel arriving mid-signal would otherwise
// surface as a wrapped ctx-canceled error (not temporal.CanceledError)
// that the workflow side wouldn't recognise via IsCanceledError.
// Suppressing here lets the outer loop exit cleanly when the activity's
// context is cancelled.
func (a *activities) maybeSignalRelease(ctx context.Context, req *shardedBatchReq, shards shardVerifyTracker) error {
	if shards.totalIdleCost(time.Now()) < req.IdleShardCost {
		return nil
	}
	releaseList := shards.awaitingRelease()
	if len(releaseList) == 0 {
		return nil
	}
	if err := a.signalReleaseShards(ctx, req, releaseList); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	shards.markReleased(releaseList)
	return nil
}

// waitNextTick sleeps until the next exec is due for retry or ctx is done.
func waitNextTick(ctx context.Context, minNextRetry time.Time) {
	sleepDur := 50 * time.Millisecond
	if !minNextRetry.IsZero() {
		if delta := time.Until(minNextRetry); delta > sleepDur {
			sleepDur = delta
		}
	}
	select {
	case <-time.After(sleepDur):
	case <-ctx.Done():
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

	vreq := &verifyReplicationTasksRequest{
		Namespace:         req.Namespace,
		NamespaceID:       req.NamespaceID,
		TargetClusterName: req.TargetClusterName,
	}

	result, err := a.verifySingleReplicationTask(ctx, vreq, remoteAdminClient, ns, ex.ExecutionInfo)
	if err != nil {
		return false, err
	}
	return result.isVerified(), nil
}

// signalReleaseShards sends the mid-flight ReleaseShards signal to the
// parent workflow, freeing the listed shards in the workflow's
// shardInFlight set so the packer can dispatch fresh batches against them
// while this activity stays running on its still-pending shards.
//
// No retry wrapping: a transient failure propagates up so the activity
// fails; the child workflow records the error via lastErr.
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
	verifiedAny  bool      // an exec on this shard has produced a verified outcome
}

type shardVerifyTracker map[int32]shardVerify

// newShardVerifyTracker builds the per-shard state from the flattened
// exec slice. Each shard's lastProgress starts at time.Now() so the
// stuck-shard backstop measures elapsed time from activity start rather
// than the activity's scheduling epoch.
func newShardVerifyTracker(execs []*shardedExecutionInfo) shardVerifyTracker {
	t := shardVerifyTracker{}
	for _, ex := range execs {
		sv := t[ex.Shard]
		sv.pending++
		t[ex.Shard] = sv
	}
	nowSeed := time.Now()
	for sh, sv := range t {
		sv.lastProgress = nowSeed
		t[sh] = sv
	}
	return t
}

func (t shardVerifyTracker) recordVerified(sh int32, now time.Time) {
	sv := t[sh]
	sv.pending--
	sv.lastProgress = now
	sv.verifiedAny = true
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

// completedShards returns shard IDs matching filter, sorted ascending
// so signal payloads and activity returns are deterministic.
func (t shardVerifyTracker) completedShards(filter func(shardVerify) bool) []int32 {
	var out []int32
	for sh, sv := range t {
		if filter(sv) {
			out = append(out, sh)
		}
	}
	slices.Sort(out)
	return out
}

// awaitingRelease returns completed-but-not-yet-signaled shard IDs in
// ascending order so the signal payload is deterministic across replays.
func (t shardVerifyTracker) awaitingRelease() []int32 {
	return t.completedShards(func(sv shardVerify) bool {
		return !sv.doneAt.IsZero()
	})
}

// allCompleted returns every shard that finished during this activity
// run — both signal-released and still awaiting release at return.
func (t shardVerifyTracker) allCompleted() []int32 {
	return t.completedShards(func(sv shardVerify) bool {
		return sv.released || !sv.doneAt.IsZero()
	})
}

// releasedShards returns the shards already signal-released this run, ascending
// so the heartbeat snapshot is deterministic across replays.
func (t shardVerifyTracker) releasedShards() []int32 {
	return t.completedShards(func(sv shardVerify) bool {
		return sv.released
	})
}

// pickStuck returns (shard, age, true) for the lowest-numbered shard
// whose cumulative no-progress duration meets or exceeds its effective
// threshold. A shard that hasn't yet produced any verified outcome gets
// double the window: the server may still be working through a backlog
// that predates our task submission. Once any exec on the shard verifies
// we expect the rest within the normal window, so the threshold reverts.
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
		effective := threshold
		if !sv.verifiedAny {
			effective = 2 * threshold
		}
		age := now.Sub(sv.lastProgress)
		if age < effective {
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

// firstUnverifiedOnShard returns the index of the first execution in the
// flattened execs slice that targets the given shard and hasn't verified
// yet, and a found flag. Callers should only invoke this for shards
// with at least one pending exec; the found=false return is a defensive
// fallback so a tracker / verified-slice drift can't crash the activity.
func firstUnverifiedOnShard(execs []*shardedExecutionInfo, verified []bool, shard int32) (int, bool) {
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
