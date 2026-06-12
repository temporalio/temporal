package migration

import (
	"errors"
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
// In-flight activities are allowed to finish naturally at cycle end;
// CycleDrainTimeout bounds the wait as a safety net against
// pathological slow drains. On timer expiry in-flights are cancelled
// and their drained execs feed the next cycle's carry-over as
// ResumeShards. On lastErr the same drain happens, but the workflow
// returns the error rather than CANing — the drained state surfaces
// only via the status query's recovery bundle.
//
// If listing wasn't exhausted, the workflow CANs with NextPageToken
// (plus any drained state from the timer-fired path). Otherwise it
// returns nil.
func ShardedForceReplicationWorkflow(ctx workflow.Context, params ShardedForceReplicationParams) error {
	// Page token at workflow entry — returned by the status query as
	// PageTokenForRestart so tooling that already knows the legacy
	// "restart from the starting position" semantic keeps working.
	// The richer Recovery* fields below carry the current page token
	// plus in-flight execs and are what the sharded restart flow
	// actually uses.
	startPageToken := params.NextPageToken

	// state is assigned after newShardedWorkflowState below; the
	// query handler closes over the pointer so it sees the live state
	// once setup completes. Queries that arrive during setup return
	// the static fields without the recovery bundle, which matches
	// the prior behaviour.
	var state *shardedWorkflowState

	// Register the status query under the same name upstream uses
	// (forceReplicationStatusQueryType = "force-replication-status")
	// so tooling that polls force-rep progress works across both
	// workflow variants.
	if err := workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		status := ForceReplicationStatus{
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
			ReplicatedWorkflowCount:            params.ReplicatedWorkflowCount,
			ReplicatedWorkflowCountPerSecond:   params.ReplicatedWorkflowCountPerSecond,
			PageTokenForRestart:                startPageToken,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
			RecoveryNextPageToken:              params.NextPageToken,
			RecoveryResumeShards:               params.ResumeShards,
			RecoveryBuckets:                    params.RecoveredBuckets,
		}
		if state != nil {
			status.RecoveryResumeShards = state.collectResumeShardsForCarryover()
			status.RecoveryBuckets = state.collectRecoveredBucketsForCarryover()
		}
		return status, nil
	}); err != nil {
		return err
	}

	if err := validateShardedForceReplicationParams(&params); err != nil {
		return err
	}

	var err error
	state, err = newShardedWorkflowState(ctx, &params)
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
		wfCount, err := countWorkflowsForReplication(ctx, params.Namespace, params.Query, shardedCountWorkflowsForReplicationTimeout)
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
	// signal-release). The packer uses this set to ensure that each
	// shard can only be present in one in-flight batch at a time.
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
	// batches return an empty InFlight). Anything left at cycle end
	// corresponds to a batch whose activity returned CanceledError
	// with no result — i.e. the activity body never ran. On the CAN
	// path those execs are recovered into the next cycle's streaming
	// buckets; on the lastErr return path they surface via the status
	// query's recovery bundle but aren't auto-dispatched.
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

	// cycleDrainTimedOut is set by the safety timer started after the
	// page loop. drainBuckets and awaitInFlightCompletion both honour
	// it so a pathological slow drain can't eat into the workflow's
	// remaining history budget — on expiry the workflow falls into
	// drainForCAN and ships unfinished work as carry-over.
	cycleDrainTimedOut bool

	nextBatchID int64

	// metricsHandler is tagged with the workflow's fixed scope +
	// namespace once at state construction; recordVerified reuses it on
	// every batch return.
	metricsHandler sdkclient.MetricsHandler
}

// defaultConcurrentBatchCount derives the in-flight-batch ceiling
// from the target cluster's shard count: a quarter of the shards,
// capped at defaultConcurrentBatchCap. The 1/4 fraction leaves worker
// slots free for unrelated activities; the absolute cap bounds the
// cluster blast radius regardless of cluster size. Returns at least 1.
func defaultConcurrentBatchCount(shards int32) int {
	return max(min(int(shards)/4, defaultConcurrentBatchCap), 1)
}

// applyShardedDefaults fills zero-valued tuning fields on params.
func applyShardedDefaults(params *ShardedForceReplicationParams, targetShardCount int32) {
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
	if params.CycleDrainTimeout <= 0 {
		params.CycleDrainTimeout = defaultCycleDrainTimeout
	}
	if params.ListWorkflowsPageSize <= 0 {
		params.ListWorkflowsPageSize = defaultShardedListPageSize
	}
	if params.PerBatchGenerateRPS <= 0 {
		params.PerBatchGenerateRPS = defaultPerBatchGenerateRPS
	}
	if params.ConcurrentBatchCount <= 0 {
		params.ConcurrentBatchCount = defaultConcurrentBatchCount(targetShardCount)
	}
	if params.EstimationMultiplier <= 0 {
		params.EstimationMultiplier = 2
	}
}

func newShardedWorkflowState(ctx workflow.Context, params *ShardedForceReplicationParams) (*shardedWorkflowState, error) {
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}
	localCtx := workflow.WithLocalActivityOptions(ctx, lao)
	var a *activities
	var md MetadataResponse
	if err := workflow.ExecuteLocalActivity(localCtx, a.GetMetadata, MetadataRequest{Namespace: params.Namespace}).Get(ctx, &md); err != nil {
		return nil, err
	}
	var targetMd DescribeTargetClusterResponse
	if err := workflow.ExecuteLocalActivity(localCtx, a.DescribeTargetCluster, DescribeTargetClusterRequest{
		TargetClusterName: params.TargetClusterName,
	}).Get(ctx, &targetMd); err != nil {
		return nil, err
	}
	applyShardedDefaults(params, targetMd.ShardCount)
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
	params.RecoveredBuckets.mergeInto(s.buckets, s.bucketCounts)
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

	// Drive ListWorkflows until either we exhaust the namespace or
	// the SDK signals that history is large enough to CAN. Errors
	// here latch into lastErr and fall through to the unified exit
	// funnel — same drain-and-decide path as activity-driven errors.
	//
	// On a CAN cycle (ContinuedAsNewCount > 0), an empty NextPageToken
	// means a prior cycle already exhausted pagination — empty token at
	// the start of cycle 0 is "haven't started", but at the start of any
	// later cycle it's "finished". Skip listing in that case; otherwise
	// a carry-over-driven CAN (drained InFlight, recovered buckets, …)
	// would re-list page 1 and re-enqueue every visible execution.
	for s.shouldList() && !workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
		if s.lastErr != nil {
			break
		}
		executions, nextPageToken, err := s.listWorkflowPage(ctx)
		if err != nil {
			s.setLastErr(err)
			break
		}
		for _, ex := range executions {
			sh := common.WorkflowIDToHistoryShard(s.namespaceID, ex.BusinessID, s.targetShardCount)
			s.addToBucket(sh, ex.BusinessID, RunEntry{
				RunID:       ex.RunID,
				ArchetypeID: ex.ArchetypeID,
			})
		}
		s.params.NextPageToken = nextPageToken

		for s.tryPackStreaming(ctx, false) { //nolint:revive // intentional empty body
		}

		if len(nextPageToken) == 0 {
			break
		}
	}

	// Start the cycle drain safety timer; see defaultCycleDrainTimeout
	// for the budget rationale.
	cancelCycleTimer := s.startCycleDrainTimer(ctx)
	defer cancelCycleTimer()

	// Drain remaining buckets only when no error has latched — on
	// error we deliberately stop scheduling new work and let the
	// already-dispatched batches finish via awaitInFlightCompletion.
	if s.lastErr == nil {
		s.drainBuckets(ctx)
	}

	// Wait for in-flight activities. Cancels them when we already
	// know we're failing or the cycle drain timer has expired;
	// otherwise waits naturally so a healthy activity isn't
	// cancelled into a CanceledError that masquerades as carry-over
	// state.
	s.awaitInFlightCompletion(ctx)

	// Single exit decision. Recovery state has the same shape on
	// either path — drainPayload + undispatched ResumeShards +
	// batchExecs + leftover buckets — so the only difference between
	// "fail with state" and "CAN with state" is the return value.
	if s.lastErr != nil {
		return s.lastErr
	}
	if !s.hasCarryover() {
		return nil
	}

	next := *s.params
	next.ContinuedAsNewCount++
	next.ResumeShards = s.collectResumeShardsForCarryover()
	next.RecoveredBuckets = s.collectRecoveredBucketsForCarryover()
	return workflow.NewContinueAsNewError(ctx, ShardedForceReplicationWorkflow, next)
}

// shouldList returns true if the page loop has more work to do this
// cycle. Cycle 0 always lists (NextPageToken is empty either way).
// On later cycles, an empty NextPageToken can only mean a prior cycle
// already drained pagination — there's nothing left to list.
func (s *shardedWorkflowState) shouldList() bool {
	return s.params.ContinuedAsNewCount == 0 || len(s.params.NextPageToken) > 0
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

	// Per-exec backoff still owns the per-exec retry; MaximumAttempts
	// lets a transient activity failure recover via heartbeat-resume
	// without losing inject progress. WaitForCancellation lets a
	// cancelled activity run drain logic and return its drain result.
	//
	// 10 attempts is sized for fleet rollouts: a rolling deploy of the
	// activity workers can burn several attempts per batch (each
	// shutdown surfaces as a retryable WorkerShutdown error from the
	// activity). 3 was tight enough that two unlucky deploys could
	// exhaust the budget on a long-running CAN cycle.
	shardedReplicateBatchRetryPolicy = &temporal.RetryPolicy{
		MaximumAttempts: 10,
	}
	shardedReplicateBatchActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         shardedReplicateBatchRetryPolicy,
		WaitForCancellation: true,
	}
)

func (s *shardedWorkflowState) listWorkflowPage(ctx workflow.Context) ([]*ExecutionInfo, []byte, error) {
	listCtx := workflow.WithActivityOptions(ctx, shardedListWorkflowsActivityOptions)
	listReq := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     s.params.Namespace,
		Query:         s.params.Query,
		PageSize:      int32(s.params.ListWorkflowsPageSize),
		NextPageToken: s.params.NextPageToken,
	}
	var a *activities
	var listResp listWorkflowsResponse
	if err := workflow.ExecuteActivity(listCtx, a.ListWorkflows, listReq).Get(ctx, &listResp); err != nil {
		return nil, nil, err
	}
	return listResp.Executions, listResp.NextPageToken, nil
}

func (s *shardedWorkflowState) replicateBatch(ctx, activityParentCtx workflow.Context, req *shardedBatchReq) (replicateBatchResult, error) {
	actx := workflow.WithActivityOptions(activityParentCtx, shardedReplicateBatchActivityOptions)
	var a *activities
	var result replicateBatchResult
	if err := workflow.ExecuteActivity(actx, a.ReplicateBatch, req).Get(ctx, &result); err != nil {
		return replicateBatchResult{}, err
	}
	return result, nil
}

// startCycleDrainTimer spawns a coroutine that sets cycleDrainTimedOut
// after CycleDrainTimeout. Returned cancel func stops the timer on
// natural exit. The flag is read by drainBuckets (stops spawning new
// batches and exits) and awaitInFlightCompletion (exits its Await and
// calls drainForCAN to cancel in-flight activities and collect their
// drain payload for the next cycle's carry-over).
func (s *shardedWorkflowState) startCycleDrainTimer(ctx workflow.Context) workflow.CancelFunc {
	timerCtx, cancel := workflow.WithCancel(ctx)
	workflow.Go(ctx, func(gCtx workflow.Context) {
		if err := workflow.NewTimer(timerCtx, s.params.CycleDrainTimeout).Get(gCtx, nil); err == nil {
			s.cycleDrainTimedOut = true
		}
	})
	return cancel
}

// awaitInFlightCompletion waits for in-flight batches to complete.
// On the clean path it just blocks until pendingDispatches drops to
// zero — a healthy activity's result isn't masked as a CanceledError.
// On lastErr or cycle-drain timeout it falls into drainForCAN, which
// cancels the in-flight activities and collects their drain payload.
// run() then either returns lastErr (drain payload surfaces only via
// the status query) or CANs with the drained state as carry-over.
func (s *shardedWorkflowState) awaitInFlightCompletion(ctx workflow.Context) {
	if s.pendingDispatches == 0 {
		return
	}
	if s.lastErr != nil {
		s.drainForCAN(ctx)
		return
	}
	_ = workflow.Await(ctx, func() bool {
		return s.pendingDispatches == 0 || s.lastErr != nil || s.cycleDrainTimedOut
	})
	if s.pendingDispatches > 0 {
		s.drainForCAN(ctx)
	}
}

// hasCarryover reports whether the workflow has any state worth
// preserving across an exit — either a remaining page token, drained
// execs from in-flight batches, cancel-before-start batches that
// never injected, undispatched resume entries, or listed-but-unpacked
// execs. Drives both the "CAN vs return nil" decision and the
// recovery bundle exposed in the status query.
func (s *shardedWorkflowState) hasCarryover() bool {
	if len(s.params.NextPageToken) > 0 {
		return true
	}
	if len(s.drainPayload) > 0 {
		return true
	}
	if len(s.batchExecs) > 0 {
		return true
	}
	if len(s.params.ResumeShards) > 0 {
		return true
	}
	return !s.bucketsEmpty()
}

// collectResumeShardsForCarryover concatenates this cycle's drained
// execs with any prior-cycle ResumeShards that didn't get dispatched
// (left in params.ResumeShards by dispatchResumeBatches when it bailed
// out on lastErr). Both groups are already shard-keyed; the next
// cycle's dispatchResumeBatches sorts and re-packs them.
func (s *shardedWorkflowState) collectResumeShardsForCarryover() []ResumeShard {
	if len(s.drainPayload) == 0 && len(s.params.ResumeShards) == 0 {
		return nil
	}
	out := make([]ResumeShard, 0, len(s.drainPayload)+len(s.params.ResumeShards))
	out = append(out, s.drainPayload...)
	out = append(out, s.params.ResumeShards...)
	return out
}

// collectRecoveredBucketsForCarryover merges the two sources of
// "execs that never made it through a verify activity this cycle":
// batches that returned CanceledError without running a body, and
// listed-but-unpacked execs still sitting in s.buckets when the
// workflow exited (either lastErr stopped the streaming packer or
// drainBuckets bailed on lastErr / cycle drain timeout partway
// through).
func (s *shardedWorkflowState) collectRecoveredBucketsForCarryover() BatchPayload {
	out := collectRecoveredBuckets(s.batchExecs)
	if !s.bucketsEmpty() {
		if out == nil {
			out = BatchPayload{}
		}
		out.merge(s.buckets)
	}
	return out
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
	// In-flight batches hold disjoint shard claims, so merging two
	// batchExecs entries never targets the same (shard, BID) key —
	// iteration order does not affect the final BatchPayload.
	//workflowcheck:ignore (writes are to disjoint keys; order-independent)
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

// extractVerifiedCountFromError pulls the partial VerifiedCount that
// wrapBatchVerifyError encoded into a BatchVerifyPartial-typed
// ApplicationError's Details on the activity side. Returns 0 when
// the error didn't come through the verify-phase wrapper (e.g.
// inject-phase failures, ctx errors, non-ApplicationError types), so
// callers can unconditionally fold the result into recordVerified.
func extractVerifiedCountFromError(err error) int64 {
	if err == nil {
		return 0
	}
	appErr, ok := errors.AsType[*temporal.ApplicationError](err)
	if !ok || appErr.Type() != batchVerifyPartialErrorType {
		return 0
	}
	var count int64
	if appErr.Details(&count) != nil {
		return 0
	}
	return count
}

// setLastErr latches the first error encountered. Subsequent errors
// are dropped so the root cause is preserved for the workflow's
// returned failure — without the latch, a stuck-shard backstop firing
// on every batch as the workflow tears down would overwrite the
// genuinely interesting first failure.
func (s *shardedWorkflowState) setLastErr(err error) {
	if s.lastErr == nil {
		s.lastErr = err
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

// resumeBatch is one packed dispatch plan: the BatchPayload that will
// become a batch's input, plus the matching per-shard no-progress
// durations. Built up front by packResumeBatchPlan so the dispatch
// loop can unpack any remainder back into ResumeShards if lastErr
// trips mid-dispatch.
type resumeBatch struct {
	payload    BatchPayload
	noProgress map[int32]time.Duration
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
//
// Plans every batch up front, then dispatches one at a time. If
// lastErr latches mid-dispatch, the remaining planned batches are
// unpacked back into s.params.ResumeShards so the recovery bundle
// (and the next CAN cycle) sees them — without the unpack step, a
// failing first resume batch would silently strand all subsequent
// resume entries.
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
	// We've taken ownership of these entries — anything not
	// dispatched gets restored below.
	s.params.ResumeShards = nil
	slices.SortFunc(entries, func(a, b ResumeShard) int {
		return int(a.Shard - b.Shard)
	})

	batches := s.packResumeBatchPlan(entries)
	for i, batch := range batches {
		// Block until a dispatch slot is free so resume payloads
		// can't overshoot ConcurrentBatchCount on cycles that
		// carried many shards across CAN. The await also wakes
		// immediately when lastErr is set.
		s.waitForDispatchSlot(ctx)
		if s.lastErr != nil {
			s.params.ResumeShards = unpackResumeBatches(batches[i:])
			return
		}
		s.spawnBatch(ctx, batch.payload, true, batch.noProgress)
	}
}

// packResumeBatchPlan groups ResumeShard entries into batches sized
// at or below BatchSize. Entries are taken whole — shardInFlight only
// admits one batch per shard at a time, so a single shard's payload
// can't be split.
func (s *shardedWorkflowState) packResumeBatchPlan(entries []ResumeShard) []resumeBatch {
	var batches []resumeBatch
	current := resumeBatch{payload: BatchPayload{}, noProgress: map[int32]time.Duration{}}
	packed := 0
	for _, rs := range entries {
		rsCount := runCount(rs.Execs)
		if packed+rsCount > s.params.BatchSize && packed > 0 {
			batches = append(batches, current)
			current = resumeBatch{payload: BatchPayload{}, noProgress: map[int32]time.Duration{}}
			packed = 0
		}
		current.payload[rs.Shard] = rs.Execs
		current.noProgress[rs.Shard] = rs.NoProgressDuration
		packed += rsCount
	}
	if packed > 0 {
		batches = append(batches, current)
	}
	return batches
}

// unpackResumeBatches reverses packResumeBatchPlan, turning planned
// batches back into a flat ResumeShard slice. Used when the dispatch
// loop aborts on lastErr so the undispatched remainder can be carried
// into the recovery bundle / next CAN cycle. The output is sorted by
// shard ID so the slice flowing into the CAN args is deterministic
// across replays.
func unpackResumeBatches(batches []resumeBatch) []ResumeShard {
	if len(batches) == 0 {
		return nil
	}
	var out []ResumeShard
	for _, b := range batches {
		out = append(out, resumeShardsFromPayload(b.payload, b.noProgress)...)
	}
	slices.SortFunc(out, func(a, b ResumeShard) int {
		return int(a.Shard - b.Shard)
	})
	return out
}

// runCount sums runs across BIDs in a single shard's payload entry.
func runCount(byBID map[string][]RunEntry) int {
	n := 0
	//workflowcheck:ignore (summation is order-independent)
	for _, runs := range byBID {
		n += len(runs)
	}
	return n
}

// drainBuckets blocks until buckets are empty (success), lastErr
// trips (failure), or the cycle drain timer expires (CAN with
// leftover buckets). Each pass packs everything currently
// dispatchable, then awaits any change in pendingDispatches +
// shardInFlight so the next pass can attempt shards just freed by
// signal-release.
func (s *shardedWorkflowState) drainBuckets(ctx workflow.Context) {
	for {
		if s.lastErr != nil || s.cycleDrainTimedOut {
			return
		}
		for s.tryPackStreaming(ctx, true) { //nolint:revive
		}
		if s.bucketsEmpty() || s.lastErr != nil || s.cycleDrainTimedOut {
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
	//workflowcheck:ignore (summation is order-independent)
	for _, n := range s.bucketCounts {
		remaining += n
	}
	s.setLastErr(temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("drainBuckets: %d execs in buckets but no batches in flight (shard-claim bookkeeping corrupted)", remaining),
		"DrainBucketsStuck", nil))
}

// drainBucketsAwaitPredicate returns true when the drainBuckets loop
// should wake up: lastErr tripped, cycle drain timer fired, a
// dispatch slot just freed, or a new free shard is ready to pack. A
// "free shard" wake-up only counts when there's also a dispatch slot
// to use it, otherwise the outer loop would busy-spin on
// tryPackStreaming returning false against the in-flight cap.
func (s *shardedWorkflowState) drainBucketsAwaitPredicate(currentPending int) func() bool {
	return func() bool {
		if s.lastErr != nil || s.cycleDrainTimedOut {
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

// drainForCAN cancels every in-flight batch and waits for them to
// return AND for the ReleaseShards signal channel to be drained.
// Called on lastErr (terminates with error; drain payload exposed via
// the status query) and on cycle drain timeout (CANs with drained
// state as carry-over). Activities honour cancellation by entering
// drain mode and returning a result whose InFlight carries their
// still-unverified execs; spawnBatch appends those entries to
// s.drainPayload. The signal channel drain is so a final ReleaseShards
// fired by an activity just before it returns doesn't get stranded
// mid-flight, which would leave shardInFlight set for shards the
// activity already considers complete.
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
	// Wait unconditionally for pendingDispatches to drain — lastErr may
	// already be set on entry, but drainPayload, batchExecs, and status
	// recovery fields only finalise once every in-flight goroutine has
	// returned.
	_ = workflow.Await(ctx, func() bool {
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
	//workflowcheck:ignore (building a set of flags is order-independent)
	for sh := range payload {
		s.shardInFlight[sh] = true
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
			//workflowcheck:ignore (deletes are commutative; order-independent)
			for sh := range s.heldByBatch[batchID] {
				delete(s.shardInFlight, sh)
			}
			delete(s.heldByBatch, batchID)
		}()
		result, err := s.replicateBatch(coroCtx, s.activityCtx, req)
		if err != nil {
			if temporal.IsCanceledError(err) {
				// Cancel-before-start: the activity body never ran,
				// so no result is available. Leaving batchExecs[batchID]
				// intact lets collectRecoveredBucketsForCarryover
				// re-bucket the execs as fresh inject+verify work next
				// cycle (only meaningful on the CAN path; on lastErr
				// they surface via the status query only).
				return
			}
			// Activity errored after partial verify — the SDK discards
			// the result on failure, but wrapBatchVerifyError on the
			// activity side carries the partial doneCount through as
			// ApplicationError details. Fold it into the running count
			// so ReplicatedWorkflowCount reflects work actually done.
			s.recordVerified(coroCtx, extractVerifiedCountFromError(err))
			s.setLastErr(err)
			return
		}
		// Activity body ran and returned cleanly — either a
		// clean completion (empty InFlight) or a drained cancel
		// (InFlight carries the still-unverified execs; reached
		// only on lastErr or cycle drain timeout). CompletedShards
		// is informational; the defer above clears heldByBatch +
		// shardInFlight either way.
		if len(result.InFlight) > 0 {
			s.drainPayload = append(s.drainPayload, result.InFlight...)
		}
		s.recordVerified(coroCtx, result.VerifiedCount)
		delete(s.batchExecs, batchID)
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
	//workflowcheck:ignore (existence check; iteration order does not affect result)
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
	//workflowcheck:ignore (output is sorted below before any observable use)
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
