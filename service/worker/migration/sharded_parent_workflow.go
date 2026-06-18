package migration

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/metrics"
)

// ShardedForceReplicationWorkflow is the parent workflow for the sharded
// force replication design. It owns the workflow lifecycle, the
// force-replication-status query surface, and child orchestration. It
// rarely CANs — only when its own history approaches the SDK's
// GetContinueAsNewSuggested hint, which at ~10 events/handover fires
// roughly every ~40 M execs (handover-bound) or ~22 h wall-clock
// (60 s rollup-bound), whichever is first.
//
// Architecture:
//
//	Parent (this) — rare CAN; query; TUD kickoff
//	  ├─ child 0  [T0, T1)   list + pack + ReplicateBatch activities
//	  ├─ child 1  [T1, T2)   (≤2 live at once during handover overlap)
//	  └─ TUD child           ABANDON policy
//
// The parent keeps the registered name "force-replication-sharded" so
// tooling that already targets that name continues to work.
func ShardedForceReplicationWorkflow(ctx workflow.Context, params ShardedForceReplicationParams) error {
	// startPageToken is the token at workflow entry — the PageTokenForRestart
	// fallback used until the first child checkpoint advances it (see the
	// query handler below).
	startPageToken := params.NextPageToken

	// ps is set after setup; the query handler closes over the pointer so
	// it sees live counts once setup completes. Queries that arrive
	// during setup return the static carry-over fields, matching prior
	// behaviour.
	var ps *shardedParentState

	if err := workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		status := ForceReplicationStatus{
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
			ReplicatedWorkflowCount:            params.ReplicatedWorkflowCount,
			ReplicatedWorkflowCountPerSecond:   params.ReplicatedWorkflowCountPerSecond,
			PageTokenForRestart:                startPageToken,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
		}
		if ps != nil {
			status.ReplicatedWorkflowCount = ps.retiredTotal + ps.liveTotalCount()
			status.ReplicatedWorkflowCountPerSecond = params.ReplicatedWorkflowCountPerSecond
			// The parent rarely CANs, so startPageToken would stay pinned at
			// the run's initial token. Once children begin checkpointing,
			// advance the restart token to the latest checkpoint so a restart
			// resumes near current progress instead of replaying the whole run.
			if ps.lastCheckpointToken != nil {
				status.PageTokenForRestart = ps.lastCheckpointToken
			}
		}
		return status, nil
	}); err != nil {
		return err
	}

	if err := validateShardedForceReplicationParams(&params); err != nil {
		return err
	}

	// Setup: fetch namespace metadata + target cluster shard count; apply
	// default parameter values.
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}
	localCtx := workflow.WithLocalActivityOptions(ctx, lao)
	var a *activities
	var md MetadataResponse
	if err := workflow.ExecuteLocalActivity(localCtx, a.GetMetadata, MetadataRequest{Namespace: params.Namespace}).Get(ctx, &md); err != nil {
		return err
	}
	var targetMd DescribeTargetClusterResponse
	if err := workflow.ExecuteLocalActivity(localCtx, a.DescribeTargetCluster, DescribeTargetClusterRequest{
		TargetClusterName: params.TargetClusterName,
	}).Get(ctx, &targetMd); err != nil {
		return err
	}
	applyShardedDefaults(&params, targetMd.ShardCount)

	// Reject configurations the packer can't honour.
	if params.MaxExecsPerShard > params.BatchSize {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("MaxExecsPerShard (%d) must be <= BatchSize (%d)", params.MaxExecsPerShard, params.BatchSize),
			"InvalidConfiguration", nil)
	}

	// QPSQueue sized off ConcurrentBatchCount; seeded with the carried-over
	// ReplicatedWorkflowCount so the first post-CAN rollup has a baseline.
	if params.QPSQueue.Data == nil {
		params.QPSQueue = NewQPSQueue(params.ConcurrentBatchCount, params.EstimationMultiplier)
		params.QPSQueue.Enqueue(ctx, params.ReplicatedWorkflowCount)
	}

	// First run: count total workflows for the status query denominator.
	if params.TotalForceReplicateWorkflowCount == 0 {
		wfCount, err := countWorkflowsForReplication(ctx, params.Namespace, params.Query, shardedCountWorkflowsForReplicationTimeout)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	// Kick off the task-queue user data replication child (ABANDON policy).
	if !params.TaskQueueUserDataReplicationStatus.Done {
		if err := maybeKickoffShardedTaskQueueUserDataReplication(ctx, &params, func(failureReason string) {
			params.TaskQueueUserDataReplicationStatus.FailureMessage = failureReason
			params.TaskQueueUserDataReplicationStatus.Done = true
		}); err != nil {
			return err
		}
	}

	ps = newShardedParentState(ctx, &params, md.NamespaceID, targetMd.ShardCount)
	if err := ps.run(ctx); err != nil {
		return err
	}

	// Terminal: namespace exhausted by the last child. Await TUD child
	// completion before returning nil.
	if err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done }); err != nil {
		return err
	}
	if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
		return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
	}
	return nil
}

// validateShardedForceReplicationParams rejects obviously broken inputs
// before any work begins.
func validateShardedForceReplicationParams(params *ShardedForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: Namespace is required", "InvalidArgument", nil)
	}
	if len(params.TargetClusterName) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: TargetClusterName is required", "InvalidArgument", nil)
	}
	return nil
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
		params.ConcurrentBatchCount = defaultConcurrentBatchCount(targetShardCount)
	}
	if params.EstimationMultiplier <= 0 {
		params.EstimationMultiplier = 2
	}
}

// defaultConcurrentBatchCount derives the in-flight-batch ceiling from
// the target cluster's shard count: a quarter of the shards, capped at
// defaultConcurrentBatchCap. The 1/4 fraction leaves worker slots free
// for unrelated activities; the absolute cap bounds the cluster blast
// radius regardless of cluster size. Returns at least 1.
func defaultConcurrentBatchCount(shards int32) int {
	return max(min(int(shards)/4, defaultConcurrentBatchCap), 1)
}

// maybeKickoffShardedTaskQueueUserDataReplication starts the task-queue
// user data replication child workflow on the first run (ContinuedAsNewCount
// == 0). The child is started with ABANDON policy so a parent failure or CAN
// does not terminate it. A coroutine listens for the child's done signal
// and calls onDone regardless of which run receives it.
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

// shardedParentState holds the parent workflow's per-run orchestration
// state. All mutation happens inside workflow coroutines (no concurrent
// goroutines), so plain maps and ints are safe without mutexes.
type shardedParentState struct {
	params           *ShardedForceReplicationParams
	namespaceID      string
	targetShardCount int32

	// retiredTotal accumulates VerifiedCount from completed children.
	// Seeded from params.ReplicatedWorkflowCount on construction so
	// CAN carry-over is additive.
	retiredTotal int64

	// liveCounts maps child runID → latest cumulative VerifiedCount
	// received via progress rollup signals. Deleted when the child
	// future resolves. Used by the status query to report a live total
	// without waiting for child completion.
	liveCounts map[string]int64 // summation is order-independent; see liveTotalCount

	// liveChildren maps child runID → child future. Used to track which
	// children are still running and to send the resumeFullRate promotion
	// signal when a predecessor completes.
	liveChildren map[string]workflow.ChildWorkflowFuture

	// liveExecs maps child runID → the full workflow.Execution (WorkflowID
	// + RunID) so the parent can address the child via SignalExternalWorkflow.
	// Child workflow IDs are auto-generated by the SDK when no explicit
	// WorkflowID is set in ChildWorkflowOptions.
	liveExecs map[string]workflow.Execution

	// liveRunIDs is the insertion-ordered slice of live child run IDs.
	// wireCount tracks how many of these have been wired into the
	// selector; after each sel.Select, newly appended children are wired.
	liveRunIDs []string
	wiredCount int

	// successorStarted tracks which child run IDs have had a successor
	// started — prevents double-starting if a checkpoint arrives twice
	// (e.g., replay).
	successorStarted map[string]bool

	// drainingForCAN is set when GetContinueAsNewSuggested fires on the
	// parent. Once set, no new successors are started; when the last live
	// child completes the parent CANs with the last checkpoint token.
	drainingForCAN bool

	// reachedEnd is set when a child returns ReachedEnd=true — it exhausted
	// the namespace (ListWorkflows returned an empty next-page token). This
	// is the authoritative terminal signal: once set the parent finishes
	// rather than continuing-as-new, because no work remains for a successor.
	reachedEnd bool

	// lastCheckpointToken is the NextPageToken from the most recently
	// received checkpoint signal. Carried into the CAN params so the
	// new parent run resumes children from the right position.
	lastCheckpointToken []byte

	// checkpointCh and progressCh are the signal channels for child→parent
	// handover and progress rollup signals respectively.
	checkpointCh workflow.ReceiveChannel
	progressCh   workflow.ReceiveChannel

	// startErr captures an error from startChild so the selector callback
	// (which cannot return an error) can surface it to the main loop.
	startErr error

	// childErr captures the first child failure so the main loop can
	// return it after the selector fires.
	childErr error

	metricsHandler sdkclient.MetricsHandler
}

// newShardedParentState constructs the parent orchestration state.
func newShardedParentState(
	ctx workflow.Context,
	params *ShardedForceReplicationParams,
	namespaceID string,
	targetShardCount int32,
) *shardedParentState {
	return &shardedParentState{
		params:           params,
		namespaceID:      namespaceID,
		targetShardCount: targetShardCount,
		retiredTotal:     params.ReplicatedWorkflowCount,
		liveCounts:       map[string]int64{},
		liveChildren:     map[string]workflow.ChildWorkflowFuture{},
		liveExecs:        map[string]workflow.Execution{},
		successorStarted: map[string]bool{},
		checkpointCh:     workflow.GetSignalChannel(ctx, shardedCheckpointSignalName),
		progressCh:       workflow.GetSignalChannel(ctx, shardedProgressSignalName),
		metricsHandler: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{
			metrics.OperationTagName: metrics.MigrationWorkflowScope,
			NamespaceTagName:         params.Namespace,
		}),
	}
}

// liveTotalCount sums the latest VerifiedCount across all live children.
// Used by the status query to report a real-time total.
func (ps *shardedParentState) liveTotalCount() int64 {
	var total int64
	//workflowcheck:ignore (summation is order-independent)
	for _, v := range ps.liveCounts {
		total += v
	}
	return total
}

// run is the parent workflow's main loop. It starts the first child then
// drives a workflow.Selector that multiplexes child completions,
// checkpoint signals, and progress rollup signals until no live children
// remain.
//
// Handover orchestration:
//   - On checkpoint: start a successor (at half rate) unless drainingForCAN.
//   - On child completion: retire its verifiedCount, record ReachedEnd, and
//     send resumeFullRate to its successor (if one was started).
//   - On GetContinueAsNewSuggested: set drainingForCAN. When the last live
//     child completes, CAN with lastCheckpointToken as NextPageToken.
//   - On exit (no live children): finish if a child reached the namespace
//     end; else CAN if draining; else fail (handover bookkeeping bug).
//
// At most two children are live at once (the cutting child plus its
// successor) and at most one handover is in flight at any time.
func (ps *shardedParentState) run(ctx workflow.Context) error {
	// Start the first child. Not in handover — no predecessor exists.
	if err := ps.startChild(ctx, ps.params.NextPageToken, false); err != nil {
		return err
	}

	sel := workflow.NewSelector(ctx)

	// Wire the initial child into the selector.
	for _, runID := range ps.liveRunIDs {
		ps.wireChild(sel, ctx, runID)
	}
	ps.wiredCount = len(ps.liveRunIDs)

	// Checkpoint signal: received when a child cuts at a page boundary.
	// Re-added to the selector each iteration because AddReceive fires
	// once per message delivery; the channel itself persists.
	sel.AddReceive(ps.checkpointCh, func(c workflow.ReceiveChannel, _ bool) {
		var payload shardedCheckpointPayload
		c.Receive(ctx, &payload)
		ps.handleCheckpoint(ctx, sel, payload)
	})

	// Progress rollup signal: updates liveCounts and QPS estimate.
	sel.AddReceive(ps.progressCh, func(c workflow.ReceiveChannel, _ bool) {
		var payload shardedProgressPayload
		c.Receive(ctx, &payload)
		ps.handleProgress(ctx, payload)
	})

	// Main loop: keep selecting until no children are live.
	for len(ps.liveChildren) > 0 {
		// Wire any children started since the last iteration (from
		// handleCheckpoint). wiredCount tracks the cursor into liveRunIDs.
		for ps.wiredCount < len(ps.liveRunIDs) {
			runID := ps.liveRunIDs[ps.wiredCount]
			ps.wireChild(sel, ctx, runID)
			ps.wiredCount++
		}

		if ps.startErr != nil {
			return ps.startErr
		}

		sel.Select(ctx)

		if ps.childErr != nil {
			return ps.childErr
		}
	}

	// All children have completed. Drain any residual checkpoint/progress
	// signals (shouldn't normally exist, but guards against race on replay).
	for ps.checkpointCh.Len() > 0 {
		sel.Select(ctx)
	}
	for ps.progressCh.Len() > 0 {
		sel.Select(ctx)
	}

	if ps.reachedEnd {
		// A child exhausted the namespace — terminal. Record the final count
		// for the terminal Await in ShardedForceReplicationWorkflow. Takes
		// precedence over drainingForCAN: once the end is reached there is
		// nothing left for a CAN'd successor to process.
		ps.params.ReplicatedWorkflowCount = ps.retiredTotal
		return nil
	}

	if ps.drainingForCAN {
		// Parent CAN: carry the last checkpoint token forward so the new
		// parent run resumes children from the right position.
		next := *ps.params
		next.ContinuedAsNewCount++
		next.NextPageToken = ps.lastCheckpointToken
		next.ReplicatedWorkflowCount = ps.retiredTotal
		return workflow.NewContinueAsNewError(ctx, ShardedForceReplicationWorkflow, next)
	}

	// No live children, yet the namespace was never exhausted and we are not
	// draining for CAN — work remains past the last checkpoint with nothing
	// scheduled to process it (a handover-bookkeeping bug). Fail loudly rather
	// than returning nil, which would silently drop those executions.
	return temporal.NewNonRetryableApplicationError(
		"parent has no live children but the namespace is not exhausted and is not draining for CAN (handover bookkeeping bug)",
		"ShardedParentStuck", nil)
}

// startChild starts a new shardedForceReplicationWorker child and records
// it in liveChildren / liveRunIDs. It blocks until the child's workflow
// execution starts (GetChildWorkflowExecution().Get) so its run ID is
// available for future signal addressing.
//
// handover=true means the child starts at half rate (Handover=true),
// awaiting a resumeFullRate signal from the parent before going full.
func (ps *shardedParentState) startChild(ctx workflow.Context, pageToken []byte, handover bool) error {
	childParams := shardedChildParams{
		Namespace:             ps.params.Namespace,
		Query:                 ps.params.Query,
		NamespaceID:           ps.namespaceID,
		TargetClusterName:     ps.params.TargetClusterName,
		TargetShardCount:      ps.targetShardCount,
		BatchSize:             ps.params.BatchSize,
		MaxExecsPerShard:      ps.params.MaxExecsPerShard,
		ListWorkflowsPageSize: ps.params.ListWorkflowsPageSize,
		ConcurrentBatchCount:  ps.params.ConcurrentBatchCount,
		DisableVerification:   ps.params.DisableVerification,
		ShardNoProgress:       ps.params.ShardNoProgress,
		IdleShardCost:         ps.params.IdleShardCost,
		PerBatchGenerateRPS:   ps.params.PerBatchGenerateRPS,
		StartPageToken:        pageToken,
		Handover:              handover,
	}

	childOpts := workflow.ChildWorkflowOptions{
		// TERMINATE so a parent failure (or parent CAN gone wrong) tears down
		// the still-running sibling rather than leaving it orphaned.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)
	fut := workflow.ExecuteChildWorkflow(childCtx, shardedForceReplicationWorker, childParams)

	// Block until the child workflow execution starts so its run ID is
	// available. This is a short, bounded wait (server scheduling latency).
	var childExec workflow.Execution
	if err := fut.GetChildWorkflowExecution().Get(ctx, &childExec); err != nil {
		return fmt.Errorf("start child worker: %w", err)
	}

	runID := childExec.RunID
	ps.liveChildren[runID] = fut
	ps.liveExecs[runID] = childExec
	ps.liveRunIDs = append(ps.liveRunIDs, runID)
	ps.liveCounts[runID] = 0
	return nil
}

// wireChild adds a child's future to the selector with a completion
// callback. The callback is invoked by sel.Select when the future resolves.
func (ps *shardedParentState) wireChild(sel workflow.Selector, ctx workflow.Context, runID string) {
	fut := ps.liveChildren[runID]
	sel.AddFuture(fut, func(f workflow.Future) {
		ps.onChildCompleted(ctx, runID, f)
	})
}

// handleCheckpoint processes a shardedCheckpointPayload from a child.
// It records the checkpoint token and starts a successor unless
// drainingForCAN or a successor was already started for this child.
// It also re-evaluates the CAN hint after each checkpoint.
func (ps *shardedParentState) handleCheckpoint(ctx workflow.Context, sel workflow.Selector, payload shardedCheckpointPayload) {
	ps.lastCheckpointToken = payload.NextPageToken

	// Check parent CAN hint. If tripped, record drainingForCAN and skip
	// starting a successor — the live children will drain to completion
	// and the parent will CAN with lastCheckpointToken.
	if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
		ps.drainingForCAN = true
	}

	if ps.drainingForCAN {
		return
	}

	if ps.successorStarted[payload.ChildRunID] {
		return
	}
	ps.successorStarted[payload.ChildRunID] = true

	// Start the successor at half rate (Handover=true). The
	// parent will promote it to full rate via resumeFullRate when the
	// current child (payload.ChildRunID) completes.
	if err := ps.startChild(ctx, payload.NextPageToken, true); err != nil {
		ps.startErr = fmt.Errorf("start successor child: %w", err)
		return
	}

	// Wire the new child into the selector. The child was just appended
	// to liveRunIDs; pick it up here immediately (without waiting for
	// the main loop's wiredCount sweep) so the selector responds to its
	// future completion in the same or next sel.Select call.
	newRunID := ps.liveRunIDs[len(ps.liveRunIDs)-1]
	ps.wireChild(sel, ctx, newRunID)
	ps.wiredCount = len(ps.liveRunIDs)
}

// handleProgress processes a shardedProgressPayload from a child.
// Updates liveCounts and refreshes the QPS estimate.
func (ps *shardedParentState) handleProgress(ctx workflow.Context, payload shardedProgressPayload) {
	if _, live := ps.liveCounts[payload.ChildRunID]; live {
		ps.liveCounts[payload.ChildRunID] = payload.VerifiedCount
	}
	ps.updateQPS(ctx)
}

// onChildCompleted handles the resolution of a child future. On success it
// retires the child's VerifiedCount into retiredTotal and promotes the
// successor (if one was started) to full rate. On error it records the
// wrapped error in childErr so the main loop can return it.
func (ps *shardedParentState) onChildCompleted(ctx workflow.Context, runID string, f workflow.Future) {
	// Remove the child from live tracking regardless of success/failure.
	delete(ps.liveChildren, runID)
	delete(ps.liveExecs, runID)
	delete(ps.liveCounts, runID)

	var result shardedChildResult
	if err := f.Get(ctx, &result); err != nil {
		if ps.childErr == nil {
			ps.childErr = fmt.Errorf("child worker %s: %w", runID, err)
		}
		return
	}

	ps.retiredTotal += result.VerifiedCount
	ps.updateQPS(ctx)

	// A child that exhausted the namespace is the chain's terminal child:
	// it started no checkpoint and needs no successor. Record it so run()
	// finishes instead of continuing-as-new.
	if result.ReachedEnd {
		ps.reachedEnd = true
	}

	// Promote the successor to full rate if one was started. The
	// successor's run ID is the last entry in liveRunIDs that is still
	// live (i.e., the one started from this child's checkpoint).
	// We use successorStarted to know whether a successor was launched
	// for this particular child; if so, find it by scanning backwards
	// through liveRunIDs for a still-live child that we haven't
	// previously promoted.
	if ps.successorStarted[runID] {
		// Find the successor: scan liveRunIDs for the first live child
		// after this one's position. Since handover is sequential (at
		// most one in flight), the successor is simply any remaining
		// live child.
		for _, candidateRunID := range ps.liveRunIDs {
			if candidateRunID == runID {
				continue
			}
			if _, live := ps.liveChildren[candidateRunID]; !live {
				continue
			}
			// Send resumeFullRate to the successor. Best-effort:
			// if the signal fails (e.g., successor already completed),
			// the child simply stays at half rate for its remaining work,
			// which is safe.
			successorExec := ps.liveExecs[candidateRunID]
			_ = workflow.SignalExternalWorkflow(ctx,
				successorExec.ID,
				successorExec.RunID,
				shardedResumeFullSignalName,
				struct{}{},
			).Get(ctx, nil)
			break
		}
	}
}

// updateQPS feeds the current aggregate verified count into the QPSQueue
// and refreshes the rate gauge. Called on each progress rollup and child
// completion so the rate tracks actual throughput.
func (ps *shardedParentState) updateQPS(ctx workflow.Context) {
	total := ps.retiredTotal + ps.liveTotalCount()
	ps.params.QPSQueue.Enqueue(ctx, total)
	ps.params.ReplicatedWorkflowCountPerSecond = ps.params.QPSQueue.CalculateQPS()
	ps.metricsHandler.Gauge(ForceReplicationRpsTagName).Update(ps.params.ReplicatedWorkflowCountPerSecond)
}
