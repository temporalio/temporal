package migration

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/metrics"
)

// ShardedForceReplicationWorkflow replicates consecutive listing ranges using one
// child at a time. A child returns its next page token only after all work in its
// range completes, so the status query always exposes a safe restart position.
// The namespace must include the destination cluster, and that configuration
// must finish propagating before replication tasks are injected.
func ShardedForceReplicationWorkflow(ctx workflow.Context, params ShardedForceReplicationParams) error {
	var liveCount int64

	err := workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		status := ForceReplicationStatus{
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
			ReplicatedWorkflowCount:            params.ReplicatedWorkflowCount + liveCount,
			ReplicatedWorkflowCountPerSecond:   params.ReplicatedWorkflowCountPerSecond,
			PageTokenForRestart:                params.NextPageToken,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
		}
		return status, nil
	})
	if err != nil {
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
	var md MetadataResponse
	if err := workflow.ExecuteLocalActivity(localCtx, shardedGetMetadataActivityName, MetadataRequest{Namespace: params.Namespace}).Get(ctx, &md); err != nil {
		return err
	}
	var targetMd DescribeTargetClusterResponse
	err = workflow.ExecuteLocalActivity(localCtx, shardedDescribeTargetClusterActivityName, DescribeTargetClusterRequest{
		TargetClusterName: params.TargetClusterName,
	}).Get(ctx, &targetMd)
	if err != nil {
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
		wfCount, err := countShardedWorkflowsForReplication(ctx, params.Namespace, params.Query)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	// Kick off the task-queue user data replication child (ABANDON policy).
	if !params.TaskQueueUserDataReplicationStatus.Done {
		err := maybeKickoffShardedTaskQueueUserDataReplication(ctx, &params, func(failureReason string) {
			params.TaskQueueUserDataReplicationStatus.FailureMessage = failureReason
			params.TaskQueueUserDataReplicationStatus.Done = true
		})
		if err != nil {
			return err
		}
	}

	if err := runShardedChildren(ctx, &params, md.NamespaceID, targetMd.ShardCount, &liveCount); err != nil {
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
	if params.MaxExecsPerShard != 0 && params.MaxExecsPerShard < minimumMaxExecsPerShard {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("MaxExecsPerShard (%d) must be at least %d", params.MaxExecsPerShard, minimumMaxExecsPerShard),
			"InvalidConfiguration", nil)
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

func countShardedWorkflowsForReplication(ctx workflow.Context, namespace, query string) (int64, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}
	var output countWorkflowResponse
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, ao),
		shardedCountWorkflowActivityName,
		&workflowservice.CountWorkflowExecutionsRequest{
			Namespace: namespace,
			Query:     query,
		},
	).Get(ctx, &output)
	if err != nil {
		return 0, err
	}
	return output.WorkflowCount, nil
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
		if !ch.Receive(ctx, &errStr) {
			return
		}
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
	child := workflow.ExecuteChildWorkflow(childCtx, shardedTaskQueueUserDataReplicationWorkflow, input)
	var childExecution workflow.Execution
	return child.GetChildWorkflowExecution().Get(ctx, &childExecution)
}

func shardedTaskQueueUserDataReplicationWorkflow(
	ctx workflow.Context,
	params TaskQueueUserDataReplicationParamsWithNamespace,
) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumInterval: 5 * time.Minute,
		},
	}
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, ao),
		shardedSeedReplicationQueueWithUserDataEntries,
		params,
	).Get(ctx, nil)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	return workflow.SignalExternalWorkflow(
		ctx,
		workflow.GetInfo(ctx).ParentWorkflowExecution.ID,
		"",
		taskQueueUserDataReplicationDoneSignalType,
		errStr,
	).Get(ctx, nil)
}

// runShardedChildren advances the restart position only after a child drains
// successfully. Progress signals affect counts, but cannot advance that position.
func runShardedChildren(
	ctx workflow.Context,
	params *ShardedForceReplicationParams,
	namespaceID string,
	targetShardCount int32,
	liveCount *int64,
) error {
	metricsHandler := workflow.GetMetricsHandler(ctx).WithTags(map[string]string{
		metrics.OperationTagName: metrics.MigrationWorkflowScope,
		NamespaceTagName:         params.Namespace,
	})
	progressCh := workflow.GetSignalChannel(ctx, shardedProgressSignalName)
	updateQPS := func() {
		params.QPSQueue.Enqueue(ctx, params.ReplicatedWorkflowCount+*liveCount)
		params.ReplicatedWorkflowCountPerSecond = params.QPSQueue.CalculateQPS()
		metricsHandler.Gauge(ForceReplicationRpsTagName).Update(params.ReplicatedWorkflowCountPerSecond)
	}
	for {
		childParams := shardedChildParams{
			Namespace:             params.Namespace,
			Query:                 params.Query,
			NamespaceID:           namespaceID,
			TargetClusterName:     params.TargetClusterName,
			TargetShardCount:      targetShardCount,
			BatchSize:             params.BatchSize,
			MaxExecsPerShard:      params.MaxExecsPerShard,
			ListWorkflowsPageSize: params.ListWorkflowsPageSize,
			ConcurrentBatchCount:  params.ConcurrentBatchCount,
			DisableVerification:   params.DisableVerification,
			ShardNoProgress:       params.ShardNoProgress,
			IdleShardCost:         params.IdleShardCost,
			PerBatchGenerateRPS:   params.PerBatchGenerateRPS,
			StartPageToken:        params.NextPageToken,
		}
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
		})
		child := workflow.ExecuteChildWorkflow(childCtx, shardedForceReplicationWorker, childParams)
		var execution workflow.Execution
		if err := child.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
			return err
		}

		var result shardedChildResult
		var childErr error
		done := false
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(child, func(f workflow.Future) {
			childErr = f.Get(ctx, &result)
			done = true
		})
		sel.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {
			childErr = ctx.Err()
			done = true
		})
		sel.AddReceive(progressCh, func(c workflow.ReceiveChannel, _ bool) {
			var progress shardedProgressPayload
			c.Receive(ctx, &progress)
			if progress.ChildRunID == execution.RunID {
				*liveCount = max(*liveCount, progress.VerifiedCount)
				updateQPS()
			}
		})
		for !done {
			sel.Select(ctx)
		}
		if childErr != nil {
			return childErr
		}
		params.ReplicatedWorkflowCount += result.VerifiedCount
		*liveCount = 0
		updateQPS()
		if len(result.NextPageToken) == 0 {
			return nil
		}
		params.NextPageToken = result.NextPageToken
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			next := *params
			next.ContinuedAsNewCount++
			return workflow.NewContinueAsNewError(ctx, ShardedForceReplicationWorkflow, next)
		}
	}
}
