package migration

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	namespaceHandoverWorkflowName   = "namespace-handover"
	namespaceHandoverWorkflowV2Name = "namespace-handover-v2"

	minimumAllowedLaggingSeconds  = 5
	maximumAllowedLaggingSeconds  = 120
	maximumHandoverTimeoutSeconds = 30
)

type (
	NamespaceHandoverParams struct {
		Namespace     string
		RemoteCluster string

		// how far behind on replication is allowed for remote cluster before handover is initiated
		AllowedLaggingSeconds int
		AllowedLaggingTasks   int64

		// how long to wait for handover to complete before rollback
		HandoverTimeoutSeconds int
	}

	replicationStatus struct {
		MaxReplicationTaskIds map[int32]int64 // max replication task id for each shard.
	}

	waitReplicationRequest struct {
		ShardCount          int32
		RemoteCluster       string          // remote cluster name
		WaitForTaskIds      map[int32]int64 // remote acked replication task needs to pass this id
		AllowedLagging      time.Duration   // allowed remote acked lagging duration
		AllowedLaggingTasks int64           // allowed remote acked task lagging
	}

	updateStateRequest struct {
		Namespace string // move this namespace into Handover state
		NewState  enumspb.ReplicationState
	}

	updateActiveClusterRequest struct {
		Namespace     string // move this namespace into Handover state
		ActiveCluster string
	}

	waitHandoverRequest struct {
		ShardCount    int32
		Namespace     string
		RemoteCluster string // remote cluster name
	}
)

func NamespaceHandoverWorkflowV2(ctx workflow.Context, params NamespaceHandoverParams) (retErr error) {
	workflowInfo := workflow.GetInfo(ctx)
	if workflowInfo.WorkflowRunTimeout > 0 {
		return temporal.NewNonRetryableApplicationError(
			"Workflow run timeout should not be set for handover workflow",
			"InvalidTimeout",
			nil,
		)
	}
	return NamespaceHandoverWorkflow(ctx, params)
}

func NamespaceHandoverWorkflow(ctx workflow.Context, params NamespaceHandoverParams) (retErr error) {
	if err := validateAndSetNamespaceHandoverParams(&params); err != nil {
		return err
	}
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		MaximumInterval:    time.Second,
		BackoffCoefficient: 1,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// ** Step 1: Get Cluster Metadata **
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	var a *activities
	err := workflow.ExecuteActivity(ctx, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	if err != nil {
		return err
	}

	// ** Step 2: Get current replication status **
	var repStatus replicationStatus
	err = workflow.ExecuteActivity(ctx, a.GetMaxReplicationTaskIDs).Get(ctx, &repStatus)
	if err != nil {
		return err
	}

	// ** Step 3: Wait for Remote Cluster to catch-up on Replication Tasks
	ao2 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx2 := workflow.WithActivityOptions(ctx, ao2)
	waitRequest := waitReplicationRequest{
		ShardCount:          metadataResp.ShardCount,
		RemoteCluster:       params.RemoteCluster,
		AllowedLagging:      time.Duration(params.AllowedLaggingSeconds) * time.Second,
		WaitForTaskIds:      repStatus.MaxReplicationTaskIds,
		AllowedLaggingTasks: params.AllowedLaggingTasks,
	}
	err = workflow.ExecuteActivity(ctx2, a.WaitReplication, waitRequest).Get(ctx2, nil)
	if err != nil {
		return err
	}

	// ** Step 4: RecoverOrInitialize Handover (WARNING: Namespace cannot serve traffic while in this state)
	handoverRequest := updateStateRequest{
		Namespace: params.Namespace,
		NewState:  enumspb.REPLICATION_STATE_HANDOVER,
	}
	err = workflow.ExecuteActivity(ctx, a.UpdateNamespaceState, handoverRequest).Get(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		// ** Final Step: Reset namespace state from Handover -> Registered. This helps ensure that whether
		//                handover failed or succeeded, the namespace (for whichever cluster it is Active on)
		//                is able to process traffic again.
		resetStateRequest := updateStateRequest{
			Namespace: params.Namespace,
			NewState:  enumspb.REPLICATION_STATE_NORMAL,
		}
		var err error
		if workflow.GetVersion(ctx, "detach-handover-ctx-20250829", workflow.DefaultVersion, 1) > workflow.DefaultVersion {
			infiniteRetryOption := workflow.ActivityOptions{StartToCloseTimeout: time.Second * 10}
			detachCtx, cancel := workflow.NewDisconnectedContext(ctx)
			resetStateCtx := workflow.WithActivityOptions(detachCtx, infiniteRetryOption)
			err = workflow.ExecuteActivity(resetStateCtx, a.UpdateNamespaceState, resetStateRequest).Get(resetStateCtx, nil)
			cancel()
		} else {
			err = workflow.ExecuteActivity(ctx, a.UpdateNamespaceState, resetStateRequest).Get(ctx, nil)
		}
		if err != nil {
			retErr = err
			return
		}
	}()

	// ** Step 5: Wait for Remote Cluster to completely drain its Replication Tasks
	ao3 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * time.Duration(params.HandoverTimeoutSeconds),
		HeartbeatTimeout:    time.Second * 10,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx3 := workflow.WithActivityOptions(ctx, ao3)
	waitHandover := waitHandoverRequest{
		ShardCount:    metadataResp.ShardCount,
		Namespace:     params.Namespace,
		RemoteCluster: params.RemoteCluster,
	}
	err = workflow.ExecuteActivity(ctx3, a.WaitHandover, waitHandover).Get(ctx3, nil)
	if err != nil {
		return err
	}

	// ** Step 6: Remote Cluster is caught up. Update Namespace to be Active on the Remote Cluster.
	updateRequest := updateActiveClusterRequest{
		Namespace:     params.Namespace,
		ActiveCluster: params.RemoteCluster,
	}
	err = workflow.ExecuteActivity(ctx, a.UpdateActiveCluster, updateRequest).Get(ctx, nil)
	if err != nil {
		return err
	}

	return err
}

func validateAndSetNamespaceHandoverParams(params *NamespaceHandoverParams) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: Namespace is required", "InvalidArgument", nil)
	}
	if len(params.RemoteCluster) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: RemoteCluster is required", "InvalidArgument", nil)
	}
	if params.AllowedLaggingSeconds <= minimumAllowedLaggingSeconds {
		params.AllowedLaggingSeconds = minimumAllowedLaggingSeconds
	}
	if params.AllowedLaggingSeconds >= maximumAllowedLaggingSeconds {
		params.AllowedLaggingSeconds = maximumAllowedLaggingSeconds
	}
	if params.HandoverTimeoutSeconds >= maximumHandoverTimeoutSeconds {
		params.HandoverTimeoutSeconds = maximumHandoverTimeoutSeconds
	}

	return nil
}
