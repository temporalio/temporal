package migration

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
)

type (
	TaskQueueUserDataReplicationParams struct {
		// PageSize for the SeedReplicationQueueWithUserDataEntries activity
		PageSize int
		// RPS limits the number of task queue user data entries pages requested per second.
		RPS float64
	}

	// TaskQueueUserDataReplicationParamsWithNamespace is used for child workflow / activity input
	TaskQueueUserDataReplicationParamsWithNamespace struct {
		TaskQueueUserDataReplicationParams
		// Namespace name
		Namespace string
	}

	ForceReplicationParams struct {
		Namespace               string `validate:"required"`
		Query                   string `validate:"required"` // query to list workflows for replication
		ConcurrentActivityCount int
		OverallRps              float64 // RPS for enqueuing of replication tasks
		GetParentInfoRPS        float64 // RPS for getting parent child info
		ListWorkflowsPageSize   int     // PageSize of ListWorkflow, will paginate through results.
		PageCountPerExecution   int     // number of pages to be processed before continue as new, max is 1000.
		NextPageToken           []byte  // used by continue as new

		// Used for verifying workflow executions were replicated successfully on target cluster.
		EnableVerification      bool
		TargetClusterEndpoint   string
		TargetClusterName       string
		VerifyIntervalInSeconds int `validate:"gte=0"`

		// Used by query handler to indicate overall progress of replication
		LastCloseTime                      time.Time
		LastStartTime                      time.Time
		ContinuedAsNewCount                int
		TaskQueueUserDataReplicationParams TaskQueueUserDataReplicationParams
		ReplicatedWorkflowCount            int64
		TotalForceReplicateWorkflowCount   int64
		ReplicatedWorkflowCountPerSecond   float64

		// Used to calculate QPS
		QPSQueue QPSQueue
		// Queue size is determined by Multiplier * Concurrency
		EstimationMultiplier int

		// Carry over the replication status after continue-as-new.
		TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus
	}

	QPSQueue struct {
		MaxSize int
		Data    []QPSData
	}

	QPSData struct {
		Count     int64
		Timestamp time.Time
	}

	ForceReplicationOutput struct {
	}

	TaskQueueUserDataReplicationStatus struct {
		Done           bool
		FailureMessage string
	}

	ForceReplicationStatus struct {
		LastCloseTime                      time.Time
		LastStartTime                      time.Time
		TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus
		ContinuedAsNewCount                int
		TotalWorkflowCount                 int64
		ReplicatedWorkflowCount            int64
		ReplicatedWorkflowCountPerSecond   float64
		PageTokenForRestart                []byte
	}
)

var (
	forceReplicationActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}

	NamespaceTagName           = "namespace"
	ForceReplicationRpsTagName = "force_replication_rps"
)

const (
	forceReplicationWorkflowName               = "force-replication"
	forceReplicationWorkflowV2Name             = "force-replication-v2"
	forceTaskQueueUserDataReplicationWorkflow  = "force-task-queue-user-data-replication"
	forceReplicationStatusQueryType            = "force-replication-status"
	taskQueueUserDataReplicationDoneSignalType = "task-queue-user-data-replication-done"
	taskQueueUserDataReplicationVersionMarker  = "replicate-task-queue-user-data"

	defaultListWorkflowsPageSize                   = 1000
	defaultPageCountPerExecution                   = 200
	maxPageCountPerExecution                       = 1000
	defaultPageSizeForTaskQueueUserDataReplication = 20
	defaultRPSForTaskQueueUserDataReplication      = 1.0
	defaultVerifyIntervalInSeconds                 = 5
)

func ForceReplicationWorkflow(ctx workflow.Context, params ForceReplicationParams) error {
	// For now, we'll return the initial page token for simplicity.
	// If we want this to be more precise, we could track processed pages.
	startPageToken := params.NextPageToken

	_ = workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		return ForceReplicationStatus{
			LastCloseTime:                      params.LastCloseTime,
			LastStartTime:                      params.LastStartTime,
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
			TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
			ReplicatedWorkflowCount:            params.ReplicatedWorkflowCount,
			ReplicatedWorkflowCountPerSecond:   params.ReplicatedWorkflowCountPerSecond,
			PageTokenForRestart:                startPageToken,
		}, nil
	})

	if err := validateAndSetForceReplicationParams(ctx, &params); err != nil {
		return err
	}

	if params.TotalForceReplicateWorkflowCount == 0 {
		wfCount, err := countWorkflowForReplication(ctx, params)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	metadataResp, err := getClusterMetadata(ctx, params)
	if err != nil {
		return err
	}

	if !params.TaskQueueUserDataReplicationStatus.Done {
		err = maybeKickoffTaskQueueUserDataReplication(ctx, params, func(failureReason string) {
			params.TaskQueueUserDataReplicationStatus.FailureMessage = failureReason
			params.TaskQueueUserDataReplicationStatus.Done = true
		})
		if err != nil {
			return err
		}
	}

	executionsCh := workflow.NewBufferedChannel(ctx, params.PageCountPerExecution)
	var listExecutions error
	workflow.Go(ctx, func(ctx workflow.Context) {
		listExecutions = listExecutionsForReplication(ctx, executionsCh, &params)

		// enqueueReplicationTasks only returns when workflowExecutionsCh is closed (or if it encounters an error).
		// Therefore, listWorkflowsErr will be set prior to their use and params will be updated.
		executionsCh.Close()
	})

	if err := enqueueReplicationTasks(ctx, executionsCh, metadataResp.NamespaceID, &params); err != nil {
		return err
	}

	if listExecutions != nil {
		return listExecutions
	}

	if params.NextPageToken == nil {
		if workflow.GetVersion(ctx, taskQueueUserDataReplicationVersionMarker, workflow.DefaultVersion, 1) > workflow.DefaultVersion {
			err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done })
			if err != nil {
				return err
			}
			if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
				return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
			}
		}
		return nil
	}

	params.ContinuedAsNewCount++

	// There are still more workflows to replicate. Continue-as-new to process on a new run.
	// This prevents history size from exceeding the server-defined limit
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflow, params)
}

func ForceReplicationWorkflowV2(ctx workflow.Context, params ForceReplicationParams) error {
	// For now, we'll return the initial page token for simplicity.
	// If we want this to be more precise, we could track processed pages.
	startPageToken := params.NextPageToken

	_ = workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		return ForceReplicationStatus{
			LastCloseTime:                      params.LastCloseTime,
			LastStartTime:                      params.LastStartTime,
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
			TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
			ReplicatedWorkflowCount:            params.ReplicatedWorkflowCount,
			ReplicatedWorkflowCountPerSecond:   params.ReplicatedWorkflowCountPerSecond,
			PageTokenForRestart:                startPageToken,
		}, nil
	})

	if err := validateAndSetForceReplicationParams(ctx, &params); err != nil {
		return err
	}

	if params.TotalForceReplicateWorkflowCount == 0 {
		wfCount, err := countWorkflowForReplication(ctx, params)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	metadataResp, err := getClusterMetadata(ctx, params)
	if err != nil {
		return err
	}

	if !params.TaskQueueUserDataReplicationStatus.Done {
		err = maybeKickoffTaskQueueUserDataReplication(ctx, params, func(failureReason string) {
			params.TaskQueueUserDataReplicationStatus.FailureMessage = failureReason
			params.TaskQueueUserDataReplicationStatus.Done = true
		})
		if err != nil {
			return err
		}
	}

	workflowExecutionsCh := workflow.NewBufferedChannel(ctx, params.PageCountPerExecution)
	var listWorkflowsErr error
	workflow.Go(ctx, func(ctx workflow.Context) {
		listWorkflowsErr = listExecutionsForReplication(ctx, workflowExecutionsCh, &params)

		// enqueueReplicationTasks only returns when workflowExecutionsCh is closed (or if it encounters an error).
		// Therefore, listWorkflowsErr will be set prior to their use and params will be updated.
		workflowExecutionsCh.Close()
	})

	if err := enqueueReplicationTasksLocal(ctx, workflowExecutionsCh, metadataResp.NamespaceID, &params); err != nil {
		return err
	}

	if listWorkflowsErr != nil {
		return listWorkflowsErr
	}

	if params.NextPageToken == nil {
		err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done })
		if err != nil {
			return err
		}
		if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
			return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
		}
		return nil
	}

	params.ContinuedAsNewCount++
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflowV2, params)
}

func maybeKickoffTaskQueueUserDataReplication(ctx workflow.Context, params ForceReplicationParams, onDone func(failureReason string)) error {
	if workflow.GetVersion(ctx, taskQueueUserDataReplicationVersionMarker, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		return nil
	}

	workflow.Go(ctx, func(ctx workflow.Context) {
		taskQueueUserDataReplicationDoneCh := workflow.GetSignalChannel(ctx, taskQueueUserDataReplicationDoneSignalType)
		var errStr string
		// We don't care if there's more data to receive
		_ = taskQueueUserDataReplicationDoneCh.Receive(ctx, &errStr)
		onDone(errStr)
	})

	// We only start the child workflow before we continue as new to avoid starting the child workflow more than once.
	if params.ContinuedAsNewCount > 0 {
		return nil
	}

	options := workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-task-queue-user-data-replicator", workflow.GetInfo(ctx).WorkflowExecution.ID),
		// We're going to continue-as-new, and cannot wait for this child to complete, instead child will notify of
		// its completion via signal.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
	childCtx := workflow.WithChildOptions(ctx, options)
	input := TaskQueueUserDataReplicationParamsWithNamespace{
		TaskQueueUserDataReplicationParams: params.TaskQueueUserDataReplicationParams,
		Namespace:                          params.Namespace,
	}

	child := workflow.ExecuteChildWorkflow(childCtx, ForceTaskQueueUserDataReplicationWorkflow, input)
	var childExecution workflow.Execution
	// Wait for the child workflow to be started.
	err := child.GetChildWorkflowExecution().Get(ctx, &childExecution)
	return err
}

func ForceTaskQueueUserDataReplicationWorkflow(ctx workflow.Context, params TaskQueueUserDataReplicationParamsWithNamespace) error {
	ao := workflow.ActivityOptions{
		// This shouldn't take "too long", just set an arbitrary long timeout here and rely on heartbeats for liveness detection.
		StartToCloseTimeout: time.Hour * 24 * 7,
		HeartbeatTimeout:    time.Second * 30,
		// Give the system some time to recover before the next attempt.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second * 10,
			MaximumInterval: time.Minute * 5,
		},
	}

	actx := workflow.WithActivityOptions(ctx, ao)
	var a *activities
	err := workflow.ExecuteActivity(actx, a.SeedReplicationQueueWithUserDataEntries, params).Get(ctx, nil)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	err = workflow.SignalExternalWorkflow(ctx, workflow.GetInfo(ctx).ParentWorkflowExecution.ID, "", taskQueueUserDataReplicationDoneSignalType, errStr).Get(ctx, nil)
	return err
}

func validateAndSetForceReplicationParams(ctx workflow.Context, params *ForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: Namespace is required", "InvalidArgument", nil)
	}

	if params.EnableVerification && len(params.TargetClusterEndpoint) == 0 && len(params.TargetClusterName) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: TargetClusterEndpoint or TargetClusterName is required with verification enabled", "InvalidArgument", nil)
	}

	if params.ConcurrentActivityCount <= 0 {
		params.ConcurrentActivityCount = 1
	}

	if params.OverallRps <= 0 {
		params.OverallRps = float64(params.ConcurrentActivityCount)
	}
	if params.GetParentInfoRPS <= 0 {
		params.GetParentInfoRPS = float64(params.ConcurrentActivityCount)
	}

	if params.ListWorkflowsPageSize <= 0 {
		params.ListWorkflowsPageSize = defaultListWorkflowsPageSize
	}

	if params.PageCountPerExecution <= 0 {
		params.PageCountPerExecution = defaultPageCountPerExecution
	}

	if params.PageCountPerExecution > maxPageCountPerExecution {
		params.PageCountPerExecution = maxPageCountPerExecution
	}

	if params.VerifyIntervalInSeconds <= 0 {
		params.VerifyIntervalInSeconds = defaultVerifyIntervalInSeconds
	}

	if params.ReplicatedWorkflowCountPerSecond <= 0 {
		params.ReplicatedWorkflowCountPerSecond = params.OverallRps
	}

	if params.EstimationMultiplier <= 0 {
		params.EstimationMultiplier = 2
	}

	if params.QPSQueue.Data == nil {
		params.QPSQueue = NewQPSQueue(params.ConcurrentActivityCount, params.EstimationMultiplier)
		params.QPSQueue.Enqueue(ctx, params.ReplicatedWorkflowCount)
	}

	return nil
}

func getClusterMetadata(ctx workflow.Context, params ForceReplicationParams) (metadataResponse, error) {
	// Get cluster metadata, we need namespace ID for history API call.
	// TODO: remove this step.
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithLocalActivityOptions(ctx, lao)
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	var a *activities
	err := workflow.ExecuteLocalActivity(actx, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	return metadataResp, err
}

func listExecutionsForReplication(ctx workflow.Context, executionsCh workflow.Channel, params *ForceReplicationParams) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithActivityOptions(ctx, ao)
	var a *activities
	for i := 0; i < params.PageCountPerExecution; i++ {
		listFuture := workflow.ExecuteActivity(
			actx,
			a.ListWorkflows,
			&workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     params.Namespace,
				PageSize:      int32(params.ListWorkflowsPageSize),
				NextPageToken: params.NextPageToken,
				Query:         params.Query,
			})

		var listResp listWorkflowsResponse
		if err := listFuture.Get(ctx, &listResp); err != nil {
			return err
		}

		executionsCh.Send(ctx, listResp.Executions)

		params.NextPageToken = listResp.NextPageToken
		params.LastCloseTime = listResp.LastCloseTime
		params.LastStartTime = listResp.LastStartTime

		if params.NextPageToken == nil {
			break
		}
	}

	return nil
}

func countWorkflowForReplication(ctx workflow.Context, params ForceReplicationParams) (int64, error) {
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

func enqueueReplicationTasks(ctx workflow.Context, executionsCh workflow.Channel, namespaceID string, params *ForceReplicationParams) error {
	selector := workflow.NewSelector(ctx)
	pendingGenerateTasks := 0
	pendingVerifyTasks := 0

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 60,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithActivityOptions(ctx, ao)
	var migrationExecutions []*replicationspb.MigrationExecutionInfo
	var lastActivityErr error
	var a *activities

	var targetClusters []string
	if params.TargetClusterName != "" {
		targetClusters = []string{params.TargetClusterName}
	}

	for executionsCh.Receive(ctx, &migrationExecutions) {
		generateTaskFuture := workflow.ExecuteActivity(
			actx,
			a.GenerateReplicationTasks,
			&generateReplicationTasksRequest{
				NamespaceID:      namespaceID,
				Executions:       migrationExecutions,
				RPS:              params.OverallRps / float64(params.ConcurrentActivityCount),
				GetParentInfoRPS: params.GetParentInfoRPS / float64(params.ConcurrentActivityCount),
				TargetClusters:   targetClusters,
			})

		pendingGenerateTasks++
		selector.AddFuture(generateTaskFuture, func(f workflow.Future) {
			pendingGenerateTasks--

			if err := f.Get(ctx, nil); err != nil {
				lastActivityErr = err
			}
		})

		if params.EnableVerification {
			verifyTaskFuture := workflow.ExecuteActivity(
				actx,
				a.VerifyReplicationTasks,
				&verifyReplicationTasksRequest{
					TargetClusterEndpoint: params.TargetClusterEndpoint,
					TargetClusterName:     params.TargetClusterName,
					Namespace:             params.Namespace,
					NamespaceID:           namespaceID,
					Executions:            migrationExecutions,
					VerifyInterval:        time.Duration(params.VerifyIntervalInSeconds) * time.Second,
				})

			pendingVerifyTasks++
			selector.AddFuture(verifyTaskFuture, func(f workflow.Future) {
				pendingVerifyTasks--

				var verifyTaskResponse verifyReplicationTasksResponse
				if err := f.Get(ctx, &verifyTaskResponse); err != nil {
					lastActivityErr = err
				} else {
					// Update replication status
					params.ReplicatedWorkflowCount += int64(verifyTaskResponse.VerifiedWorkflowCount)
					params.QPSQueue.Enqueue(ctx, params.ReplicatedWorkflowCount)
					params.ReplicatedWorkflowCountPerSecond = params.QPSQueue.CalculateQPS()

					// Report new QPS to metrics
					tags := map[string]string{
						metrics.OperationTagName: metrics.MigrationWorkflowScope,
						NamespaceTagName:         params.Namespace,
					}
					workflow.GetMetricsHandler(ctx).WithTags(tags).Gauge(ForceReplicationRpsTagName).Update(params.ReplicatedWorkflowCountPerSecond)
				}
			})
		}

		for pendingGenerateTasks >= params.ConcurrentActivityCount || pendingVerifyTasks >= params.ConcurrentActivityCount {
			selector.Select(ctx) // this will block until one of the in-flight activities completes
			if lastActivityErr != nil {
				return lastActivityErr
			}
		}
	}

	for pendingGenerateTasks > 0 || pendingVerifyTasks > 0 {
		selector.Select(ctx)
		if lastActivityErr != nil {
			return lastActivityErr
		}
	}

	return nil
}

func enqueueReplicationTasksLocal(
	ctx workflow.Context,
	executionsCh workflow.Channel,
	namespaceID string,
	params *ForceReplicationParams,
) error {
	selector := workflow.NewSelector(ctx)
	pendingGenerateTasks := 0
	pendingVerifyTasks := 0

	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Hour,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	lactx := workflow.WithLocalActivityOptions(ctx, lao)
	var migrationExecutions []*replicationspb.MigrationExecutionInfo
	var lastActivityErr error
	var a *activities

	var targetClusters []string
	if params.TargetClusterName != "" {
		targetClusters = []string{params.TargetClusterName}
	}

	for executionsCh.Receive(ctx, &migrationExecutions) {
		executions := migrationExecutions

		verifyTaskDone := func(f workflow.Future) {
			var verifyTaskResponse verifyReplicationTasksResponse
			if err := f.Get(ctx, &verifyTaskResponse); err != nil {
				lastActivityErr = err
			} else {
				// Update replication status
				params.ReplicatedWorkflowCount += int64(verifyTaskResponse.VerifiedWorkflowCount)
				params.QPSQueue.Enqueue(ctx, params.ReplicatedWorkflowCount)
				params.ReplicatedWorkflowCountPerSecond = params.QPSQueue.CalculateQPS()

				// Report new QPS to metrics
				tags := map[string]string{
					metrics.OperationTagName: metrics.MigrationWorkflowScope,
					NamespaceTagName:         params.Namespace,
				}
				workflow.GetMetricsHandler(ctx).WithTags(tags).Gauge(ForceReplicationRpsTagName).Update(params.ReplicatedWorkflowCountPerSecond)
			}

			pendingVerifyTasks--
		}

		verifyTask := func() {
			verifyTaskFuture := workflow.ExecuteLocalActivity(
				lactx,
				a.VerifyReplicationTasks,
				&verifyReplicationTasksRequest{
					TargetClusterEndpoint: params.TargetClusterEndpoint,
					TargetClusterName:     params.TargetClusterName,
					Namespace:             params.Namespace,
					NamespaceID:           namespaceID,
					Executions:            executions,
					VerifyInterval:        time.Duration(params.VerifyIntervalInSeconds) * time.Second,
				})

			pendingVerifyTasks++
			selector.AddFuture(verifyTaskFuture, verifyTaskDone)
		}

		generateTaskFuture := workflow.ExecuteLocalActivity(
			lactx,
			a.GenerateReplicationTasks,
			&generateReplicationTasksRequest{
				NamespaceID:      namespaceID,
				Executions:       executions,
				RPS:              params.OverallRps / float64(params.ConcurrentActivityCount),
				GetParentInfoRPS: params.GetParentInfoRPS / float64(params.ConcurrentActivityCount),
				TargetClusters:   targetClusters,
			})

		pendingGenerateTasks++
		selector.AddFuture(generateTaskFuture, func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil {
				lastActivityErr = err
			}

			if params.EnableVerification {
				verifyTask()
			}
			pendingGenerateTasks--
		})

		for pendingGenerateTasks >= params.ConcurrentActivityCount || pendingVerifyTasks >= params.ConcurrentActivityCount {
			selector.Select(ctx) // this will block until one of the in-flight activities completes
			if lastActivityErr != nil {
				return lastActivityErr
			}
		}
	}

	for pendingGenerateTasks > 0 || pendingVerifyTasks > 0 {
		selector.Select(ctx)
		if lastActivityErr != nil {
			return lastActivityErr
		}
	}

	return nil
}

// NewQPSQueue initializes a QPSQueue to collect data points for each workflow execution.
// The queue size is set to concurrency + 1 to account for up to 'concurrency' activities
// running simultaneously and the initial starting point.
func NewQPSQueue(concurrentActivityCount int, estimationMultiplier int) QPSQueue {
	return QPSQueue{
		Data:    make([]QPSData, 0, max(0, estimationMultiplier*concurrentActivityCount+1)),
		MaxSize: concurrentActivityCount + 1,
	}
}

func (q *QPSQueue) Enqueue(ctx workflow.Context, count int64) {
	data := QPSData{Count: count, Timestamp: workflow.Now(ctx)}

	// If queue length reaches max capacity, remove the oldest item
	if len(q.Data) >= q.MaxSize {
		q.Data = q.Data[1:]
	}

	q.Data = append(q.Data, data)
}

func (q *QPSQueue) CalculateQPS() float64 {
	// Check if the queue has at least two items
	if len(q.Data) < 2 {
		return 0.0
	}

	first := q.Data[0]
	last := q.Data[len(q.Data)-1]

	// Calculate the count difference and time difference
	countDiff := last.Count - first.Count
	timeDiff := last.Timestamp.Sub(first.Timestamp).Seconds()

	// If count difference is <= 0 or time difference is <= 0, return a rate of 0
	if countDiff <= 0 || timeDiff <= 0 {
		return 0.0
	}

	// Calculate the QPS
	qps := float64(countDiff) / timeDiff
	return qps
}
