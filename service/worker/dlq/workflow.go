// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package dlq contains the workflow for deleting and re-enqueueing DLQ tasks. Both of these operations are performed by
// the same workflow to avoid concurrent deletion and re-enqueueing of the same task.
package dlq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	// WorkflowParams is the single argument to the DLQ workflow.
	WorkflowParams struct {
		// WorkflowType options are available via the WorkflowType* constants.
		WorkflowType string
		// DeleteParams is only used for WorkflowTypeDelete.
		DeleteParams DeleteParams
		// MergeParams is only used for WorkflowTypeMerge.
		MergeParams MergeParams
	}

	// Key uniquely identifies a DLQ.
	Key struct {
		// TaskCategoryID is the id used by [go.temporal.io/server/service/history/tasks.TaskCategoryRegistry].
		TaskCategoryID int
		// SourceCluster is the cluster that the replication tasks are coming from if the task category is replication.
		// Otherwise, it is equal to TargetCluster, which is the cluster that both the DLQ workflow is running in, and
		// the cluster that contains the DLQ itself.
		SourceCluster string
		// TargetCluster is always the cluster that the DLQ workflow is running in currently. However, that may change
		// if we add cross-cluster tasks in the future.
		TargetCluster string
	}

	// DeleteParams contain the target DLQ and the max message ID to delete up to.
	DeleteParams struct {
		Key
		// MaxMessageID is inclusive.
		MaxMessageID int64
	}

	// MergeParams contain the target DLQ and the max message ID to merge up to.
	MergeParams struct {
		Key
		// MaxMessageID is inclusive.
		MaxMessageID int64
		// BatchSize controls the number of tasks to both read and re-enqueue at a time.
		// The maximum is MaxMergeBatchSize. The default is DefaultMergeBatchSize.
		BatchSize int
	}
	// ProgressQueryResponse is the response to progress query.
	ProgressQueryResponse struct {
		MaxMessageIDToProcess     int64
		LastProcessedMessageID    int64
		NumberOfMessagesProcessed int64
		WorkflowType              string
		DlqKey                    Key
	}

	// HistoryClient contains the subset of methods from [historyservice.HistoryServiceClient] that we need, to make it
	// easier to implement in tests.
	HistoryClient interface {
		DeleteDLQTasks(
			ctx context.Context,
			in *historyservice.DeleteDLQTasksRequest,
			opts ...grpc.CallOption,
		) (*historyservice.DeleteDLQTasksResponse, error)
		GetDLQTasks(
			ctx context.Context,
			in *historyservice.GetDLQTasksRequest,
			opts ...grpc.CallOption,
		) (*historyservice.GetDLQTasksResponse, error)
	}

	// TaskClient contains the subset of methods from [adminservice.AdminServiceClient] that we need, to make it easier
	// to implement in tests.
	TaskClient interface {
		AddTasks(
			ctx context.Context,
			in *adminservice.AddTasksRequest,
		) (*adminservice.AddTasksResponse, error)
	}

	// AddTasksFn provides a convenient method for implementing the [TaskClient] interface.
	AddTasksFn func(
		ctx context.Context,
		req *adminservice.AddTasksRequest,
	) (*adminservice.AddTasksResponse, error)

	// TaskClientDialer is a function that returns a [TaskClient] given a cluster name.
	TaskClientDialer interface {
		// Dial returns a [TaskClient] given a cluster name. You don't need to close this client. Note that some
		// implementations will cache clients.
		Dial(ctx context.Context, cluster string) (TaskClient, error)
	}

	// TaskClientDialerFn is a function that returns a [TaskClient] given an address.
	TaskClientDialerFn func(ctx context.Context, address string) (TaskClient, error)

	// CurrentClusterName is its own type just to make fx injection easier. It's similar to the same type that exists
	// in the persistence package, but I thought that re-using that would look weird here because it has nothing to do
	// with persistence.
	CurrentClusterName string

	workerComponentParams struct {
		fx.In
		HistoryClient      HistoryClient
		CurrentClusterName CurrentClusterName
		TaskClientDialer   TaskClientDialer
	}

	workerComponent struct {
		historyClient      HistoryClient
		taskClientDialer   TaskClientDialer
		currentClusterName string
	}
)

const (
	// WorkflowName is the name of the DLQ workflow.
	WorkflowName = "temporal-sys-dlq-workflow"
	// WorkflowTypeDelete is what the value of WorkflowParams.WorkflowType should be to delete DLQ tasks. When this is
	// specified, the workflow will simply delete all tasks up to the specified max message ID.
	WorkflowTypeDelete = "delete"
	// WorkflowTypeMerge is for re-enqueuing DLQ tasks. When this is specified, the workflow will operate in batches.
	// For each batch, it will read up to MergeParams.BatchSize tasks from the DLQ, re-enqueue them, and then delete
	// them from the DLQ. It will repeat this process until it reaches the specified max message ID.
	WorkflowTypeMerge = "merge"
	// MaxMergeBatchSize is the maximum value for MergeParams.BatchSize.
	MaxMergeBatchSize = 1000
	// DefaultMergeBatchSize is the default value for MergeParams.BatchSize.
	DefaultMergeBatchSize = 100
	// QueryTypeProgress is the query to get the progress of the DLQ workflow.
	QueryTypeProgress = "dlq-job-progress-query"

	errorTypeInvalidWorkflowType = "dlq-error-type-invalid-workflow-type"
	errorTypeInvalidRequest      = "dlq-error-type-invalid-request"
	deleteTasksActivityName      = "dlq-delete-tasks-activity"
	readTasksActivityName        = "dlq-read-tasks-activity"
	reEnqueueTasksActivityName   = "dlq-re-enqueue-tasks-activity"

	// deleteTasksActivityTimeout is long because all tasks are deleted in a single go. This only applies when using the
	// purge workflow, not when the delete activity is used in the merge workflow.
	deleteTasksActivityTimeout = 5 * time.Minute * debug.TimeoutMultiplier
	// mergeTasksActivityTimeout controls the timeout of all activities used in the merge workflow. It is relatively
	// short because we're only processing a single batch of tasks at a time.
	mergeTasksActivityTimeout = 15 * time.Second * debug.TimeoutMultiplier
)

var (
	// Module provides a [workercommon.WorkerComponent] annotated with [workercommon.WorkerComponentTag] to the graph,
	// given a [HistoryClient], a [TaskClientDialer], and a value for [CurrentClusterName].
	Module = workercommon.AnnotateWorkerComponentProvider(newComponent)

	ErrNegativeBatchSize      = errors.New("BatchSize must be positive or 0 to use the default")
	ErrMergeBatchSizeTooLarge = errors.New("BatchSize too large")

	// deleteActivityRetryPolicy is the retry policy for the delete activity. Currently, delete processes all messages
	// in one batch, so this could be expensive. As a result, we want to increase the backoff quickly to not peg the
	// system.
	deleteActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2.0,
		MaximumAttempts:    10,
	}
	// mergeActivityRetryPolicy is the retry policy for the merge activities. Currently, merge processes one batch of
	// messages at a time, and each batch has a capped size determined by MaxMergeBatchSize, so this is relatively
	// cheap. As a result, we want to increase the backoff slowly because it's unlikely that this will hurt the system.
	mergeActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 1.2,
		MaximumAttempts:    10,
	}
)

func newComponent(params workerComponentParams) workercommon.WorkerComponent {
	return &workerComponent{
		historyClient:      params.HistoryClient,
		currentClusterName: string(params.CurrentClusterName),
		taskClientDialer:   params.TaskClientDialer,
	}
}

//revive:disable:import-shadowing this doesn't actually shadow imports because it's a method, not a function
func (c *workerComponent) workflow(ctx workflow.Context, params WorkflowParams) error {
	queryResponse := ProgressQueryResponse{}
	queryResponse.WorkflowType = params.WorkflowType
	err := workflow.SetQueryHandler(ctx, QueryTypeProgress, func() (ProgressQueryResponse, error) {
		return queryResponse, nil
	})
	if err != nil {
		return err
	}
	if params.WorkflowType == WorkflowTypeDelete {
		queryResponse.MaxMessageIDToProcess = params.DeleteParams.MaxMessageID
		queryResponse.DlqKey = Key{
			TaskCategoryID: params.DeleteParams.TaskCategoryID,
			SourceCluster:  params.DeleteParams.SourceCluster,
			TargetCluster:  params.DeleteParams.TargetCluster,
		}
		var response historyservice.DeleteDLQTasksResponse
		err = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:           primitives.DLQActivityTQ,
				RetryPolicy:         deleteActivityRetryPolicy,
				StartToCloseTimeout: deleteTasksActivityTimeout,
			}),
			deleteTasksActivityName,
			params.DeleteParams,
		).Get(ctx, &response)
		if err != nil {
			return err
		}
		queryResponse.LastProcessedMessageID = params.DeleteParams.MaxMessageID
		queryResponse.NumberOfMessagesProcessed = response.MessagesDeleted
		return nil
	} else if params.WorkflowType == WorkflowTypeMerge {
		queryResponse.MaxMessageIDToProcess = params.MergeParams.MaxMessageID
		queryResponse.DlqKey = Key{
			TaskCategoryID: params.MergeParams.TaskCategoryID,
			SourceCluster:  params.MergeParams.SourceCluster,
			TargetCluster:  params.MergeParams.TargetCluster,
		}
		return c.mergeTasks(ctx, params.MergeParams, &queryResponse.LastProcessedMessageID, &queryResponse.NumberOfMessagesProcessed)
	}

	return temporal.NewNonRetryableApplicationError(params.WorkflowType, errorTypeInvalidWorkflowType, nil)
}

func (c *workerComponent) deleteTasks(ctx context.Context, params DeleteParams) (*historyservice.DeleteDLQTasksResponse, error) {
	req := &historyservice.DeleteDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  int32(params.TaskCategoryID),
			SourceCluster: params.SourceCluster,
			TargetCluster: params.TargetCluster,
		},
		InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
			MessageId: params.MaxMessageID,
		},
	}

	resp, err := c.historyClient.DeleteDLQTasks(ctx, req)
	if err != nil {
		return nil, c.convertServerErr(err, "DeleteDLQTasks failed")
	}

	return resp, err
}

func (c *workerComponent) mergeTasks(
	ctx workflow.Context,
	params MergeParams,
	lastProcessedMessageID *int64,
	numberOfMessagesProcessed *int64,
) error {
	params, err := parseMergeParams(params)
	if err != nil {
		return err
	}

	var nextPageToken []byte

	for {
		// 1. Read tasks from DLQ.
		// 1.a. Execute activity to get tasks.
		var response adminservice.GetDLQTasksResponse

		ctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: mergeTasksActivityTimeout,
			TaskQueue:           primitives.DLQActivityTQ,
			RetryPolicy:         mergeActivityRetryPolicy,
		})

		err := workflow.ExecuteActivity(ctx, readTasksActivityName, params, nextPageToken).Get(ctx, &response)
		if err != nil {
			return err
		}

		if len(response.DlqTasks) == 0 {
			return nil
		}

		nextPageToken = response.NextPageToken

		// 1.b. Filter out tasks from messages beyond the last-desired message.
		historyTasks := make([]*commonspb.HistoryTask, 0, len(response.DlqTasks))
		maxBatchMessageID := int64(persistence.FirstQueueMessageID)

		for _, task := range response.DlqTasks {
			if task.Metadata.MessageId <= params.MaxMessageID {
				historyTasks = append(historyTasks, task.Payload)
				maxBatchMessageID = max(maxBatchMessageID, task.Metadata.MessageId)
			}
		}

		// 2. Re-enqueue tasks.
		err = workflow.ExecuteActivity(ctx, reEnqueueTasksActivityName, params, historyTasks).Get(ctx, nil)
		if err != nil {
			return err
		}

		// 3. Delete tasks from the DLQ.
		err = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:           primitives.DLQActivityTQ,
				RetryPolicy:         deleteActivityRetryPolicy,
				StartToCloseTimeout: deleteTasksActivityTimeout,
			}),
			deleteTasksActivityName,
			DeleteParams{
				Key:          params.Key,
				MaxMessageID: maxBatchMessageID,
			},
		).Get(ctx, nil)
		if err != nil {
			return err
		}
		*lastProcessedMessageID = maxBatchMessageID
		*numberOfMessagesProcessed += int64(len(historyTasks))
		// 4. Check if we're done.
		if len(nextPageToken) == 0 {
			return nil
		}

		if maxBatchMessageID == params.MaxMessageID {
			return nil
		}
	}
}

func parseMergeParams(params MergeParams) (MergeParams, error) {
	// Note that it's not strictly necessary to return a non-retryable error here because this is only called from
	// within the workflow, and any errors returned from workflows are already non-retryable. However, we're returning
	// a non-retryable error here in case this code is ever used within an activity.
	if params.BatchSize < 0 {
		return params, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Invalid batch size of %v", params.BatchSize),
			errorTypeInvalidRequest,
			ErrNegativeBatchSize,
		)
	}

	if params.BatchSize > MaxMergeBatchSize {
		return params, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Batch size, %d, must be less than or equal to %d", params.BatchSize, MaxMergeBatchSize),
			errorTypeInvalidRequest,
			ErrMergeBatchSizeTooLarge,
		)
	}

	if params.BatchSize == 0 {
		params.BatchSize = DefaultMergeBatchSize
	}

	return params, nil
}

// reEnqueueTasks groups tasks by shard ID and then sends individual requests to the history service to re-enqueue tasks
// for each shard. We do this because the history service requires that all tasks in a single request be for the same
// shard (because shards could live on different hosts, and the history service doesn't do its own fan-out).
func (c *workerComponent) reEnqueueTasks(
	ctx context.Context,
	params MergeParams,
	historyTasks []*commonspb.HistoryTask,
) error {
	// Group tasks by shard ID.
	tasksByShard := make(map[int32][]*adminservice.AddTasksRequest_Task)
	for _, task := range historyTasks {
		newTask := &adminservice.AddTasksRequest_Task{
			CategoryId: int32(params.TaskCategoryID),
			Blob:       task.Blob,
		}
		tasksByShard[task.ShardId] = append(tasksByShard[task.ShardId], newTask)
	}

	// Connect to the admin service with the source cluster.
	taskClient, err := c.taskClientDialer.Dial(ctx, params.SourceCluster)
	if err != nil {
		return fmt.Errorf("unable to dial admin service for cluster %q: %w", params.SourceCluster, err)
	}

	for shardID, batch := range tasksByShard {
		_, err := taskClient.AddTasks(ctx, &adminservice.AddTasksRequest{
			ShardId: shardID,
			Tasks:   batch,
		})
		if err != nil {
			return c.convertServerErr(err, fmt.Sprintf(
				"AddTasks failed while re-enqueuing tasks to shard %d", shardID,
			))
		}
	}

	return nil
}

func (c *workerComponent) convertServerErr(err error, msg string) error {
	if code := serviceerror.ToStatus(err).Code(); code == codes.InvalidArgument || code == codes.NotFound {
		// Don't retry invalid-argument or not-found errors.
		return temporal.NewNonRetryableApplicationError(
			msg, errorTypeInvalidRequest, err,
		)
	}

	// All other errors will be retried.
	return err
}

func (c *workerComponent) readTasks(
	ctx context.Context,
	params MergeParams,
	nextPageToken []byte,
) (*historyservice.GetDLQTasksResponse, error) {
	req := &historyservice.GetDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  int32(params.TaskCategoryID),
			SourceCluster: params.SourceCluster,
			TargetCluster: params.TargetCluster,
		},
		PageSize:      int32(params.BatchSize),
		NextPageToken: nextPageToken,
	}

	resp, err := c.historyClient.GetDLQTasks(ctx, req)
	if err != nil {
		return nil, c.convertServerErr(err, "GetDLQTasks failed")
	}

	return resp, nil
}

func (c *workerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(c.workflow, workflow.RegisterOptions{
		Name: WorkflowName,
	})
}

func (c *workerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (c *workerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivityWithOptions(c.deleteTasks, activity.RegisterOptions{
		Name: deleteTasksActivityName,
	})
	registry.RegisterActivityWithOptions(c.readTasks, activity.RegisterOptions{
		Name: readTasksActivityName,
	})
	registry.RegisterActivityWithOptions(c.reEnqueueTasks, activity.RegisterOptions{
		Name: reEnqueueTasksActivityName,
	})
}

func (c *workerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.DLQActivityTQ,
		Options: sdkworker.Options{
			BackgroundActivityContext: headers.SetCallerType(
				context.Background(),
				headers.CallerTypePreemptable,
			),
		},
	}
}

// Dial implements [TaskClientDialer] by calling the [TaskClientDialerFn] with the cluster name.
func (f TaskClientDialerFn) Dial(ctx context.Context, cluster string) (TaskClient, error) {
	return f(ctx, cluster)
}

// AddTasks implements [TaskClient] by calling the [AddTasksFn] with the request.
func (f AddTasksFn) AddTasks(
	ctx context.Context,
	in *adminservice.AddTasksRequest,
) (*adminservice.AddTasksResponse, error) {
	return f(ctx, in)
}
