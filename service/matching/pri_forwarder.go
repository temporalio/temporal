package matching

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	// priForwarder is the type that contains state pertaining to
	// the api call forwarder component
	priForwarder struct {
		cfg       *forwarderConfig
		queue     *PhysicalTaskQueueKey
		partition *tqid.NormalPartition
		client    matchingservice.MatchingServiceClient
	}
)

// TODO(pri): old matcher cleanup, move to here
// var errInvalidTaskQueueType = errors.New("unrecognized task queue type")

// newPriForwarder returns an instance of priForwarder object which
// can be used to forward api request calls from a task queue
// child partition to a task queue parent partition. The returned
// forwarder is tied to a single task queue partition. All exposed
// methods can return the following errors:
// Returns following errors:
//   - taskqueue.ErrNoParent, taskqueue.ErrInvalidDegree: If this task queue doesn't have a parent to forward to
func newPriForwarder(
	cfg *forwarderConfig,
	queue *PhysicalTaskQueueKey,
	client matchingservice.MatchingServiceClient,
) (*priForwarder, error) {
	partition, ok := queue.Partition().(*tqid.NormalPartition)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("physical queue of normal partition expected")
	}
	return &priForwarder{
		cfg:       cfg,
		client:    client,
		partition: partition,
		queue:     queue,
	}, nil
}

// ForwardTask forwards an activity or workflow task to the parent task queue partition if it exists
func (f *priForwarder) ForwardTask(ctx context.Context, task *internalTask) error {
	degree := f.cfg.ForwarderMaxChildrenPerNode()
	target, err := f.partition.ParentPartition(degree)
	if err != nil {
		return err
	}

	var expirationDuration *durationpb.Duration
	var expirationTime time.Time
	if task.event.GetData().HasExpiryTime() {
		expirationTime = task.event.GetData().GetExpiryTime().AsTime()
		remaining := time.Until(expirationTime)
		if remaining <= 0 {
			return nil
		}
		expirationDuration = durationpb.New(remaining)
	}

	// nolint:exhaustive // there's a default clause
	switch f.partition.TaskType() {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		_, err = f.client.AddWorkflowTask(
			ctx, matchingservice.AddWorkflowTaskRequest_builder{
				NamespaceId: task.event.GetData().GetNamespaceId(),
				Execution:   task.workflowExecution(),
				TaskQueue: taskqueuepb.TaskQueue_builder{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				}.Build(),
				ScheduledEventId:       task.event.GetData().GetScheduledEventId(),
				Clock:                  task.event.GetData().GetClock(),
				ScheduleToStartTimeout: expirationDuration,
				ForwardInfo:            f.getForwardInfo(task),
				VersionDirective:       task.event.GetData().GetVersionDirective(),
				Stamp:                  task.event.GetData().GetStamp(),
				Priority:               task.event.GetData().GetPriority(),
			}.Build(),
		)
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		_, err = f.client.AddActivityTask(
			ctx, matchingservice.AddActivityTaskRequest_builder{
				NamespaceId: task.event.GetData().GetNamespaceId(),
				Execution:   task.workflowExecution(),
				TaskQueue: taskqueuepb.TaskQueue_builder{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				}.Build(),
				ScheduledEventId:       task.event.GetData().GetScheduledEventId(),
				Clock:                  task.event.GetData().GetClock(),
				ScheduleToStartTimeout: expirationDuration,
				ForwardInfo:            f.getForwardInfo(task),
				VersionDirective:       task.event.GetData().GetVersionDirective(),
				Stamp:                  task.event.GetData().GetStamp(),
				Priority:               task.event.GetData().GetPriority(),
				ComponentRef:           task.event.GetData().GetComponentRef(),
			}.Build(),
		)
	default:
		return errInvalidTaskQueueType
	}

	return err
}

func (f *priForwarder) getForwardInfo(task *internalTask) *taskqueuespb.TaskForwardInfo {
	if task.isForwarded() {
		// task is already forwarded from a child partition, only overwrite SourcePartition
		clone := common.CloneProto(task.forwardInfo)
		clone.SetSourcePartition(f.partition.RpcName())
		return clone
	}
	// task is forwarded for the first time
	return taskqueuespb.TaskForwardInfo_builder{
		TaskSource:         task.source,
		SourcePartition:    f.partition.RpcName(),
		DispatchBuildId:    f.queue.Version().BuildId(),
		DispatchVersionSet: f.queue.Version().VersionSet(),
		RedirectInfo:       task.redirectInfo,
	}.Build()
}

// ForwardQueryTask forwards a query task to parent task queue partition, if it exists
func (f *priForwarder) ForwardQueryTask(
	ctx context.Context,
	task *internalTask,
) (*matchingservice.QueryWorkflowResponse, error) {
	degree := f.cfg.ForwarderMaxChildrenPerNode()
	target, err := f.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.QueryWorkflow(ctx, matchingservice.QueryWorkflowRequest_builder{
		NamespaceId: task.query.request.GetNamespaceId(),
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: target.RpcName(),
			Kind: f.partition.Kind(),
		}.Build(),
		QueryRequest:     task.query.request.GetQueryRequest(),
		VersionDirective: task.query.request.GetVersionDirective(),
		ForwardInfo:      f.getForwardInfo(task),
	}.Build())

	return resp, err
}

// ForwardNexusTask forwards a nexus task to parent task queue partition, if it exists.
func (f *priForwarder) ForwardNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error) {
	degree := f.cfg.ForwarderMaxChildrenPerNode()
	target, err := f.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.DispatchNexusTask(ctx, matchingservice.DispatchNexusTaskRequest_builder{
		NamespaceId: task.nexus.request.GetNamespaceId(),
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: target.RpcName(),
			Kind: f.partition.Kind(),
		}.Build(),
		Request:     task.nexus.request.GetRequest(),
		ForwardInfo: f.getForwardInfo(task),
	}.Build())

	return resp, err
}

// ForwardPoll forwards a poll request to parent task queue partition if it exist
// TODO(pri): remove this and update tests to call ForwardPollWithTarget directly
func (f *priForwarder) ForwardPoll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	degree := f.cfg.ForwarderMaxChildrenPerNode()
	target, err := f.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	return ForwardPollWithTarget(ctx, pollMetadata, f.client, f.partition, target)
}

// ForwardPollWithTarget forwards a poll request to another partition
func ForwardPollWithTarget(
	ctx context.Context,
	pollMetadata *pollMetadata,
	client matchingservice.MatchingServiceClient,
	source tqid.Partition,
	target *tqid.NormalPartition,
) (*internalTask, error) {
	pollerID, _ := ctx.Value(pollerIDKey).(string) // nolint:revive
	identity, _ := ctx.Value(identityKey).(string) // nolint:revive

	// nolint:exhaustive // there's a default clause
	switch target.TaskType() {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := client.PollWorkflowTaskQueue(ctx, matchingservice.PollWorkflowTaskQueueRequest_builder{
			NamespaceId: target.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			PollRequest: workflowservice.PollWorkflowTaskQueueRequest_builder{
				TaskQueue: taskqueuepb.TaskQueue_builder{
					Name: target.RpcName(),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				}.Build(),
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				DeploymentOptions:         pollMetadata.deploymentOptions,
			}.Build(),
			ForwardedSource: source.RpcName(),
			Conditions:      pollMetadata.conditions,
		}.Build())
		if err != nil {
			return nil, err
		} else if len(resp.GetTaskToken()) == 0 {
			return nil, errNoTasks
		}
		return newInternalStartedTask(&startedTaskInfo{workflowTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := client.PollActivityTaskQueue(ctx, matchingservice.PollActivityTaskQueueRequest_builder{
			NamespaceId: target.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			PollRequest: workflowservice.PollActivityTaskQueueRequest_builder{
				TaskQueue: taskqueuepb.TaskQueue_builder{
					Name: target.RpcName(),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				}.Build(),
				Identity:                  identity,
				TaskQueueMetadata:         pollMetadata.taskQueueMetadata,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				DeploymentOptions:         pollMetadata.deploymentOptions,
			}.Build(),
			ForwardedSource: source.RpcName(),
			Conditions:      pollMetadata.conditions,
		}.Build())
		if err != nil {
			return nil, err
		} else if len(resp.GetTaskToken()) == 0 {
			return nil, errNoTasks
		}
		return newInternalStartedTask(&startedTaskInfo{activityTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_NEXUS:
		resp, err := client.PollNexusTaskQueue(ctx, matchingservice.PollNexusTaskQueueRequest_builder{
			NamespaceId: target.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			Request: workflowservice.PollNexusTaskQueueRequest_builder{
				TaskQueue: taskqueuepb.TaskQueue_builder{
					Name: target.RpcName(),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				}.Build(),
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				DeploymentOptions:         pollMetadata.deploymentOptions,
				// Namespace is ignored here.
			}.Build(),
			ForwardedSource: source.RpcName(),
			Conditions:      pollMetadata.conditions,
		}.Build())
		if err != nil {
			return nil, err
		} else if !resp.HasResponse() {
			return nil, errNoTasks
		}
		return newInternalStartedTask(&startedTaskInfo{nexusTaskInfo: resp}), nil
	default:
		return nil, errInvalidTaskQueueType
	}
}
