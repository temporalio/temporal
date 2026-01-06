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
	if task.event.Data.ExpiryTime != nil {
		expirationTime = task.event.Data.ExpiryTime.AsTime()
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
			ctx, &matchingservice.AddWorkflowTaskRequest{
				NamespaceId: task.event.Data.GetNamespaceId(),
				Execution:   task.workflowExecution(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				},
				ScheduledEventId:       task.event.Data.GetScheduledEventId(),
				Clock:                  task.event.Data.GetClock(),
				ScheduleToStartTimeout: expirationDuration,
				ForwardInfo:            f.getForwardInfo(task),
				VersionDirective:       task.event.Data.GetVersionDirective(),
				Stamp:                  task.event.Data.GetStamp(),
				Priority:               task.event.Data.GetPriority(),
			},
		)
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		_, err = f.client.AddActivityTask(
			ctx, &matchingservice.AddActivityTaskRequest{
				NamespaceId: task.event.Data.GetNamespaceId(),
				Execution:   task.workflowExecution(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				},
				ScheduledEventId:       task.event.Data.GetScheduledEventId(),
				Clock:                  task.event.Data.GetClock(),
				ScheduleToStartTimeout: expirationDuration,
				ForwardInfo:            f.getForwardInfo(task),
				VersionDirective:       task.event.Data.GetVersionDirective(),
				Stamp:                  task.event.Data.GetStamp(),
				Priority:               task.event.Data.GetPriority(),
				ComponentRef:           task.event.Data.GetComponentRef(),
			},
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
		clone.SourcePartition = f.partition.RpcName()
		return clone
	}
	// task is forwarded for the first time
	return &taskqueuespb.TaskForwardInfo{
		TaskSource:         task.source,
		SourcePartition:    f.partition.RpcName(),
		DispatchBuildId:    f.queue.Version().BuildId(),
		DispatchVersionSet: f.queue.Version().VersionSet(),
		RedirectInfo:       task.redirectInfo,
	}
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

	resp, err := f.client.QueryWorkflow(ctx, &matchingservice.QueryWorkflowRequest{
		NamespaceId: task.query.request.GetNamespaceId(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: target.RpcName(),
			Kind: f.partition.Kind(),
		},
		QueryRequest:     task.query.request.QueryRequest,
		VersionDirective: task.query.request.VersionDirective,
		ForwardInfo:      f.getForwardInfo(task),
	})

	return resp, err
}

// ForwardNexusTask forwards a nexus task to parent task queue partition, if it exists.
func (f *priForwarder) ForwardNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error) {
	degree := f.cfg.ForwarderMaxChildrenPerNode()
	target, err := f.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: task.nexus.request.GetNamespaceId(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: target.RpcName(),
			Kind: f.partition.Kind(),
		},
		Request:     task.nexus.request.Request,
		ForwardInfo: f.getForwardInfo(task),
	})

	return resp, err
}

// ForwardPoll forwards a poll request to parent task queue partition if it exist
func (f *priForwarder) ForwardPoll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	degree := f.cfg.ForwarderMaxChildrenPerNode()
	target, err := f.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	pollerID, _ := ctx.Value(pollerIDKey).(string) // nolint:revive
	identity, _ := ctx.Value(identityKey).(string) // nolint:revive

	// nolint:exhaustive // there's a default clause
	switch f.partition.TaskType() {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := f.client.PollWorkflowTaskQueue(ctx, &matchingservice.PollWorkflowTaskQueueRequest{
			NamespaceId: f.partition.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				},
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				DeploymentOptions:         pollMetadata.deploymentOptions,
			},
			ForwardedSource: f.partition.RpcName(),
			Conditions:      pollMetadata.conditions,
		})
		if err != nil {
			return nil, err
		} else if resp.TaskToken == nil {
			return nil, errNoTasks
		}
		return newInternalStartedTask(&startedTaskInfo{workflowTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := f.client.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: f.partition.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				},
				Identity:                  identity,
				TaskQueueMetadata:         pollMetadata.taskQueueMetadata,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				DeploymentOptions:         pollMetadata.deploymentOptions,
			},
			ForwardedSource: f.partition.RpcName(),
			Conditions:      pollMetadata.conditions,
		})
		if err != nil {
			return nil, err
		} else if resp.TaskToken == nil {
			return nil, errNoTasks
		}
		return newInternalStartedTask(&startedTaskInfo{activityTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_NEXUS:
		resp, err := f.client.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: f.partition.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: f.partition.Kind(),
				},
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				DeploymentOptions:         pollMetadata.deploymentOptions,
				// Namespace is ignored here.
			},
			ForwardedSource: f.partition.RpcName(),
			Conditions:      pollMetadata.conditions,
		})
		if err != nil {
			return nil, err
		} else if resp.Response == nil {
			return nil, errNoTasks
		}
		return newInternalStartedTask(&startedTaskInfo{nexusTaskInfo: resp}), nil
	default:
		return nil, errInvalidTaskQueueType
	}
}
