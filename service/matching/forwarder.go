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

package matching

import (
	"context"
	"errors"
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
	// Forwarder is the type that contains state pertaining to
	// the api call forwarder component
	Forwarder struct {
		cfg       *forwarderConfig
		queue     *PhysicalTaskQueueKey
		partition *tqid.NormalPartition
		client    matchingservice.MatchingServiceClient
	}
)

var (
	errInvalidTaskQueueType = errors.New("unrecognized task queue type")
)

// newForwarder returns an instance of Forwarder object which
// can be used to forward api request calls from a task queue
// child partition to a task queue parent partition. The returned
// forwarder is tied to a single task queue partition. All exposed
// methods can return the following errors:
// Returns following errors:
//   - taskqueue.ErrNoParent, taskqueue.ErrInvalidDegree: If this task queue doesn't have a parent to forward to
func newForwarder(
	cfg *forwarderConfig,
	queue *PhysicalTaskQueueKey,
	client matchingservice.MatchingServiceClient,
) (*Forwarder, error) {
	partition, ok := queue.Partition().(*tqid.NormalPartition)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("physical queue of normal partition expected")
	}
	return &Forwarder{
		cfg:       cfg,
		client:    client,
		partition: partition,
		queue:     queue,
	}, nil
}

// ForwardTask forwards an activity or workflow task to the parent task queue partition if it exists
func (fwdr *Forwarder) ForwardTask(ctx context.Context, task *internalTask) error {
	degree := fwdr.cfg.ForwarderMaxChildrenPerNode()
	target, err := fwdr.partition.ParentPartition(degree)
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

	switch fwdr.partition.TaskType() {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		_, err = fwdr.client.AddWorkflowTask(
			ctx, &matchingservice.AddWorkflowTaskRequest{
				NamespaceId: task.event.Data.GetNamespaceId(),
				Execution:   task.workflowExecution(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: fwdr.partition.Kind(),
				},
				ScheduledEventId:       task.event.Data.GetScheduledEventId(),
				Clock:                  task.event.Data.GetClock(),
				ScheduleToStartTimeout: expirationDuration,
				ForwardInfo:            fwdr.getForwardInfo(task),
				VersionDirective:       task.event.Data.GetVersionDirective(),
				Priority:               task.event.Data.GetPriority(),
			},
		)
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		_, err = fwdr.client.AddActivityTask(
			ctx, &matchingservice.AddActivityTaskRequest{
				NamespaceId: task.event.Data.GetNamespaceId(),
				Execution:   task.workflowExecution(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: fwdr.partition.Kind(),
				},
				ScheduledEventId:       task.event.Data.GetScheduledEventId(),
				Clock:                  task.event.Data.GetClock(),
				ScheduleToStartTimeout: expirationDuration,
				ForwardInfo:            fwdr.getForwardInfo(task),
				Stamp:                  task.event.Data.GetStamp(),
				VersionDirective:       task.event.Data.GetVersionDirective(),
				Priority:               task.event.Data.GetPriority(),
			},
		)
	default:
		return errInvalidTaskQueueType
	}

	return err
}

func (fwdr *Forwarder) getForwardInfo(task *internalTask) *taskqueuespb.TaskForwardInfo {
	if task.isForwarded() {
		// task is already forwarded from a child partition, only overwrite SourcePartition
		clone := common.CloneProto(task.forwardInfo)
		clone.SourcePartition = fwdr.partition.RpcName()
		return clone
	}
	// task is forwarded for the first time
	return &taskqueuespb.TaskForwardInfo{
		TaskSource:         task.source,
		SourcePartition:    fwdr.partition.RpcName(),
		DispatchBuildId:    fwdr.queue.Version().BuildId(),
		DispatchVersionSet: fwdr.queue.Version().VersionSet(),
		RedirectInfo:       task.redirectInfo,
	}
}

// ForwardQueryTask forwards a query task to parent task queue partition, if it exists
func (fwdr *Forwarder) ForwardQueryTask(
	ctx context.Context,
	task *internalTask,
) (*matchingservice.QueryWorkflowResponse, error) {
	degree := fwdr.cfg.ForwarderMaxChildrenPerNode()
	target, err := fwdr.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	resp, err := fwdr.client.QueryWorkflow(ctx, &matchingservice.QueryWorkflowRequest{
		NamespaceId: task.query.request.GetNamespaceId(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: target.RpcName(),
			Kind: fwdr.partition.Kind(),
		},
		QueryRequest:     task.query.request.QueryRequest,
		VersionDirective: task.query.request.VersionDirective,
		ForwardInfo:      fwdr.getForwardInfo(task),
	})

	return resp, err
}

// ForwardNexusTask forwards a nexus task to parent task queue partition, if it exists.
func (fwdr *Forwarder) ForwardNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error) {
	degree := fwdr.cfg.ForwarderMaxChildrenPerNode()
	target, err := fwdr.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	resp, err := fwdr.client.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: task.nexus.request.GetNamespaceId(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: target.RpcName(),
			Kind: fwdr.partition.Kind(),
		},
		Request:     task.nexus.request.Request,
		ForwardInfo: fwdr.getForwardInfo(task),
	})

	return resp, err
}

// ForwardPoll forwards a poll request to parent task queue partition if it exist
func (fwdr *Forwarder) ForwardPoll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	degree := fwdr.cfg.ForwarderMaxChildrenPerNode()
	target, err := fwdr.partition.ParentPartition(degree)
	if err != nil {
		return nil, err
	}

	pollerID, _ := ctx.Value(pollerIDKey).(string)
	identity, _ := ctx.Value(identityKey).(string)

	switch fwdr.partition.TaskType() {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := fwdr.client.PollWorkflowTaskQueue(ctx, &matchingservice.PollWorkflowTaskQueueRequest{
			NamespaceId: fwdr.partition.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: fwdr.partition.Kind(),
				},
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
			},
			ForwardedSource: fwdr.partition.RpcName(),
		})
		if err != nil {
			return nil, err
		}
		return newInternalStartedTask(&startedTaskInfo{workflowTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := fwdr.client.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: fwdr.partition.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: fwdr.partition.Kind(),
				},
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
			},
			ForwardedSource: fwdr.partition.RpcName(),
		})
		if err != nil {
			return nil, err
		}
		return newInternalStartedTask(&startedTaskInfo{activityTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_NEXUS:
		resp, err := fwdr.client.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: fwdr.partition.TaskQueue().NamespaceId(),
			PollerId:    pollerID,
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: target.RpcName(),
					Kind: fwdr.partition.Kind(),
				},
				Identity:                  identity,
				WorkerVersionCapabilities: pollMetadata.workerVersionCapabilities,
				// Namespace is ignored here.
			},
			ForwardedSource: fwdr.partition.RpcName(),
		})
		if err != nil {
			return nil, err
		}
		return newInternalStartedTask(&startedTaskInfo{nexusTaskInfo: resp}), nil
	default:
		return nil, errInvalidTaskQueueType
	}
}
