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
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/quotas"
)

type (
	// Forwarder is the type that contains state pertaining to
	// the api call forwarder component
	Forwarder struct {
		cfg       *forwarderConfig
		queue     *PhysicalTaskQueueKey
		partition *tqid.NormalPartition
		client    matchingservice.MatchingServiceClient

		// token channels that vend tokens necessary to make
		// API calls exposed by forwarder. Tokens are used
		// to enforce maxOutstanding forwarded calls from this
		// instance. And channels are used so that the caller
		// can use them in a select{} block along with other
		// conditions
		addReqToken  atomic.Value
		pollReqToken atomic.Value

		// cached values of maxOutstanding dynamic config values.
		// these are used to detect changes
		outstandingTasksLimit int32
		outstandingPollsLimit int32

		// todo: implement a rate limiter that automatically
		// adjusts rate based on ServiceBusy errors from API calls
		limiter *quotas.DynamicRateLimiterImpl
	}
	// ForwarderReqToken is the token that must be acquired before
	// making forwarder API calls. This type contains the state
	// for the token itself
	ForwarderReqToken struct {
		ch chan *ForwarderReqToken
	}
)

var (
	errInvalidTaskQueueType = errors.New("unrecognized task queue type")
	errForwarderSlowDown    = errors.New("limit exceeded")
)

// newForwarder returns an instance of Forwarder object which
// can be used to forward api request calls from a task queue
// child partition to a task queue parent partition. The returned
// forwarder is tied to a single task queue partition. All exposed
// methods can return the following errors:
// Returns following errors:
//   - taskqueue.ErrNoParent, taskqueue.ErrInvalidDegree: If this task queue doesn't have a parent to forward to
//   - errForwarderSlowDown: When the rate limit is exceeded
func newForwarder(
	cfg *forwarderConfig,
	queue *PhysicalTaskQueueKey,
	client matchingservice.MatchingServiceClient,
) (*Forwarder, error) {
	partition, ok := queue.Partition().(*tqid.NormalPartition)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("physical queue of normal partition expected")
	}

	fwdr := &Forwarder{
		cfg:                   cfg,
		client:                client,
		partition:             partition,
		queue:                 queue,
		outstandingTasksLimit: int32(cfg.ForwarderMaxOutstandingTasks()),
		outstandingPollsLimit: int32(cfg.ForwarderMaxOutstandingPolls()),
		limiter: quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(cfg.ForwarderMaxRatePerSecond()) },
		),
	}
	fwdr.addReqToken.Store(newForwarderReqToken(cfg.ForwarderMaxOutstandingTasks()))
	fwdr.pollReqToken.Store(newForwarderReqToken(cfg.ForwarderMaxOutstandingPolls()))
	return fwdr, nil
}

// ForwardTask forwards an activity or workflow task to the parent task queue partition if it exists
func (fwdr *Forwarder) ForwardTask(ctx context.Context, task *internalTask) error {
	degree := fwdr.cfg.ForwarderMaxChildrenPerNode()
	target, err := fwdr.partition.ParentPartition(degree)
	if err != nil {
		return err
	}

	if !fwdr.limiter.Allow() {
		return errForwarderSlowDown
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
			},
		)
	default:
		return errInvalidTaskQueueType
	}

	return fwdr.handleErr(err)
}

func (fwdr *Forwarder) getForwardInfo(task *internalTask) *taskqueuespb.TaskForwardInfo {
	if task.isForwarded() {
		// task is already forwarded from a child partition, only overwrite SourcePartition
		clone := common.CloneProto(task.forwardInfo)
		clone.SourcePartition = fwdr.partition.RpcName()
		return clone
	}
	// task is forwarded for the first time
	forwardInfo := &taskqueuespb.TaskForwardInfo{
		TaskSource:         task.source,
		SourcePartition:    fwdr.partition.RpcName(),
		DispatchBuildId:    fwdr.queue.BuildId(),
		DispatchVersionSet: fwdr.queue.VersionSet(),
		RedirectInfo:       task.redirectInfo,
	}
	return forwardInfo
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

	return resp, fwdr.handleErr(err)
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

	return resp, fwdr.handleErr(err)
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
			NamespaceId: fwdr.partition.TaskQueue().NamespaceId().String(),
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
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{workflowTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := fwdr.client.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: fwdr.partition.TaskQueue().NamespaceId().String(),
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
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{activityTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_NEXUS:
		resp, err := fwdr.client.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: fwdr.partition.TaskQueue().NamespaceId().String(),
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
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{nexusTaskInfo: resp}), nil
	default:
		return nil, errInvalidTaskQueueType
	}
}

// AddReqTokenC returns a channel that can be used to wait for a token
// that's necessary before making a ForwardTask or ForwardQueryTask API call.
// After the API call is invoked, token.release() must be invoked
func (fwdr *Forwarder) AddReqTokenC() <-chan *ForwarderReqToken {
	fwdr.refreshTokenC(&fwdr.addReqToken, &fwdr.outstandingTasksLimit, int32(fwdr.cfg.ForwarderMaxOutstandingTasks()))
	return fwdr.addReqToken.Load().(*ForwarderReqToken).ch
}

// PollReqTokenC returns a channel that can be used to wait for a token
// that's necessary before making a ForwardPoll API call. After the API
// call is invoked, token.release() must be invoked
func (fwdr *Forwarder) PollReqTokenC() <-chan *ForwarderReqToken {
	fwdr.refreshTokenC(&fwdr.pollReqToken, &fwdr.outstandingPollsLimit, int32(fwdr.cfg.ForwarderMaxOutstandingPolls()))
	return fwdr.pollReqToken.Load().(*ForwarderReqToken).ch
}

func (fwdr *Forwarder) refreshTokenC(value *atomic.Value, curr *int32, maxLimit int32) {
	currLimit := atomic.LoadInt32(curr)
	if currLimit != maxLimit {
		if atomic.CompareAndSwapInt32(curr, currLimit, maxLimit) {
			value.Store(newForwarderReqToken(int(maxLimit)))
		}
	}
}

func (fwdr *Forwarder) handleErr(err error) error {
	if _, ok := err.(*serviceerror.ResourceExhausted); ok {
		return errForwarderSlowDown
	}
	return err
}

func newForwarderReqToken(maxOutstanding int) *ForwarderReqToken {
	reqToken := &ForwarderReqToken{ch: make(chan *ForwarderReqToken, maxOutstanding)}
	for i := 0; i < maxOutstanding; i++ {
		reqToken.ch <- reqToken
	}
	return reqToken
}

func (token *ForwarderReqToken) release() {
	token.ch <- token
}
