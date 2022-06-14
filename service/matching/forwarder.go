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

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
)

type (
	// Forwarder is the type that contains state pertaining to
	// the api call forwarder component
	Forwarder struct {
		cfg           *forwarderConfig
		taskQueueID   *taskQueueID
		taskQueueKind enumspb.TaskQueueKind
		client        matchingservice.MatchingServiceClient

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
	errNoParent             = errors.New("cannot find parent task queue for forwarding")
	errTaskQueueKind        = errors.New("forwarding is not supported on sticky task queue")
	errInvalidTaskQueueType = errors.New("unrecognized task queue type")
	errForwarderSlowDown    = errors.New("limit exceeded")
)

// newForwarder returns an instance of Forwarder object which
// can be used to forward api request calls from a task queue
// child partition to a task queue parent partition. The returned
// forwarder is tied to a single task queue. All of the exposed
// methods can return the following errors:
// Returns following errors:
//  - errNoParent: If this task queue doesn't have a parent to forward to
//  - errTaskQueueKind: If the task queue is a sticky task queue. Sticky task queues are never partitioned
//  - errForwarderSlowDown: When the rate limit is exceeded
//  - errInvalidTaskType: If the task queue type is invalid
func newForwarder(
	cfg *forwarderConfig,
	taskQueueID *taskQueueID,
	kind enumspb.TaskQueueKind,
	client matchingservice.MatchingServiceClient,
) *Forwarder {
	fwdr := &Forwarder{
		cfg:                   cfg,
		client:                client,
		taskQueueID:           taskQueueID,
		taskQueueKind:         kind,
		outstandingTasksLimit: int32(cfg.ForwarderMaxOutstandingTasks()),
		outstandingPollsLimit: int32(cfg.ForwarderMaxOutstandingPolls()),
		limiter: quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(cfg.ForwarderMaxRatePerSecond()) },
		),
	}
	fwdr.addReqToken.Store(newForwarderReqToken(cfg.ForwarderMaxOutstandingTasks()))
	fwdr.pollReqToken.Store(newForwarderReqToken(cfg.ForwarderMaxOutstandingPolls()))
	return fwdr
}

// ForwardTask forwards an activity or workflow task to the parent task queue partition if it exist
func (fwdr *Forwarder) ForwardTask(ctx context.Context, task *internalTask) error {
	if fwdr.taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		return errTaskQueueKind
	}

	name := fwdr.taskQueueID.Parent(fwdr.cfg.ForwarderMaxChildrenPerNode())
	if name == "" {
		return errNoParent
	}

	if !fwdr.limiter.Allow() {
		return errForwarderSlowDown
	}

	var err error

	var expirationDuration time.Duration
	expirationTime := timestamp.TimeValue(task.event.Data.ExpiryTime)
	if expirationTime.IsZero() {
		// noop
	} else {
		expirationDuration = time.Until(expirationTime)
		if expirationDuration <= 0 {
			return nil
		}
	}

	switch fwdr.taskQueueID.taskType {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		_, err = fwdr.client.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
			NamespaceId: task.event.Data.GetNamespaceId(),
			Execution:   task.workflowExecution(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: name,
				Kind: fwdr.taskQueueKind,
			},
			ScheduledEventId:       task.event.Data.GetScheduledEventId(),
			Clock:                  task.event.Data.GetClock(),
			Source:                 task.source,
			ScheduleToStartTimeout: &expirationDuration,
			ForwardedSource:        fwdr.taskQueueID.name,
		})
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		_, err = fwdr.client.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
			NamespaceId:       task.event.Data.GetNamespaceId(),
			SourceNamespaceId: task.event.Data.GetNamespaceId(),
			Execution:         task.workflowExecution(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: name,
				Kind: fwdr.taskQueueKind,
			},
			ScheduledEventId:       task.event.Data.GetScheduledEventId(),
			Clock:                  task.event.Data.GetClock(),
			Source:                 task.source,
			ScheduleToStartTimeout: &expirationDuration,
			ForwardedSource:        fwdr.taskQueueID.name,
		})
	default:
		return errInvalidTaskQueueType
	}

	return fwdr.handleErr(err)
}

// ForwardQueryTask forwards a query task to parent task queue partition, if it exist
func (fwdr *Forwarder) ForwardQueryTask(
	ctx context.Context,
	task *internalTask,
) (*matchingservice.QueryWorkflowResponse, error) {

	if fwdr.taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, errTaskQueueKind
	}

	name := fwdr.taskQueueID.Parent(fwdr.cfg.ForwarderMaxChildrenPerNode())
	if name == "" {
		return nil, errNoParent
	}

	resp, err := fwdr.client.QueryWorkflow(ctx, &matchingservice.QueryWorkflowRequest{
		NamespaceId: task.query.request.GetNamespaceId(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: name,
			Kind: fwdr.taskQueueKind,
		},
		QueryRequest:    task.query.request.QueryRequest,
		ForwardedSource: fwdr.taskQueueID.name,
	})

	return resp, fwdr.handleErr(err)
}

// ForwardPoll forwards a poll request to parent task queue partition if it exist
func (fwdr *Forwarder) ForwardPoll(ctx context.Context) (*internalTask, error) {
	if fwdr.taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, errTaskQueueKind
	}

	name := fwdr.taskQueueID.Parent(fwdr.cfg.ForwarderMaxChildrenPerNode())
	if name == "" {
		return nil, errNoParent
	}

	pollerID, _ := ctx.Value(pollerIDKey).(string)
	identity, _ := ctx.Value(identityKey).(string)

	switch fwdr.taskQueueID.taskType {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := fwdr.client.PollWorkflowTaskQueue(ctx, &matchingservice.PollWorkflowTaskQueueRequest{
			NamespaceId: fwdr.taskQueueID.namespaceID.String(),
			PollerId:    pollerID,
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: name,
					Kind: fwdr.taskQueueKind,
				},
				Identity: identity,
			},
			ForwardedSource: fwdr.taskQueueID.name,
		})
		if err != nil {
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{workflowTaskInfo: resp}), nil
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := fwdr.client.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: fwdr.taskQueueID.namespaceID.String(),
			PollerId:    pollerID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: name,
					Kind: fwdr.taskQueueKind,
				},
				Identity: identity,
			},
			ForwardedSource: fwdr.taskQueueID.name,
		})
		if err != nil {
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{activityTaskInfo: resp}), nil
	}

	return nil, errInvalidTaskQueueType
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
