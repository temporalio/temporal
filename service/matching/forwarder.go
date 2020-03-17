// Copyright (c) 2019 Uber Technologies, Inc.
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
	"math"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/types"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/quotas"
)

type (
	// Forwarder is the type that contains state pertaining to
	// the api call forwarder component
	Forwarder struct {
		cfg          *forwarderConfig
		taskListID   *taskListID
		taskListKind enums.TaskListKind
		client       matching.Client

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
		limiter *quotas.DynamicRateLimiter

		// cached metric scopes for API calls
		//nolint
		scope struct {
			forwardTask  metrics.Scope
			forwardQuery metrics.Scope
			forwardPoll  metrics.Scope
		}
		scopeFunc func() metrics.Scope
	}
	// ForwarderReqToken is the token that must be acquired before
	// making forwarder API calls. This type contains the state
	// for the token itself
	ForwarderReqToken struct {
		ch chan *ForwarderReqToken
	}
)

var (
	errNoParent            = errors.New("cannot find parent task list for forwarding")
	errTaskListKind        = errors.New("forwarding is not supported on sticky task list")
	errInvalidTaskListType = errors.New("unrecognized task list type")
	errForwarderSlowDown   = errors.New("limit exceeded")
)

// noopForwarderTokenC refers to a token channel that blocks forever
var noopForwarderTokenC <-chan *ForwarderReqToken = make(chan *ForwarderReqToken)

// newForwarder returns an instance of Forwarder object which
// can be used to forward api request calls from a task list
// child partition to a task list parent partition. The returned
// forwarder is tied to a single task list. All of the exposed
// methods can return the following errors:
// Returns following errors:
//  - errNoParent: If this task list doesn't have a parent to forward to
//  - errTaskListKind: If the task list is a sticky task list. Sticky task lists are never partitioned
//  - errForwarderSlowDown: When the rate limit is exceeded
//  - errInvalidTaskType: If the task list type is invalid
func newForwarder(
	cfg *forwarderConfig,
	taskListID *taskListID,
	kind enums.TaskListKind,
	client matching.Client,
	scopeFunc func() metrics.Scope,
) *Forwarder {
	rpsFunc := func() float64 { return float64(cfg.ForwarderMaxRatePerSecond()) }
	fwdr := &Forwarder{
		cfg:                   cfg,
		client:                client,
		taskListID:            taskListID,
		taskListKind:          kind,
		outstandingTasksLimit: int32(cfg.ForwarderMaxOutstandingTasks()),
		outstandingPollsLimit: int32(cfg.ForwarderMaxOutstandingPolls()),
		limiter:               quotas.NewDynamicRateLimiter(rpsFunc),
		scopeFunc:             scopeFunc,
	}
	fwdr.addReqToken.Store(newForwarderReqToken(cfg.ForwarderMaxOutstandingTasks()))
	fwdr.pollReqToken.Store(newForwarderReqToken(cfg.ForwarderMaxOutstandingPolls()))
	return fwdr
}

// ForwardTask forwards an activity or decision task to the parent task list partition if it exist
func (fwdr *Forwarder) ForwardTask(ctx context.Context, task *internalTask) error {
	if fwdr.taskListKind == enums.TaskListKindSticky {
		return errTaskListKind
	}

	name := fwdr.taskListID.Parent(fwdr.cfg.ForwarderMaxChildrenPerNode())
	if name == "" {
		return errNoParent
	}

	if !fwdr.limiter.Allow() {
		return errForwarderSlowDown
	}

	var err error

	// todo: Vet recomputing ScheduleToStart and rechecking expiry here
	expiryGo, err := types.TimestampFromProto(task.event.Data.Expiry)
	if err != nil {
		return err
	}

	newScheduleToStartTimeout := int32(math.Ceil(time.Until(expiryGo).Seconds()))

	// Todo - should we noop expired tasks? This will be moot once history stamp absolute time
	/*if newScheduleToStartTimeout <= 0 {
		return nil
	}*/

	switch fwdr.taskListID.taskType {
	case persistence.TaskListTypeDecision:
		_, err = fwdr.client.AddDecisionTask(ctx, &matchingservice.AddDecisionTaskRequest{
			DomainUUID: primitives.UUIDString(task.event.Data.DomainID),
			Execution:  task.workflowExecution(),
			TaskList: &commonproto.TaskList{
				Name: name,
				Kind: enums.TaskListKind(fwdr.taskListKind),
			},
			ScheduleId:                    task.event.Data.ScheduleID,
			Source:                        task.source,
			ScheduleToStartTimeoutSeconds: newScheduleToStartTimeout,
			ForwardedFrom:                 fwdr.taskListID.name,
		})
	case persistence.TaskListTypeActivity:
		_, err = fwdr.client.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
			DomainUUID:       fwdr.taskListID.domainID,
			SourceDomainUUID: primitives.UUIDString(task.event.Data.DomainID),
			Execution:        task.workflowExecution(),
			TaskList: &commonproto.TaskList{
				Name: name,
				Kind: enums.TaskListKind(fwdr.taskListKind),
			},
			ScheduleId:                    task.event.Data.ScheduleID,
			Source:                        task.source,
			ScheduleToStartTimeoutSeconds: newScheduleToStartTimeout,
			ForwardedFrom:                 fwdr.taskListID.name,
		})
	default:
		return errInvalidTaskListType
	}

	return fwdr.handleErr(err)
}

// ForwardQueryTask forwards a query task to parent task list partition, if it exist
func (fwdr *Forwarder) ForwardQueryTask(
	ctx context.Context,
	task *internalTask,
) (*matchingservice.QueryWorkflowResponse, error) {

	if fwdr.taskListKind == enums.TaskListKindSticky {
		return nil, errTaskListKind
	}

	name := fwdr.taskListID.Parent(fwdr.cfg.ForwarderMaxChildrenPerNode())
	if name == "" {
		return nil, errNoParent
	}

	resp, err := fwdr.client.QueryWorkflow(ctx, &matchingservice.QueryWorkflowRequest{
		DomainUUID: task.query.request.GetDomainUUID(),
		TaskList: &commonproto.TaskList{
			Name: name,
			Kind: fwdr.taskListKind,
		},
		QueryRequest:  task.query.request.QueryRequest,
		ForwardedFrom: fwdr.taskListID.name,
	})

	return resp, fwdr.handleErr(err)
}

// ForwardPoll forwards a poll request to parent task list partition if it exist
func (fwdr *Forwarder) ForwardPoll(ctx context.Context) (*internalTask, error) {
	if fwdr.taskListKind == enums.TaskListKindSticky {
		return nil, errTaskListKind
	}

	name := fwdr.taskListID.Parent(fwdr.cfg.ForwarderMaxChildrenPerNode())
	if name == "" {
		return nil, errNoParent
	}

	pollerID, _ := ctx.Value(pollerIDKey).(string)
	identity, _ := ctx.Value(identityKey).(string)

	switch fwdr.taskListID.taskType {
	case persistence.TaskListTypeDecision:
		resp, err := fwdr.client.PollForDecisionTask(ctx, &matchingservice.PollForDecisionTaskRequest{
			DomainUUID: fwdr.taskListID.domainID,
			PollerID:   pollerID,
			PollRequest: &workflowservice.PollForDecisionTaskRequest{
				TaskList: &commonproto.TaskList{
					Name: name,
					Kind: enums.TaskListKind(fwdr.taskListKind),
				},
				Identity: identity,
			},
			ForwardedFrom: fwdr.taskListID.name,
		})
		if err != nil {
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{decisionTaskInfo: resp}), nil
	case persistence.TaskListTypeActivity:
		resp, err := fwdr.client.PollForActivityTask(ctx, &matchingservice.PollForActivityTaskRequest{
			DomainUUID: fwdr.taskListID.domainID,
			PollerID:   pollerID,
			PollRequest: &workflowservice.PollForActivityTaskRequest{
				TaskList: &commonproto.TaskList{
					Name: name,
					Kind: enums.TaskListKind(fwdr.taskListKind),
				},
				Identity: identity,
			},
			ForwardedFrom: fwdr.taskListID.name,
		})
		if err != nil {
			return nil, fwdr.handleErr(err)
		}
		return newInternalStartedTask(&startedTaskInfo{activityTaskInfo: resp}), nil
	}

	return nil, errInvalidTaskListType
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
