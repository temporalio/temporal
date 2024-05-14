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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/historyservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
)

type (
	activities struct {
		activityDeps
		namespace   namespace.Name
		namespaceID namespace.ID
		// Rate limiter for start workflow requests. Note that the scope is all schedules in
		// this namespace on this worker.
		startWorkflowRateLimiter quotas.RateLimiter
		maxBlobSize              dynamicconfig.IntPropertyFn
		localActivitySleepLimit  dynamicconfig.DurationPropertyFn
	}

	errFollow string

	rateLimitedDetails struct {
		Delay time.Duration
	}
)

const (
	eventStorageSize = 2 * 1024 * 1024
	// I do not know the real overhead size, 1024 is just a number
	recordOverheadSize = 1024
)

var (
	errTryAgain             = errors.New("try again")
	errWrongChain           = errors.New("found running workflow with wrong FirstExecutionRunId")
	errNoEvents             = errors.New("GetEvents didn't return any events")
	errNoAttrs              = errors.New("last event did not have correct attrs")
	errBlocked              = errors.New("rate limiter doesn't allow any progress")
	errUnkownWorkflowStatus = errors.New("unknown workflow status")
)

func (e errFollow) Error() string { return string(e) }

func (a *activities) StartWorkflow(ctx context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
	if err := a.waitForRateLimiterPermission(req); err != nil {
		return nil, err
	}

	req.Request.Namespace = a.namespace.String()

	res, err := a.FrontendClient.StartWorkflowExecution(ctx, req.Request)
	if err != nil {
		return nil, translateError(err, "StartWorkflowExecution")
	}

	// this will not match the time in the workflow execution started event
	// exactly, but it's just informational so it's close enough.
	now := time.Now()

	return &schedspb.StartWorkflowResponse{
		RunId:         res.RunId,
		RealStartTime: timestamppb.New(now),
	}, nil
}

func (a *activities) waitForRateLimiterPermission(req *schedspb.StartWorkflowRequest) error {
	if req.CompletedRateLimitSleep {
		return nil
	}
	reservation := a.startWorkflowRateLimiter.Reserve()
	if !reservation.OK() {
		return translateError(errBlocked, "StartWorkflowExecution")
	}
	delay := reservation.Delay()
	if delay > a.localActivitySleepLimit() {
		// for a long sleep, ask the workflow to do it in workflow logic
		return temporal.NewNonRetryableApplicationError(
			rateLimitedErrorType, rateLimitedErrorType, nil, rateLimitedDetails{Delay: delay})
	}
	// short sleep can be done in-line
	time.Sleep(delay)
	return nil
}

func (a *activities) tryWatchWorkflow(ctx context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
	if req.LongPoll {
		// make sure we return and heartbeat 5s before the timeout. this is only
		// for long polls, for refreshes we just use the local activity timeout.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, activity.GetInfo(ctx).HeartbeatTimeout-5*time.Second)
		defer cancel()
	}

	// poll history service directly instead of just going to frontend to avoid
	// using resources on frontend while waiting.
	// note that on the first time through the loop, Execution.RunId will be
	// empty, so we'll get the latest run, whatever it is (whether it's part of
	// the desired chain or not). if we have to follow (unlikely), we'll end up
	// back here with non-empty RunId.
	pollReq := &historyservice.PollMutableStateRequest{
		NamespaceId: a.namespaceID.String(),
		Execution:   req.Execution,
	}
	if req.LongPoll {
		pollReq.ExpectedNextEventId = common.EndEventID
	}
	// if long-polling, this will block up for workflow completion to 20s (default) and return
	// the current mutable state at that point. otherwise it should return immediately.
	pollRes, err := a.HistoryClient.PollMutableState(ctx, pollReq)
	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
			// just turn this into a success, with unspecified status
			return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED}, nil
		}
		a.Logger.Error("error from PollMutableState", tag.Error(err), tag.WorkflowID(req.Execution.WorkflowId))
		return nil, err
	}

	if pollRes.FirstExecutionRunId != req.FirstExecutionRunId {
		if len(req.Execution.RunId) == 0 {
			// there is a workflow running but it's not part of the chain we're
			// looking for. search for the one we want by runid.
			return nil, errFollow(req.FirstExecutionRunId)
		}
		// we explicitly searched for a chain we started by runid, and found
		// something that's part of a different chain. this should never happen.
		return nil, errWrongChain
	}

	rb := newResponseBuilder(
		req,
		pollRes.WorkflowStatus,
		a.Logger,
		a.maxBlobSize()-recordOverheadSize,
	)
	if pollRes.WorkflowStatus == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return rb.Build(nil)
	}

	// get last event from history
	histReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              a.namespace.String(),
		Execution:              req.Execution,
		MaximumPageSize:        1,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		SkipArchival:           true, // should be recently closed, no need for archival
	}
	histRes, err := a.FrontendClient.GetWorkflowExecutionHistory(ctx, histReq)

	if err != nil {
		a.Logger.Error("error from GetWorkflowExecutionHistory", tag.Error(err), tag.WorkflowID(req.Execution.WorkflowId))
		return nil, err
	}

	events := histRes.GetHistory().GetEvents()
	if len(events) < 1 {
		a.Logger.Error("GetWorkflowExecutionHistory returned no events", tag.WorkflowID(req.Execution.WorkflowId))
		return nil, errNoEvents
	}
	lastEvent := events[0]

	return rb.Build(lastEvent)
}

func (a *activities) WatchWorkflow(ctx context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
	if !req.LongPoll {
		// Go SDK currently doesn't set context timeout based on local activity
		// StartToCloseTimeout if ScheduleToCloseTimeout is set, so add a timeout here.
		// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultLocalActivityOptions.StartToCloseTimeout)
		defer cancel()
	}

	for ctx.Err() == nil {
		activity.RecordHeartbeat(ctx)
		res, err := a.tryWatchWorkflow(ctx, req)
		// long poll should return before our deadline, but even if it doesn't,
		// we can still try again within the same activity
		if req.LongPoll && (err == errTryAgain || common.IsContextDeadlineExceededErr(err)) {
			continue
		}
		if newRunID, ok := err.(errFollow); ok {
			req.Execution.RunId = string(newRunID)
			continue
		}
		return res, translateError(err, "WatchWorkflow")
	}
	return nil, translateError(ctx.Err(), "WatchWorkflow")
}

func (a *activities) CancelWorkflow(ctx context.Context, req *schedspb.CancelWorkflowRequest) error {
	// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, defaultLocalActivityOptions.StartToCloseTimeout)
	defer cancel()

	rreq := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: a.namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: a.namespace.String(),
			// only set WorkflowId so we cancel the latest, but restricted by FirstExecutionRunId
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: req.Execution.WorkflowId},
			Identity:            req.Identity,
			RequestId:           req.RequestId,
			FirstExecutionRunId: req.Execution.RunId,
			Reason:              req.Reason,
		},
	}
	_, err := a.HistoryClient.RequestCancelWorkflowExecution(ctx, rreq)

	return translateError(err, "RequestCancelWorkflowExecution")
}

func (a *activities) TerminateWorkflow(ctx context.Context, req *schedspb.TerminateWorkflowRequest) error {
	// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, defaultLocalActivityOptions.StartToCloseTimeout)
	defer cancel()

	rreq := &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: a.namespaceID.String(),
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: a.namespace.String(),
			// only set WorkflowId so we cancel the latest, but restricted by FirstExecutionRunId
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: req.Execution.WorkflowId},
			Reason:              req.Reason,
			Identity:            req.Identity,
			FirstExecutionRunId: req.Execution.RunId,
		},
	}
	_, err := a.HistoryClient.TerminateWorkflowExecution(ctx, rreq)

	return translateError(err, "TerminateWorkflowExecution")
}

func errType(err error) string {
	return reflect.TypeOf(err).Name()
}

func translateError(err error, msgPrefix string) error {
	if err == nil {
		return nil
	}
	message := fmt.Sprintf("%s: %s", msgPrefix, err.Error())
	if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
		return temporal.NewApplicationErrorWithCause(message, errType(err), err)
	}
	return temporal.NewNonRetryableApplicationError(message, errType(err), err)
}

type responseBuilder struct {
	request        *schedspb.WatchWorkflowRequest
	workflowStatus enumspb.WorkflowExecutionStatus
	logger         log.Logger
	maxBlobSize    int
}

func newResponseBuilder(
	request *schedspb.WatchWorkflowRequest,
	workflowStatus enumspb.WorkflowExecutionStatus,
	logger log.Logger,
	maxBlobSize int,
) responseBuilder {
	return responseBuilder{
		request:        request,
		workflowStatus: workflowStatus,
		logger:         logger,
		maxBlobSize:    maxBlobSize,
	}
}

//nolint:revive
func (r responseBuilder) Build(event *historypb.HistoryEvent) (*schedspb.WatchWorkflowResponse, error) {
	switch r.workflowStatus {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		if r.request.LongPoll {
			return nil, errTryAgain // not closed yet, just try again
		}
		return r.makeResponse(nil, nil), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		if attrs := event.GetWorkflowExecutionCompletedEventAttributes(); attrs == nil {
			return nil, errNoAttrs
		} else if len(attrs.NewExecutionRunId) > 0 {
			// this shouldn't happen because we don't allow old-cron workflows as scheduled, but follow it anyway
			return nil, errFollow(attrs.NewExecutionRunId)
		} else {
			result := attrs.Result
			if r.isTooBig(result) {
				r.logger.Error(
					fmt.Sprintf("result dropped due to its size %d", proto.Size(result)),
					tag.WorkflowID(r.request.Execution.WorkflowId))
				result = nil
			}
			return r.makeResponse(result, nil), nil
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		if attrs := event.GetWorkflowExecutionFailedEventAttributes(); attrs == nil {
			return nil, errNoAttrs
		} else if len(attrs.NewExecutionRunId) > 0 {
			return nil, errFollow(attrs.NewExecutionRunId)
		} else {
			failure := attrs.Failure
			if r.isTooBig(failure) {
				r.logger.Error(
					fmt.Sprintf("failure dropped due to its size %d", proto.Size(failure)),
					tag.WorkflowID(r.request.Execution.WorkflowId))
				failure = nil
			}
			return r.makeResponse(nil, failure), nil
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return r.makeResponse(nil, nil), nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		if attrs := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); attrs == nil {
			return nil, errNoAttrs
		} else {
			return nil, errFollow(attrs.NewExecutionRunId)
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		if attrs := event.GetWorkflowExecutionTimedOutEventAttributes(); attrs == nil {
			return nil, errNoAttrs
		} else if len(attrs.NewExecutionRunId) > 0 {
			return nil, errFollow(attrs.NewExecutionRunId)
		} else {
			return r.makeResponse(nil, nil), nil
		}
	}
	return nil, errUnkownWorkflowStatus
}

func (r responseBuilder) isTooBig(m proto.Message) bool {
	return proto.Size(m) > r.maxBlobSize
}

func (r responseBuilder) makeResponse(result *commonpb.Payloads, failure *failurepb.Failure) *schedspb.WatchWorkflowResponse {
	res := &schedspb.WatchWorkflowResponse{Status: r.workflowStatus}
	if result != nil {
		res.ResultFailure = &schedspb.WatchWorkflowResponse_Result{Result: result}
	} else if failure != nil {
		res.ResultFailure = &schedspb.WatchWorkflowResponse_Failure{Failure: failure}
	}
	return res
}
