package polltimeskipping

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/ffnotifier"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionTimeSkippingRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	ffNotifier ffnotifier.Notifier,
) (*historyservice.PollWorkflowExecutionTimeSkippingResponse, error) {
	if err := api.ValidateNamespaceUUID(namespace.ID(req.GetNamespaceId())); err != nil {
		return nil, err
	}
	execution := req.GetRequest().GetWorkflowExecution()
	requestedFFID := req.GetRequest().GetFastForwardId()
	ns, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	// Subscribe before reading current state so any fast-forward update persisted
	// after our read still delivers a wake-up on the channel (no lost notification).
	watchKey := ffnotifier.NewKey(req.GetNamespaceId(), execution.GetWorkflowId())
	subscriberID, channel, err := ffNotifier.Watch(watchKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = ffNotifier.Unwatch(watchKey, subscriberID) }()

	// an initial read to see we can short-circuit the polling
	ffinfo, wfClosed, err := readFastForwardInfo(ctx, req, workflowConsistencyChecker)
	if err != nil {
		return nil, err
	}
	if ffinfo == nil || ffinfo.GetFastForwardId() != requestedFFID {
		return newResponse(ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND), nil
	}
	if ffinfo.GetHasCompleted() {
		return newResponse(ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_WORKFLOW_END_BEFORE_FAST_FORWARD_COMPLETION), nil
	}
	if wfClosed && !ffinfo.GetHasCompleted() {
		return newResponse(ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND), nil
	}

	// Fast-forward is pending and matches the request: long-poll for a change.
	// The workflow lease was released by readFastForwardInfo, so the wait does not
	// block the workflow.
	softTimeout := shard.GetConfig().LongPollExpirationInterval(ns.Name().String())
	updatedFFInfo, result := waitFastForwardNotification(ctx, channel, softTimeout, requestedFFID, ffinfo)
	return newResponse(updatedFFInfo, result), nil
}

// readFastForwardInfo acquires the workflow lease, reads the current fast-forward
// info, and releases the lease before returning, so the caller can long-poll
// without holding the lease.
func readFastForwardInfo(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionTimeSkippingRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *commonpb.TimeSkippingFastForwardInfo, closed bool, retError error) {
	execution := req.GetRequest().GetWorkflowExecution()
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(req.GetNamespaceId(), execution.GetWorkflowId(), execution.GetRunId()),
		locks.PriorityLow, // testing api
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	ms := workflowLease.GetMutableState()
	ffinfo := workflow.NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo()).ToFastForwardInfo()
	// A closed run with no continuation (retry / cron / CaN set NewExecutionRunId)
	// can never complete a pending fast-forward.
	closed = !ms.IsWorkflowExecutionRunning() && ms.GetExecutionInfo().GetNewExecutionRunId() == ""
	return ffinfo, closed, nil
}

// waitFastForwardNotification blocks until a fast-forward update for the polled
// execution arrives, the requested fast-forward is overridden or completed, or
// the soft timeout expires. pending is the fast-forward info observed at read
// time; it is returned unchanged on timeout so the caller can re-poll.
func waitFastForwardNotification(
	ctx context.Context,
	channel <-chan *ffnotifier.Notification,
	softTimeout time.Duration,
	requestedFFID string,
	pending *commonpb.TimeSkippingFastForwardInfo,
) (*commonpb.TimeSkippingFastForwardInfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_Result) {
	stCtx, stCancel := context.WithTimeout(ctx, softTimeout)
	defer stCancel()

	for {
		select {
		case notification := <-channel:
			ffinfo := notification.FastForwardInfo
			if ffinfo.GetHasCompleted() {
				return ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_COMPLETED
			}
			if ffinfo.GetFastForwardId() != requestedFFID {
				// A new fast-forward replaced the one being polled.
				return ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_OVERRIDDEN
			}
			if notification.Closed {
				// The run ended without completing the fast-forward: it never will.
				return ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND
			}
			// Same pending fast-forward (no meaningful change); keep waiting.
		case <-stCtx.Done():
			return pending, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_POLL_TIMEOUT
		}
	}
}

func newResponse(
	ffinfo *commonpb.TimeSkippingFastForwardInfo,
	result workflowservice.PollWorkflowExecutionTimeSkippingResponse_Result,
) *historyservice.PollWorkflowExecutionTimeSkippingResponse {
	return &historyservice.PollWorkflowExecutionTimeSkippingResponse{
		Response: &workflowservice.PollWorkflowExecutionTimeSkippingResponse{
			FastForwardInfo: ffinfo,
			Result:          result,
		},
	}
}
