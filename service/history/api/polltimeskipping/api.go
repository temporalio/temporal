package polltimeskipping

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/notification"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionTimeSkippingRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	ffNotifier notification.FastForwardNotifier,
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

	// step-1: Subscribe before reading current state so any fast-forward update persisted
	// after our read still delivers a wake-up on the channel (no lost notification).
	watchKey := notification.NewKey(req.GetNamespaceId(), execution.GetWorkflowId())
	subscriberID, channel, err := ffNotifier.Watch(watchKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = ffNotifier.Unwatch(watchKey, subscriberID) }()

	// step-2: an initial read to see we can short-circuit the polling
	ffinfo, wfClosed, err := readFastForwardInfo(ctx, req, workflowConsistencyChecker)
	if err != nil {
		return nil, err
	}
	if ffinfo == nil || ffinfo.GetFastForwardId() != requestedFFID {
		return newResponse(ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND), nil
	}
	if ffinfo.GetHasCompleted() {
		return newResponse(ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_COMPLETED), nil
	}
	// The run ended before the fast-forward completed: it never will.
	if wfClosed {
		return newResponse(ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_WORKFLOW_END_BEFORE_FAST_FORWARD_COMPLETION), nil
	}

	// step-3: Fast-forward is pending and matches the request: long-poll for a change.
	//
	softTimeout := shard.GetConfig().LongPollExpirationInterval(ns.Name().String())
	updatedFFInfo, result, err := waitFastForwardNotification(ctx, channel, softTimeout, requestedFFID, ffinfo)
	if err != nil {
		return nil, err
	}
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
// execution arrives. The workflow lease is not hold in this step so as not to block the workflow.
func waitFastForwardNotification(
	ctx context.Context,
	channel <-chan *notification.FastForwardNotification,
	softTimeout time.Duration,
	requestedFFID string,
	pending *commonpb.TimeSkippingFastForwardInfo,
) (*commonpb.TimeSkippingFastForwardInfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_Result, error) {
	stCtx, stCancel := context.WithTimeout(ctx, softTimeout)
	defer stCancel()

	for {
		select {
		case notif, ok := <-channel:
			if !ok {
				// The pub-sub notifier never closes a live subscription's channel; a closed
				// channel means we were handed a dead one (e.g. a misconfigured noop notifier),
				// which should never happen for a real poll.
				return nil, 0, serviceerror.NewInternal("fast-forward notification channel closed unexpectedly")
			}
			ffinfo := notif.FastForwardInfo
			switch {
			case ffinfo == nil || ffinfo.GetFastForwardId() != requestedFFID:
				// there is a edge case users udpate fast forward with the same ID again with a different target time
				// we detect this by comparing versioned transition in the ffinfo, but right now we don't do this
				// with the assumption that if the fast-forward id is the same, the user may still want to wait on it
				return ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND, nil
			case ffinfo.GetHasCompleted():
				return ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_COMPLETED, nil
			case notif.WorkflowExecutionCompleted:
				return ffinfo, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_WORKFLOW_END_BEFORE_FAST_FORWARD_COMPLETION, nil
			default:
				// False alert (benign config write / skip transition / continuation run-stop):
				// the pending fast-forward is unchanged, so keep waiting.
				continue
			}
		case <-stCtx.Done():
			return pending, workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_POLL_TIMEOUT, nil
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
