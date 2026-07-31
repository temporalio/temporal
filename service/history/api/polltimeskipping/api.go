package polltimeskipping

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/notification"
	"go.temporal.io/server/service/history/workflow"
)

// Reasons reported alongside FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED. They are part of the
// API response, so they describe the cause in caller terms rather than naming server internals.
const (
	failedReasonIDMismatch           = "the requested fast_forward_id does not match the execution's current fast-forward"
	failedReasonReset                = "the fast-forward was reset"
	failedReasonExecutionEnded       = "the execution ended before the fast-forward completed"
	failedReasonTimeSkippingDisabled = "time skipping was disabled before the fast-forward completed"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionTimeSkippingRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	ffNotifier notification.TimeSkippingFastForwardNotifier,
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
	watchKey := notification.NewTimeSkippingNotificationKey(
		req.GetNamespaceId(), execution.GetWorkflowId(), chasm.WorkflowArchetypeID)
	subscriberID, channel, err := ffNotifier.Watch(watchKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = ffNotifier.Unwatch(watchKey, subscriberID) }()

	// step-2: an initial read to see we can short-circuit the polling
	ffinfo, tsDisabled, wfClosed, err := readFastForwardInfo(ctx, req, workflowConsistencyChecker)
	if err != nil {
		return nil, err
	}
	if ffinfo == nil || ffinfo.GetFastForwardId() != requestedFFID {
		return newFailedResponse(ffinfo, failedReasonIDMismatch), nil
	}
	if ffinfo.GetHasCompleted() {
		return newResponse(ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED, ""), nil
	}
	if wfClosed {
		return newFailedResponse(ffinfo, failedReasonExecutionEnded), nil
	}
	if tsDisabled {
		return newFailedResponse(ffinfo, failedReasonTimeSkippingDisabled), nil
	}

	// step-3: Fast-forward is pending and matches the request: long-poll for a change.
	softTimeout := shard.GetConfig().LongPollExpirationInterval(ns.Name().String())
	updatedFFInfo, result, failedReason, err := waitFastForwardNotification(ctx, shard.GetLifecycleContext(), channel, softTimeout, requestedFFID, ffinfo)
	if err != nil {
		return nil, err
	}
	return newResponse(updatedFFInfo, result, failedReason), nil
}

// readFastForwardInfo acquires the workflow lease, reads the current fast-forward
// info, and releases the lease before returning, so the caller can long-poll
// without holding the lease.
func readFastForwardInfo(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionTimeSkippingRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *commonpb.TimeSkippingFastForwardInfo, tsDisabled bool, closed bool, retError error) {
	execution := req.GetRequest().GetWorkflowExecution()
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(req.GetNamespaceId(), execution.GetWorkflowId(), execution.GetRunId()),
		locks.PriorityLow, // testing api
	)
	if err != nil {
		return nil, false, false, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	ms := workflowLease.GetMutableState()
	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	ffinfo := workflow.NewTimeSkippingInfoUtil(tsi).ToFastForwardInfo()
	tsDisabled = !tsi.GetConfig().GetEnabled()
	// A closed run with no continuation (retry / cron / CaN set NewExecutionRunId)
	// can never complete a pending fast-forward.
	closed = !ms.IsWorkflowExecutionRunning() && ms.GetExecutionInfo().GetNewExecutionRunId() == ""
	return ffinfo, tsDisabled, closed, nil
}

// waitFastForwardNotification blocks until a fast-forward update for the polled
// execution arrives. The workflow lease is not hold in this step so as not to block the workflow.
func waitFastForwardNotification(
	ctx context.Context,
	shardLifecycleCtx context.Context,
	channel <-chan *notification.TimeSkippingFastForwardNotification,
	softTimeout time.Duration,
	requestedFFID string,
	pending *commonpb.TimeSkippingFastForwardInfo,
) (*commonpb.TimeSkippingFastForwardInfo, enumspb.FastForwardPollingResult, string, error) {
	stCtx, stCancel := context.WithTimeout(ctx, softTimeout)
	defer stCancel()

	for {
		select {
		// (1) A notification arrived.
		case notif, ok := <-channel:
			if !ok {
				// The pub-sub notifier never closes a live subscription's channel; a closed
				// channel means we were handed a dead one (e.g. a misconfigured noop notifier),
				// which should never happen for a real poll.
				return nil, enumspb.FAST_FORWARD_POLLING_RESULT_UNSPECIFIED, "", serviceerror.NewInternal("fast-forward notification channel closed unexpectedly")
			}
			ffinfo := workflow.NewTimeSkippingInfoUtil(notif.GetTimeSkippingInfo()).ToFastForwardInfo()
			tsDisabled := !notif.GetTimeSkippingInfo().GetConfig().GetEnabled()
			switch {
			// should always verify ffid first
			case ffinfo == nil || ffinfo.GetFastForwardId() != requestedFFID:
				return ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, failedReasonIDMismatch, nil
			case !ffinfo.GetTargetTime().AsTime().Equal(pending.GetTargetTime().AsTime()):
				return ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, failedReasonReset, nil
			case ffinfo.GetHasCompleted():
				return ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED, "", nil
			case notif.GetWorkflowExecutionCompleted():
				return ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, failedReasonExecutionEnded, nil
			case tsDisabled:
				return ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, failedReasonTimeSkippingDisabled, nil
			default:
				continue
			}
		// (2) long-poll budget elapsed / (4) caller's context ended. stCtx = WithTimeout(ctx,
		// softTimeout); softTimeout is the server-side budget, configured shorter than the caller's
		// RPC deadline (as in pollupdate's WaitLifecycleStage), so it normally fires first and we
		// return a graceful poll-timeout. If instead the caller's context ended first (shorter
		// deadline / client disconnect / shutdown), propagate that error rather than faking a timeout.
		case <-stCtx.Done():
			if ctx.Err() != nil {
				return nil, enumspb.FAST_FORWARD_POLLING_RESULT_UNSPECIFIED, "", ctx.Err()
			}
			return pending, enumspb.FAST_FORWARD_POLLING_RESULT_POLL_TIMEOUT, "", nil
		// (3) The shard moved or closed. Release promptly instead of holding the subscriber slot
		// until the soft timeout; the client's retry routes to the new shard owner.
		case <-shardLifecycleCtx.Done():
			return pending, enumspb.FAST_FORWARD_POLLING_RESULT_POLL_TIMEOUT, "", nil
		}
	}
}

func newFailedResponse(
	ffinfo *commonpb.TimeSkippingFastForwardInfo,
	failedReason string,
) *historyservice.PollWorkflowExecutionTimeSkippingResponse {
	return newResponse(ffinfo, enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED, failedReason)
}

func newResponse(
	ffinfo *commonpb.TimeSkippingFastForwardInfo,
	result enumspb.FastForwardPollingResult,
	failedReason string,
) *historyservice.PollWorkflowExecutionTimeSkippingResponse {
	return &historyservice.PollWorkflowExecutionTimeSkippingResponse{
		Response: &workflowservice.PollWorkflowExecutionTimeSkippingResponse{
			FastForwardInfo:          ffinfo,
			FastForwardPollingResult: result,
			FailedReason:             failedReason,
		},
	}
}
