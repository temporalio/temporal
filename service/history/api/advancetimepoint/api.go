package advancetimepoint

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/describeworkflow"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	historyservice "go.temporal.io/server/api/historyservice/v1"
)

// timePointKind indicates what type of time point was found.
type timePointKind int

const (
	timePointKindNone timePointKind = iota
	timePointKindUserTimer
	timePointKindActivityTimeout
	timePointKindWorkflowRunTimeout
	timePointKindWorkflowExecutionTimeout
)

// timePoint represents a single schedulable time point.
type timePoint struct {
	kind       timePointKind
	fireTime   time.Time
	timerSeqID workflow.TimerSequenceID // for user timers and activity timeouts
	timerID    string                   // user timer: TimerId
	activityID string                   // activity timeout: ActivityId
}

// toProto converts this time point to an UpcomingTimePointInfo proto.
func (tp timePoint) toProto() *workflowpb.UpcomingTimePointInfo {
	info := &workflowpb.UpcomingTimePointInfo{
		FireTime: timestamppb.New(tp.fireTime),
	}
	switch tp.kind {
	case timePointKindUserTimer:
		info.Source = &workflowpb.UpcomingTimePointInfo_Timer{
			Timer: &workflowpb.UpcomingTimePointInfo_TimerTimePoint{
				TimerId:        tp.timerID,
				StartedEventId: tp.timerSeqID.EventID,
			},
		}
	case timePointKindActivityTimeout:
		info.Source = &workflowpb.UpcomingTimePointInfo_ActivityTimeout{
			ActivityTimeout: &workflowpb.UpcomingTimePointInfo_ActivityTimeoutTimePoint{
				ActivityId:  tp.activityID,
				TimeoutType: tp.timerSeqID.TimerType,
			},
		}
	case timePointKindWorkflowRunTimeout:
		info.Source = &workflowpb.UpcomingTimePointInfo_WorkflowTimeout{
			WorkflowTimeout: &workflowpb.UpcomingTimePointInfo_WorkflowTimeoutTimePoint{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		}
	case timePointKindWorkflowExecutionTimeout:
		info.Source = &workflowpb.UpcomingTimePointInfo_WorkflowTimeout{
			WorkflowTimeout: &workflowpb.UpcomingTimePointInfo_WorkflowTimeoutTimePoint{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		}
	}
	return info
}

func Invoke(
	ctx context.Context,
	request *historyservice.AdvanceWorkflowExecutionTimePointRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.AdvanceWorkflowExecutionTimePointResponse, error) {
	advanceReq := request.GetAdvanceRequest()
	if _, err := api.GetActiveNamespace(shardCtx, namespace.ID(request.GetNamespaceId()), advanceReq.GetWorkflowExecution().GetWorkflowId()); err != nil {
		return nil, err
	}

	resp := &historyservice.AdvanceWorkflowExecutionTimePointResponse{}

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			advanceReq.GetWorkflowExecution().GetWorkflowId(),
			advanceReq.GetWorkflowExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			now := time.Now()
			executionInfo := mutableState.GetExecutionInfo()

			// Apply time-skipping config update if provided on the request.
			if newConfig := advanceReq.GetTimeSkippingConfig(); newConfig != nil {
				if err := api.ValidateAndApplyTimeSkippingConfig(mutableState, newConfig, now); err != nil {
					return nil, err
				}
			} else if executionInfo.GetTimeSkippingConfig() == nil {
				return nil, serviceerror.NewFailedPrecondition("time-skipping is not enabled for this workflow execution")
			}

			virtualNow := now.Add(executionInfo.GetVirtualTimeOffset().AsDuration())

			// Resolve the up_to target to an absolute virtual time (if set).
			var upToTarget time.Time
			switch v := advanceReq.GetUpTo().(type) {
			case *workflowservicepb.AdvanceWorkflowExecutionTimePointRequest_UpToTime:
				upToTarget = v.UpToTime.AsTime()
			case *workflowservicepb.AdvanceWorkflowExecutionTimePointRequest_UpToDuration:
				upToTarget = virtualNow.Add(v.UpToDuration.AsDuration())
			}

			// Find all time points at the earliest fire time.
			toFire := findTimePointsAtMinTime(mutableState)

			// Determine the effective advance target: min(next time point, up_to).
			var advanceTarget time.Time
			hasTimePoint := len(toFire) > 0
			if hasTimePoint && !upToTarget.IsZero() {
				if toFire[0].fireTime.Before(upToTarget) || toFire[0].fireTime.Equal(upToTarget) {
					advanceTarget = toFire[0].fireTime
				} else {
					// up_to is before next time point — advance to up_to, don't fire.
					advanceTarget = upToTarget
					toFire = nil
				}
			} else if hasTimePoint {
				advanceTarget = toFire[0].fireTime
			} else if !upToTarget.IsZero() {
				// No time points, but up_to is set — advance offset to up_to.
				advanceTarget = upToTarget
			} else {
				// No time points and no up_to — noop.
				resp.UpcomingTimePoints = describeworkflow.BuildUpcomingTimePoints(mutableState, executionInfo, virtualNow)
				resp.TimeSkippingInfo = describeworkflow.BuildTimeSkippingInfo(executionInfo, resp.UpcomingTimePoints, hasNonAutoSkipWork(mutableState), mutableState.IsWorkflowExecutionRunning(), virtualNow)
				return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
			}

			// Record the TIME_POINT_ADVANCED event.
			if _, err := mutableState.AddWorkflowExecutionTimePointAdvancedEvent(
				advanceTarget,
				advanceReq.GetIdentity(),
				advanceReq.GetRequestId(),
			); err != nil {
				return nil, err
			}

			// Fire all time points at this time (if any). Workflow timeouts are
			// sorted last so timers/activities fire before the workflow closes.
			for _, tp := range toFire {
				if !mutableState.IsWorkflowExecutionRunning() {
					break
				}
				resp.AdvancedTimePoints = append(resp.AdvancedTimePoints, tp.toProto())
				if err := fireTimePoint(mutableState, tp); err != nil {
					return nil, err
				}
			}

			// Recompute virtualNow after the advance (offset changed).
			virtualNow = now.Add(executionInfo.GetVirtualTimeOffset().AsDuration())
			resp.UpcomingTimePoints = describeworkflow.BuildUpcomingTimePoints(mutableState, executionInfo, virtualNow)
			resp.TimeSkippingInfo = describeworkflow.BuildTimeSkippingInfo(executionInfo, resp.UpcomingTimePoints, hasNonAutoSkipWork(mutableState), mutableState.IsWorkflowExecutionRunning(), virtualNow)

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shardCtx,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// findTimePointsAtMinTime does a single-pass O(n) scan over user timers,
// activity timeouts (excluding heartbeat), and workflow timeouts to find all
// time points at the earliest fire time. Workflow timeouts are placed at the
// end of the returned slice so they fire last.
//
// NOTE: Nexus operation timeouts and other CHASM/HSM state machine timers are
// not yet included; they will be added in a follow-up.
func findTimePointsAtMinTime(mutableState historyi.MutableState) []timePoint {
	var minTime time.Time
	var regular []timePoint   // timers, activity timeouts
	var wfTimeout []timePoint // workflow timeouts (fire last)

	consider := func(tp timePoint) {
		if tp.fireTime.IsZero() {
			return
		}
		isWfTimeout := tp.kind == timePointKindWorkflowRunTimeout || tp.kind == timePointKindWorkflowExecutionTimeout
		if minTime.IsZero() || tp.fireTime.Before(minTime) {
			minTime = tp.fireTime
			regular = regular[:0]
			wfTimeout = wfTimeout[:0]
			if isWfTimeout {
				wfTimeout = append(wfTimeout, tp)
			} else {
				regular = append(regular, tp)
			}
		} else if tp.fireTime.Equal(minTime) {
			if isWfTimeout {
				wfTimeout = append(wfTimeout, tp)
			} else {
				regular = append(regular, tp)
			}
		}
	}

	// User timers.
	for _, timerInfo := range mutableState.GetPendingTimerInfos() {
		expiryTime := timestamp.TimeValue(timerInfo.ExpiryTime)
		consider(timePoint{
			kind:     timePointKindUserTimer,
			fireTime: expiryTime,
			timerSeqID: workflow.TimerSequenceID{
				EventID:   timerInfo.GetStartedEventId(),
				Timestamp: expiryTime,
				TimerType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				Attempt:   1,
			},
			timerID: timerInfo.TimerId,
		})
	}

	// Activity timeouts (excluding heartbeat).
	for _, ai := range mutableState.GetPendingActivityInfos() {
		if ai.Paused {
			continue
		}
		considerActivityTimeouts(ai, consider)
	}

	// Workflow run timeout.
	execInfo := mutableState.GetExecutionInfo()
	runExp := timestamp.TimeValue(execInfo.WorkflowRunExpirationTime)
	if !runExp.IsZero() {
		consider(timePoint{
			kind:     timePointKindWorkflowRunTimeout,
			fireTime: runExp,
		})
	}

	// Workflow execution timeout.
	execExp := timestamp.TimeValue(execInfo.WorkflowExecutionExpirationTime)
	if !execExp.IsZero() {
		consider(timePoint{
			kind:     timePointKindWorkflowExecutionTimeout,
			fireTime: execExp,
		})
	}

	return append(regular, wfTimeout...)
}

// considerActivityTimeouts computes the non-heartbeat timeouts for a single
// activity and calls consider for each.
func considerActivityTimeouts(
	ai *persistencespb.ActivityInfo,
	consider func(timePoint),
) {
	if ai.ScheduledEventId == common.EmptyEventID {
		return
	}

	makeTP := func(kind timePointKind, t time.Time, timerType enumspb.TimeoutType) timePoint {
		return timePoint{
			kind:     kind,
			fireTime: t,
			timerSeqID: workflow.TimerSequenceID{
				EventID:   ai.ScheduledEventId,
				Timestamp: t,
				TimerType: timerType,
				Attempt:   ai.Attempt,
			},
			activityID: ai.ActivityId,
		}
	}

	// Schedule-to-start: only if not yet started.
	if ai.StartedEventId == common.EmptyEventID {
		if d := timestamp.DurationValue(ai.ScheduleToStartTimeout); d > 0 {
			t := timestamp.TimeValue(ai.ScheduledTime).Add(d)
			consider(makeTP(timePointKindActivityTimeout, t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START))
		}
	}

	// Schedule-to-close.
	if d := timestamp.DurationValue(ai.ScheduleToCloseTimeout); d > 0 {
		base := ai.FirstScheduledTime
		if base == nil {
			base = ai.ScheduledTime
		}
		t := timestamp.TimeValue(base).Add(d)
		consider(makeTP(timePointKindActivityTimeout, t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE))
	}

	// Start-to-close: only if started.
	if ai.StartedEventId != common.EmptyEventID {
		if d := timestamp.DurationValue(ai.StartToCloseTimeout); d > 0 {
			t := timestamp.TimeValue(ai.StartedTime).Add(d)
			consider(makeTP(timePointKindActivityTimeout, t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
		}
	}
}

// fireTimePoint fires the given time point by calling the appropriate mutable state method.
func fireTimePoint(mutableState historyi.MutableState, tp timePoint) error {
	switch tp.kind {
	case timePointKindUserTimer:
		return fireUserTimer(mutableState, tp.timerSeqID)
	case timePointKindActivityTimeout:
		return fireActivityTimeout(mutableState, tp.timerSeqID)
	case timePointKindWorkflowRunTimeout, timePointKindWorkflowExecutionTimeout:
		return fireWorkflowTimeout(mutableState)
	default:
		return serviceerror.NewInternal("unknown time point kind")
	}
}

func fireUserTimer(mutableState historyi.MutableState, seqID workflow.TimerSequenceID) error {
	timerInfo, ok := mutableState.GetUserTimerInfoByEventID(seqID.EventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("user timer not found for event ID %d", seqID.EventID))
	}
	_, err := mutableState.AddTimerFiredEvent(timerInfo.TimerId)
	return err
}

func fireActivityTimeout(mutableState historyi.MutableState, seqID workflow.TimerSequenceID) error {
	ai, ok := mutableState.GetActivityInfo(seqID.EventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("activity not found for event ID %d", seqID.EventID))
	}

	failureMsg := fmt.Sprintf(common.FailureReasonActivityTimeout, seqID.TimerType.String())
	timeoutFailure := failure.NewTimeoutFailure(failureMsg, seqID.TimerType)
	retryState, err := mutableState.RetryActivity(ai, timeoutFailure)
	if err != nil {
		return err
	}

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		// Activity will be retried; no timeout event needed.
		return nil
	}

	// Convert timeout type for historical consistency.
	if retryState == enumspb.RETRY_STATE_TIMEOUT && seqID.TimerType != enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		timeoutFailure = failure.NewTimeoutFailure(
			"Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying",
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		)
	}

	timeoutFailure.GetTimeoutFailureInfo().LastHeartbeatDetails = ai.LastHeartbeatDetails

	_, err = mutableState.AddActivityTaskTimedOutEvent(
		ai.ScheduledEventId,
		ai.StartedEventId,
		timeoutFailure,
		retryState,
	)
	return err
}

func fireWorkflowTimeout(mutableState historyi.MutableState) error {
	// For workflow timeouts, we use RETRY_STATE_TIMEOUT and no new execution.
	_, err := mutableState.AddTimeoutWorkflowEvent(
		mutableState.GetNextEventID(),
		enumspb.RETRY_STATE_TIMEOUT,
		"",
	)
	return err
}

// TryAutoSkip checks whether auto-skip should fire and, if so, advances to the
// next time point. Returns true if a time point was fired (caller should schedule
// a new workflow task). The caller must already hold the workflow lock.
//
// Auto-skip fires when ALL of these are true:
//   - auto_skip config is set on the workflow
//   - workflow is running
//   - no pending activities (workflow is "idle")
//   - firings_used < effective max_firings
//   - there is at least one upcoming time point
//   - the next fire time is within the deadline (if set)
func TryAutoSkip(mutableState historyi.MutableState) (bool, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return false, nil
	}

	executionInfo := mutableState.GetExecutionInfo()
	autoSkip := executionInfo.GetTimeSkippingConfig().GetAutoSkip()
	if autoSkip == nil {
		return false, nil
	}

	// Pause auto-skip when activities, child workflows, or Nexus
	// operations are in-flight.
	if hasNonAutoSkipWork(mutableState) {
		return false, nil
	}

	// Check firings limit.
	maxFirings := api.EffectiveAutoSkipMaxFirings(autoSkip)
	if executionInfo.GetAutoSkipFiringsUsed() >= maxFirings {
		return false, nil
	}

	// Find next time point(s).
	toFire := findTimePointsAtMinTime(mutableState)
	if len(toFire) == 0 {
		return false, nil
	}

	// Check deadline (nil means unbounded).
	if dl := executionInfo.GetAutoSkipDeadline(); dl != nil && dl.IsValid() {
		if toFire[0].fireTime.After(dl.AsTime()) {
			return false, nil
		}
	}

	// Record TIME_POINT_ADVANCED event.
	if _, err := mutableState.AddWorkflowExecutionTimePointAdvancedEvent(
		toFire[0].fireTime,
		"auto-skip",
		uuid.NewString(),
	); err != nil {
		return false, err
	}

	// Fire all time points at this time.
	fired := int32(0)
	for _, tp := range toFire {
		if !mutableState.IsWorkflowExecutionRunning() {
			break
		}
		if err := fireTimePoint(mutableState, tp); err != nil {
			return false, err
		}
		fired++
	}

	executionInfo.AutoSkipFiringsUsed += fired
	return true, nil
}

// hasNonAutoSkipWork returns true if there are any pending activities, child
// workflows, or Nexus operations that should pause auto-skip. Advancing time
// while external work is in-flight would fire their timeouts unexpectedly.
func hasNonAutoSkipWork(mutableState historyi.MutableState) bool {
	if len(mutableState.GetPendingActivityInfos()) > 0 {
		return true
	}
	if len(mutableState.GetPendingChildExecutionInfos()) > 0 {
		return true
	}
	opColl := nexusoperations.MachineCollection(mutableState.HSM())
	for _, node := range opColl.List() {
		op, err := opColl.Data(node.Key.ID)
		if err != nil {
			// Can't load state — conservatively treat as pending.
			return true
		}
		switch op.State() {
		case enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
			enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
			enumsspb.NEXUS_OPERATION_STATE_STARTED:
			return true
		}
	}
	return false
}
