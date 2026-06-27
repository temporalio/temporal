package workflow

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/components/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// =============================================================================
// Time Skipping Configuration Management
// =============================================================================
func (ms *MutableStateImpl) initTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
	timeSkippingStatePropagation *commonpb.TimeSkippingStatePropagation,
	currentEventID int64,
) {
	initialSkip := timeSkippingStatePropagation.GetInitialSkippedDuration()
	if config == nil && initialSkip == nil {
		return
	}

	ms.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		Config:                     config,
		AccumulatedSkippedDuration: initialSkip,
	}
	ms.wrapTimeSourceWithTimeSkipping()
	ms.wrapExecutionTimes(initialSkip)
	ms.applyFastForward(currentEventID, timeSkippingStatePropagation.GetFastForwardTargetTime())
	ms.timeSkippingInfoUpdated = true
}

func (ms *MutableStateImpl) updateTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
	currentEventID int64,
) {
	ms.executionInfo.TimeSkippingInfo.Config = config
	ms.applyFastForward(currentEventID, nil)
	ms.timeSkippingInfoUpdated = true
}

// applyFastForward (re)computes the FastForwardInfo using the new TimeSkippingConfig (TSC) and propagated time-skippingstates.
// This method should be called whenever the TimeSkippingConfig is initialized or updated.
// An invariant of the FastForwardInfo is that after this method is called, if the current TSC has a FastForward value,
// the FastForwardInfo should never be nil.
func (ms *MutableStateImpl) applyFastForward(currentEventID int64, propagatedTargetTime *timestamppb.Timestamp) {

	tsc := ms.GetExecutionInfo().GetTimeSkippingInfo().GetConfig()
	tsi := ms.executionInfo.TimeSkippingInfo

	// clear fast forward if disabled or zero max_elapsed_duration
	if !tsc.GetEnabled() || tsc.GetFastForward().AsDuration() <= 0 {
		if tsi.FastForwardInfo != nil {
			tsi.FastForwardInfo = nil
		}
		return
	}

	var targetTime time.Time
	if propagatedTargetTime != nil {
		targetTime = propagatedTargetTime.AsTime()
	} else {
		// if there is no propagated target time,
		// fast-forward refers to a new duration from now.
		targetTime = ms.Now().Add(tsc.GetFastForward().AsDuration())
	}

	// always install a fresh fast-forward bound
	tsi.FastForwardInfo = &persistencespb.FastForwardInfo{
		TargetTime:    timestamppb.New(targetTime),
		SourceEventId: currentEventID,
		HasReached:    false,
	}
	ms.AddTasks(&tasks.TimeSkippingTimerTask{
		WorkflowKey:         ms.GetWorkflowKey(),
		VisibilityTimestamp: targetTime,
		EventID:             currentEventID,
	})
}

func (ms *MutableStateImpl) wrapExecutionTimes(initialSkippedDuration *durationpb.Duration) {
	if initialSkippedDuration == nil || initialSkippedDuration.AsDuration() == 0 {
		return
	}
	accum := initialSkippedDuration.AsDuration()
	if !timeNotSet(ms.executionState.StartTime) {
		ms.executionState.StartTime = timestamppb.New(ms.executionState.StartTime.AsTime().Add(accum))
	}
	if !timeNotSet(ms.executionInfo.StartTime) {
		ms.executionInfo.StartTime = timestamppb.New(ms.executionInfo.StartTime.AsTime().Add(accum))
	}
	if !timeNotSet(ms.executionInfo.ExecutionTime) {
		ms.executionInfo.ExecutionTime = timestamppb.New(ms.executionInfo.ExecutionTime.AsTime().Add(accum))
	}
	if !timeNotSet(ms.executionInfo.WorkflowRunExpirationTime) {
		ms.executionInfo.WorkflowRunExpirationTime = timestamppb.New(ms.executionInfo.WorkflowRunExpirationTime.AsTime().Add(accum))
	}
	if !timeNotSet(ms.executionInfo.WorkflowExecutionExpirationTime) {
		ms.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(ms.executionInfo.WorkflowExecutionExpirationTime.AsTime().Add(accum))
	}
}

// -- Propagation Methods of Time Skipping

// propagateTimeSkippingToNextRun propagates both time skipping config and state to the next run in
// the chain (CaN, retry, cron). The config is deep-cloned so the next run can mutate it without
// affecting the source.
func propagateTimeSkippingToNextRun(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	previousTSC := source.GetTimeSkippingInfo().GetConfig()

	// if disabled, we just return nil for the new TSC
	var newTSC *commonpb.TimeSkippingConfig
	if previousTSC.GetEnabled() {
		newTSC = common.CloneProto(previousTSC)
	}

	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum := accumulatedSkippedDuration(source); accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
		}
	}

	if ff := source.GetTimeSkippingInfo().GetFastForwardInfo(); ff != nil && !ff.GetHasReached() {
		stateProp.FastForwardTargetTime = ff.GetTargetTime()
	}
	return newTSC, stateProp
}

// propagateTimeSkippingToChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// FastForward is never propagated to children.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	accum := accumulatedSkippedDuration(source)
	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
		}
	}

	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	disableChildPropagation := source.GetTimeSkippingInfo().GetConfig().GetDisableChildPropagation()
	if !enabled || disableChildPropagation {
		return nil, stateProp
	}

	return &commonpb.TimeSkippingConfig{
		Enabled: enabled,
	}, stateProp
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}

// =============================================================================
// Time Skipping Runtime Time Impacts
// =============================================================================
// wrapTimeSourceWithTimeSkipping wraps ms.timeSource (and the hBuilder's copy) with a time-skipping
// wrapper. The closure captures ms so the offset tracks ms.executionInfo.TimeSkippingInfo as it
// evolves — no need to re-wrap when TimeSkippingInfo is created or replaced. Called once per MS
// lifetime from the constructors; the type-assertion guard makes any repeat call a no-op.
func (ms *MutableStateImpl) wrapTimeSourceWithTimeSkipping() {
	if _, ok := ms.timeSource.(*clock.TimeSkippingTimeSourceWrapper); ok {
		return
	}
	ms.timeSource = clock.WrapTimeSourceWithTimeSkipping(
		ms.timeSource, ms.accumulatedSkippedDuration)
	ms.hBuilder.SetTimeSource(ms.timeSource)
}

func (ms *MutableStateImpl) accumulatedSkippedDuration() time.Duration {
	return accumulatedSkippedDuration(ms.executionInfo)
}

// =============================================================================
// Time Skipping Runtime Data Structure
// =============================================================================
type TimeSkippingTransition struct {
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// NewTimeSkippingTransition creates a new time-skipping transition with the current time.
// Methods provided by this data structure cannot be used without a current time.
func NewTimeSkippingTransition(currentTime time.Time) *TimeSkippingTransition {
	return &TimeSkippingTransition{CurrentTime: currentTime}
}

// IsValid reports whether the transition is worth applying: a real skip target, or a bare disable
// signal. Nil-safe.
func (t *TimeSkippingTransition) IsValid() bool {
	return t != nil && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

// TrackEarliestFutureTime tracks the earliest future time from the candidate,
// and the final target time will be the target time to skip to.
func (t *TimeSkippingTransition) TrackEarliestFutureTime(candidate time.Time) {
	if t == nil || t.CurrentTime.IsZero() || candidate.IsZero() || candidate.Before(t.CurrentTime) {
		return
	}
	if t.TargetTime.IsZero() || candidate.Before(t.TargetTime) {
		t.TargetTime = candidate
	}
}

func (t *TimeSkippingTransition) GateByFastForward(ff *persistencespb.FastForwardInfo) {
	if ff.GetHasReached() || ff.GetTargetTime().AsTime().IsZero() {
		return
	}
	if t == nil || t.CurrentTime.IsZero() {
		return
	}
	ffTargetTime := ff.GetTargetTime().AsTime()
	t.TrackEarliestFutureTime(ffTargetTime)
	if !ffTargetTime.After(t.CurrentTime) {
		t.DisabledAfterFastForward = true
	}
}

// =============================================================================
// Time Skipping Runtime Methods for Workflow-based Executions
// =============================================================================

// Workflow-based execution time skipping checks:
func (ms *MutableStateImpl) hasInflightWork() (bool, string) {
	// HasPendingWorkflowTask covers both normal and speculative workflow tasks
	if ms.HasPendingWorkflowTask() {
		return true, "has pending workflow task"
	}
	// A pending activity blocks time skipping unless it has failed and is still
	// waiting out its retry backoff (next attempt strictly in the future) — that one
	// is a skip target, not in-flight work (see calculateTimeSkippingTransition). The
	// strict future check is what keeps a just-scheduled or already-due activity (next
	// attempt <= now) blocking.
	for _, ai := range ms.GetPendingActivityInfos() {
		// if this activity is just a retry with backoff scheduled in the future
		if activityPendingRetry(ai) && ms.Now().Before(ai.GetScheduledTime().AsTime()) {
			continue
		}
		return true, "has pending activity"
	}
	if nexusoperations.MachineCollection(ms.HSM()).Size() > 0 {
		return true, "has pending nexus operations"
	}
	if len(ms.GetPendingChildExecutionInfos()) > 0 {
		return true, "has pending child execution"
	}
	if len(ms.GetPendingSignalExternalInfos()) > 0 {
		return true, "has pending signal external"
	}
	if len(ms.GetPendingRequestCancelExternalInfos()) > 0 {
		return true, "has pending request cancel external"
	}
	return false, ""
}

func (ms *MutableStateImpl) isWorkflowSkippable() bool {
	noSkippingReason := ""
	defer func() {
		if noSkippingReason != "" {
			ms.logger.Debug(fmt.Sprintf("time skipping skipped for: %s", noSkippingReason),
				tag.WorkflowID(ms.GetExecutionInfo().WorkflowId),
				tag.WorkflowRunID(ms.GetExecutionState().RunId),
			)
		}
	}()

	// (1) gate by time skipping configuration
	tsc := ms.GetExecutionInfo().GetTimeSkippingInfo().GetConfig()
	if tsc == nil || !tsc.Enabled {
		noSkippingReason = "time skipping is not enabled"
		return false
	}

	// (2) gate by workflow state and status
	if !ms.IsWorkflowExecutionRunning() {
		noSkippingReason = "workflow is not running"
		return false
	}
	if ms.IsWorkflowExecutionStatusPaused() {
		noSkippingReason = "workflow is paused"
		return false
	}

	// (3) gate by inflight work
	if hasPendingWork, detailedReason := ms.hasInflightWork(); hasPendingWork {
		noSkippingReason = fmt.Sprintf("pending work: %s", detailedReason)
		return false
	}
	return true
}

func (ms *MutableStateImpl) calculateWorkflowTimeSkippingTransition() (*TimeSkippingTransition, error) {

	// 1: gate by skippable check
	if !ms.isWorkflowSkippable() {
		return nil, nil
	}

	// 2: find next skip target, is there is no target, time skipping is not needed
	var transition *TimeSkippingTransition
	for _, timerInfo := range ms.GetPendingTimerInfos() {
		transition.TrackEarliestFutureTime(timerInfo.ExpiryTime.AsTime())
	}

	// Activities waiting out a retry backoff are skip targets: advance to the earliest
	// next-attempt time. No clock comparison is needed here — the idle check already
	// guarantees each pending activity's next attempt is in the future when we get here.
	for _, ai := range ms.GetPendingActivityInfos() {
		if activityPendingRetry(ai) && ms.Now().Before(ai.GetScheduledTime().AsTime()) {
			transition.TrackEarliestFutureTime(ai.ScheduledTime.AsTime())
		}
	}
	if !ms.HadOrHasWorkflowTask() {
		// Support start-with-delay, cron, retry, and CaN-with-backoff: the workflow is
		// waiting on a WorkflowBackoffTimerTask. Two extra checks are needed:
		//   - ExecutionTime > StartTime: a backoff is actually configured (FirstWorkflowTaskBackoff > 0).
		//     For child workflows, !HadOrHasWorkflowTask is also true between "start event applied"
		//     and "ScheduleWorkflowTask API call" but no backoff exists, so ExecutionTime == StartTime.
		//   - ExecutionTime > ms.Now(): the candidate is in the (virtual) future. Defends against
		//     CaN-with-backoff that inherits accumulated > backoff — past candidates would produce
		//     a negative delta in ApplyWorkflowExecutionTimeSkippingTransitionedEvent and decrement accumulated.
		executionTime := ms.executionInfo.GetExecutionTime().AsTime()
		startTime := ms.executionInfo.GetStartTime().AsTime()
		if executionTime.After(startTime) && executionTime.After(ms.Now()) {
			transition.TrackEarliestFutureTime(executionTime)
		}
	}
	// allow skipping to the run/execution timeout
	if t := ms.executionInfo.GetWorkflowRunExpirationTime(); t != nil && !t.AsTime().IsZero() {
		transition.TrackEarliestFutureTime(t.AsTime())
	}
	if t := ms.executionInfo.GetWorkflowExecutionExpirationTime(); t != nil && !t.AsTime().IsZero() {
		transition.TrackEarliestFutureTime(t.AsTime())
	}

	// fast-forward is also a target time, and this is the furthest target time a time skipping can skip to
	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	if !tsi.GetFastForwardInfo().GetHasReached() && tsi.GetFastForwardInfo().GetTargetTime() != nil {
		transition.GateByFastForward(tsi.GetFastForwardInfo())
	}

	if transition.IsValid() {
		return transition, nil
	}
	return nil, nil
}

func (ms *MutableStateImpl) closeTransactionHandleWorkflowTimeSkipping(
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (needRegenTasks bool) {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		// 1. calculate the time skipping transition (all conditions gated inside it)
		transition, err := ms.calculateWorkflowTimeSkippingTransition()
		if err != nil {
			ms.logger.Error("failed to calculate time-skipping transition", tag.Error(err))
			return false
		}
		if !transition.IsValid() {
			return false
		}
		// 2. state change
		_, err = ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
			ctx, transition.TargetTime, transition.DisabledAfterFastForward)
		if err != nil {
			ms.logger.Error("failed to add workflow execution time skipping transitioned event", tag.Error(err))
			return false
		}
		// 3. task regeneration
		return true
	case historyi.TransactionPolicyPassive:
		return false
	default:
		ms.logger.Error(fmt.Sprintf("closeTransactionHandleTimeSkipping: unknown transaction policy: %v", transactionPolicy),
			tag.WorkflowID(ms.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(ms.GetExecutionState().RunId),
		)
		return false
	}
}

func (ms *MutableStateImpl) closeTransactionRegenTimerTasksForWorkflowTimeSkipping(
	transactionPolicy historyi.TransactionPolicy,
) error {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		return ms.taskGenerator.RegenerateTimerTasksForTimeSkipping()
	case historyi.TransactionPolicyPassive:
		return nil
	default:
		return serviceerror.NewInternalf("unknown transaction policy: %v", transactionPolicy)
	}
}
