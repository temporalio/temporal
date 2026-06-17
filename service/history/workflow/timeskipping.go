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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

	stateProp := &commonpb.TimeSkippingStatePropagation{
		InitialSkippedDuration: durationpb.New(accumulatedSkippedDuration(source)),
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

func (ms *MutableStateImpl) initTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
	timeSkippingStatePropagation *commonpb.TimeSkippingStatePropagation,
	currentEventID int64,
) {
	// we only need to init time skipping info if
	// either config is not nil or it has initial skip
	initialSkip := timeSkippingStatePropagation.GetInitialSkippedDuration()
	if config == nil && initialSkip == nil {
		return
	}
	ms.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		Config:                     config,
		AccumulatedSkippedDuration: initialSkip,
	}
	ms.wrapTimeSourceWithTimeSkipping()
	ms.shiftWorkflowTimes(initialSkip)
	ms.applyFastForward(currentEventID, timeSkippingStatePropagation.GetFastForwardTargetTime())
	ms.timeSkippingInfoUpdated = true
}

// updateTimeSkippingInfo updates the time skipping info with
// with new config and the event ID that updates the config
// we allow updating the config to nil when users want to remove the TSC
func (ms *MutableStateImpl) updateTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
	currentEventID int64,
) {
	ms.executionInfo.TimeSkippingInfo.Config = config
	// Options update: the new ff duration is a fresh budget measured from now.
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

func (ms *MutableStateImpl) hasInflightWorkToPreventTimeSkipping() (bool, string) {
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

// ShouldExecuteTimeSkipping checks if one mutable state should execute time skipping,
// i.e. there is no in-flight work and there is a time point to skip to.
func (ms *MutableStateImpl) shouldExecuteTimeSkipping() (bool, *timeSkippingTransition) {
	// configuration check
	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	if tsi == nil {
		return false, nil
	}
	config := tsi.GetConfig()
	if config == nil || !config.Enabled {
		return false, nil
	}

	// runtime check
	noSkippingReason := ""
	defer func() {
		if noSkippingReason != "" {
			ms.logger.Debug(fmt.Sprintf("time skipping skipped for: %s", noSkippingReason),
				tag.WorkflowID(ms.GetExecutionInfo().WorkflowId),
				tag.WorkflowRunID(ms.GetExecutionState().RunId),
			)
		}
	}()
	if !ms.IsWorkflowExecutionRunning() {
		noSkippingReason = "workflow is not running"
		return false, nil
	}
	if ms.IsWorkflowExecutionStatusPaused() {
		noSkippingReason = "workflow is paused"
		return false, nil
	}
	if hasPendingWork, detailedReason := ms.hasInflightWorkToPreventTimeSkipping(); hasPendingWork {
		noSkippingReason = fmt.Sprintf("pending work: %s", detailedReason)
		return false, nil
	}

	// Compute the transition early so we can short-circuit before allocating an event.
	// todo(@time-skipping): replace error with nil
	transition, err := ms.calculateTimeSkippingTransition()
	if err != nil {
		noSkippingReason = fmt.Sprintf("error calculating time skipping decision: %v", err)
		ms.logger.Error(
			"error calculating time skipping decision, and ignore this error and continue",
			tag.WorkflowID(ms.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(ms.GetExecutionState().RunId),
			tag.Error(err),
		)
		return false, nil
	}
	if !transition.isValid() {
		noSkippingReason = "time skipping has no candidate target time nor disabled after fast-forward flag"
		return false, nil
	}
	return true, &transition
}

type timeSkippingTransition struct {
	targetTime               time.Time
	disabledAfterFastForward bool
}

func (d timeSkippingTransition) isValid() bool {
	return !d.targetTime.IsZero() || d.disabledAfterFastForward
}

// calculateTimeSkippingTransition determines the next skip target.
// Candidates (in collection order): pending user timers, activity retry backoffs,
// workflow start-with-delay/CaN/retry backoff, and the fast-forward.
// The run/execution timeout is NOT a standalone candidate — it only applies as
// a cap: if any candidate wins, the skip target is clamped to min(target,
// runExpiry, execExpiry). This ensures we never advance virtual time past the
// workflow timeout, even when a user timer or fast-forward would otherwise overshoot.
func (ms *MutableStateImpl) calculateTimeSkippingTransition() (timeSkippingTransition, error) {
	var transition timeSkippingTransition
	advance := func(candidate time.Time, dueToFastForward bool) {
		if transition.targetTime.IsZero() || candidate.Before(transition.targetTime) {
			transition.targetTime = candidate
			transition.disabledAfterFastForward = dueToFastForward
		}
	}

	for _, timerInfo := range ms.GetPendingTimerInfos() {
		advance(timerInfo.ExpiryTime.AsTime(), false)
	}

	// Activities waiting out a retry backoff are skip targets: advance to the earliest
	// next-attempt time. No clock comparison is needed here — the idle check already
	// guarantees each pending activity's next attempt is in the future when we get here.
	for _, ai := range ms.GetPendingActivityInfos() {
		if activityPendingRetry(ai) && ms.Now().Before(ai.GetScheduledTime().AsTime()) {
			advance(ai.ScheduledTime.AsTime(), false)
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
			advance(executionTime, false)
		}
	}

	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	if !tsi.GetFastForwardInfo().GetHasReached() && tsi.GetFastForwardInfo().GetTargetTime() != nil {
		advance(tsi.GetFastForwardInfo().GetTargetTime().AsTime(), true)
	}

	// Cap any skip target at the run/execution timeout: never advance virtual time past
	// them. Timeouts alone do not create a skip target — only existing candidates
	// (timers, backoffs, fast-forward) do. This also handles the case where a user
	// timer fires past the workflow timeout: we cap the skip so the timeout fires on schedule.
	if !transition.targetTime.IsZero() {
		if t := ms.executionInfo.GetWorkflowRunExpirationTime(); t != nil && !t.AsTime().IsZero() {
			advance(t.AsTime(), false)
		}
		if t := ms.executionInfo.GetWorkflowExecutionExpirationTime(); t != nil && !t.AsTime().IsZero() {
			advance(t.AsTime(), false)
		}
	}
	return transition, nil
}

func (ms *MutableStateImpl) shiftWorkflowTimes(initialSkippedDuration *durationpb.Duration) {
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

func (ms *MutableStateImpl) closeTransactionHandleTimeSkipping(
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (regenTimerTasksForTimeSkipping bool) {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		if !ms.IsWorkflowExecutionRunning() {
			return false
		}
		if shouldExecute, transition := ms.shouldExecuteTimeSkipping(); shouldExecute {
			_, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
				ctx, transition.targetTime, transition.disabledAfterFastForward)
			if err != nil {
				ms.metricsHandler.Counter(metrics.ExecutionTimeSkippingTransitionedErrorCounter.Name()).Record(1)
				ms.logger.Error(
					"failed to add workflow execution time skipping transitioned event, and ignore this error and continue",
					tag.WorkflowID(ms.GetExecutionInfo().WorkflowId),
					tag.WorkflowRunID(ms.GetExecutionState().RunId),
					tag.Error(err),
				)
				return false
			}
			if transition.targetTime.IsZero() {
				return false
			}
			return true
		}
		return false
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

func (ms *MutableStateImpl) closeTransactionRegenerateTimerTasksForTimeSkipping(
	transactionPolicy historyi.TransactionPolicy,
) error {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		if !ms.IsWorkflowExecutionRunning() {
			return nil
		}
		return ms.taskGenerator.RegenerateTimerTasksForTimeSkipping()
	case historyi.TransactionPolicyPassive:
		return nil
	default:
		return serviceerror.NewInternalf("unknown transaction policy: %v", transactionPolicy)
	}
}
