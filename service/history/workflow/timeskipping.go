package workflow

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
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
// initTimeSkippingInfo can be called either when (1) a user starts an execution or
// (2) another run is started with in the same execution (the workflow chain-of-runs model)
// `propagatedState` is only set in the latter case.
func (ms *MutableStateImpl) initTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
	propagatedState *commonpb.TimeSkippingStatePropagation,
) {
	initialSkip := propagatedState.GetInitialSkippedDuration()
	if config == nil && initialSkip == nil {
		return
	}
	tsi := &persistencespb.TimeSkippingInfo{
		Config:                     config,
		AccumulatedSkippedDuration: initialSkip,
		SessionSkipCount:           propagatedState.GetInitialSkipCount(),
	}
	ms.executionInfo.TimeSkippingInfo = tsi
	ms.wrapTimeSourceWithTimeSkipping()
	ms.wrapExecutionTimes(initialSkip)
	ms.applyFastForward(propagatedState.GetFastForwardTargetTime())
	ms.timeSkippingInfoUpdated = true
}

func (ms *MutableStateImpl) updateTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
) {
	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if tsi == nil {
		return
	}
	ms.executionInfo.TimeSkippingInfo.Config = config
	if !config.GetEnabled() {
		tsi.StopReason = enumspb.TIME_SKIPPING_STOP_REASON_USER_DISABLED
	}
	ms.applyFastForward(nil)
	ms.timeSkippingInfoUpdated = true
	tsi.SessionSkipCount = 0
}

// applyFastForward (re)computes the FastForwardInfo using the new TimeSkippingConfig (TSC) and propagated time-skippingstates.
// This method should be called whenever the TimeSkippingConfig is initialized or updated.
// An invariant of the FastForwardInfo is that after this method is called, if the current TSC has a FastForward value,
// the FastForwardInfo should never be nil.
func (ms *MutableStateImpl) applyFastForward(propagatedTargetTime *timestamppb.Timestamp) {
	tsc := ms.GetExecutionInfo().GetTimeSkippingInfo().GetConfig()
	tsi := ms.executionInfo.TimeSkippingInfo

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

	currentVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: ms.GetCurrentVersion(),
		TransitionCount:          ms.NextTransitionCount(),
	}

	tsi.FastForwardInfo = &persistencespb.FastForwardInfo{
		TargetTime:                    timestamppb.New(targetTime),
		HasReached:                    false,
		LastUpdateVersionedTransition: currentVersionedTransition,
	}
	ms.AddTasks(&tasks.TimeSkippingTimerTask{
		WorkflowKey:         ms.GetWorkflowKey(),
		VisibilityTimestamp: targetTime,
		VersionedTransition: currentVersionedTransition,
		ArchetypeID:         ms.ChasmTree().ArchetypeID(),
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

	tsi := source.GetTimeSkippingInfo()
	util := NewTimeSkippingInfoUtil(tsi)

	// if disabled, we just return nil for the new TSC
	var newTSC *commonpb.TimeSkippingConfig
	if util.IsEnabled() {
		newTSC = common.CloneProto(tsi.GetConfig())
	}

	// state propagation (virtual time, fast forward, skip count)
	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum := util.GetAccumulatedSkippedDuration(); accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
		}
	}
	if util.HasPendingFastForward() {
		if stateProp == nil {
			stateProp = &commonpb.TimeSkippingStatePropagation{}
		}
		stateProp.FastForwardTargetTime = tsi.GetFastForwardInfo().GetTargetTime()
	}
	// The skip count is scoped to a session; once time skipping is disabled the session has
	// ended, so the count does not carry to the next run.
	if skipCount := tsi.GetSessionSkipCount(); skipCount > 0 && util.IsEnabled() {
		if stateProp == nil {
			stateProp = &commonpb.TimeSkippingStatePropagation{}
		}
		stateProp.InitialSkipCount = skipCount
	}
	return newTSC, stateProp
}

// propagateTimeSkippingToChild snapshots the parent's time skipping into a child workflow. The
// child shares the parent's virtual clock (its start time is shifted forward by the parent's
// accumulated skipped duration) but is otherwise a fresh execution:
//   - FastForward is per-execution and is never propagated to children.
//   - The child starts a fresh skip session: SessionSkipCount is not propagated (starts at 0).
//   - It does inherit the parent's per-session budget (MaxSkipPerSession). Children start
//     internally, bypassing the frontend that populates the default budget, so inheriting the
//     parent's already-resolved limit avoids a 0 that would disable skipping on the first skip.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	tsi := source.GetTimeSkippingInfo()
	tsc := tsi.GetConfig()
	util := NewTimeSkippingInfoUtil(tsi)

	accum := util.GetAccumulatedSkippedDuration()
	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
			InitialSkipCount:       0,
		}
	}

	// only propagates state
	if !util.IsEnabled() || tsc.GetDisablePropagation() {
		return nil, stateProp
	}

	// propagate both config and state
	return &commonpb.TimeSkippingConfig{
		Enabled:           true,
		MaxSkipPerSession: tsc.GetMaxSkipPerSession(),
	}, stateProp
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
	return NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo()).GetAccumulatedSkippedDuration()
}

// =============================================================================
// Time Skipping Runtime Data Structure
// =============================================================================
type timeSkippingTransition struct {
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// NewTimeSkippingTransition creates a new time-skipping transition with the current time.
// Methods provided by this data structure cannot be used without a current time.
//
// todo@time-skipping: the methods will be used by CHASM so keep as public.
func NewTimeSkippingTransition(currentTime time.Time) *timeSkippingTransition {
	return &timeSkippingTransition{CurrentTime: currentTime}
}

// IsValid reports whether the transition is worth applying: a real skip target, or a bare disable
// signal. Nil-safe. A transition without a current time is never valid — every meaningful field is
// derived relative to the current time, so without it there is nothing to apply.
func (t *timeSkippingTransition) IsValid() bool {
	return t != nil && !t.CurrentTime.IsZero() && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

func (t *timeSkippingTransition) TrackEarliestFutureTime(candidate time.Time) {
	if t == nil || t.CurrentTime.IsZero() || candidate.IsZero() || candidate.Before(t.CurrentTime) {
		return
	}
	if t.TargetTime.IsZero() || candidate.Before(t.TargetTime) {
		t.TargetTime = candidate
	}
}

func (t *timeSkippingTransition) GateByFastForward(ff *persistencespb.FastForwardInfo) {
	if t == nil || t.CurrentTime.IsZero() {
		return
	}
	if ff == nil || ff.GetHasReached() || ff.GetTargetTime() == nil ||
		ff.GetTargetTime().AsTime().IsZero() {
		return
	}
	ffTargetTime := ff.GetTargetTime().AsTime()
	// If a real candidate is scheduled strictly before the fast-forward target, we skip to
	// that and the fast-forward budget is not yet exhausted — leave time skipping enabled.
	if !t.TargetTime.IsZero() && t.TargetTime.Before(ffTargetTime) {
		return
	}
	// Otherwise the fast-forward target is the earliest target: skip to it (clamped to the
	// present by TrackEarliestFutureTime) and disable time skipping — the budget is reached.
	// This is what lets the budget cap a chain of runs: a run with no earlier candidate
	// consumes the remaining budget by skipping to the fast-forward and disabling.
	t.TrackEarliestFutureTime(ffTargetTime)
	t.DisabledAfterFastForward = true
}

// =============================================================================
// Time Skipping Utility Functions
// =============================================================================

func NewTimeSkippingInfoUtil(tsi *persistencespb.TimeSkippingInfo) *TimeSkippingInfoUtil {
	return &TimeSkippingInfoUtil{tsi: tsi}
}

// TimeSkippingInfoUtil provides read-only helpers over a TimeSkippingInfo, guarding against nil
// info/config/fast-forward so callers don't have to repeat the nil checks.
type TimeSkippingInfoUtil struct {
	tsi *persistencespb.TimeSkippingInfo
}

func (util *TimeSkippingInfoUtil) GetAccumulatedSkippedDuration() time.Duration {
	if util == nil || util.tsi == nil {
		return 0
	}
	return util.tsi.GetAccumulatedSkippedDuration().AsDuration()
}

// HasPendingFastForward reports whether time skipping is enabled and carries a fast-forward that has
// not yet been reached and has a real (non-zero) target time. A fast-forward on a disabled config is
// not "pending" because it can never fire. All accesses go through nil-safe proto getters, so a nil
// util, nil info, nil config, or nil fast-forward all yield false.
func (util *TimeSkippingInfoUtil) HasPendingFastForward() bool {
	if util == nil || !util.IsEnabled() {
		return false
	}
	ff := util.tsi.GetFastForwardInfo()
	return ff != nil &&
		!ff.GetHasReached() &&
		ff.GetTargetTime() != nil &&
		!ff.GetTargetTime().AsTime().IsZero()
}

// IsEnabled reports whether time skipping is enabled. Nil-safe: a nil util, nil info, or nil config
// all yield false.
func (util *TimeSkippingInfoUtil) IsEnabled() bool {
	if util == nil || util.tsi == nil {
		return false
	}
	return util.tsi.GetConfig().GetEnabled()
}

func (util *TimeSkippingInfoUtil) ToDescribeInfo(currentTime time.Time) *commonpb.TimeSkippingInfo {
	if util == nil || util.tsi == nil {
		return nil
	}
	return &commonpb.TimeSkippingInfo{
		CurrentTime:     timestamppb.New(currentTime),
		IsRunning:       util.IsEnabled(),
		FastForwardInfo: util.ToFastForwardInfo(),
		StopReason:      util.tsi.GetStopReason(),
	}
}

func (util *TimeSkippingInfoUtil) ToFastForwardInfo() *commonpb.TimeSkippingFastForwardInfo {
	if util == nil || util.tsi == nil {
		return nil
	}
	ff := util.tsi.GetFastForwardInfo()
	if ff == nil {
		return nil
	}
	return &commonpb.TimeSkippingFastForwardInfo{
		TargetTime:    ff.GetTargetTime(),
		HasCompleted:  ff.GetHasReached(),
		FastForwardId: util.tsi.GetConfig().GetFastForwardId(),
	}
}

// =============================================================================
// Time Skipping Runtime Methods for Workflow-based Executions
// =============================================================================

// isWorkflowSkippable checks if current workflow can skip time,
// if checks if time skipping is enabled, if the workflow has in-flight work,
// and if the workflow is at the correct state and status to skip time.
// And if there is a time point to skip to is not the scope of this method.
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
	// HasPendingWorkflowTask covers both normal and speculative workflow tasks
	if ms.HasPendingWorkflowTask() {
		noSkippingReason = "has pending workflow task"
		return false
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
		noSkippingReason = "has pending activity"
		return false
	}
	if nexusoperations.MachineCollection(ms.HSM()).Size() > 0 {
		noSkippingReason = "has pending nexus operations"
		return false
	}
	if len(ms.GetPendingChildExecutionInfos()) > 0 {
		noSkippingReason = "has pending child execution"
		return false
	}
	if len(ms.GetPendingSignalExternalInfos()) > 0 {
		noSkippingReason = "has pending signal external"
		return false
	}
	if len(ms.GetPendingRequestCancelExternalInfos()) > 0 {
		noSkippingReason = "has pending request cancel external"
		return false
	}
	return true
}

// findNextSkipTarget finds the next skip target from the pending timers, activity-retries,
// workflow backoff timers, and workflow execution timeout, etc that those are skippable and scheduled in the future
// it should only be called after isWorkflowSkippable returns true
func (ms *MutableStateImpl) findNextSkipTarget() *timeSkippingTransition {
	transition := NewTimeSkippingTransition(ms.Now())
	for _, timerInfo := range ms.GetPendingTimerInfos() {
		transition.TrackEarliestFutureTime(timerInfo.ExpiryTime.AsTime())
	}

	// Activities waiting out a retry backoff are skip targets: advance to the earliest
	// next-attempt time.
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
	if ms.HadOrHasWorkflowTask() {
		// The run/execution timeout is a valid skip target, but only once the workflow has
		// actually started executing (it has had a workflow task). For example, a freshly-created child has a
		// brief window — start event applied, first workflow task not yet scheduled — where it
		// looks idle with no timers. If the timeout were an unconditional target, the child would
		// skip all the way to its run timeout before ever running its first task, so it never sets
		// its internal timers.
		if t := ms.executionInfo.GetWorkflowRunExpirationTime(); t != nil && !t.AsTime().IsZero() {
			transition.TrackEarliestFutureTime(t.AsTime())
		}
		if t := ms.executionInfo.GetWorkflowExecutionExpirationTime(); t != nil && !t.AsTime().IsZero() {
			transition.TrackEarliestFutureTime(t.AsTime())
		}
	}

	// fast-forward is also a target time, and this is the furthest target time a time skipping can skip to
	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	if !tsi.GetFastForwardInfo().GetHasReached() && tsi.GetFastForwardInfo().GetTargetTime() != nil {
		transition.GateByFastForward(tsi.GetFastForwardInfo())
	}

	if transition.IsValid() {
		return transition
	}
	return nil
}

func (ms *MutableStateImpl) closeTransactionHandleWorkflowTimeSkipping(
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (needRegenTasks bool) {
	if !ms.IsWorkflow() {
		return false
	}
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		// 1. gate: only a running, time-skipping-enabled, idle workflow may skip time
		if !ms.isWorkflowSkippable() {
			return false
		}
		// 2. find the next skip target; if there is none, time skipping is not needed
		transition := ms.findNextSkipTarget()
		if !transition.IsValid() {
			return false
		}
		// 3. state change.
		_, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
			ctx, transition.TargetTime, transition.DisabledAfterFastForward)
		if err != nil {
			ms.logger.Error("failed to add workflow execution time skipping transitioned event", tag.Error(err))
			return false
		}
		// 4. task regeneration
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

func (ms *MutableStateImpl) AddWorkflowExecutionTimeSkippingTransitionedEvent(
	ctx context.Context, targetTime time.Time, disabledAfterFastForward bool) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowExecutionTimeSkippingTransitioned
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddWorkflowExecutionTimeSkippingTransitionedEvent(
		targetTime, disabledAfterFastForward)
	return event, ms.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(ctx, event)
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionTimeSkippingTransitionedEvent(ctx context.Context, event *historypb.HistoryEvent) error {
	// todo: merge with chasm time skipping
	attr := event.GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	tsi := ms.executionInfo.GetTimeSkippingInfo()

	opTag := tag.WorkflowActionWorkflowExecutionTimeSkippingTransitioned
	invalidTransitionError := serviceerror.NewInternal("TimeSkippingTransitionedEvent failed to apply")
	if tsi == nil {
		ms.logError("TimeSkippingTransitionedEvent failed to apply: TimeSkippingInfo is nil", opTag)
		return invalidTransitionError
	}
	if attr.TargetTime == nil && !attr.GetDisabledAfterFastForward() {
		ms.logError("TimeSkippingTransitionedEvent failed to apply: TargetTime is nil and disabled after fast-forward is false", opTag)
		return invalidTransitionError
	}

	// update time
	if !timeNotSet(attr.TargetTime) {
		asd := ms.accumulatedSkippedDuration() + attr.TargetTime.AsTime().Sub(event.GetEventTime().AsTime())
		tsi.AccumulatedSkippedDuration = durationpb.New(asd)
	}
	// update enabled state
	if attr.GetDisabledAfterFastForward() && tsi.GetFastForwardInfo() != nil {
		tsi.GetFastForwardInfo().HasReached = true
		tsi.Config.Enabled = false
		tsi.StopReason = enumspb.TIME_SKIPPING_STOP_REASON_FAST_FORWARD_COMPLETED
	}
	// update skip
	tsi.SessionSkipCount += 1
	if tsi.SessionSkipCount >= tsi.Config.GetMaxSkipPerSession() && tsi.Config.Enabled {
		tsi.Config.Enabled = false
		tsi.StopReason = enumspb.TIME_SKIPPING_STOP_REASON_MAX_SKIP_PER_SESSION_REACHED
	}

	ms.timeSkippingInfoUpdated = true
	return nil
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
