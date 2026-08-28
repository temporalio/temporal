package workflow

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
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
	ms.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		Config:                     config,
		AccumulatedSkippedDuration: initialSkip,
		SessionSkipCount:           propagatedState.GetInitialSkipCount(),
	}
	ms.wrapTimeSourceWithTimeSkipping()
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
	// we allow setting config to nil in updating tsc
	ms.executionInfo.TimeSkippingInfo.Config = config
	tsi.SessionSkipCount = 0
	ms.applyFastForward(nil)
	ms.timeSkippingInfoUpdated = true
}

// applyFastForward (re)computes the FastForwardInfo using the new TimeSkippingConfig (TSC) and propagated time-skippingstates.
// This method should be called whenever the TimeSkippingConfig is initialized or updated.
//
// Invariant: FastForwardInfo is non-nil whenever the config still has a fast-forward config, and nil
// otherwise. Keeping it alive even when skipping is disabled lets a poller still observe a completed
// fast-forward carried over from a prior run.
func (ms *MutableStateImpl) applyFastForward(propagatedTargetTime *timestamppb.Timestamp) {
	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if tsi == nil {
		return
	}
	tsc := tsi.GetConfig()
	ffConfig := tsc.GetFastForwardConfig()

	// no ff
	if ffConfig == nil {
		if tsi.FastForwardInfo != nil {
			ms.setAndStampFastForwardInfo(nil)
		}
		return
	}

	// ff of different states:
	var targetTime time.Time
	var hasReached bool
	if propagatedTargetTime != nil {
		targetTime = propagatedTargetTime.AsTime()
		// A propagated target may already be due; a due target is a completed fast-forward.
		hasReached = !ms.Now().Before(targetTime)
	} else {
		// if there is no propagated target time,
		// fast-forward refers to a new duration from now.
		targetTime = ms.Now().Add(ffConfig.GetDuration().AsDuration())
	}

	ffVersionedTransition := ms.setAndStampFastForwardInfo(&persistencespb.FastForwardInfo{
		TargetTime: timestamppb.New(targetTime),
		HasReached: hasReached,
	})

	// actions based on ff states: 1) disable time skipping 2) add ff timer task
	if hasReached {
		tsc.Enabled = false
		return
	}

	// schedule the wake-up timer only while skipping is still enabled
	if tsc.GetEnabled() {
		ms.AddTasks(&tasks.TimeSkippingTimerTask{
			WorkflowKey:         ms.GetWorkflowKey(),
			VisibilityTimestamp: targetTime,
			VersionedTransition: ffVersionedTransition,
			ArchetypeID:         ms.ChasmTree().ArchetypeID(),
		})
	}
}

// setAndStampFastForwardInfo sets the fast-forward info and stamps it with the current transaction's versioned transition.
// Nothing else may write these two fields; routing every update through here is what keeps them in lockstep.
func (ms *MutableStateImpl) setAndStampFastForwardInfo(
	ffInfo *persistencespb.FastForwardInfo,
) *persistencespb.VersionedTransition {
	tsi := ms.executionInfo.TimeSkippingInfo
	tsi.FastForwardInfo = ffInfo
	tsi.FastForwardInfoLastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: ms.GetCurrentVersion(),
		TransitionCount:          ms.NextTransitionCount(),
	}
	return tsi.FastForwardInfoLastUpdateVersionedTransition
}

// -- Propagation Methods of Time Skipping

// propagateTimeSkippingToNextRun propagates both the time-skipping config and state to the next run
// in the chain of runs(continue-as-new, retry, cron). The config is propagated regardless of whether time
// skipping is actively running, so reading APIs can always retrieve the latest effective configuration from the current run.
func propagateTimeSkippingToNextRun(
	tsi *persistencespb.TimeSkippingInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	if tsi == nil {
		return nil, nil
	}
	util := NewTimeSkippingInfoUtil(tsi)
	var newTSC *commonpb.TimeSkippingConfig
	if tsi.Config != nil {
		newTSC = common.CloneProto(tsi.GetConfig())
	}
	stateProp := &commonpb.TimeSkippingStatePropagation{
		InitialSkipCount:       tsi.GetSessionSkipCount(),
		InitialSkippedDuration: durationpb.New(util.GetAccumulatedSkippedDuration()),
		FastForwardTargetTime:  util.GetFastForwardTargetTime(),
	}
	return newTSC, stateProp
}

// propagateTimeSkippingToOtherExecution snapshots the current execution's time skipping into another
// execution (e.g. a child workflow), which shares the current execution's virtual clock. Two rules:
//  1. State: nothing propagates except virtual time.
//  2. Config: everything propagates except the fast-forward config, and the whole config can be
//     suppressed by DisablePropagation.
func propagateTimeSkippingToOtherExecution(
	tsi *persistencespb.TimeSkippingInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	if tsi == nil {
		return nil, nil
	}
	tsc := tsi.GetConfig()
	accum := NewTimeSkippingInfoUtil(tsi).GetAccumulatedSkippedDuration()

	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
			InitialSkipCount:       0,
		}
	}

	if tsc == nil || tsc.GetDisablePropagation() {
		return nil, stateProp
	}

	// Propagate the whole config except the per-execution fast-forward.
	newTSC := common.CloneProto(tsc)
	newTSC.FastForwardConfig = nil
	return newTSC, stateProp
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
	return t.isInitialized() && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

func (t *timeSkippingTransition) isInitialized() bool {
	return t != nil && !t.CurrentTime.IsZero()
}

func (t *timeSkippingTransition) TrackEarliestFutureTime(candidate time.Time) {
	if !t.isInitialized() || candidate.IsZero() || candidate.Before(t.CurrentTime) {
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
	if !ffTargetTime.After(t.CurrentTime) {
		t.TargetTime = time.Time{}
		t.DisabledAfterFastForward = true
		return
	}

	if !t.TargetTime.IsZero() && t.TargetTime.Before(ffTargetTime) {
		return
	}
	t.TargetTime = ffTargetTime
	t.DisabledAfterFastForward = true
}

// =============================================================================
// Time Skipping Utility Functions
// =============================================================================

// AdjustNowWithTimeSkipping converts a real clock reading to the virtual clock frame inherited
// through time-skipping state propagation.
func AdjustNowWithTimeSkipping(
	now time.Time,
	statePropagation *commonpb.TimeSkippingStatePropagation,
) time.Time {
	return now.Add(statePropagation.GetInitialSkippedDuration().AsDuration())
}

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
	ffTargetTime := util.GetFastForwardTargetTime()
	if ff == nil || ffTargetTime == nil {
		return false
	}
	return !ff.GetHasReached()
}

func (util *TimeSkippingInfoUtil) GetFastForwardTargetTime() *timestamppb.Timestamp {
	if util == nil || util.tsi == nil {
		return nil
	}
	ff := util.tsi.GetFastForwardInfo()
	if ff == nil || ff.GetTargetTime() == nil {
		return nil
	}
	targetTime := ff.GetTargetTime().AsTime()
	if targetTime.IsZero() {
		return nil
	}
	return ff.TargetTime
}

// IsEnabled reports whether time skipping is still running for this execution. Prefer this over
// reading the config's Enabled flag directly.
// The `enabled` field is disabled internally in three places:
// 1) initTimeSkippingInfo and updateTimeSkippingInfo, 2) runtime time-skipping transition, 3) fast-forward timer task.
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
		EffectiveConfig: common.CloneProto(util.tsi.GetConfig()),
		FastForwardInfo: util.ToFastForwardInfo(),
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
	config := util.tsi.GetConfig()
	return &commonpb.TimeSkippingFastForwardInfo{
		TargetTime:          ff.GetTargetTime(),
		HasCompleted:        ff.GetHasReached(),
		FastForwardId:       config.GetFastForwardConfig().GetId(),
		FastForwardDuration: config.GetFastForwardConfig().GetDuration(),
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
		reachedFFInfo := tsi.GetFastForwardInfo()
		reachedFFInfo.HasReached = true
		ms.setAndStampFastForwardInfo(reachedFFInfo)
		tsi.Config.Enabled = false
	}
	// update skip
	tsi.SessionSkipCount += 1
	if tsi.SessionSkipCount >= tsi.Config.GetMaxSessionSkipCount() && tsi.Config.Enabled {
		tsi.Config.Enabled = false
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
