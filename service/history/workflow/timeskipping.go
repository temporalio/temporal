package workflow

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
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
) error {
	initialSkip := timeSkippingStatePropagation.GetInitialSkippedDuration()
	if config == nil && initialSkip == nil {
		return nil
	}

	if ms.executionInfo.TimeSkippingInfo != nil {
		return serviceerror.NewInternal("time skipping info already initialized")
	}

	ms.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		Config:                     config,
		AccumulatedSkippedDuration: initialSkip,
	}
	ms.wrapTimeSourceWithTimeSkipping()
	ms.wrapExecutionTimes(initialSkip)
	ms.applyFastForward(currentEventID, timeSkippingStatePropagation.GetFastForwardTargetTime())
	ms.timeSkippingInfoUpdated = true
	return nil
}

func (ms *MutableStateImpl) updateTimeSkippingInfo(
	config *commonpb.TimeSkippingConfig,
	currentEventID int64,
) error {
	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if tsi == nil {
		return serviceerror.NewInternal("time skipping info not initialized when updating")
	}
	ms.executionInfo.TimeSkippingInfo.Config = config
	ms.applyFastForward(currentEventID, nil)
	ms.timeSkippingInfoUpdated = true
	return nil
}

// applyFastForward (re)computes the FastForwardInfo using the new TimeSkippingConfig (TSC) and propagated time-skippingstates.
// This method should be called whenever the TimeSkippingConfig is initialized or updated.
// An invariant of the FastForwardInfo is that after this method is called, if the current TSC has a FastForward value,
// the FastForwardInfo should never be nil.
func (ms *MutableStateImpl) applyFastForward(currentEventID int64, propagatedTargetTime *timestamppb.Timestamp) {

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
		if stateProp == nil {
			stateProp = &commonpb.TimeSkippingStatePropagation{}
		}
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
	// is a skip target, not in-flight work (see findNextSkipTarget). The
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
	// This is the workflow (history-event) time-skipping path. CHASM executions (e.g. standalone
	// activities) run their own decision in the CHASM tree's closeTransactionHandleTimeSkipping;
	// running this path for them would scan only workflow-level targets (timers, pending activities,
	// run timeout) — none of which exist for a CHASM component — and its GateByFastForward would then
	// skip straight to the fast-forward and disable time skipping before the CHASM path ever runs.
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
		// 3. state change
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
	tsi.Config.Enabled = !attr.GetDisabledAfterFastForward()
	if attr.GetDisabledAfterFastForward() && tsi.GetFastForwardInfo() != nil {
		tsi.FastForwardInfo.HasReached = true
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

// =============================================================================
// Time Skipping for CHASM-based Executions
// =============================================================================

// applyTimeSkippingTransition mutates the execution's TimeSkippingInfo for a single skip transition:
// it advances AccumulatedSkippedDuration by (TargetTime - CurrentTime), toggles Config.Enabled, and
// marks the fast-forward target reached. This is the CHASM (non-workflow) apply path, invoked by
// RecordTimeSkippingTransition; the workflow path applies the equivalent mutation from its history
// event in ApplyWorkflowExecutionTimeSkippingTransitionedEvent.
//
// A zero TargetTime means "no skip" (only valid together with DisabledAfterFastForward, e.g. a bound
// hit exactly). transition.CurrentTime must be in the virtual frame (ms.Now() / the event's virtual
// EventTime).
func (ms *MutableStateImpl) applyTimeSkippingTransition(transition chasm.TimeSkippingTransition) error {
	opTag := tag.WorkflowActionWorkflowExecutionTimeSkippingTransitioned
	invalidTransitionError := serviceerror.NewInternal("TimeSkippingTransitionedEvent failed to apply")
	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if tsi == nil {
		ms.logError("TimeSkippingTransitionedEvent failed to apply: TimeSkippingInfo is nil", opTag)
		return invalidTransitionError
	}
	if transition.TargetTime.IsZero() && !transition.DisabledAfterFastForward {
		ms.logError("TimeSkippingTransitionedEvent failed to apply: TargetTime is nil and disabled after fast-forward is false", opTag)
		return invalidTransitionError
	}

	if tsi.GetAccumulatedSkippedDuration() == nil {
		tsi.AccumulatedSkippedDuration = durationpb.New(0)
	}
	accumulatedSkippedDuration := tsi.GetAccumulatedSkippedDuration().AsDuration()
	if !transition.TargetTime.IsZero() {
		accumulatedSkippedDuration += transition.TargetTime.Sub(transition.CurrentTime)
	}
	tsi.AccumulatedSkippedDuration = durationpb.New(accumulatedSkippedDuration)
	tsi.Config.Enabled = !transition.DisabledAfterFastForward

	if transition.DisabledAfterFastForward && tsi.GetFastForwardInfo() != nil {
		tsi.FastForwardInfo.HasReached = true
	}

	ms.timeSkippingInfoUpdated = true

	// Re-stamp the CHASM fast-forward wake so its wall-clock fire time tracks the new accumulated skip
	// (real fire = TargetTime - accumulatedSkippedDuration). Bundled with the accumulated update here
	// rather than routed through the chasm task pipeline; no-op for workflows and once the target is
	// reached.
	ms.regenerateChasmFastForwardWakeTask()
	return nil
}

// chasmNoEventID is the sentinel event ID used when (re)computing time-skipping info for CHASM
// executions, which have no history events to anchor the configuration to. Real event IDs start at
// 1, so -1 is unambiguous. For CHASM the fast-forward wake is emitted as a CHASM task during
// CloseTransaction (anchored on the VersionedTransition), so this stub only ends up in the unused
// FastForwardInfo.SourceEventId field.
//
// TODO(time-skipping/chasm): emit the fast-forward wake as a ChasmTaskPure on the root component and
// anchor its staleness check on the VersionedTransition (as ChasmTaskInfo does) rather than an
// event ID.
const chasmNoEventID int64 = -1

// SetTimeSkippingConfig sets the execution's time-skipping configuration for a CHASM execution. It
// backs chasm.MutableContext.SetTimeSkippingConfig (via the chasm.NodeBackend interface): the first
// call initializes the TimeSkippingInfo, and subsequent calls update the config in place (preserving
// the accumulated skipped duration). It reuses the workflow path's initTimeSkippingInfo /
// updateTimeSkippingInfo, which are archetype-aware (no physical workflow timer task for CHASM — see
// applyFastForward). CHASM executions have no history events to anchor to (see chasmNoEventID) and no
// propagated state on this path.
func (ms *MutableStateImpl) SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig) {
	if ms.executionInfo.GetTimeSkippingInfo() == nil {
		ms.initTimeSkippingInfo(config, nil, chasmNoEventID)
	} else {
		ms.updateTimeSkippingInfo(config, chasmNoEventID)
	}
}

// RecordTimeSkippingTransition records a single time-skipping transition for the execution. It is the
// archetype-aware apply sink: the CHASM framework's closeTransactionHandleTimeSkipping calls it for
// non-workflow executions after building the transition, and the fast-forward wake timer executor
// calls it (for either archetype) to disable time skipping once the fast-forward target is hit. The
// caller is responsible for computing the final transition — including the min of the component
// candidate and the fast-forward target; this method only records it.
//
// archetype selects how the skip is recorded:
//   - the workflow archetype records a history event (AddWorkflowExecutionTimeSkippingTransitionedEvent),
//     which in turn applies the transition to the TimeSkippingInfo;
//   - all other archetypes have no history, so the transition is applied directly to the TSI.
//
// For CHASM executions, after this returns the physical timer tasks regenerated later in the same
// CloseTransaction pick up the new accumulated offset via AddTasks -> ToRealTime, so no separate
// timer-task regeneration is needed.
func (ms *MutableStateImpl) RecordTimeSkippingTransition(
	ctx context.Context,
	transition chasm.TimeSkippingTransition,
	archetype chasm.ArchetypeID,
) error {
	if !transition.IsValid() {
		return nil
	}
	switch archetype {
	case chasm.WorkflowArchetypeID:
		// workflows require applying the transition through a history event
		_, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
			ctx, transition.TargetTime, transition.DisabledAfterFastForward)
		return err
	default:
		return ms.applyTimeSkippingTransition(transition)
	}
}
func (ms *MutableStateImpl) regenerateChasmFastForwardWakeTask() {
	if ms.IsWorkflow() {
		return
	}
	ff := ms.executionInfo.GetTimeSkippingInfo().GetFastForwardInfo()
	if ff == nil || ff.GetHasReached() || ff.GetTargetTime() == nil {
		return
	}
	ms.AddTasks(&tasks.TimeSkippingTimerTask{
		WorkflowKey:         ms.GetWorkflowKey(),
		VisibilityTimestamp: ff.GetTargetTime().AsTime(), // virtual; AddTasks -> ToRealTime shifts to wall clock
		EventID:             chasmNoEventID,
	})
}
