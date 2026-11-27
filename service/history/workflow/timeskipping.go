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
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// =============================================================================
// Time Skipping Configuration Management
// =============================================================================

// IsTimeSkippingFeatureEnabled reports whether the namespace flag permits time skipping for an archetype.
func (ms *MutableStateImpl) IsTimeSkippingFeatureEnabled(archetypeID chasm.ArchetypeID) bool {
	namespace := ms.GetNamespaceEntry().Name().String()
	switch archetypeID {
	case chasm.WorkflowArchetypeID:
		return ms.config.WorkflowTimeSkippingEnabled(namespace)
	case chasm.SchedulerArchetypeID:
		return ms.config.CHASMSchedulerTimeSkippingEnabled(namespace)
	default:
		return false
	}
}

func (ms *MutableStateImpl) TimeSkippingMaxAccumulatedDuration() time.Duration {
	return ms.config.TimeSkippingMaxAccumulatedDuration()
}

// initTimeSkippingInfo creates TimeSkippingInfo when an execution first receives a time-skipping
// config or propagated virtual-time state. This can happen at execution start, through an options
// update on an existing execution, or when a CHASM component first sets its config.
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
		InitialSkippedDuration:     initialSkip,
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

func (ms *MutableStateImpl) SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig) {
	// todo: migrate workflow init and update to this unified method
	if ms.executionInfo.GetTimeSkippingInfo() == nil {
		ms.initTimeSkippingInfo(config, nil)
	} else {
		ms.updateTimeSkippingInfo(config)
	}
	// TODO: Find a generic way to get the archetype before SetRootComponent.
	// Scheduler is the only caller today.
	metrics.ExecutionTimeSkippingConfigUpdatedCounter.With(ms.metricsHandler).Record(
		1,
		metrics.NamespaceTag(ms.namespaceEntry.Name().String()),
		metrics.ArchetypeTag(chasm.SchedulerComponentName),
	)
}

func (ms *MutableStateImpl) emitTimeSkippingConfigUpdatedMetric() {
	metrics.ExecutionTimeSkippingConfigUpdatedCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
}

func (ms *MutableStateImpl) timeSkippingMetricTags() []metrics.Tag {
	tags := []metrics.Tag{metrics.NamespaceTag(ms.namespaceEntry.Name().String())}
	if archetypeTag, ok := getArchetypeMetricTag(ms.shard.ChasmRegistry(), ms.chasmTree.ArchetypeID()); ok {
		tags = append(tags, archetypeTag)
	}
	return tags
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

	ms.setAndStampFastForwardInfo(&persistencespb.FastForwardInfo{
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
		ms.addTimeSkippingFastForwardTask()
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

// =============================================================================
// Time Skipping Runtime Time Impacts
// =============================================================================
// wrapTimeSourceWithTimeSkipping wraps ms.timeSource (and the hBuilder's copy) with a time-skipping
// wrapper. The closure captures ms so the offset tracks ms.executionInfo.TimeSkippingInfo as it
// evolves — no need to re-wrap when TimeSkippingInfo is replaced. The type-assertion guard makes
// repeat calls from initialization, DB loading, or replication a no-op.
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

// ToPhysicalTimeWithCurrentTimeSkippingOffset converts logical time to physical time
// using the current accumulated skipped duration. Zero time is returned unchanged.
func (util *TimeSkippingInfoUtil) ToPhysicalTimeWithCurrentTimeSkippingOffset(logicalTime time.Time) time.Time {
	if logicalTime.IsZero() {
		return logicalTime
	}
	return logicalTime.Add(-util.GetAccumulatedSkippedDuration())
}

// ToLogicalTimeWithCurrentTimeSkippingOffset converts physical time to logical time
// using the current accumulated skipped duration. Zero time is returned unchanged.
func (util *TimeSkippingInfoUtil) ToLogicalTimeWithCurrentTimeSkippingOffset(physicalTime time.Time) time.Time {
	if physicalTime.IsZero() {
		return physicalTime
	}
	return physicalTime.Add(util.GetAccumulatedSkippedDuration())
}

// CreateTime returns startTime minus the initial skipped duration, which is when the execution was
// created. It ignores the accumulated skipped duration, which grows as the execution skips.
func (util *TimeSkippingInfoUtil) CreateTime(startTime time.Time) time.Time {
	if util == nil || util.tsi == nil || startTime.IsZero() {
		return startTime
	}
	return startTime.Add(-util.tsi.GetInitialSkippedDuration().AsDuration())
}

// HasTimeSkippingClockOffset reports whether accumulated skipped time shifts the workflow clock.
func (util *TimeSkippingInfoUtil) HasTimeSkippingClockOffset() bool {
	return util.GetAccumulatedSkippedDuration() != 0
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
		CurrentTime:             timestamppb.New(currentTime),
		EffectiveConfig:         common.CloneProto(util.tsi.GetConfig()),
		FastForwardInfo:         util.ToFastForwardInfo(),
		CurrentSessionSkipCount: util.tsi.GetSessionSkipCount(),
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
	// (1) gate by time skipping configuration; runs for every workflow, so it is not logged
	if !NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo()).IsEnabled() {
		return false
	}

	noSkippingReason := ""
	defer func() {
		if noSkippingReason != "" {
			ms.logger.Debug("workflow time skipping skipped", tag.String("reason", noSkippingReason))
		}
	}()

	// The feature flag may be disabled after a workflow enabled time skipping.
	if !ms.IsTimeSkippingFeatureEnabled(chasm.WorkflowArchetypeID) {
		noSkippingReason = "time skipping feature flag is disabled"
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
	if ms.ChasmEnabled() {
		wf, chasmCtx, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
		if err != nil {
			metrics.ExecutionTimeSkippingTransitionedErrorCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
			ms.logger.Error("failed to check CHASM workflow time skipping eligibility", tag.Error(err))
			return false
		}
		if !wf.IsExecutionSkippable(chasmCtx) {
			noSkippingReason = "has pending CHASM nexus operations"
			return false
		}
	}
	if ms.hasInFlightPendingChild() {
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

// hasInFlightPendingChild reports whether a child is still running.
// A child whose terminal event is deferred has already finished. It does not block
// time skipping. The parent may skip up to that child's close time so its own timers
// and timeouts are delivered first.
func (ms *MutableStateImpl) hasInFlightPendingChild() bool {
	for _, ci := range ms.GetPendingChildExecutionInfos() {
		if ci.GetDeferredCompletionEvent() == nil {
			return true
		}
	}
	return false
}

// findNextSkipTarget finds the earliest future target time among pending timers, activity retries,
// workflow backoff, and execution timeouts, then applies the fast-forward bound. A pending task due
// now or earlier blocks time skipping until it is processed. Call only after isWorkflowSkippable returns true.
func (ms *MutableStateImpl) findNextSkipTarget() *chasm.TimeSkippingTransition {
	transition := chasm.NewTimeSkippingTransition(ms.Now())
	blocked := false
	ms.forEachTimeSkippingDeadline(func(deadline time.Time, name string) {
		if blocked {
			return
		}
		if ms.trackFutureOrReportDueTaskForWorkflow(transition, deadline, name) {
			blocked = true
		}
	})
	if blocked {
		return nil
	}

	// fast-forward is also a target time, and this is the furthest target time a time skipping can skip to
	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	if !tsi.GetFastForwardInfo().GetHasReached() && tsi.GetFastForwardInfo().GetTargetTime() != nil {
		transition.GateByFastForward(tsi.GetFastForwardInfo())
	}

	if !transition.IsValid() || ms.skipTargetPassesDeferredChild(transition) {
		return nil
	}
	return transition
}

// forEachTimeSkippingDeadline visits user timers, activity retry backoffs, workflow
// backoff, and run/execution timeouts. Fast-forward is applied separately because
// reaching it also disables time skipping.
func (ms *MutableStateImpl) forEachTimeSkippingDeadline(fn func(deadline time.Time, name string)) {
	for _, timerInfo := range ms.GetPendingTimerInfos() {
		fn(timerInfo.GetExpiryTime().AsTime(), "user timer")
	}

	// Activities waiting out a retry backoff are skip targets: advance to the earliest
	// next-attempt time.
	for _, ai := range ms.GetPendingActivityInfos() {
		if !activityPendingRetry(ai) {
			continue
		}
		fn(ai.GetScheduledTime().AsTime(), "activity retry")
	}
	if !ms.HadOrHasWorkflowTask() {
		// Support start-with-delay, cron, retry, and CaN-with-backoff: the workflow is
		// waiting on a WorkflowBackoffTimerTask. ExecutionTime > StartTime means a backoff
		// is configured. A child workflow can have no workflow task yet without a backoff;
		// in that case ExecutionTime == StartTime.
		executionTime := ms.executionInfo.GetExecutionTime().AsTime()
		startTime := ms.executionInfo.GetStartTime().AsTime()
		if executionTime.After(startTime) {
			fn(executionTime, "workflow backoff")
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
			fn(t.AsTime(), "workflow run timeout")
		}
		if t := ms.executionInfo.GetWorkflowExecutionExpirationTime(); t != nil && !t.AsTime().IsZero() {
			fn(t.AsTime(), "workflow execution timeout")
		}
	}
}

// trackFutureOrReportDueTaskForWorkflow reports whether a due task blocks skipping and tracks a
// future task time as a skip target.
func (ms *MutableStateImpl) trackFutureOrReportDueTaskForWorkflow(
	transition *chasm.TimeSkippingTransition,
	deadline time.Time,
	targetName string,
) (hasDueTask bool) {
	if !deadline.After(transition.CurrentTime) {
		ms.logger.Debug("a pending task stops workflow time skipping in the current transaction",
			tag.String("pending-task-type", targetName),
			tag.Time("pending-task-deadline", deadline))
		return true
	}
	transition.TrackEarliestFutureTime(deadline)
	return false
}

// skipTargetPassesDeferredChild reports that this transition would move the parent
// past a deferred child's close time. A disable-only fast-forward has no target
// and does not move the clock.
func (ms *MutableStateImpl) skipTargetPassesDeferredChild(transition *chasm.TimeSkippingTransition) bool {
	closeTime, ok := ms.earliestDeferredChildClose()
	if !ok || transition == nil {
		return false
	}
	target := transition.GetTargetTime()
	if target.IsZero() {
		return false
	}
	return target.After(closeTime)
}

// DeferOutrunningChildCompletion holds a child's terminal event when that child's
// virtual close time is still ahead of the parent. The parent then delivers its own
// earlier timers and timeouts, and advances to the child's close time, before the
// completion is written.
func (ms *MutableStateImpl) DeferOutrunningChildCompletion(
	initiatedID int64,
	completionEvent *historypb.HistoryEvent,
) (bool, error) {
	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || completionEvent == nil {
		return false, nil
	}
	if ci.GetDeferredCompletionEvent() != nil {
		return true, nil
	}
	closeTime := completionEvent.GetEventTime().AsTime()
	if closeTime.IsZero() || !closeTime.After(ms.Now()) {
		return false, nil
	}
	if !ms.IsWorkflow() {
		return false, nil
	}
	if !NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo()).IsEnabled() ||
		!ms.IsTimeSkippingFeatureEnabled(chasm.WorkflowArchetypeID) {
		return false, nil
	}

	ms.approximateSize -= ci.Size()
	ci.DeferredCompletionEvent = common.CloneProto(completionEvent)
	ms.updateChildExecutionInfos[initiatedID] = ci
	ms.approximateSize += ci.Size()
	return true, nil
}

// closeTransactionFlushDeferredChildCompletions writes deferred child completions
// whose close time is not after every remaining parent deadline. When a deadline
// is still earlier, the completion stays deferred so that deadline is delivered first.
func (ms *MutableStateImpl) closeTransactionFlushDeferredChildCompletions(
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (bool, error) {
	if transactionPolicy != historyi.TransactionPolicyActive || !ms.IsWorkflow() {
		return false, nil
	}
	if !ms.IsWorkflowExecutionRunning() || ms.HasPendingWorkflowTask() || ms.IsWorkflowExecutionStatusPaused() {
		return false, nil
	}
	closeTime, ok := ms.earliestDeferredChildClose()
	if !ok {
		return false, nil
	}
	if ms.hasTimeSkippingDeadlineBefore(closeTime) {
		return false, nil
	}

	advanced, err := ms.advanceVirtualTimeTo(ctx, closeTime)
	if err != nil {
		return false, err
	}
	if err := ms.recordDeferredChildCompletions(ms.Now()); err != nil {
		return false, err
	}
	if ms.IsWorkflowExecutionRunning() {
		if err := ScheduleWorkflowTask(ms); err != nil {
			return false, err
		}
	}
	return advanced, nil
}

// hasTimeSkippingDeadlineBefore reports whether a parent timer, timeout, backoff,
// activity retry, or pending fast-forward is at or before limit.
func (ms *MutableStateImpl) hasTimeSkippingDeadlineBefore(limit time.Time) bool {
	if limit.IsZero() {
		return false
	}
	found := false
	consider := func(deadline time.Time) {
		if deadline.IsZero() || deadline.After(limit) {
			return
		}
		found = true
	}
	ms.forEachTimeSkippingDeadline(func(deadline time.Time, _ string) {
		consider(deadline)
	})
	if target, ok := ms.pendingFastForwardTarget(); ok {
		consider(target)
	}
	return found
}

func (ms *MutableStateImpl) pendingFastForwardTarget() (time.Time, bool) {
	util := NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo())
	if !util.HasPendingFastForward() {
		return time.Time{}, false
	}
	target := util.GetFastForwardTargetTime().AsTime()
	if target.IsZero() {
		return time.Time{}, false
	}
	return target, true
}

func (ms *MutableStateImpl) earliestDeferredChildClose() (time.Time, bool) {
	_, ci, ok := ms.earliestDeferredChild()
	if !ok {
		return time.Time{}, false
	}
	closeTime := ci.GetDeferredCompletionEvent().GetEventTime().AsTime()
	if closeTime.IsZero() {
		return time.Time{}, false
	}
	return closeTime, true
}

func (ms *MutableStateImpl) earliestDeferredChild() (int64, *persistencespb.ChildExecutionInfo, bool) {
	var (
		foundID int64
		found   *persistencespb.ChildExecutionInfo
		foundAt time.Time
	)
	ok := false
	for id, ci := range ms.GetPendingChildExecutionInfos() {
		event := ci.GetDeferredCompletionEvent()
		if event.GetEventTime() == nil {
			continue
		}
		closeTime := event.GetEventTime().AsTime()
		if closeTime.IsZero() {
			continue
		}
		if !ok || closeTime.Before(foundAt) {
			foundID = id
			found = ci
			foundAt = closeTime
			ok = true
		}
	}
	return foundID, found, ok
}

func (ms *MutableStateImpl) advanceVirtualTimeTo(ctx context.Context, target time.Time) (bool, error) {
	if !target.After(ms.Now()) {
		return false, nil
	}
	if !NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo()).IsEnabled() ||
		!ms.IsTimeSkippingFeatureEnabled(chasm.WorkflowArchetypeID) {
		return false, nil
	}
	skipped := target.Sub(ms.Now())
	accumulated := ms.accumulatedSkippedDuration()
	maxDuration := ms.TimeSkippingMaxAccumulatedDuration()
	if skipped > maxDuration-accumulated {
		metrics.ExecutionTimeSkippingTransitionedErrorCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
		ms.logger.Warn("workflow time skipping exceeds maximum accumulated duration",
			tag.Duration("accumulated-skipped-duration", accumulated),
			tag.Duration("proposed-skipped-duration", skipped),
			tag.Duration("maximum-accumulated-skipped-duration", maxDuration))
		return false, nil
	}
	if _, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(ctx, target, false); err != nil {
		metrics.ExecutionTimeSkippingTransitionedErrorCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
		ms.logger.Error("failed to add workflow execution time skipping transitioned event", tag.Error(err))
		return false, err
	}
	return true, nil
}

func (ms *MutableStateImpl) recordDeferredChildCompletions(until time.Time) error {
	type readyChild struct {
		id int64
		ci *persistencespb.ChildExecutionInfo
	}
	var ready []readyChild
	for id, ci := range ms.GetPendingChildExecutionInfos() {
		event := ci.GetDeferredCompletionEvent()
		if event == nil {
			continue
		}
		closeTime := event.GetEventTime().AsTime()
		if closeTime.After(until) {
			continue
		}
		ready = append(ready, readyChild{id: id, ci: ci})
	}
	if len(ready) == 0 {
		// The clock could not reach the child's close time. Record the earliest
		// completion anyway so the parent does not wait on an event it will not skip to.
		id, ci, ok := ms.earliestDeferredChild()
		if !ok {
			return nil
		}
		ready = append(ready, readyChild{id: id, ci: ci})
	}
	for _, child := range ready {
		if err := ms.recordDeferredChildCompletion(child.id, child.ci); err != nil {
			return err
		}
	}
	return nil
}

func (ms *MutableStateImpl) recordDeferredChildCompletion(
	initiatedID int64,
	ci *persistencespb.ChildExecutionInfo,
) error {
	event := ci.GetDeferredCompletionEvent()
	childExecution := &commonpb.WorkflowExecution{
		WorkflowId: ci.GetStartedWorkflowId(),
		RunId:      ci.GetStartedRunId(),
	}
	var err error
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		_, err = ms.AddChildWorkflowExecutionCompletedEvent(
			initiatedID, childExecution, event.GetWorkflowExecutionCompletedEventAttributes())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		_, err = ms.AddChildWorkflowExecutionFailedEvent(
			initiatedID, childExecution, event.GetWorkflowExecutionFailedEventAttributes())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		_, err = ms.AddChildWorkflowExecutionCanceledEvent(
			initiatedID, childExecution, event.GetWorkflowExecutionCanceledEventAttributes())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		_, err = ms.AddChildWorkflowExecutionTerminatedEvent(initiatedID, childExecution)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		_, err = ms.AddChildWorkflowExecutionTimedOutEvent(
			initiatedID, childExecution, event.GetWorkflowExecutionTimedOutEventAttributes())
	default:
		ms.logger.Error("deferred child completion event type is unknown",
			tag.WorkflowInitiatedID(initiatedID))
		ms.clearDeferredChildCompletion(ci)
		return nil
	}
	return err
}

func (ms *MutableStateImpl) clearDeferredChildCompletion(ci *persistencespb.ChildExecutionInfo) {
	if ci.GetDeferredCompletionEvent() == nil {
		return
	}
	ms.approximateSize -= ci.Size()
	ci.DeferredCompletionEvent = nil
	ms.updateChildExecutionInfos[ci.GetInitiatedEventId()] = ci
	ms.approximateSize += ci.Size()
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
		// 3. Check that the transition stays within the maximum accumulated skip duration.
		accumulatedDuration := ms.accumulatedSkippedDuration()
		maxDuration := ms.TimeSkippingMaxAccumulatedDuration()
		if transition.GetSkippedDuration() > maxDuration-accumulatedDuration {
			metrics.ExecutionTimeSkippingTransitionedErrorCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
			ms.logger.Warn("workflow time skipping exceeds maximum accumulated duration",
				tag.Duration("accumulated-skipped-duration", accumulatedDuration),
				tag.Duration("proposed-skipped-duration", transition.GetSkippedDuration()),
				tag.Duration("maximum-accumulated-skipped-duration", maxDuration))
			return false
		}
		// 4. State change.
		previousDuration := ms.accumulatedSkippedDuration()
		_, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
			ctx, transition.GetTargetTime(), transition.DisabledAfterFastForward)
		if err != nil {
			metrics.ExecutionTimeSkippingTransitionedErrorCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
			ms.logger.Error("failed to add workflow execution time skipping transitioned event", tag.Error(err))
			return false
		}
		// 5. Timer tasks only need to be re-stamped when the virtual-time offset changed.
		// A valid transition may only disable time skipping or update session state.
		return ms.accumulatedSkippedDuration() != previousDuration
	case historyi.TransactionPolicyPassive:
		return false
	default:
		ms.logger.Error(fmt.Sprintf("workflow time skipping: unknown transaction policy: %v", transactionPolicy))
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
	if err := ms.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(ctx, event); err != nil {
		return nil, err
	}
	metrics.ExecutionTimeSkippingTransitionedCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionTimeSkippingTransitionedEvent(ctx context.Context, event *historypb.HistoryEvent) error {
	attr := event.GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	tsi := ms.executionInfo.GetTimeSkippingInfo()

	opTag := tag.WorkflowActionWorkflowExecutionTimeSkippingTransitioned
	invalidTransitionError := serviceerror.NewInternal("TimeSkippingTransitionedEvent failed to apply")
	if tsi == nil {
		ms.logError("workflow time skipping transitioned event failed to apply: TimeSkippingInfo is nil", opTag)
		return invalidTransitionError
	}
	if attr.TargetTime == nil && !attr.GetDisabledAfterFastForward() {
		ms.logError("workflow time skipping transitioned event failed to apply: TargetTime is nil and disabled after fast-forward is false", opTag)
		return invalidTransitionError
	}

	hasSkipTarget := !timeNotSet(attr.TargetTime)
	var skippedDuration time.Duration
	if hasSkipTarget {
		skippedDuration = attr.TargetTime.AsTime().Sub(event.GetEventTime().AsTime())
	}
	ms.executeTimeSkippingTransition(
		skippedDuration,
		hasSkipTarget,
		attr.GetDisabledAfterFastForward(),
	)
	return nil
}

func (ms *MutableStateImpl) regenerateTimerTasksForWorkflowTimeSkipping(
	transactionPolicy historyi.TransactionPolicy,
) error {
	if !ms.IsWorkflow() {
		return nil
	}
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
// Time Skipping Runtime Methods for Chasm-based Executions
// =============================================================================

func (ms *MutableStateImpl) RecordTimeSkippingTransition(transition *chasm.TimeSkippingTransition) {
	if ms.IsWorkflow() || !transition.IsValid() {
		return
	}

	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if tsi == nil {
		return
	}
	skippedDuration := transition.GetSkippedDuration()
	ms.executeTimeSkippingTransition(
		skippedDuration,
		!transition.GetTargetTime().IsZero(),
		transition.DisabledAfterFastForward,
	)
	metrics.ExecutionTimeSkippingTransitionedCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
	if skippedDuration > 0 {
		ms.addTimeSkippingFastForwardTask()
	}
}

func (ms *MutableStateImpl) executeTimeSkippingTransition(
	skippedDuration time.Duration,
	hasSkipTarget bool,
	disabledAfterFastForward bool,
) {
	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if hasSkipTarget {
		tsi.AccumulatedSkippedDuration = durationpb.New(
			ms.accumulatedSkippedDuration() + skippedDuration)
	}
	if disabledAfterFastForward && tsi.GetFastForwardInfo() != nil {
		reachedFFInfo := tsi.GetFastForwardInfo()
		reachedFFInfo.HasReached = true
		ms.setAndStampFastForwardInfo(reachedFFInfo)
		tsi.Config.Enabled = false
		ms.logger.Info("execution time skipping disabled: fast-forward reached",
			tag.Time("fast-forward-target-time", reachedFFInfo.GetTargetTime().AsTime()),
			tag.Duration("accumulated-skipped-duration", ms.accumulatedSkippedDuration()))
	}

	tsi.SessionSkipCount++
	if tsi.SessionSkipCount >= tsi.GetConfig().GetMaxSessionSkipCount() && tsi.GetConfig().GetEnabled() {
		tsi.Config.Enabled = false
		ms.logger.Info("execution time skipping disabled: max session skip count reached",
			tag.Int32("max-session-skip-count", tsi.GetConfig().GetMaxSessionSkipCount()),
			tag.Int32("session-skip-count", tsi.GetSessionSkipCount()),
			tag.Duration("accumulated-skipped-duration", ms.accumulatedSkippedDuration()))
	}
	ms.timeSkippingInfoUpdated = true
}

// flagSkipDurationUpdateInPassive marks the chasm tree to re-stamp timer tasks against the
// replicated accumulated skip, detected via changes to TimeSkippingInfo. On the active cluster,
// tasks are regenerated as time is skipped; on a passive cluster those time changes instead arrive
// via replication from the active cluster, so this flag is how the passive side triggers the
// equivalent task regeneration. It is called from the replication paths in ApplyMutation and
// ApplySnapshot.
func (ms *MutableStateImpl) flagSkipDurationUpdateInPassive(
	prevTimeSkippingVT *persistencespb.VersionedTransition,
	preAccumulatedSkipDuration time.Duration,
) {
	if ms.IsWorkflow() {
		// Workflow executions now re-stamp time-skipping timers via TaskRefresher.refreshTasksForTimeSkipping.
		return
	}
	currTimeSkippingVT := ms.executionInfo.GetTimeSkippingInfo().GetLastUpdateVersionedTransition()
	if transitionhistory.Compare(currTimeSkippingVT, prevTimeSkippingVT) <= 0 {
		return
	}
	if ms.accumulatedSkippedDuration() == preAccumulatedSkipDuration {
		return
	}
	ms.chasmTree.MarkTotalTimeSkippedUpdatedInPassive()
	ms.addTimeSkippingFastForwardTask()
}

// =============================================================================
// Time Skipping Utility Methods
// =============================================================================

func (ms *MutableStateImpl) addTimeSkippingFastForwardTask() {
	tsi := ms.GetExecutionInfo().GetTimeSkippingInfo()
	if !NewTimeSkippingInfoUtil(tsi).HasPendingFastForward() {
		return
	}
	ms.AddTasks(&tasks.TimeSkippingFastForwardTimerTask{
		WorkflowKey:         ms.GetWorkflowKey(),
		VisibilityTimestamp: tsi.GetFastForwardInfo().GetTargetTime().AsTime(),
		VersionedTransition: tsi.GetFastForwardInfoLastUpdateVersionedTransition(),
		ArchetypeID:         ms.ChasmTree().ArchetypeID(),
	})
}
