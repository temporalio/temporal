package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/hsm"
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
	_, skippable := ms.workflowTimeSkippingEligibility()
	return skippable
}

// workflowTimeSkippingEligibility evaluates the workflow gates and returns the Nexus timer
// targets discovered by the final, comparatively expensive gate. The close path passes those
// targets directly to findNextSkipTargetWithNexus so CHASM/HSM operations are loaded only once.
func (ms *MutableStateImpl) workflowTimeSkippingEligibility() ([]time.Time, bool) {
	// (1) gate by time skipping configuration; runs for every workflow, so it is not logged
	if !NewTimeSkippingInfoUtil(ms.GetExecutionInfo().GetTimeSkippingInfo()).IsEnabled() {
		return nil, false
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
		return nil, false
	}

	// (2) gate by workflow state and status
	if !ms.IsWorkflowExecutionRunning() {
		noSkippingReason = "workflow is not running"
		return nil, false
	}
	if ms.IsWorkflowExecutionStatusPaused() {
		noSkippingReason = "workflow is paused"
		return nil, false
	}

	// (3) gate by inflight work
	// HasPendingWorkflowTask covers both normal and speculative workflow tasks
	if ms.HasPendingWorkflowTask() {
		noSkippingReason = "has pending workflow task"
		return nil, false
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
		return nil, false
	}
	if len(ms.GetPendingChildExecutionInfos()) > 0 {
		noSkippingReason = "has pending child execution"
		return nil, false
	}
	if len(ms.GetPendingSignalExternalInfos()) > 0 {
		noSkippingReason = "has pending signal external"
		return nil, false
	}
	if len(ms.GetPendingRequestCancelExternalInfos()) > 0 {
		noSkippingReason = "has pending request cancel external"
		return nil, false
	}

	nexusOperationTargetTimes, allBackingOff, err := ms.nexusOperationSkipTargetTimes()
	if err != nil {
		noSkippingReason = "failed to inspect nexus operations"
		ms.logger.Error("failed to inspect Nexus operations while checking workflow time skipping", tag.Error(err))
		return nil, false
	}
	if !allBackingOff {
		noSkippingReason = "has pending nexus operations"
		return nil, false
	}
	return nexusOperationTargetTimes, true
}

// findNextSkipTarget finds the earliest future target time among pending timers, activity retries,
// workflow backoff, and execution timeouts, then applies the fast-forward bound. A pending task due
// now or earlier blocks time skipping until it is processed. Call only after isWorkflowSkippable returns true.
func (ms *MutableStateImpl) findNextSkipTarget() *chasm.TimeSkippingTransition {
	nexusOperationTargetTimes, allBackingOff, err := ms.nexusOperationSkipTargetTimes()
	if err != nil {
		ms.logger.Error("failed to inspect Nexus operations while finding workflow time-skipping target", tag.Error(err))
		return nil
	}
	if !allBackingOff {
		return nil
	}
	return ms.findNextSkipTargetWithNexus(nexusOperationTargetTimes)
}

func (ms *MutableStateImpl) findNextSkipTargetWithNexus(
	nexusOperationTargetTimes []time.Time,
) *chasm.TimeSkippingTransition {
	transition := chasm.NewTimeSkippingTransition(ms.Now())
	if ms.trackWorkflowTimerTargets(transition, nexusOperationTargetTimes) ||
		ms.trackWorkflowBackoffTarget(transition) ||
		ms.trackWorkflowTimeoutTargets(transition) {
		return nil
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

func (ms *MutableStateImpl) trackWorkflowTimerTargets(
	transition *chasm.TimeSkippingTransition,
	nexusOperationTargetTimes []time.Time,
) bool {
	for _, timerInfo := range ms.GetPendingTimerInfos() {
		if ms.trackFutureOrReportDueTaskForWorkflow(transition, timerInfo.ExpiryTime.AsTime(), "user timer") {
			return true
		}
	}

	// Activities waiting out a retry backoff are skip targets: advance to the earliest
	// next-attempt time.
	for _, ai := range ms.GetPendingActivityInfos() {
		if !activityPendingRetry(ai) {
			continue
		}
		if ms.trackFutureOrReportDueTaskForWorkflow(transition, ai.GetScheduledTime().AsTime(), "activity retry") {
			return true
		}
	}
	for _, targetTime := range nexusOperationTargetTimes {
		if ms.trackFutureOrReportDueTaskForWorkflow(transition, targetTime, "nexus operation timer") {
			return true
		}
	}
	return false
}

func (ms *MutableStateImpl) trackWorkflowBackoffTarget(transition *chasm.TimeSkippingTransition) bool {
	if ms.HadOrHasWorkflowTask() {
		return false
	}
	// Support start-with-delay, cron, retry, and CaN-with-backoff. A child workflow can
	// have no workflow task yet without a backoff; then ExecutionTime == StartTime.
	executionTime := ms.executionInfo.GetExecutionTime().AsTime()
	if !executionTime.After(ms.executionInfo.GetStartTime().AsTime()) {
		return false
	}
	return ms.trackFutureOrReportDueTaskForWorkflow(transition, executionTime, "workflow backoff")
}

func (ms *MutableStateImpl) trackWorkflowTimeoutTargets(transition *chasm.TimeSkippingTransition) bool {
	if !ms.HadOrHasWorkflowTask() {
		return false
	}
	// Run/execution timeouts are valid targets only after the workflow has started executing.
	// Otherwise, a newly created child could skip to its timeout before its first workflow task.
	if t := ms.executionInfo.GetWorkflowRunExpirationTime(); t != nil && !t.AsTime().IsZero() &&
		ms.trackFutureOrReportDueTaskForWorkflow(transition, t.AsTime(), "workflow run timeout") {
		return true
	}
	if t := ms.executionInfo.GetWorkflowExecutionExpirationTime(); t != nil && !t.AsTime().IsZero() &&
		ms.trackFutureOrReportDueTaskForWorkflow(transition, t.AsTime(), "workflow execution timeout") {
		return true
	}
	return false
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
		nexusOperationTargetTimes, skippable := ms.workflowTimeSkippingEligibility()
		if !skippable {
			return false
		}
		// 2. find the next skip target; if there is none, time skipping is not needed
		transition := ms.findNextSkipTargetWithNexus(nexusOperationTargetTimes)
		if !transition.IsValid() {
			return false
		}
		// 3. state change.
		previousDuration := ms.accumulatedSkippedDuration()
		_, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
			ctx, transition.GetTargetTime(), transition.DisabledAfterFastForward)
		if err != nil {
			metrics.ExecutionTimeSkippingTransitionedErrorCounter.With(ms.metricsHandler).Record(1, ms.timeSkippingMetricTags()...)
			ms.logger.Error("failed to add workflow execution time skipping transitioned event", tag.Error(err))
			return false
		}
		// 4. Timer tasks only need to be re-stamped when the virtual-time offset changed.
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

// nexusOperationSkipTargetTimes returns retry and timeout times for pending Nexus operations.
// The boolean is false when any operation is not waiting in retry backoff or any of its active
// timers is not strictly in the future.
func (ms *MutableStateImpl) nexusOperationSkipTargetTimes() ([]time.Time, bool, error) {
	now := ms.Now()
	targetTimes, ok, err := ms.hsmNexusOperationSkipTargetTimes(now)
	if err != nil || !ok || !ms.ChasmEnabled() {
		return targetTimes, ok, err
	}
	chasmTargetTimes, ok, err := ms.chasmNexusOperationSkipTargetTimes(now)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append(targetTimes, chasmTargetTimes...), true, nil
}

func (ms *MutableStateImpl) hsmNexusOperationSkipTargetTimes(now time.Time) ([]time.Time, bool, error) {
	var targetTimes []time.Time
	for _, node := range nexusoperations.MachineCollection(ms.HSM()).List() {
		op, err := hsm.MachineData[nexusoperations.Operation](node)
		if err != nil {
			return nil, false, err
		}
		if op.State() != enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF {
			return nil, false, nil
		}
		opTargetTimes, ok := nexusOperationTargetTimes(
			now,
			op.GetScheduledTime().AsTime(),
			op.GetNextAttemptScheduleTime().AsTime(),
			op.GetScheduleToCloseTimeout().AsDuration(),
			op.GetScheduleToStartTimeout().AsDuration(),
		)
		if !ok {
			return nil, false, nil
		}
		targetTimes = append(targetTimes, opTargetTimes...)
	}
	return targetTimes, true, nil
}

func (ms *MutableStateImpl) chasmNexusOperationSkipTargetTimes(now time.Time) ([]time.Time, bool, error) {
	wf, chasmCtx, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return nil, true, nil
		}
		return nil, false, err
	}
	var targetTimes []time.Time
	for _, field := range wf.Operations {
		op := field.Get(chasmCtx)
		if op.GetStatus() != nexusoperationpb.OPERATION_STATUS_BACKING_OFF {
			return nil, false, nil
		}
		opTargetTimes, ok := nexusOperationTargetTimes(
			now,
			op.GetScheduledTime().AsTime(),
			op.GetNextAttemptScheduleTime().AsTime(),
			op.GetScheduleToCloseTimeout().AsDuration(),
			op.GetScheduleToStartTimeout().AsDuration(),
		)
		if !ok {
			return nil, false, nil
		}
		targetTimes = append(targetTimes, opTargetTimes...)
	}
	return targetTimes, true, nil
}

func nexusOperationTargetTimes(
	now time.Time,
	scheduledTime time.Time,
	nextAttemptTime time.Time,
	scheduleToCloseTimeout time.Duration,
	scheduleToStartTimeout time.Duration,
) ([]time.Time, bool) {
	targetTimes := []time.Time{nextAttemptTime}
	if scheduleToCloseTimeout > 0 {
		targetTimes = append(targetTimes, scheduledTime.Add(scheduleToCloseTimeout))
	}
	if scheduleToStartTimeout > 0 {
		targetTimes = append(targetTimes, scheduledTime.Add(scheduleToStartTimeout))
	}
	for _, targetTime := range targetTimes {
		if !now.Before(targetTime) {
			return nil, false
		}
	}
	return targetTimes, true
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
