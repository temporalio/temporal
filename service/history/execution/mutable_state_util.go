// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package execution

import (
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/persistence"
)

type (
	// TransactionPolicy is the policy used for updating workflow execution
	TransactionPolicy int
)

const (
	// TransactionPolicyActive updates workflow execution as active
	TransactionPolicyActive TransactionPolicy = 0
	// TransactionPolicyPassive updates workflow execution as passive
	TransactionPolicyPassive TransactionPolicy = 1
)

// Ptr returns a pointer to the current transaction policy
func (policy TransactionPolicy) Ptr() *TransactionPolicy {
	return &policy
}

// NOTE: do not use make(type, len(input))
// since this will assume initial length being len(inputs)
// always use make(type, 0, len(input))

func convertPendingActivityInfos(
	inputs map[int64]*persistence.ActivityInfo,
) []*persistence.ActivityInfo {

	outputs := make([]*persistence.ActivityInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateActivityInfos(
	inputs map[*persistence.ActivityInfo]struct{},
) []*persistence.ActivityInfo {

	outputs := make([]*persistence.ActivityInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertDeleteActivityInfos(
	inputs map[int64]struct{},
) []int64 {

	outputs := make([]int64, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertSyncActivityInfos(
	activityInfos map[int64]*persistence.ActivityInfo,
	inputs map[int64]struct{},
) []persistence.Task {
	outputs := make([]persistence.Task, 0, len(inputs))
	for item := range inputs {
		activityInfo, ok := activityInfos[item]
		if ok {
			outputs = append(outputs, &persistence.SyncActivityTask{
				Version:     activityInfo.Version,
				ScheduledID: activityInfo.ScheduleID,
			})
		}
	}
	return outputs
}

func convertPendingTimerInfos(
	inputs map[string]*persistence.TimerInfo,
) []*persistence.TimerInfo {

	outputs := make([]*persistence.TimerInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateTimerInfos(
	inputs map[*persistence.TimerInfo]struct{},
) []*persistence.TimerInfo {

	outputs := make([]*persistence.TimerInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertDeleteTimerInfos(
	inputs map[string]struct{},
) []string {

	outputs := make([]string, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingChildExecutionInfos(
	inputs map[int64]*persistence.ChildExecutionInfo,
) []*persistence.ChildExecutionInfo {

	outputs := make([]*persistence.ChildExecutionInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateChildExecutionInfos(
	inputs map[*persistence.ChildExecutionInfo]struct{},
) []*persistence.ChildExecutionInfo {

	outputs := make([]*persistence.ChildExecutionInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingRequestCancelInfos(
	inputs map[int64]*persistence.RequestCancelInfo,
) []*persistence.RequestCancelInfo {

	outputs := make([]*persistence.RequestCancelInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateRequestCancelInfos(
	inputs map[*persistence.RequestCancelInfo]struct{},
) []*persistence.RequestCancelInfo {

	outputs := make([]*persistence.RequestCancelInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingSignalInfos(
	inputs map[int64]*persistence.SignalInfo,
) []*persistence.SignalInfo {

	outputs := make([]*persistence.SignalInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateSignalInfos(
	inputs map[*persistence.SignalInfo]struct{},
) []*persistence.SignalInfo {

	outputs := make([]*persistence.SignalInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertSignalRequestedIDs(
	inputs map[string]struct{},
) []string {

	outputs := make([]string, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func setTaskInfo(
	version int64,
	timestamp time.Time,
	transferTasks []persistence.Task,
	timerTasks []persistence.Task,
) {
	// set both the task version, as well as the timestamp on the transfer tasks
	for _, task := range transferTasks {
		task.SetVersion(version)
		task.SetVisibilityTimestamp(timestamp)
	}
	for _, task := range timerTasks {
		task.SetVersion(version)
	}
}

// FailDecision fails the current decision task
func FailDecision(
	mutableState MutableState,
	decision *DecisionInfo,
	decisionFailureCause workflow.DecisionTaskFailedCause,
) error {

	if _, err := mutableState.AddDecisionTaskFailedEvent(
		decision.ScheduleID,
		decision.StartedID,
		decisionFailureCause,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}

	return mutableState.FlushBufferedEvents()
}

// ScheduleDecision schedules a new decision task
func ScheduleDecision(
	mutableState MutableState,
) error {

	if mutableState.HasPendingDecision() {
		return nil
	}

	_, err := mutableState.AddDecisionTaskScheduledEvent(false)
	if err != nil {
		return &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
	}
	return nil
}

// FindAutoResetPoint returns the auto reset point
func FindAutoResetPoint(
	timeSource clock.TimeSource,
	badBinaries *workflow.BadBinaries,
	autoResetPoints *workflow.ResetPoints,
) (string, *workflow.ResetPointInfo) {
	if badBinaries == nil || badBinaries.Binaries == nil || autoResetPoints == nil || autoResetPoints.Points == nil {
		return "", nil
	}
	nowNano := timeSource.Now().UnixNano()
	for _, p := range autoResetPoints.Points {
		bin, ok := badBinaries.Binaries[p.GetBinaryChecksum()]
		if ok && p.GetResettable() {
			if p.GetExpiringTimeNano() > 0 && nowNano > p.GetExpiringTimeNano() {
				// reset point has expired and we may already deleted the history
				continue
			}
			return bin.GetReason(), p
		}
	}
	return "", nil
}

// CreatePersistenceMutableState creates a persistence mutable state based on the its in-memory version
func CreatePersistenceMutableState(ms MutableState) *persistence.WorkflowMutableState {
	builder := ms.(*mutableStateBuilder)
	builder.FlushBufferedEvents() //nolint:errcheck
	info := CopyWorkflowExecutionInfo(builder.GetExecutionInfo())
	stats := &persistence.ExecutionStats{}
	activityInfos := make(map[int64]*persistence.ActivityInfo)
	for id, info := range builder.GetPendingActivityInfos() {
		activityInfos[id] = CopyActivityInfo(info)
	}
	timerInfos := make(map[string]*persistence.TimerInfo)
	for id, info := range builder.GetPendingTimerInfos() {
		timerInfos[id] = CopyTimerInfo(info)
	}
	cancellationInfos := make(map[int64]*persistence.RequestCancelInfo)
	for id, info := range builder.GetPendingRequestCancelExternalInfos() {
		cancellationInfos[id] = CopyCancellationInfo(info)
	}
	signalInfos := make(map[int64]*persistence.SignalInfo)
	for id, info := range builder.GetPendingSignalExternalInfos() {
		signalInfos[id] = CopySignalInfo(info)
	}
	childInfos := make(map[int64]*persistence.ChildExecutionInfo)
	for id, info := range builder.GetPendingChildExecutionInfos() {
		childInfos[id] = CopyChildInfo(info)
	}

	builder.FlushBufferedEvents() //nolint:errcheck
	var bufferedEvents []*workflow.HistoryEvent
	if len(builder.bufferedEvents) > 0 {
		bufferedEvents = append(bufferedEvents, builder.bufferedEvents...)
	}
	if len(builder.updateBufferedEvents) > 0 {
		bufferedEvents = append(bufferedEvents, builder.updateBufferedEvents...)
	}
	var replicationState *persistence.ReplicationState
	if builder.GetReplicationState() != nil {
		replicationState = CopyReplicationState(builder.GetReplicationState())
	}

	return &persistence.WorkflowMutableState{
		ExecutionInfo:       info,
		ExecutionStats:      stats,
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		BufferedEvents:      bufferedEvents,
		SignalInfos:         signalInfos,
		RequestCancelInfos:  cancellationInfos,
		ChildExecutionInfos: childInfos,
		ReplicationState:    replicationState,
	}
}

// CopyWorkflowExecutionInfo copies WorkflowExecutionInfo
func CopyWorkflowExecutionInfo(sourceInfo *persistence.WorkflowExecutionInfo) *persistence.WorkflowExecutionInfo {
	return &persistence.WorkflowExecutionInfo{
		DomainID:                           sourceInfo.DomainID,
		WorkflowID:                         sourceInfo.WorkflowID,
		RunID:                              sourceInfo.RunID,
		ParentDomainID:                     sourceInfo.ParentDomainID,
		ParentWorkflowID:                   sourceInfo.ParentWorkflowID,
		ParentRunID:                        sourceInfo.ParentRunID,
		InitiatedID:                        sourceInfo.InitiatedID,
		CompletionEventBatchID:             sourceInfo.CompletionEventBatchID,
		CompletionEvent:                    sourceInfo.CompletionEvent,
		TaskList:                           sourceInfo.TaskList,
		StickyTaskList:                     sourceInfo.StickyTaskList,
		StickyScheduleToStartTimeout:       sourceInfo.StickyScheduleToStartTimeout,
		WorkflowTypeName:                   sourceInfo.WorkflowTypeName,
		WorkflowTimeout:                    sourceInfo.WorkflowTimeout,
		DecisionStartToCloseTimeout:        sourceInfo.DecisionStartToCloseTimeout,
		ExecutionContext:                   sourceInfo.ExecutionContext,
		State:                              sourceInfo.State,
		CloseStatus:                        sourceInfo.CloseStatus,
		LastFirstEventID:                   sourceInfo.LastFirstEventID,
		LastEventTaskID:                    sourceInfo.LastEventTaskID,
		NextEventID:                        sourceInfo.NextEventID,
		LastProcessedEvent:                 sourceInfo.LastProcessedEvent,
		StartTimestamp:                     sourceInfo.StartTimestamp,
		LastUpdatedTimestamp:               sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:                    sourceInfo.CreateRequestID,
		SignalCount:                        sourceInfo.SignalCount,
		DecisionVersion:                    sourceInfo.DecisionVersion,
		DecisionScheduleID:                 sourceInfo.DecisionScheduleID,
		DecisionStartedID:                  sourceInfo.DecisionStartedID,
		DecisionRequestID:                  sourceInfo.DecisionRequestID,
		DecisionTimeout:                    sourceInfo.DecisionTimeout,
		DecisionAttempt:                    sourceInfo.DecisionAttempt,
		DecisionStartedTimestamp:           sourceInfo.DecisionStartedTimestamp,
		DecisionOriginalScheduledTimestamp: sourceInfo.DecisionOriginalScheduledTimestamp,
		CancelRequested:                    sourceInfo.CancelRequested,
		CancelRequestID:                    sourceInfo.CancelRequestID,
		CronSchedule:                       sourceInfo.CronSchedule,
		ClientLibraryVersion:               sourceInfo.ClientLibraryVersion,
		ClientFeatureVersion:               sourceInfo.ClientFeatureVersion,
		ClientImpl:                         sourceInfo.ClientImpl,
		AutoResetPoints:                    sourceInfo.AutoResetPoints,
		Memo:                               sourceInfo.Memo,
		SearchAttributes:                   sourceInfo.SearchAttributes,
		Attempt:                            sourceInfo.Attempt,
		HasRetryPolicy:                     sourceInfo.HasRetryPolicy,
		InitialInterval:                    sourceInfo.InitialInterval,
		BackoffCoefficient:                 sourceInfo.BackoffCoefficient,
		MaximumInterval:                    sourceInfo.MaximumInterval,
		ExpirationTime:                     sourceInfo.ExpirationTime,
		MaximumAttempts:                    sourceInfo.MaximumAttempts,
		NonRetriableErrors:                 sourceInfo.NonRetriableErrors,
		BranchToken:                        sourceInfo.BranchToken,
		ExpirationSeconds:                  sourceInfo.ExpirationSeconds,
	}
}

// CopyActivityInfo copies ActivityInfo
func CopyActivityInfo(sourceInfo *persistence.ActivityInfo) *persistence.ActivityInfo {
	details := make([]byte, len(sourceInfo.Details))
	copy(details, sourceInfo.Details)

	var scheduledEvent *workflow.HistoryEvent
	var startedEvent *workflow.HistoryEvent
	if sourceInfo.ScheduledEvent != nil {
		scheduledEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.ScheduledEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = scheduledEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	if sourceInfo.StartedEvent != nil {
		startedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.StartedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = startedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}

	return &persistence.ActivityInfo{
		Version:                  sourceInfo.Version,
		ScheduleID:               sourceInfo.ScheduleID,
		ScheduledEventBatchID:    sourceInfo.ScheduledEventBatchID,
		ScheduledEvent:           scheduledEvent,
		StartedID:                sourceInfo.StartedID,
		StartedEvent:             startedEvent,
		ActivityID:               sourceInfo.ActivityID,
		RequestID:                sourceInfo.RequestID,
		Details:                  details,
		ScheduledTime:            sourceInfo.ScheduledTime,
		StartedTime:              sourceInfo.StartedTime,
		ScheduleToStartTimeout:   sourceInfo.ScheduleToStartTimeout,
		ScheduleToCloseTimeout:   sourceInfo.ScheduleToCloseTimeout,
		StartToCloseTimeout:      sourceInfo.StartToCloseTimeout,
		HeartbeatTimeout:         sourceInfo.HeartbeatTimeout,
		LastHeartBeatUpdatedTime: sourceInfo.LastHeartBeatUpdatedTime,
		CancelRequested:          sourceInfo.CancelRequested,
		CancelRequestID:          sourceInfo.CancelRequestID,
		TimerTaskStatus:          sourceInfo.TimerTaskStatus,
		Attempt:                  sourceInfo.Attempt,
		DomainID:                 sourceInfo.DomainID,
		StartedIdentity:          sourceInfo.StartedIdentity,
		TaskList:                 sourceInfo.TaskList,
		HasRetryPolicy:           sourceInfo.HasRetryPolicy,
		InitialInterval:          sourceInfo.InitialInterval,
		BackoffCoefficient:       sourceInfo.BackoffCoefficient,
		MaximumInterval:          sourceInfo.MaximumInterval,
		ExpirationTime:           sourceInfo.ExpirationTime,
		MaximumAttempts:          sourceInfo.MaximumAttempts,
		NonRetriableErrors:       sourceInfo.NonRetriableErrors,
		LastFailureReason:        sourceInfo.LastFailureReason,
		LastWorkerIdentity:       sourceInfo.LastWorkerIdentity,
		LastFailureDetails:       sourceInfo.LastFailureDetails,
		// Not written to database - This is used only for deduping heartbeat timer creation
		LastHeartbeatTimeoutVisibilityInSeconds: sourceInfo.LastHeartbeatTimeoutVisibilityInSeconds,
	}
}

// CopyTimerInfo copies TimerInfo
func CopyTimerInfo(sourceInfo *persistence.TimerInfo) *persistence.TimerInfo {
	return &persistence.TimerInfo{
		Version:    sourceInfo.Version,
		TimerID:    sourceInfo.TimerID,
		StartedID:  sourceInfo.StartedID,
		ExpiryTime: sourceInfo.ExpiryTime,
		TaskStatus: sourceInfo.TaskStatus,
	}
}

// CopyCancellationInfo copies RequestCancelInfo
func CopyCancellationInfo(sourceInfo *persistence.RequestCancelInfo) *persistence.RequestCancelInfo {
	return &persistence.RequestCancelInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		CancelRequestID: sourceInfo.CancelRequestID,
	}
}

// CopySignalInfo copies SignalInfo
func CopySignalInfo(sourceInfo *persistence.SignalInfo) *persistence.SignalInfo {
	result := &persistence.SignalInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		SignalRequestID: sourceInfo.SignalRequestID,
		SignalName:      sourceInfo.SignalName,
	}
	result.Input = make([]byte, len(sourceInfo.Input))
	copy(result.Input, sourceInfo.Input)
	result.Control = make([]byte, len(sourceInfo.Control))
	copy(result.Control, sourceInfo.Control)
	return result
}

// CopyChildInfo copies ChildExecutionInfo
func CopyChildInfo(sourceInfo *persistence.ChildExecutionInfo) *persistence.ChildExecutionInfo {
	result := &persistence.ChildExecutionInfo{
		Version:               sourceInfo.Version,
		InitiatedID:           sourceInfo.InitiatedID,
		InitiatedEventBatchID: sourceInfo.InitiatedEventBatchID,
		StartedID:             sourceInfo.StartedID,
		StartedWorkflowID:     sourceInfo.StartedWorkflowID,
		StartedRunID:          sourceInfo.StartedRunID,
		CreateRequestID:       sourceInfo.CreateRequestID,
		DomainName:            sourceInfo.DomainName,
		WorkflowTypeName:      sourceInfo.WorkflowTypeName,
		ParentClosePolicy:     sourceInfo.ParentClosePolicy,
	}

	if sourceInfo.InitiatedEvent != nil {
		result.InitiatedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.InitiatedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = result.InitiatedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	if sourceInfo.StartedEvent != nil {
		result.StartedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.StartedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = result.StartedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	return result
}

// CopyReplicationState copies workflow ReplicationState
func CopyReplicationState(source *persistence.ReplicationState) *persistence.ReplicationState {
	var lastReplicationInfo map[string]*persistence.ReplicationInfo
	if source.LastReplicationInfo != nil {
		lastReplicationInfo = map[string]*persistence.ReplicationInfo{}
		for k, v := range source.LastReplicationInfo {
			lastReplicationInfo[k] = &persistence.ReplicationInfo{
				Version:     v.Version,
				LastEventID: v.LastEventID,
			}
		}
	}

	return &persistence.ReplicationState{
		CurrentVersion:      source.CurrentVersion,
		StartVersion:        source.StartVersion,
		LastWriteVersion:    source.LastWriteVersion,
		LastWriteEventID:    source.LastWriteEventID,
		LastReplicationInfo: lastReplicationInfo,
	}
}
