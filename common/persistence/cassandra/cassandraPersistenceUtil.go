// Copyright (c) 2019 Uber Technologies, Inc.
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

package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
)

func (d *cassandraPersistence) applyWorkflowMutationBatch(
	batch *gocql.Batch,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	executionInfo := workflowMutation.ExecutionInfo
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	condition := workflowMutation.Condition
	replicationState := workflowMutation.ReplicationState

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateCloseStatus(
		executionInfo.State,
		executionInfo.CloseStatus); err != nil {
		return err
	}

	d.updateMutableState(
		batch,
		executionInfo,
		replicationState,
		cqlNowTimestamp,
		true,
		condition,
	)

	if err := d.updateActivityInfos(
		batch,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		domainID,
		workflowID,
		runID,
		condition,
	); err != nil {
		return err
	}

	d.updateTimerInfos(
		batch,
		workflowMutation.UpserTimerInfos,
		workflowMutation.DeleteTimerInfos,
		domainID,
		workflowID,
		runID,
		condition,
	)

	if err := d.updateChildExecutionInfos(
		batch,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfo,
		domainID,
		workflowID,
		runID,
		condition,
	); err != nil {
		return err
	}

	d.updateRequestCancelInfos(
		batch,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfo,
		domainID,
		workflowID,
		runID,
		condition,
	)

	d.updateSignalInfos(
		batch,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfo,
		domainID,
		workflowID,
		runID,
		condition,
	)

	d.updateSignalsRequested(
		batch,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedID,
		domainID,
		workflowID,
		runID,
		condition,
	)

	d.updateBufferedEvents(
		batch,
		workflowMutation.NewBufferedEvents,
		workflowMutation.ClearBufferedEvents,
		domainID,
		workflowID,
		runID,
		condition,
	)

	// transfer / replication / timer tasks

	d.createTransferTasks(
		batch,
		workflowMutation.TransferTasks,
		domainID,
		workflowID,
		runID,
	)

	d.createReplicationTasks(
		batch,
		workflowMutation.ReplicationTasks,
		domainID,
		workflowID,
		runID,
	)

	d.createTimerTasks(
		batch,
		workflowMutation.TimerTasks,
		domainID,
		workflowID,
		runID,
		cqlNowTimestamp,
	)

	return nil
}

func (d *cassandraPersistence) applyWorkflowSnapshotBatch(
	batch *gocql.Batch,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	executionInfo := workflowSnapshot.ExecutionInfo
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	condition := workflowSnapshot.Condition
	replicationState := workflowSnapshot.ReplicationState

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateCloseStatus(
		executionInfo.State,
		executionInfo.CloseStatus,
	); err != nil {
		return err
	}

	d.updateMutableState(
		batch,
		executionInfo,
		replicationState,
		cqlNowTimestamp,
		true,
		condition,
	)

	if err := d.resetActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		domainID,
		workflowID,
		runID,
		true,
		condition,
	); err != nil {
		return err
	}

	d.resetTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		domainID,
		workflowID,
		runID,
		true,
		condition,
	)

	if err := d.resetChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		domainID,
		workflowID,
		runID,
		true,
		condition,
	); err != nil {
		return err
	}

	d.resetRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		domainID,
		workflowID,
		runID,
		true,
		condition,
	)

	d.resetSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		domainID,
		workflowID,
		runID,
		true,
		condition,
	)

	d.resetSignalRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		domainID,
		workflowID,
		runID,
		true,
		condition,
	)

	d.deleteBufferedEvents(
		batch,
		domainID,
		workflowID,
		runID,
		condition,
	)

	// transfer / replication / timer tasks

	d.createTransferTasks(
		batch,
		workflowSnapshot.TransferTasks,
		domainID,
		workflowID,
		runID,
	)

	d.createReplicationTasks(
		batch,
		workflowSnapshot.ReplicationTasks,
		domainID,
		workflowID,
		runID,
	)

	d.createTimerTasks(
		batch,
		workflowSnapshot.TimerTasks,
		domainID,
		workflowID,
		runID,
		cqlNowTimestamp,
	)

	return nil
}

func (d *cassandraPersistence) createTransferTasks(
	batch *gocql.Batch,
	transferTasks []p.Task,
	domainID string,
	workflowID string,
	runID string,
) {
	targetDomainID := domainID
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64
		targetWorkflowID := p.TransferTaskTransferTargetWorkflowID
		targetRunID := p.TransferTaskTransferTargetRunID
		targetChildWorkflowOnly := false
		recordVisibility := false

		switch task.GetType() {
		case p.TransferTaskTypeActivityTask:
			targetDomainID = task.(*p.ActivityTask).DomainID
			taskList = task.(*p.ActivityTask).TaskList
			scheduleID = task.(*p.ActivityTask).ScheduleID

		case p.TransferTaskTypeDecisionTask:
			targetDomainID = task.(*p.DecisionTask).DomainID
			taskList = task.(*p.DecisionTask).TaskList
			scheduleID = task.(*p.DecisionTask).ScheduleID
			recordVisibility = task.(*p.DecisionTask).RecordVisibility

		case p.TransferTaskTypeCancelExecution:
			targetDomainID = task.(*p.CancelExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.CancelExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.CancelExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = p.TransferTaskTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.CancelExecutionTask).InitiatedID

		case p.TransferTaskTypeSignalExecution:
			targetDomainID = task.(*p.SignalExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.SignalExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.SignalExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = p.TransferTaskTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.SignalExecutionTask).InitiatedID

		case p.TransferTaskTypeStartChildExecution:
			targetDomainID = task.(*p.StartChildExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.StartChildExecutionTask).TargetWorkflowID
			scheduleID = task.(*p.StartChildExecutionTask).InitiatedID

		case p.TransferTaskTypeCloseExecution,
			p.TransferTaskTypeRecordWorkflowStarted,
			p.TransferTaskTypeResetWorkflow:
			// No explicit property needs to be set

		default:
			d.logger.Fatal("Unknown Transfer Task.")
		}

		batch.Query(templateCreateTransferTaskQuery,
			d.shardID,
			rowTypeTransferTask,
			rowTypeTransferDomainID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			domainID,
			workflowID,
			runID,
			task.GetVisibilityTimestamp(),
			task.GetTaskID(),
			targetDomainID,
			targetWorkflowID,
			targetRunID,
			targetChildWorkflowOnly,
			taskList,
			task.GetType(),
			scheduleID,
			recordVisibility,
			task.GetVersion(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createReplicationTasks(
	batch *gocql.Batch,
	replicationTasks []p.Task,
	domainID string,
	workflowID string,
	runID string,
) {

	for _, task := range replicationTasks {
		// Replication task specific information
		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion
		var lastReplicationInfo map[string]map[string]interface{}
		activityScheduleID := common.EmptyEventID
		var eventStoreVersion, newRunEventStoreVersion int32
		var branchToken, newRunBranchToken []byte
		resetWorkflow := false

		switch task.GetType() {
		case p.ReplicationTaskTypeHistory:
			histTask := task.(*p.HistoryReplicationTask)
			eventStoreVersion = histTask.EventStoreVersion
			newRunEventStoreVersion = histTask.NewRunEventStoreVersion
			branchToken = histTask.BranchToken
			newRunBranchToken = histTask.NewRunBranchToken
			firstEventID = histTask.FirstEventID
			nextEventID = histTask.NextEventID
			version = task.GetVersion()
			lastReplicationInfo = make(map[string]map[string]interface{})
			for k, v := range histTask.LastReplicationInfo {
				lastReplicationInfo[k] = createReplicationInfoMap(v)
			}
			resetWorkflow = histTask.ResetWorkflow
		case p.ReplicationTaskTypeSyncActivity:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID
			// cassandra does not like null
			lastReplicationInfo = make(map[string]map[string]interface{})
		default:
			d.logger.Fatal("Unknown Replication Task.")
		}

		batch.Query(templateCreateReplicationTaskQuery,
			d.shardID,
			rowTypeReplicationTask,
			rowTypeReplicationDomainID,
			rowTypeReplicationWorkflowID,
			rowTypeReplicationRunID,
			domainID,
			workflowID,
			runID,
			task.GetTaskID(),
			task.GetType(),
			firstEventID,
			nextEventID,
			version,
			lastReplicationInfo,
			activityScheduleID,
			eventStoreVersion,
			branchToken,
			resetWorkflow,
			newRunEventStoreVersion,
			newRunBranchToken,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createTimerTasks(
	batch *gocql.Batch,
	timerTasks []p.Task,
	domainID string,
	workflowID string,
	runID string,
	cqlNowTimestamp int64,
) {

	for _, task := range timerTasks {
		var eventID int64
		var attempt int64

		timeoutType := 0

		switch t := task.(type) {
		case *p.DecisionTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.ScheduleAttempt
		case *p.ActivityTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.Attempt
		case *p.UserTimerTask:
			eventID = t.EventID
		case *p.ActivityRetryTimerTask:
			eventID = t.EventID
			attempt = int64(t.Attempt)
		case *p.WorkflowBackoffTimerTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
		}

		// Ignoring possible type cast errors.
		ts := p.UnixNanoToDBTimestamp(task.GetVisibilityTimestamp().UnixNano())

		batch.Query(templateCreateTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerDomainID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			domainID,
			workflowID,
			runID,
			ts,
			task.GetTaskID(),
			task.GetType(),
			timeoutType,
			eventID,
			attempt,
			task.GetVersion(),
			ts,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) updateActivityInfos(
	batch *gocql.Batch,
	activityInfos []*p.InternalActivityInfo,
	deleteInfos []int64,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) error {

	for _, a := range activityInfos {
		encoding := common.EncodingTypeUnknown
		if a.ScheduledEvent != nil {
			encoding = string(a.ScheduledEvent.GetEncoding())
		}
		scheduledEventData, _ := p.FromDataBlob(a.ScheduledEvent)
		startedEventData, _ := p.FromDataBlob(a.StartedEvent)
		if a.ScheduledEvent != nil && a.StartedEvent != nil && a.StartedEvent.Encoding != a.ScheduledEvent.Encoding {
			return p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}

		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			a.Version,
			a.ScheduleID,
			a.ScheduledEventBatchID,
			scheduledEventData,
			a.ScheduledTime,
			a.StartedID,
			startedEventData,
			a.StartedTime,
			a.ActivityID,
			a.RequestID,
			a.Details,
			a.ScheduleToStartTimeout,
			a.ScheduleToCloseTimeout,
			a.StartToCloseTimeout,
			a.HeartbeatTimeout,
			a.CancelRequested,
			a.CancelRequestID,
			a.LastHeartBeatUpdatedTime,
			a.TimerTaskStatus,
			a.Attempt,
			a.TaskList,
			a.StartedIdentity,
			a.HasRetryPolicy,
			a.InitialInterval,
			a.BackoffCoefficient,
			a.MaximumInterval,
			a.ExpirationTime,
			a.MaximumAttempts,
			a.NonRetriableErrors,
			encoding,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	for _, deleteInfo := range deleteInfos {
		batch.Query(templateDeleteActivityInfoQuery,
			deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
	return nil
}

func (d *cassandraPersistence) deleteBufferedEvents(
	batch *gocql.Batch,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) {

	batch.Query(templateDeleteBufferedEventsQuery,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)

}

func (d *cassandraPersistence) resetActivityInfos(
	batch *gocql.Batch,
	activityInfos []*p.InternalActivityInfo,
	domainID string,
	workflowID string,
	runID string,
	useCondition bool,
	condition int64,
) error {

	infoMap, err := resetActivityInfoMap(activityInfos)
	if err != nil {
		return err
	}

	batchQueryHelper(batch, templateResetActivityInfoQuery, useCondition, condition,
		infoMap,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func (d *cassandraPersistence) updateTimerInfos(
	batch *gocql.Batch,
	timerInfos []*p.TimerInfo,
	deleteInfos []string,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) {

	for _, a := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			a.TimerID,
			a.Version,
			a.TimerID,
			a.StartedID,
			a.ExpiryTime,
			a.TaskID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	for _, t := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			t,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetTimerInfos(
	batch *gocql.Batch,
	timerInfos []*p.TimerInfo,
	domainID string,
	workflowID string,
	runID string,
	useCondition bool,
	condition int64,
) {

	batchQueryHelper(batch, templateResetTimerInfoQuery, useCondition, condition,
		resetTimerInfoMap(timerInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos []*p.InternalChildExecutionInfo,
	deleteInfo *int64,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) error {

	for _, c := range childExecutionInfos {
		initiatedEventData, encoding := p.FromDataBlob(c.InitiatedEvent)

		var startedEventData []byte
		if c.StartedEvent != nil {
			startedEventData = c.StartedEvent.Data
			if string(c.StartedEvent.GetEncoding()) != encoding {
				return p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", encoding, c.StartedEvent.GetEncoding()))
			}
		}
		startedRunID := emptyRunID
		if c.StartedRunID != "" {
			startedRunID = c.StartedRunID
		}
		batch.Query(templateUpdateChildExecutionInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.InitiatedEventBatchID,
			initiatedEventData,
			c.StartedID,
			c.StartedWorkflowID,
			startedRunID,
			startedEventData,
			c.CreateRequestID,
			encoding,
			c.DomainName,
			c.WorkflowTypeName,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for ChildInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
	return nil
}

func (d *cassandraPersistence) resetChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos []*p.InternalChildExecutionInfo,
	domainID string,
	workflowID string,
	runID string,
	useCondition bool,
	condition int64,
) error {

	infoMap, err := resetChildExecutionInfoMap(childExecutionInfos, d.logger)
	if err != nil {
		return err
	}
	batchQueryHelper(batch, templateResetChildExecutionInfoQuery, useCondition, condition,
		infoMap,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func (d *cassandraPersistence) updateRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos []*p.RequestCancelInfo,
	deleteInfo *int64,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) {

	for _, c := range requestCancelInfos {
		batch.Query(templateUpdateRequestCancelInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.CancelRequestID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for RequestCancelInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos []*p.RequestCancelInfo,
	domainID string,
	workflowID string,
	runID string,
	useCondition bool,
	condition int64,
) {

	batchQueryHelper(batch, templateResetRequestCancelInfoQuery, useCondition, condition,
		resetRequestCancelInfoMap(requestCancelInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateSignalInfos(
	batch *gocql.Batch,
	signalInfos []*p.SignalInfo,
	deleteInfo *int64,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) {

	for _, c := range signalInfos {
		batch.Query(templateUpdateSignalInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.SignalRequestID,
			c.SignalName,
			c.Input,
			c.Control,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for SignalInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteSignalInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetSignalInfos(
	batch *gocql.Batch,
	signalInfos []*p.SignalInfo,
	domainID string,
	workflowID string,
	runID string,
	useCondition bool,
	condition int64,
) {

	batchQueryHelper(batch, templateResetSignalInfoQuery, useCondition, condition,
		resetSignalInfoMap(signalInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateSignalsRequested(
	batch *gocql.Batch,
	signalReqIDs []string,
	deleteSignalReqID string,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			signalReqIDs,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	if deleteSignalReqID != "" {
		req := []string{deleteSignalReqID} // for cassandra set binding
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			req,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetSignalRequested(
	batch *gocql.Batch,
	signalRequested []string,
	domainID string,
	workflowID string,
	runID string,
	useCondition bool,
	condition int64,
) {

	batchQueryHelper(batch, templateResetSignalRequestedQuery, useCondition, condition,
		signalRequested,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateBufferedEvents(
	batch *gocql.Batch,
	newBufferedEvents *p.DataBlob,
	clearBufferedEvents bool,
	domainID string,
	workflowID string,
	runID string,
	condition int64,
) {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if newBufferedEvents != nil {
		values := make(map[string]interface{})
		values["encoding_type"] = newBufferedEvents.Encoding
		values["version"] = int64(0)
		values["data"] = newBufferedEvents.Data
		newEventValues := []map[string]interface{}{values}
		batch.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func createShardInfo(currentCluster string, result map[string]interface{}) *p.ShardInfo {
	info := &p.ShardInfo{}
	for k, v := range result {
		switch k {
		case "shard_id":
			info.ShardID = v.(int)
		case "owner":
			info.Owner = v.(string)
		case "range_id":
			info.RangeID = v.(int64)
		case "stolen_since_renew":
			info.StolenSinceRenew = v.(int)
		case "updated_at":
			info.UpdatedAt = v.(time.Time)
		case "replication_ack_level":
			info.ReplicationAckLevel = v.(int64)
		case "transfer_ack_level":
			info.TransferAckLevel = v.(int64)
		case "timer_ack_level":
			info.TimerAckLevel = v.(time.Time)
		case "cluster_transfer_ack_level":
			info.ClusterTransferAckLevel = v.(map[string]int64)
		case "cluster_timer_ack_level":
			info.ClusterTimerAckLevel = v.(map[string]time.Time)
		case "domain_notification_version":
			info.DomainNotificationVersion = v.(int64)
		}
	}

	if info.ClusterTransferAckLevel == nil {
		info.ClusterTransferAckLevel = map[string]int64{
			currentCluster: info.TransferAckLevel,
		}
	}
	if info.ClusterTimerAckLevel == nil {
		info.ClusterTimerAckLevel = map[string]time.Time{
			currentCluster: info.TimerAckLevel,
		}
	}

	return info
}

func createWorkflowExecutionInfo(result map[string]interface{}) *p.InternalWorkflowExecutionInfo {
	info := &p.InternalWorkflowExecutionInfo{}
	var completionEventData []byte
	var completionEventEncoding common.EncodingType
	var autoResetPoints []byte
	var autoResetPointsEncoding common.EncodingType

	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "parent_domain_id":
			info.ParentDomainID = v.(gocql.UUID).String()
			if info.ParentDomainID == emptyDomainID {
				info.ParentDomainID = ""
			}
		case "parent_workflow_id":
			info.ParentWorkflowID = v.(string)
		case "parent_run_id":
			info.ParentRunID = v.(gocql.UUID).String()
			if info.ParentRunID == emptyRunID {
				info.ParentRunID = ""
			}
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "completion_event_batch_id":
			info.CompletionEventBatchID = v.(int64)
		case "completion_event":
			completionEventData = v.([]byte)
		case "completion_event_data_encoding":
			completionEventEncoding = common.EncodingType(v.(string))
		case "auto_reset_points":
			autoResetPoints = v.([]byte)
		case "auto_reset_points_encoding":
			autoResetPointsEncoding = common.EncodingType(v.(string))
		case "task_list":
			info.TaskList = v.(string)
		case "workflow_type_name":
			info.WorkflowTypeName = v.(string)
		case "workflow_timeout":
			info.WorkflowTimeout = int32(v.(int))
		case "decision_task_timeout":
			info.DecisionTimeoutValue = int32(v.(int))
		case "execution_context":
			info.ExecutionContext = v.([]byte)
		case "state":
			info.State = v.(int)
		case "close_status":
			info.CloseStatus = v.(int)
		case "last_first_event_id":
			info.LastFirstEventID = v.(int64)
		case "last_event_task_id":
			info.LastEventTaskID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "last_processed_event":
			info.LastProcessedEvent = v.(int64)
		case "start_time":
			info.StartTimestamp = v.(time.Time)
		case "last_updated_time":
			info.LastUpdatedTimestamp = v.(time.Time)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "signal_count":
			info.SignalCount = int32(v.(int))
		case "history_size":
			info.HistorySize = v.(int64)
		case "decision_version":
			info.DecisionVersion = v.(int64)
		case "decision_schedule_id":
			info.DecisionScheduleID = v.(int64)
		case "decision_started_id":
			info.DecisionStartedID = v.(int64)
		case "decision_request_id":
			info.DecisionRequestID = v.(string)
		case "decision_timeout":
			info.DecisionTimeout = int32(v.(int))
		case "decision_attempt":
			info.DecisionAttempt = v.(int64)
		case "decision_timestamp":
			info.DecisionStartedTimestamp = v.(int64)
		case "decision_scheduled_timestamp":
			info.DecisionScheduledTimestamp = v.(int64)
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		case "sticky_task_list":
			info.StickyTaskList = v.(string)
		case "sticky_schedule_to_start_timeout":
			info.StickyScheduleToStartTimeout = int32(v.(int))
		case "client_library_version":
			info.ClientLibraryVersion = v.(string)
		case "client_feature_version":
			info.ClientFeatureVersion = v.(string)
		case "client_impl":
			info.ClientImpl = v.(string)
		case "attempt":
			info.Attempt = int32(v.(int))
		case "has_retry_policy":
			info.HasRetryPolicy = v.(bool)
		case "init_interval":
			info.InitialInterval = int32(v.(int))
		case "backoff_coefficient":
			info.BackoffCoefficient = v.(float64)
		case "max_interval":
			info.MaximumInterval = int32(v.(int))
		case "max_attempts":
			info.MaximumAttempts = int32(v.(int))
		case "expiration_time":
			info.ExpirationTime = v.(time.Time)
		case "non_retriable_errors":
			info.NonRetriableErrors = v.([]string)
		case "event_store_version":
			info.EventStoreVersion = int32(v.(int))
		case "branch_token":
			info.BranchToken = v.([]byte)
		case "cron_schedule":
			info.CronSchedule = v.(string)
		case "expiration_seconds":
			info.ExpirationSeconds = int32(v.(int))
		case "search_attributes":
			info.SearchAttributes = v.(map[string][]byte)
		}
	}
	info.CompletionEvent = p.NewDataBlob(completionEventData, completionEventEncoding)
	info.AutoResetPoints = p.NewDataBlob(autoResetPoints, autoResetPointsEncoding)
	return info
}

func createReplicationState(result map[string]interface{}) *p.ReplicationState {
	if len(result) == 0 {
		return nil
	}

	info := &p.ReplicationState{}
	for k, v := range result {
		switch k {
		case "current_version":
			info.CurrentVersion = v.(int64)
		case "start_version":
			info.StartVersion = v.(int64)
		case "last_write_version":
			info.LastWriteVersion = v.(int64)
		case "last_write_event_id":
			info.LastWriteEventID = v.(int64)
		case "last_replication_info":
			info.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		}
	}

	return info
}

func createTransferTaskInfo(result map[string]interface{}) *p.TransferTaskInfo {
	info := &p.TransferTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "target_domain_id":
			info.TargetDomainID = v.(gocql.UUID).String()
		case "target_workflow_id":
			info.TargetWorkflowID = v.(string)
		case "target_run_id":
			info.TargetRunID = v.(gocql.UUID).String()
			if info.TargetRunID == p.TransferTaskTransferTargetRunID {
				info.TargetRunID = ""
			}
		case "target_child_workflow_only":
			info.TargetChildWorkflowOnly = v.(bool)
		case "task_list":
			info.TaskList = v.(string)
		case "type":
			info.TaskType = v.(int)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "record_visibility":
			info.RecordVisibility = v.(bool)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationTaskInfo(result map[string]interface{}) *p.ReplicationTaskInfo {
	info := &p.ReplicationTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "first_event_id":
			info.FirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "version":
			info.Version = v.(int64)
		case "last_replication_info":
			info.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		case "scheduled_id":
			info.ScheduledID = v.(int64)
		case "event_store_version":
			info.EventStoreVersion = int32(v.(int))
		case "branch_token":
			info.BranchToken = v.([]byte)
		case "reset_workflow":
			info.ResetWorkflow = v.(bool)
		case "new_run_event_store_version":
			info.NewRunEventStoreVersion = int32(v.(int))
		case "new_run_branch_token":
			info.NewRunBranchToken = v.([]byte)
		}
	}

	return info
}

func createActivityInfo(domainID string, result map[string]interface{}) *p.InternalActivityInfo {
	info := &p.InternalActivityInfo{}
	var sharedEncoding common.EncodingType
	var scheduledEventData, startedEventData []byte
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "scheduled_event_batch_id":
			info.ScheduledEventBatchID = v.(int64)
		case "scheduled_event":
			scheduledEventData = v.([]byte)
		case "scheduled_time":
			info.ScheduledTime = v.(time.Time)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			startedEventData = v.([]byte)
		case "started_time":
			info.StartedTime = v.(time.Time)
		case "activity_id":
			info.ActivityID = v.(string)
		case "request_id":
			info.RequestID = v.(string)
		case "details":
			info.Details = v.([]byte)
		case "schedule_to_start_timeout":
			info.ScheduleToStartTimeout = int32(v.(int))
		case "schedule_to_close_timeout":
			info.ScheduleToCloseTimeout = int32(v.(int))
		case "start_to_close_timeout":
			info.StartToCloseTimeout = int32(v.(int))
		case "heart_beat_timeout":
			info.HeartbeatTimeout = int32(v.(int))
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(int64)
		case "last_hb_updated_time":
			info.LastHeartBeatUpdatedTime = v.(time.Time)
		case "timer_task_status":
			info.TimerTaskStatus = int32(v.(int))
		case "attempt":
			info.Attempt = int32(v.(int))
		case "task_list":
			info.TaskList = v.(string)
		case "started_identity":
			info.StartedIdentity = v.(string)
		case "has_retry_policy":
			info.HasRetryPolicy = v.(bool)
		case "init_interval":
			info.InitialInterval = (int32)(v.(int))
		case "backoff_coefficient":
			info.BackoffCoefficient = v.(float64)
		case "max_interval":
			info.MaximumInterval = (int32)(v.(int))
		case "max_attempts":
			info.MaximumAttempts = (int32)(v.(int))
		case "expiration_time":
			info.ExpirationTime = v.(time.Time)
		case "non_retriable_errors":
			info.NonRetriableErrors = v.([]string)
		case "event_data_encoding":
			sharedEncoding = common.EncodingType(v.(string))
		}
	}
	info.DomainID = domainID
	info.ScheduledEvent = p.NewDataBlob(scheduledEventData, sharedEncoding)
	info.StartedEvent = p.NewDataBlob(startedEventData, sharedEncoding)

	return info
}

func createTimerInfo(result map[string]interface{}) *p.TimerInfo {
	info := &p.TimerInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "timer_id":
			info.TimerID = v.(string)
		case "started_id":
			info.StartedID = v.(int64)
		case "expiry_time":
			info.ExpiryTime = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		}
	}
	return info
}

func createChildExecutionInfo(result map[string]interface{}, logger log.Logger) *p.InternalChildExecutionInfo {
	info := &p.InternalChildExecutionInfo{}
	var encoding common.EncodingType
	var initiatedData []byte
	var startedData []byte
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event_batch_id":
			info.InitiatedEventBatchID = v.(int64)
		case "initiated_event":
			initiatedData = v.([]byte)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_workflow_id":
			info.StartedWorkflowID = v.(string)
		case "started_run_id":
			info.StartedRunID = v.(gocql.UUID).String()
		case "started_event":
			startedData = v.([]byte)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "event_data_encoding":
			encoding = common.EncodingType(v.(string))
		case "domain_name":
			info.DomainName = v.(string)
		case "workflow_type_name":
			info.WorkflowTypeName = v.(string)
		}
	}
	info.InitiatedEvent = p.NewDataBlob(initiatedData, encoding)
	info.StartedEvent = p.NewDataBlob(startedData, encoding)
	return info
}

func createRequestCancelInfo(result map[string]interface{}) *p.RequestCancelInfo {
	info := &p.RequestCancelInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		}
	}

	return info
}

func createSignalInfo(result map[string]interface{}) *p.SignalInfo {
	info := &p.SignalInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "signal_request_id":
			info.SignalRequestID = v.(gocql.UUID).String()
		case "signal_name":
			info.SignalName = v.(string)
		case "input":
			info.Input = v.([]byte)
		case "control":
			info.Control = v.([]byte)
		}
	}

	return info
}

func resetActivityInfoMap(activityInfos []*p.InternalActivityInfo) (map[int64]map[string]interface{}, error) {

	aMap := make(map[int64]map[string]interface{})
	for _, a := range activityInfos {
		if a.StartedEvent != nil && a.ScheduledEvent.Encoding != a.StartedEvent.Encoding {
			return nil, p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}
		scheduledEventData, encoding := p.FromDataBlob(a.ScheduledEvent)
		startedEventData, _ := p.FromDataBlob(a.StartedEvent)
		aInfo := make(map[string]interface{})
		aInfo["version"] = a.Version
		aInfo["event_data_encoding"] = encoding
		aInfo["schedule_id"] = a.ScheduleID
		aInfo["scheduled_event_batch_id"] = a.ScheduledEventBatchID
		aInfo["scheduled_event"] = scheduledEventData
		aInfo["scheduled_time"] = a.ScheduledTime
		aInfo["started_id"] = a.StartedID
		aInfo["started_event"] = startedEventData
		aInfo["started_time"] = a.StartedTime
		aInfo["activity_id"] = a.ActivityID
		aInfo["request_id"] = a.RequestID
		aInfo["details"] = a.Details
		aInfo["schedule_to_start_timeout"] = a.ScheduleToStartTimeout
		aInfo["schedule_to_close_timeout"] = a.ScheduleToCloseTimeout
		aInfo["start_to_close_timeout"] = a.StartToCloseTimeout
		aInfo["heart_beat_timeout"] = a.HeartbeatTimeout
		aInfo["cancel_requested"] = a.CancelRequested
		aInfo["cancel_request_id"] = a.CancelRequestID
		aInfo["last_hb_updated_time"] = a.LastHeartBeatUpdatedTime
		aInfo["timer_task_status"] = a.TimerTaskStatus
		aInfo["attempt"] = a.Attempt
		aInfo["task_list"] = a.TaskList
		aInfo["started_identity"] = a.StartedIdentity
		aInfo["has_retry_policy"] = a.HasRetryPolicy
		aInfo["init_interval"] = a.InitialInterval
		aInfo["backoff_coefficient"] = a.BackoffCoefficient
		aInfo["max_interval"] = a.MaximumInterval
		aInfo["expiration_time"] = a.ExpirationTime
		aInfo["max_attempts"] = a.MaximumAttempts
		aInfo["non_retriable_errors"] = a.NonRetriableErrors

		aMap[a.ScheduleID] = aInfo
	}

	return aMap, nil
}

func resetTimerInfoMap(timerInfos []*p.TimerInfo) map[string]map[string]interface{} {
	tMap := make(map[string]map[string]interface{})
	for _, t := range timerInfos {
		tInfo := make(map[string]interface{})
		tInfo["version"] = t.Version
		tInfo["timer_id"] = t.TimerID
		tInfo["started_id"] = t.StartedID
		tInfo["expiry_time"] = t.ExpiryTime
		tInfo["task_id"] = t.TaskID

		tMap[t.TimerID] = tInfo
	}

	return tMap
}

func resetChildExecutionInfoMap(childExecutionInfos []*p.InternalChildExecutionInfo, logger log.Logger) (map[int64]map[string]interface{}, error) {
	cMap := make(map[int64]map[string]interface{})
	for _, c := range childExecutionInfos {
		cInfo := make(map[string]interface{})
		startedEvent := c.StartedEvent
		if startedEvent != nil {
			if startedEvent.Encoding != c.InitiatedEvent.Encoding {
				return nil, p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", c.InitiatedEvent.Encoding, startedEvent.Encoding))
			}
			cInfo["started_event"] = startedEvent.Data
		} else {
			cInfo["started_event"] = []byte{}
		}
		cInfo["event_data_encoding"] = c.InitiatedEvent.Encoding
		cInfo["version"] = c.Version
		cInfo["initiated_id"] = c.InitiatedID
		cInfo["initiated_event_batch_id"] = c.InitiatedEventBatchID
		cInfo["initiated_event"] = c.InitiatedEvent.Data
		cInfo["create_request_id"] = c.CreateRequestID
		cInfo["started_id"] = c.StartedID
		cInfo["started_workflow_id"] = c.StartedWorkflowID
		startedRunID := emptyRunID
		if c.StartedRunID != "" {
			startedRunID = c.StartedRunID
		}
		cInfo["started_run_id"] = startedRunID
		cInfo["domain_name"] = c.DomainName
		cInfo["workflow_type_name"] = c.WorkflowTypeName

		cMap[c.InitiatedID] = cInfo
	}

	return cMap, nil
}

func resetRequestCancelInfoMap(requestCancelInfos []*p.RequestCancelInfo) map[int64]map[string]interface{} {
	rcMap := make(map[int64]map[string]interface{})
	for _, rc := range requestCancelInfos {
		rcInfo := make(map[string]interface{})
		rcInfo["version"] = rc.Version
		rcInfo["initiated_id"] = rc.InitiatedID
		rcInfo["cancel_request_id"] = rc.CancelRequestID

		rcMap[rc.InitiatedID] = rcInfo
	}

	return rcMap
}

func resetSignalInfoMap(signalInfos []*p.SignalInfo) map[int64]map[string]interface{} {
	sMap := make(map[int64]map[string]interface{})
	for _, s := range signalInfos {
		sInfo := make(map[string]interface{})
		sInfo["version"] = s.Version
		sInfo["initiated_id"] = s.InitiatedID
		sInfo["signal_request_id"] = s.SignalRequestID
		sInfo["signal_name"] = s.SignalName
		sInfo["input"] = s.Input
		sInfo["control"] = s.Control

		sMap[s.InitiatedID] = sInfo
	}

	return sMap
}

func createHistoryEventBatchBlob(result map[string]interface{}) *p.DataBlob {
	eventBatch := &p.DataBlob{Encoding: common.EncodingTypeJSON}
	for k, v := range result {
		switch k {
		case "encoding_type":
			eventBatch.Encoding = common.EncodingType(v.(string))
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}

func createTaskInfo(result map[string]interface{}) *p.TaskInfo {
	info := &p.TaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "created_time":
			info.CreatedTime = v.(time.Time)
		}
	}

	return info
}

func createTimerTaskInfo(result map[string]interface{}) *p.TimerTaskInfo {
	info := &p.TimerTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "timeout_type":
			info.TimeoutType = v.(int)
		case "event_id":
			info.EventID = v.(int64)
		case "schedule_attempt":
			info.ScheduleAttempt = v.(int64)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationInfo(result map[string]interface{}) *p.ReplicationInfo {
	info := &p.ReplicationInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "last_event_id":
			info.LastEventID = v.(int64)
		}
	}

	return info
}

func createReplicationInfoMap(info *p.ReplicationInfo) map[string]interface{} {
	rInfoMap := make(map[string]interface{})
	rInfoMap["version"] = info.Version
	rInfoMap["last_event_id"] = info.LastEventID

	return rInfoMap
}

func isTimeoutError(err error) bool {
	if err == gocql.ErrTimeoutNoResponse {
		return true
	}
	if err == gocql.ErrConnectionClosed {
		return true
	}
	_, ok := err.(*gocql.RequestErrWriteTimeout)
	return ok
}

func isThrottlingError(err error) bool {
	if req, ok := err.(gocql.RequestError); ok {
		// gocql does not expose the constant errOverloaded = 0x1001
		return req.Code() == 0x1001
	}
	return false
}
