// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
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

package cassandra

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/service/history/tasks"
)

func applyWorkflowMutationBatch(
	batch gocql.Batch,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	// TODO update all call sites to update LastUpdatetime
	// cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
	runID := workflowMutation.RunID

	if err := updateExecution(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowMutation.ExecutionInfoBlob,
		workflowMutation.ExecutionState,
		workflowMutation.ExecutionStateBlob,
		workflowMutation.NextEventID,
		workflowMutation.Condition,
		workflowMutation.DBRecordVersion,
		workflowMutation.Checksum,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		batch,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		batch,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		batch,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		batch,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		batch,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	updateBufferedEvents(
		batch,
		workflowMutation.NewBufferedEvents,
		workflowMutation.ClearBufferedEvents,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		workflowMutation.Tasks,
	)
}

func applyWorkflowSnapshotBatchAsReset(
	batch gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	// TODO: update call site
	// cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := updateExecution(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowSnapshot.ExecutionInfoBlob,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.ExecutionStateBlob,
		workflowSnapshot.NextEventID,
		workflowSnapshot.Condition,
		workflowSnapshot.DBRecordVersion,
		workflowSnapshot.Checksum,
	); err != nil {
		return err
	}

	if err := resetActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	resetSignalRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	deleteBufferedEvents(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		workflowSnapshot.Tasks,
	)
}

func applyWorkflowSnapshotBatchAsNew(
	batch gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {
	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := createExecution(
		batch,
		shardID,
		workflowSnapshot,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		workflowSnapshot.Tasks,
	)
}

func createExecution(
	batch gocql.Batch,
	shardID int32,
	snapshot *p.InternalWorkflowSnapshot,
) error {
	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		snapshot.ExecutionState.State,
		snapshot.ExecutionState.Status); err != nil {
		return err
	}

	// TODO also need to set the start / current / last write version
	batch.Query(templateCreateWorkflowExecutionQuery,
		shardID,
		snapshot.NamespaceID,
		snapshot.WorkflowID,
		snapshot.RunID,
		rowTypeExecution,
		snapshot.ExecutionInfoBlob.Data,
		snapshot.ExecutionInfoBlob.EncodingType.String(),
		snapshot.ExecutionStateBlob.Data,
		snapshot.ExecutionStateBlob.EncodingType.String(),
		snapshot.NextEventID,
		snapshot.DBRecordVersion,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		snapshot.Checksum.Data,
		snapshot.Checksum.EncodingType.String(),
	)

	return nil
}

func updateExecution(
	batch gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	executionInfoBlob *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	executionStateBlob *commonpb.DataBlob,
	nextEventID int64,
	condition int64,
	dbRecordVersion int64,
	checksumBlob *commonpb.DataBlob,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	if dbRecordVersion == 0 {
		batch.Query(templateUpdateWorkflowExecutionQueryDeprecated,
			executionInfoBlob.Data,
			executionInfoBlob.EncodingType.String(),
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			nextEventID,
			dbRecordVersion,
			checksumBlob.Data,
			checksumBlob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition,
		)
	} else {
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionInfoBlob.Data,
			executionInfoBlob.EncodingType.String(),
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			nextEventID,
			dbRecordVersion,
			checksumBlob.Data,
			checksumBlob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			dbRecordVersion-1,
		)
	}

	return nil
}

func applyTasks(
	batch gocql.Batch,
	shardID int32,
	insertTasks map[tasks.Category][]p.InternalHistoryTask,
) error {

	var err error
	for category, tasksByCategory := range insertTasks {
		switch category.ID() {
		case tasks.CategoryIDTransfer:
			err = createTransferTasks(batch, tasksByCategory, shardID)
		case tasks.CategoryIDTimer:
			err = createTimerTasks(batch, tasksByCategory, shardID)
		case tasks.CategoryIDVisibility:
			err = createVisibilityTasks(batch, tasksByCategory, shardID)
		case tasks.CategoryIDReplication:
			err = createReplicationTasks(batch, tasksByCategory, shardID)
		default:
			err = createHistoryTasks(batch, category, tasksByCategory, shardID)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func createTransferTasks(
	batch gocql.Batch,
	transferTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range transferTasks {
		batch.Query(templateCreateTransferTaskQuery,
			shardID,
			rowTypeTransferTask,
			rowTypeTransferNamespaceID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func createTimerTasks(
	batch gocql.Batch,
	timerTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range timerTasks {
		batch.Query(templateCreateTimerTaskQuery,
			shardID,
			rowTypeTimerTask,
			rowTypeTimerNamespaceID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			p.UnixMilliseconds(task.Key.FireTime),
			task.Key.TaskID,
		)
	}
	return nil
}

func createReplicationTasks(
	batch gocql.Batch,
	replicationTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range replicationTasks {
		batch.Query(templateCreateReplicationTaskQuery,
			shardID,
			rowTypeReplicationTask,
			rowTypeReplicationNamespaceID,
			rowTypeReplicationWorkflowID,
			rowTypeReplicationRunID,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func createVisibilityTasks(
	batch gocql.Batch,
	visibilityTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range visibilityTasks {
		batch.Query(templateCreateVisibilityTaskQuery,
			shardID,
			rowTypeVisibilityTask,
			rowTypeVisibilityTaskNamespaceID,
			rowTypeVisibilityTaskWorkflowID,
			rowTypeVisibilityTaskRunID,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func createHistoryTasks(
	batch gocql.Batch,
	category tasks.Category,
	historyTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
	for _, task := range historyTasks {
		visibilityTimestamp := defaultVisibilityTimestamp
		if isScheduledTask {
			visibilityTimestamp = p.UnixMilliseconds(task.Key.FireTime)
		}
		batch.Query(templateCreateHistoryTaskQuery,
			shardID,
			category.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			visibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func updateActivityInfos(
	batch gocql.Batch,
	activityInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for scheduledEventID, blob := range activityInfos {
		batch.Query(templateUpdateActivityInfoQuery,
			scheduledEventID,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteActivityInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func deleteBufferedEvents(
	batch gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {
	batch.Query(templateDeleteBufferedEventsQuery,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
	)
}

func resetActivityInfos(
	batch gocql.Batch,
	activityInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	infoMap, encoding, err := resetActivityInfoMap(activityInfos)
	if err != nil {
		return err
	}

	batch.Query(templateResetActivityInfoQuery,
		infoMap,
		encoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func updateTimerInfos(
	batch gocql.Batch,
	timerInfos map[string]*commonpb.DataBlob,
	deleteInfos map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	for timerID, blob := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			timerID,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteInfoID := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			deleteInfoID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	return nil
}

func resetTimerInfos(
	batch gocql.Batch,
	timerInfos map[string]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	timerMap, timerMapEncoding, err := resetTimerInfoMap(timerInfos)
	if err != nil {
		return err
	}

	batch.Query(templateResetTimerInfoQuery,
		timerMap,
		timerMapEncoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateChildExecutionInfos(
	batch gocql.Batch,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range childExecutionInfos {
		batch.Query(templateUpdateChildExecutionInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetChildExecutionInfos(
	batch gocql.Batch,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	infoMap, encoding, err := resetChildExecutionInfoMap(childExecutionInfos)
	if err != nil {
		return err
	}
	batch.Query(templateResetChildExecutionInfoQuery,
		infoMap,
		encoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func updateRequestCancelInfos(
	batch gocql.Batch,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range requestCancelInfos {
		batch.Query(templateUpdateRequestCancelInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetRequestCancelInfos(
	batch gocql.Batch,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	rciMap, rciMapEncoding, err := resetRequestCancelInfoMap(requestCancelInfos)

	if err != nil {
		return err
	}

	batch.Query(templateResetRequestCancelInfoQuery,
		rciMap,
		rciMapEncoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateSignalInfos(
	batch gocql.Batch,
	signalInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range signalInfos {
		batch.Query(templateUpdateSignalInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteSignalInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetSignalInfos(
	batch gocql.Batch,
	signalInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	sMap, sMapEncoding, err := resetSignalInfoMap(signalInfos)

	if err != nil {
		return err
	}

	batch.Query(templateResetSignalInfoQuery,
		sMap,
		sMapEncoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateSignalsRequested(
	batch gocql.Batch,
	signalReqIDs map[string]struct{},
	deleteSignalReqIDs map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			convert.StringSetToSlice(signalReqIDs),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	if len(deleteSignalReqIDs) > 0 {
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			convert.StringSetToSlice(deleteSignalReqIDs),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetSignalRequested(
	batch gocql.Batch,
	signalRequested map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetSignalRequestedQuery,
		convert.StringSetToSlice(signalRequested),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateBufferedEvents(
	batch gocql.Batch,
	newBufferedEvents *commonpb.DataBlob,
	clearBufferedEvents bool,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else if newBufferedEvents != nil {
		values := make(map[string]interface{})
		values["encoding_type"] = newBufferedEvents.EncodingType.String()
		values["version"] = int64(0)
		values["data"] = newBufferedEvents.Data
		newEventValues := []map[string]interface{}{values}
		batch.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetActivityInfoMap(
	activityInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	aMap := make(map[int64][]byte)
	for scheduledEventID, blob := range activityInfos {
		aMap[scheduledEventID] = blob.Data
		encoding = blob.EncodingType
	}

	return aMap, encoding, nil
}

func resetTimerInfoMap(
	timerInfos map[string]*commonpb.DataBlob,
) (map[string][]byte, enumspb.EncodingType, error) {

	tMap := make(map[string][]byte)
	var encoding enumspb.EncodingType
	for timerID, blob := range timerInfos {
		encoding = blob.EncodingType
		tMap[timerID] = blob.Data
	}

	return tMap, encoding, nil
}

func resetChildExecutionInfoMap(
	childExecutionInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	cMap := make(map[int64][]byte)
	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	for initiatedID, blob := range childExecutionInfos {
		cMap[initiatedID] = blob.Data
		encoding = blob.EncodingType
	}

	return cMap, encoding, nil
}

func resetRequestCancelInfoMap(
	requestCancelInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	rcMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for initiatedID, blob := range requestCancelInfos {
		encoding = blob.EncodingType
		rcMap[initiatedID] = blob.Data
	}

	return rcMap, encoding, nil
}

func resetSignalInfoMap(
	signalInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	sMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for initiatedID, blob := range signalInfos {
		encoding = blob.EncodingType
		sMap[initiatedID] = blob.Data
	}

	return sMap, encoding, nil
}

func createHistoryEventBatchBlob(
	result map[string]interface{},
) *commonpb.DataBlob {
	eventBatch := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_UNSPECIFIED}
	for k, v := range result {
		switch k {
		case "encoding_type":
			encodingStr := v.(string)
			if encoding, ok := enumspb.EncodingType_value[encodingStr]; ok {
				eventBatch.EncodingType = enumspb.EncodingType(encoding)
			}
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}
