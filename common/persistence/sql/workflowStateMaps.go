// Copyright (c) 2018 Uber Technologies, Inc.
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

package sql

import (
	"fmt"

	"github.com/uber/cadence/common"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"

	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

func updateActivityInfos(tx sqldb.Tx,
	activityInfos []*persistence.InternalActivityInfo,
	deleteInfos []int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {

	if len(activityInfos) > 0 {
		rows := make([]sqldb.ActivityInfoMapsRow, len(activityInfos))
		for i, v := range activityInfos {
			rows[i] = sqldb.ActivityInfoMapsRow{
				ShardID:                  int64(shardID),
				DomainID:                 domainID,
				WorkflowID:               workflowID,
				RunID:                    runID,
				ScheduleID:               v.ScheduleID,
				ScheduledEventBatchID:    v.ScheduledEventBatchID,
				Version:                  v.Version,
				ScheduledTime:            v.ScheduledTime,
				StartedID:                v.StartedID,
				StartedTime:              v.StartedTime,
				ActivityID:               v.ActivityID,
				RequestID:                v.RequestID,
				ScheduleToStartTimeout:   int64(v.ScheduleToStartTimeout),
				ScheduleToCloseTimeout:   int64(v.ScheduleToCloseTimeout),
				StartToCloseTimeout:      int64(v.StartToCloseTimeout),
				HeartbeatTimeout:         int64(v.HeartbeatTimeout),
				CancelRequested:          boolToInt64(v.CancelRequested),
				CancelRequestID:          v.CancelRequestID,
				LastHeartbeatUpdatedTime: v.LastHeartBeatUpdatedTime,
				TimerTaskStatus:          int64(v.TimerTaskStatus),
				Attempt:                  int64(v.Attempt),
				TaskList:                 v.TaskList,
				StartedIdentity:          v.StartedIdentity,
				HasRetryPolicy:           boolToInt64(v.HasRetryPolicy),
				InitInterval:             int64(v.InitialInterval),
				BackoffCoefficient:       v.BackoffCoefficient,
				MaxInterval:              int64(v.MaximumInterval),
				ExpirationTime:           v.ExpirationTime,
				MaxAttempts:              int64(v.MaximumAttempts),
			}
			if v.StartedEvent != nil {
				rows[i].StartedEvent = &v.StartedEvent.Data
				rows[i].StartedEventEncoding = string(v.StartedEvent.Encoding)
			}
			if v.ScheduledEvent != nil {
				rows[i].ScheduledEvent = v.ScheduledEvent.Data
				rows[i].ScheduledEventEncoding = string(v.ScheduledEvent.Encoding)
			}

			if v.Details != nil {
				rows[i].Details = &v.Details
			}

			if v.NonRetriableErrors != nil {
				nonRetriableErrors, err := gobSerialize(&v.NonRetriableErrors)
				if err != nil {
					return &workflow.InternalServiceError{
						Message: fmt.Sprintf("Failed to update activity info. Failed to serialize ActivityInfo.NonRetriableErrors. Error: %v", err),
					}
				}
				rows[i].NonRetriableErrors = &nonRetriableErrors
			}
		}

		if _, err := tx.ReplaceIntoActivityInfoMaps(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to execute update query. Error: %v", err),
			}
		}
	}

	if len(deleteInfos) > 0 {
		for _, v := range deleteInfos {
			result, err := tx.DeleteFromActivityInfoMaps(&sqldb.ActivityInfoMapsFilter{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				ScheduleID: &v,
			})
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Failed to execute delete query. Error: %v", err),
				}
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Failed to verify number of rows deleted. Error: %v", err),
				}
			}
			if int(rowsAffected) != 1 {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Deleted %v rows instead of 1", rowsAffected),
				}
			}
		}
	}

	return nil
}

func getActivityInfoMap(db sqldb.Interface,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.InternalActivityInfo, error) {
	rows, err := db.SelectFromActivityInfoMaps(&sqldb.ActivityInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get activity info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.InternalActivityInfo)
	for _, v := range rows {
		info := &persistence.InternalActivityInfo{
			Version:                  v.Version,
			ScheduleID:               v.ScheduleID,
			ScheduledEventBatchID:    v.ScheduledEventBatchID,
			ScheduledEvent:           persistence.NewDataBlob(v.ScheduledEvent, common.EncodingType(v.ScheduledEventEncoding)),
			ScheduledTime:            v.ScheduledTime,
			StartedID:                v.StartedID,
			StartedTime:              v.StartedTime,
			ActivityID:               v.ActivityID,
			RequestID:                v.RequestID,
			ScheduleToStartTimeout:   int32(v.ScheduleToStartTimeout),
			ScheduleToCloseTimeout:   int32(v.ScheduleToCloseTimeout),
			StartToCloseTimeout:      int32(v.StartToCloseTimeout),
			HeartbeatTimeout:         int32(v.HeartbeatTimeout),
			CancelRequested:          int64ToBool(v.CancelRequested),
			CancelRequestID:          v.CancelRequestID,
			LastHeartBeatUpdatedTime: v.LastHeartbeatUpdatedTime,
			TimerTaskStatus:          int32(v.TimerTaskStatus),
			Attempt:                  int32(v.Attempt),
			DomainID:                 v.DomainID,
			StartedIdentity:          v.StartedIdentity,
			TaskList:                 v.TaskList,
			HasRetryPolicy:           int64ToBool(v.HasRetryPolicy),
			InitialInterval:          int32(v.InitInterval),
			BackoffCoefficient:       v.BackoffCoefficient,
			MaximumInterval:          int32(v.MaxInterval),
			ExpirationTime:           v.ExpirationTime,
			MaximumAttempts:          int32(v.MaxAttempts),
		}
		if v.StartedEvent != nil {
			info.StartedEvent = persistence.NewDataBlob(*v.StartedEvent, common.EncodingType(v.ScheduledEventEncoding))
		}
		ret[v.ScheduleID] = info

		if v.Details != nil {
			ret[v.ScheduleID].Details = *v.Details
		}

		if v.NonRetriableErrors != nil {
			ret[v.ScheduleID].NonRetriableErrors = []string{}
			if err := gobDeserialize(*v.NonRetriableErrors, &ret[v.ScheduleID].NonRetriableErrors); err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to get activity info. Failed to deserialize ActivityInfo.NonRetriableErrors. %v", err),
				}
			}
		}
	}

	return ret, nil
}

func deleteActivityInfoMap(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromActivityInfoMaps(&sqldb.ActivityInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete activity info map. Error: %v", err),
		}
	}
	return nil
}

func updateTimerInfos(tx sqldb.Tx,
	timerInfos []*persistence.TimerInfo,
	deleteInfos []string,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(timerInfos) > 0 {
		rows := make([]sqldb.TimerInfoMapsRow, len(timerInfos))
		for i, v := range timerInfos {
			rows[i] = sqldb.TimerInfoMapsRow{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				TimerID:    v.TimerID,
				Version:    v.Version,
				StartedID:  v.StartedID,
				ExpiryTime: v.ExpiryTime,
				TaskID:     v.TaskID,
			}
		}
		if _, err := tx.ReplaceIntoTimerInfoMaps(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if len(deleteInfos) > 0 {
		for _, v := range deleteInfos {
			result, err := tx.DeleteFromTimerInfoMaps(&sqldb.TimerInfoMapsFilter{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				TimerID:    &v,
			})
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Failed to execute delete query. Error: %v", err),
				}
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Failed to verify number of rows deleted. Error: %v", err),
				}
			}
			if int(rowsAffected) != 1 {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Deleted %v rows instead of 1", rowsAffected),
				}
			}
		}
	}
	return nil
}

func getTimerInfoMap(db sqldb.Interface,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[string]*persistence.TimerInfo, error) {
	rows, err := db.SelectFromTimerInfoMaps(&sqldb.TimerInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get timer info. Error: %v", err),
		}
	}
	ret := make(map[string]*persistence.TimerInfo)
	for _, v := range rows {
		ret[v.TimerID] = &persistence.TimerInfo{
			Version:    v.Version,
			TimerID:    v.TimerID,
			StartedID:  v.StartedID,
			ExpiryTime: v.ExpiryTime,
			TaskID:     v.TaskID,
		}
	}

	return ret, nil
}

func deleteTimerInfoMap(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromTimerInfoMaps(&sqldb.TimerInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete timer info map. Error: %v", err),
		}
	}
	return nil
}

func updateChildExecutionInfos(tx sqldb.Tx,
	childExecutionInfos []*persistence.InternalChildExecutionInfo,
	deleteInfos *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(childExecutionInfos) > 0 {
		rows := make([]sqldb.ChildExecutionInfoMapsRow, len(childExecutionInfos))
		for i, v := range childExecutionInfos {
			rows[i] = sqldb.ChildExecutionInfoMapsRow{
				ShardID:                int64(shardID),
				DomainID:               domainID,
				WorkflowID:             workflowID,
				RunID:                  runID,
				InitiatedID:            v.InitiatedID,
				InitiatedEventBatchID:  v.InitiatedEventBatchID,
				Version:                v.Version,
				StartedID:              v.StartedID,
				StartedWorkflowID:      v.StartedWorkflowID,
				StartedRunID:           v.StartedRunID,
				InitiatedEvent:         &v.InitiatedEvent.Data,
				InitiatedEventEncoding: string(v.InitiatedEvent.Encoding),
				CreateRequestID:        v.CreateRequestID,
				DomainName:             v.DomainName,
				WorkflowTypeName:       v.WorkflowTypeName,
			}
			if v.StartedEvent != nil {
				rows[i].StartedEvent = &v.StartedEvent.Data
				rows[i].StartedEventEncoding = string(v.StartedEvent.Encoding)
			}
		}
		if _, err := tx.ReplaceIntoChildExecutionInfoMaps(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if deleteInfos != nil {
		if _, err := tx.DeleteFromChildExecutionInfoMaps(&sqldb.ChildExecutionInfoMapsFilter{
			ShardID:     int64(shardID),
			DomainID:    domainID,
			WorkflowID:  workflowID,
			RunID:       runID,
			InitiatedID: deleteInfos,
		}); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute delete query. Error: %v", err),
			}
		}
	}

	return nil
}

func getChildExecutionInfoMap(db sqldb.Interface,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.InternalChildExecutionInfo, error) {
	rows, err := db.SelectFromChildExecutionInfoMaps(&sqldb.ChildExecutionInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get timer info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.InternalChildExecutionInfo)
	for _, v := range rows {
		info := &persistence.InternalChildExecutionInfo{
			InitiatedID:           v.InitiatedID,
			InitiatedEventBatchID: v.InitiatedEventBatchID,
			Version:               v.Version,
			StartedID:             v.StartedID,
			StartedWorkflowID:     v.StartedWorkflowID,
			StartedRunID:          v.StartedRunID,
			CreateRequestID:       v.CreateRequestID,
			DomainName:            v.DomainName,
			WorkflowTypeName:      v.WorkflowTypeName,
		}
		if v.InitiatedEvent != nil {
			info.InitiatedEvent = persistence.NewDataBlob(*v.InitiatedEvent, common.EncodingType(v.InitiatedEventEncoding))
		}
		if v.StartedEvent != nil {
			info.StartedEvent = persistence.NewDataBlob(*v.StartedEvent, common.EncodingType(v.InitiatedEventEncoding))
		}
		ret[v.InitiatedID] = info
	}

	return ret, nil
}

func deleteChildExecutionInfoMap(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromChildExecutionInfoMaps(&sqldb.ChildExecutionInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete timer info map. Error: %v", err),
		}
	}
	return nil
}

func updateRequestCancelInfos(tx sqldb.Tx,
	requestCancelInfos []*persistence.RequestCancelInfo,
	deleteInfo *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(requestCancelInfos) > 0 {
		rows := make([]sqldb.RequestCancelInfoMapsRow, len(requestCancelInfos))
		for i, v := range requestCancelInfos {
			rows[i] = sqldb.RequestCancelInfoMapsRow{
				ShardID:         int64(shardID),
				DomainID:        domainID,
				WorkflowID:      workflowID,
				RunID:           runID,
				InitiatedID:     v.InitiatedID,
				Version:         v.Version,
				CancelRequestID: v.CancelRequestID,
			}
		}

		if _, err := tx.ReplaceIntoRequestCancelInfoMaps(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update request cancel info. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if deleteInfo == nil {
		return nil
	}
	result, err := tx.DeleteFromRequestCancelInfoMaps(&sqldb.RequestCancelInfoMapsFilter{
		ShardID:     int64(shardID),
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: deleteInfo,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update request cancel info. Failed to execute delete query. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update request cancel info. Failed to verify number of rows deleted. Error: %v", err),
		}
	}
	if int(rowsAffected) != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update request cancel info. Deleted %v rows instead of 1", rowsAffected),
		}
	}
	return nil
}

func getRequestCancelInfoMap(db sqldb.Interface,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.RequestCancelInfo, error) {
	rows, err := db.SelectFromRequestCancelInfoMaps(&sqldb.RequestCancelInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get request cancel info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.RequestCancelInfo)
	for _, v := range rows {
		ret[v.InitiatedID] = &persistence.RequestCancelInfo{
			Version:         v.Version,
			CancelRequestID: v.CancelRequestID,
			InitiatedID:     v.InitiatedID,
		}
	}

	return ret, nil
}

func deleteRequestCancelInfoMap(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromRequestCancelInfoMaps(&sqldb.RequestCancelInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete request cancel info map. Error: %v", err),
		}
	}
	return nil
}

func updateSignalInfos(tx sqldb.Tx,
	signalInfos []*persistence.SignalInfo,
	deleteInfo *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(signalInfos) > 0 {
		rows := make([]sqldb.SignalInfoMapsRow, len(signalInfos))
		for i, v := range signalInfos {
			rows[i] = sqldb.SignalInfoMapsRow{
				ShardID:         int64(shardID),
				DomainID:        domainID,
				WorkflowID:      workflowID,
				RunID:           runID,
				InitiatedID:     v.InitiatedID,
				Version:         v.Version,
				SignalRequestID: v.SignalRequestID,
				SignalName:      v.SignalName,
				Input:           takeAddressIfNotNil(v.Input),
				Control:         takeAddressIfNotNil(v.Control),
			}
		}

		if _, err := tx.ReplaceIntoSignalInfoMaps(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signal info. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if deleteInfo == nil {
		return nil
	}
	result, err := tx.DeleteFromSignalInfoMaps(&sqldb.SignalInfoMapsFilter{
		ShardID:     int64(shardID),
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: deleteInfo,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update signal info. Failed to execute delete query. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update signal info. Failed to verify number of rows deleted. Error: %v", err),
		}
	}
	if int(rowsAffected) != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update signal info. Deleted %v rows instead of 1", rowsAffected),
		}
	}
	return nil
}

func getSignalInfoMap(db sqldb.Interface,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.SignalInfo, error) {
	rows, err := db.SelectFromSignalInfoMaps(&sqldb.SignalInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get signal info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.SignalInfo)
	for _, v := range rows {
		ret[v.InitiatedID] = &persistence.SignalInfo{
			Version:         v.Version,
			InitiatedID:     v.InitiatedID,
			SignalRequestID: v.SignalRequestID,
			SignalName:      v.SignalName,
			Input:           dereferenceIfNotNil(v.Input),
			Control:         dereferenceIfNotNil(v.Control),
		}
	}

	return ret, nil
}

func deleteSignalInfoMap(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromSignalInfoMaps(&sqldb.SignalInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete signal info map. Error: %v", err),
		}
	}
	return nil
}

func updateBufferedReplicationTasks(tx sqldb.Tx,
	newBufferedReplicationTask *persistence.InternalBufferedReplicationTask,
	deleteInfo *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if newBufferedReplicationTask != nil {
		newRunHistoryData, newRunHistoryEncoding := persistence.FromDataBlob(newBufferedReplicationTask.NewRunHistory)
		historyBlob := newBufferedReplicationTask.History
		row := &sqldb.BufferedReplicationTaskMapsRow{
			ShardID:      int64(shardID),
			DomainID:     domainID,
			WorkflowID:   workflowID,
			RunID:        runID,
			FirstEventID: newBufferedReplicationTask.FirstEventID,
			Version:      newBufferedReplicationTask.Version,
			NextEventID:  newBufferedReplicationTask.NextEventID,
		}
		if historyBlob != nil {
			row.History = &historyBlob.Data
			row.HistoryEncoding = string(historyBlob.Encoding)
		}
		if newRunHistoryData != nil {
			row.NewRunHistory = &newRunHistoryData
			row.NewRunHistoryEncoding = newRunHistoryEncoding
		}
		if _, err := tx.ReplaceIntoBufferedReplicationTasks(row); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if deleteInfo == nil {
		return nil
	}
	result, err := tx.DeleteFromBufferedReplicationTasks(&sqldb.BufferedReplicationTaskMapsFilter{
		ShardID:      int64(shardID),
		DomainID:     domainID,
		WorkflowID:   workflowID,
		RunID:        runID,
		FirstEventID: deleteInfo,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to execute delete query. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to verify number of rows deleted. Error: %v", err),
		}
	}
	if int(rowsAffected) != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update buffered replication tasks. Deleted %v rows instead of 1", rowsAffected),
		}
	}
	return nil
}

func getBufferedReplicationTasks(db sqldb.Interface,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.InternalBufferedReplicationTask, error) {
	rows, err := db.SelectFromBufferedReplicationTasks(&sqldb.BufferedReplicationTaskMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})

	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get buffered replication tasks. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.InternalBufferedReplicationTask)
	for _, v := range rows {
		task := &persistence.InternalBufferedReplicationTask{
			Version:      v.Version,
			FirstEventID: v.FirstEventID,
			NextEventID:  v.NextEventID,
		}
		if v.History != nil {
			task.History = persistence.NewDataBlob(*v.History, common.EncodingType(v.HistoryEncoding))
		}
		if v.NewRunHistory != nil {
			task.NewRunHistory = persistence.NewDataBlob(*v.NewRunHistory,
				common.EncodingType(v.NewRunHistoryEncoding))
		}
		ret[v.FirstEventID] = task
	}
	return ret, nil
}

func deleteBufferedReplicationTasksMap(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromBufferedReplicationTasks(&sqldb.BufferedReplicationTaskMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete buffered replication tasks map. Error: %v", err),
		}
	}
	return nil
}
