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
	"database/sql"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

func updateActivityInfos(
	tx sqlplugin.Tx,
	activityInfos []*persistence.InternalActivityInfo,
	deleteInfos []int64,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if len(activityInfos) > 0 {
		rows := make([]sqlplugin.ActivityInfoMapsRow, len(activityInfos))
		for i, v := range activityInfos {
			scheduledEvent, scheduledEncoding := persistence.FromDataBlob(v.ScheduledEvent)
			startEvent, startEncoding := persistence.FromDataBlob(v.StartedEvent)

			info := &sqlblobs.ActivityInfo{
				Version:                       &v.Version,
				ScheduledEventBatchID:         &v.ScheduledEventBatchID,
				ScheduledEvent:                scheduledEvent,
				ScheduledEventEncoding:        common.StringPtr(scheduledEncoding),
				ScheduledTimeNanos:            common.Int64Ptr(v.ScheduledTime.UnixNano()),
				StartedID:                     &v.StartedID,
				StartedEvent:                  startEvent,
				StartedEventEncoding:          common.StringPtr(startEncoding),
				StartedTimeNanos:              common.Int64Ptr(v.StartedTime.UnixNano()),
				ActivityID:                    &v.ActivityID,
				RequestID:                     &v.RequestID,
				ScheduleToStartTimeoutSeconds: &v.ScheduleToStartTimeout,
				ScheduleToCloseTimeoutSeconds: &v.ScheduleToCloseTimeout,
				StartToCloseTimeoutSeconds:    &v.StartToCloseTimeout,
				HeartbeatTimeoutSeconds:       &v.HeartbeatTimeout,
				CancelRequested:               &v.CancelRequested,
				CancelRequestID:               &v.CancelRequestID,
				TimerTaskStatus:               &v.TimerTaskStatus,
				Attempt:                       &v.Attempt,
				TaskList:                      &v.TaskList,
				StartedIdentity:               &v.StartedIdentity,
				HasRetryPolicy:                &v.HasRetryPolicy,
				RetryInitialIntervalSeconds:   &v.InitialInterval,
				RetryBackoffCoefficient:       &v.BackoffCoefficient,
				RetryMaximumIntervalSeconds:   &v.MaximumInterval,
				RetryExpirationTimeNanos:      common.Int64Ptr(v.ExpirationTime.UnixNano()),
				RetryMaximumAttempts:          &v.MaximumAttempts,
				RetryNonRetryableErrors:       v.NonRetriableErrors,
				RetryLastFailureReason:        &v.LastFailureReason,
				RetryLastWorkerIdentity:       &v.LastWorkerIdentity,
				RetryLastFailureDetails:       v.LastFailureDetails,
			}
			blob, err := activityInfoToBlob(info)
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.ActivityInfoMapsRow{
				ShardID:                  int64(shardID),
				DomainID:                 domainID,
				WorkflowID:               workflowID,
				RunID:                    runID,
				ScheduleID:               v.ScheduleID,
				LastHeartbeatUpdatedTime: v.LastHeartBeatUpdatedTime,
				LastHeartbeatDetails:     v.Details,
				Data:                     blob.Data,
				DataEncoding:             string(blob.Encoding),
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
			result, err := tx.DeleteFromActivityInfoMaps(&sqlplugin.ActivityInfoMapsFilter{
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

func getActivityInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) (map[int64]*persistence.InternalActivityInfo, error) {

	rows, err := db.SelectFromActivityInfoMaps(&sqlplugin.ActivityInfoMapsFilter{
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
		decoded, err := activityInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		info := &persistence.InternalActivityInfo{
			DomainID:                 v.DomainID.String(),
			ScheduleID:               v.ScheduleID,
			Details:                  v.LastHeartbeatDetails,
			LastHeartBeatUpdatedTime: v.LastHeartbeatUpdatedTime,
			Version:                  decoded.GetVersion(),
			ScheduledEventBatchID:    decoded.GetScheduledEventBatchID(),
			ScheduledEvent:           persistence.NewDataBlob(decoded.ScheduledEvent, common.EncodingType(decoded.GetScheduledEventEncoding())),
			ScheduledTime:            time.Unix(0, decoded.GetScheduledTimeNanos()),
			StartedID:                decoded.GetStartedID(),
			StartedTime:              time.Unix(0, decoded.GetStartedTimeNanos()),
			ActivityID:               decoded.GetActivityID(),
			RequestID:                decoded.GetRequestID(),
			ScheduleToStartTimeout:   decoded.GetScheduleToStartTimeoutSeconds(),
			ScheduleToCloseTimeout:   decoded.GetScheduleToCloseTimeoutSeconds(),
			StartToCloseTimeout:      decoded.GetStartToCloseTimeoutSeconds(),
			HeartbeatTimeout:         decoded.GetHeartbeatTimeoutSeconds(),
			CancelRequested:          decoded.GetCancelRequested(),
			CancelRequestID:          decoded.GetCancelRequestID(),
			TimerTaskStatus:          decoded.GetTimerTaskStatus(),
			Attempt:                  decoded.GetAttempt(),
			StartedIdentity:          decoded.GetStartedIdentity(),
			TaskList:                 decoded.GetTaskList(),
			HasRetryPolicy:           decoded.GetHasRetryPolicy(),
			InitialInterval:          decoded.GetRetryInitialIntervalSeconds(),
			BackoffCoefficient:       decoded.GetRetryBackoffCoefficient(),
			MaximumInterval:          decoded.GetRetryMaximumIntervalSeconds(),
			ExpirationTime:           time.Unix(0, decoded.GetRetryExpirationTimeNanos()),
			MaximumAttempts:          decoded.GetRetryMaximumAttempts(),
			NonRetriableErrors:       decoded.GetRetryNonRetryableErrors(),
			LastFailureReason:        decoded.GetRetryLastFailureReason(),
			LastWorkerIdentity:       decoded.GetRetryLastWorkerIdentity(),
			LastFailureDetails:       decoded.GetRetryLastFailureDetails(),
		}
		if decoded.StartedEvent != nil {
			info.StartedEvent = persistence.NewDataBlob(decoded.StartedEvent, common.EncodingType(decoded.GetStartedEventEncoding()))
		}
		ret[v.ScheduleID] = info
	}

	return ret, nil
}

func deleteActivityInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromActivityInfoMaps(&sqlplugin.ActivityInfoMapsFilter{
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

func updateTimerInfos(
	tx sqlplugin.Tx,
	timerInfos []*persistence.TimerInfo,
	deleteInfos []string,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if len(timerInfos) > 0 {
		rows := make([]sqlplugin.TimerInfoMapsRow, len(timerInfos))
		for i, v := range timerInfos {
			blob, err := timerInfoToBlob(&sqlblobs.TimerInfo{
				Version:         &v.Version,
				StartedID:       &v.StartedID,
				ExpiryTimeNanos: common.Int64Ptr(v.ExpiryTime.UnixNano()),
				// TaskID is a misleading variable, it actually serves
				// the purpose of indicating whether a timer task is
				// generated for this timer info
				TaskID: &v.TaskStatus,
			})
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.TimerInfoMapsRow{
				ShardID:      int64(shardID),
				DomainID:     domainID,
				WorkflowID:   workflowID,
				RunID:        runID,
				TimerID:      v.TimerID,
				Data:         blob.Data,
				DataEncoding: string(blob.Encoding),
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
			result, err := tx.DeleteFromTimerInfoMaps(&sqlplugin.TimerInfoMapsFilter{
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

func getTimerInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) (map[string]*persistence.TimerInfo, error) {

	rows, err := db.SelectFromTimerInfoMaps(&sqlplugin.TimerInfoMapsFilter{
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
		info, err := timerInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[v.TimerID] = &persistence.TimerInfo{
			TimerID:    v.TimerID,
			Version:    info.GetVersion(),
			StartedID:  info.GetStartedID(),
			ExpiryTime: time.Unix(0, info.GetExpiryTimeNanos()),
			// TaskID is a misleading variable, it actually serves
			// the purpose of indicating whether a timer task is
			// generated for this timer info
			TaskStatus: info.GetTaskID(),
		}
	}

	return ret, nil
}

func deleteTimerInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromTimerInfoMaps(&sqlplugin.TimerInfoMapsFilter{
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

func updateChildExecutionInfos(
	tx sqlplugin.Tx,
	childExecutionInfos []*persistence.InternalChildExecutionInfo,
	deleteInfos *int64,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if len(childExecutionInfos) > 0 {
		rows := make([]sqlplugin.ChildExecutionInfoMapsRow, len(childExecutionInfos))
		for i, v := range childExecutionInfos {
			initiateEvent, initiateEncoding := persistence.FromDataBlob(v.InitiatedEvent)
			startEvent, startEncoding := persistence.FromDataBlob(v.StartedEvent)

			info := &sqlblobs.ChildExecutionInfo{
				Version:                &v.Version,
				InitiatedEventBatchID:  &v.InitiatedEventBatchID,
				InitiatedEvent:         initiateEvent,
				InitiatedEventEncoding: &initiateEncoding,
				StartedEvent:           startEvent,
				StartedEventEncoding:   &startEncoding,
				StartedID:              &v.StartedID,
				StartedWorkflowID:      &v.StartedWorkflowID,
				StartedRunID:           sqlplugin.MustParseUUID(v.StartedRunID),
				CreateRequestID:        &v.CreateRequestID,
				DomainName:             &v.DomainName,
				WorkflowTypeName:       &v.WorkflowTypeName,
				ParentClosePolicy:      common.Int32Ptr(int32(v.ParentClosePolicy)),
			}
			blob, err := childExecutionInfoToBlob(info)
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.ChildExecutionInfoMapsRow{
				ShardID:      int64(shardID),
				DomainID:     domainID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  v.InitiatedID,
				Data:         blob.Data,
				DataEncoding: string(blob.Encoding),
			}
		}
		if _, err := tx.ReplaceIntoChildExecutionInfoMaps(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if deleteInfos != nil {
		if _, err := tx.DeleteFromChildExecutionInfoMaps(&sqlplugin.ChildExecutionInfoMapsFilter{
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

func getChildExecutionInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) (map[int64]*persistence.InternalChildExecutionInfo, error) {

	rows, err := db.SelectFromChildExecutionInfoMaps(&sqlplugin.ChildExecutionInfoMapsFilter{
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
		rowInfo, err := childExecutionInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		info := &persistence.InternalChildExecutionInfo{
			InitiatedID:           v.InitiatedID,
			InitiatedEventBatchID: rowInfo.GetInitiatedEventBatchID(),
			Version:               rowInfo.GetVersion(),
			StartedID:             rowInfo.GetStartedID(),
			StartedWorkflowID:     rowInfo.GetStartedWorkflowID(),
			StartedRunID:          sqlplugin.UUID(rowInfo.GetStartedRunID()).String(),
			CreateRequestID:       rowInfo.GetCreateRequestID(),
			DomainName:            rowInfo.GetDomainName(),
			WorkflowTypeName:      rowInfo.GetWorkflowTypeName(),
			ParentClosePolicy:     workflow.ParentClosePolicy(rowInfo.GetParentClosePolicy()),
		}
		if rowInfo.InitiatedEvent != nil {
			info.InitiatedEvent = persistence.NewDataBlob(rowInfo.InitiatedEvent, common.EncodingType(rowInfo.GetInitiatedEventEncoding()))
		}
		if rowInfo.StartedEvent != nil {
			info.StartedEvent = persistence.NewDataBlob(rowInfo.StartedEvent, common.EncodingType(rowInfo.GetStartedEventEncoding()))
		}
		ret[v.InitiatedID] = info
	}

	return ret, nil
}

func deleteChildExecutionInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromChildExecutionInfoMaps(&sqlplugin.ChildExecutionInfoMapsFilter{
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

func updateRequestCancelInfos(
	tx sqlplugin.Tx,
	requestCancelInfos []*persistence.RequestCancelInfo,
	deleteInfo *int64,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if len(requestCancelInfos) > 0 {
		rows := make([]sqlplugin.RequestCancelInfoMapsRow, len(requestCancelInfos))
		for i, v := range requestCancelInfos {
			blob, err := requestCancelInfoToBlob(&sqlblobs.RequestCancelInfo{
				Version:               &v.Version,
				InitiatedEventBatchID: &v.InitiatedEventBatchID,
				CancelRequestID:       &v.CancelRequestID,
			})
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.RequestCancelInfoMapsRow{
				ShardID:      int64(shardID),
				DomainID:     domainID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  v.InitiatedID,
				Data:         blob.Data,
				DataEncoding: string(blob.Encoding),
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
	result, err := tx.DeleteFromRequestCancelInfoMaps(&sqlplugin.RequestCancelInfoMapsFilter{
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

func getRequestCancelInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) (map[int64]*persistence.RequestCancelInfo, error) {

	rows, err := db.SelectFromRequestCancelInfoMaps(&sqlplugin.RequestCancelInfoMapsFilter{
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
		rowInfo, err := requestCancelInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[v.InitiatedID] = &persistence.RequestCancelInfo{
			Version:               rowInfo.GetVersion(),
			InitiatedID:           v.InitiatedID,
			InitiatedEventBatchID: rowInfo.GetInitiatedEventBatchID(),
			CancelRequestID:       rowInfo.GetCancelRequestID(),
		}
	}

	return ret, nil
}

func deleteRequestCancelInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromRequestCancelInfoMaps(&sqlplugin.RequestCancelInfoMapsFilter{
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

func updateSignalInfos(
	tx sqlplugin.Tx,
	signalInfos []*persistence.SignalInfo,
	deleteInfo *int64,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if len(signalInfos) > 0 {
		rows := make([]sqlplugin.SignalInfoMapsRow, len(signalInfos))
		for i, v := range signalInfos {
			blob, err := signalInfoToBlob(&sqlblobs.SignalInfo{
				Version:               &v.Version,
				InitiatedEventBatchID: &v.InitiatedEventBatchID,
				RequestID:             &v.SignalRequestID,
				Name:                  &v.SignalName,
				Input:                 v.Input,
				Control:               v.Control,
			})
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.SignalInfoMapsRow{
				ShardID:      int64(shardID),
				DomainID:     domainID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  v.InitiatedID,
				Data:         blob.Data,
				DataEncoding: string(blob.Encoding),
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
	result, err := tx.DeleteFromSignalInfoMaps(&sqlplugin.SignalInfoMapsFilter{
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

func getSignalInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) (map[int64]*persistence.SignalInfo, error) {

	rows, err := db.SelectFromSignalInfoMaps(&sqlplugin.SignalInfoMapsFilter{
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
		rowInfo, err := signalInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[v.InitiatedID] = &persistence.SignalInfo{
			Version:               rowInfo.GetVersion(),
			InitiatedID:           v.InitiatedID,
			InitiatedEventBatchID: rowInfo.GetInitiatedEventBatchID(),
			SignalRequestID:       rowInfo.GetRequestID(),
			SignalName:            rowInfo.GetName(),
			Input:                 rowInfo.GetInput(),
			Control:               rowInfo.GetControl(),
		}
	}

	return ret, nil
}

func deleteSignalInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromSignalInfoMaps(&sqlplugin.SignalInfoMapsFilter{
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
