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

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
	"github.com/temporalio/temporal/common/primitives"
)

func updateActivityInfos(
	tx sqlplugin.Tx,
	activityInfos []*persistence.InternalActivityInfo,
	deleteInfos []int64,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(activityInfos) > 0 {
		rows := make([]sqlplugin.ActivityInfoMapsRow, len(activityInfos))
		for i, v := range activityInfos {
			blob, err := serialization.ActivityInfoToBlob(v.ToProto())
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
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update activity info. Failed to execute update query. Error: %v", err))
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
				return serviceerror.NewInternal(fmt.Sprintf("Failed to update activity info. Failed to execute delete query. Error: %v", err))
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return serviceerror.NewInternal(fmt.Sprintf("Failed to update activity info. Failed to verify number of rows deleted. Error: %v", err))
			}
			if int(rowsAffected) != 1 {
				return serviceerror.NewInternal(fmt.Sprintf("Failed to update activity info. Deleted %v rows instead of 1", rowsAffected))
			}
		}
	}

	return nil
}

func getActivityInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistence.InternalActivityInfo, error) {

	rows, err := db.SelectFromActivityInfoMaps(&sqlplugin.ActivityInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get activity info. Error: %v", err))
	}

	ret := make(map[int64]*persistence.InternalActivityInfo)
	for _, v := range rows {
		decoded, err := serialization.ActivityInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		info := persistence.ProtoActivityInfoToInternalActivityInfo(decoded)
		ret[v.ScheduleID] = info
	}

	return ret, nil
}

func deleteActivityInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromActivityInfoMaps(&sqlplugin.ActivityInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete activity info map. Error: %v", err))
	}
	return nil
}

func updateTimerInfos(
	tx sqlplugin.Tx,
	timerInfos []*persistenceblobs.TimerInfo,
	deleteInfos []string,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(timerInfos) > 0 {
		rows := make([]sqlplugin.TimerInfoMapsRow, len(timerInfos))
		for i, v := range timerInfos {
			blob, err := serialization.TimerInfoToBlob(v)
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
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update timer info. Failed to execute update query. Error: %v", err))
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
				return serviceerror.NewInternal(fmt.Sprintf("Failed to update timer info. Failed to execute delete query. Error: %v", err))
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return serviceerror.NewInternal(fmt.Sprintf("Failed to update timer info. Failed to verify number of rows deleted. Error: %v", err))
			}
			if int(rowsAffected) != 1 {
				return serviceerror.NewInternal(fmt.Sprintf("Failed to update timer info. Deleted %v rows instead of 1", rowsAffected))
			}
		}
	}
	return nil
}

func getTimerInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[string]*persistenceblobs.TimerInfo, error) {

	rows, err := db.SelectFromTimerInfoMaps(&sqlplugin.TimerInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get timer info. Error: %v", err))
	}
	ret := make(map[string]*persistenceblobs.TimerInfo)
	for _, v := range rows {
		info, err := serialization.TimerInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[v.TimerID] = info
	}

	return ret, nil
}

func deleteTimerInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromTimerInfoMaps(&sqlplugin.TimerInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete timer info map. Error: %v", err))
	}
	return nil
}

func updateChildExecutionInfos(
	tx sqlplugin.Tx,
	childExecutionInfos []*persistence.InternalChildExecutionInfo,
	deleteInfos *int64,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(childExecutionInfos) > 0 {
		rows := make([]sqlplugin.ChildExecutionInfoMapsRow, len(childExecutionInfos))
		for i, v := range childExecutionInfos {
			blob, err := serialization.ChildExecutionInfoToBlob(v.ToProto())
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
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err))
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
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update child execution info. Failed to execute delete query. Error: %v", err))
		}
	}

	return nil
}

func getChildExecutionInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistence.InternalChildExecutionInfo, error) {

	rows, err := db.SelectFromChildExecutionInfoMaps(&sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get timer info. Error: %v", err))
	}

	ret := make(map[int64]*persistence.InternalChildExecutionInfo)
	for _, v := range rows {
		rowInfo, err := serialization.ChildExecutionInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		info := persistence.ProtoChildExecutionInfoToInternal(rowInfo)
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
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromChildExecutionInfoMaps(&sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete timer info map. Error: %v", err))
	}
	return nil
}

func updateRequestCancelInfos(
	tx sqlplugin.Tx,
	requestCancelInfos []*persistenceblobs.RequestCancelInfo,
	deleteInfo *int64,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(requestCancelInfos) > 0 {
		rows := make([]sqlplugin.RequestCancelInfoMapsRow, len(requestCancelInfos))
		for i, v := range requestCancelInfos {
			blob, err := serialization.RequestCancelInfoToBlob(v)
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
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update request cancel info. Failed to execute update query. Error: %v", err))
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
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update request cancel info. Failed to execute delete query. Error: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update request cancel info. Failed to verify number of rows deleted. Error: %v", err))
	}
	if int(rowsAffected) != 1 {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update request cancel info. Deleted %v rows instead of 1", rowsAffected))
	}
	return nil
}

func getRequestCancelInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistenceblobs.RequestCancelInfo, error) {

	rows, err := db.SelectFromRequestCancelInfoMaps(&sqlplugin.RequestCancelInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get request cancel info. Error: %v", err))
	}

	ret := make(map[int64]*persistenceblobs.RequestCancelInfo)
	for _, v := range rows {
		rowInfo, err := serialization.RequestCancelInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[v.InitiatedID] = &persistenceblobs.RequestCancelInfo{
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
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromRequestCancelInfoMaps(&sqlplugin.RequestCancelInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete request cancel info map. Error: %v", err))
	}
	return nil
}

func updateSignalInfos(
	tx sqlplugin.Tx,
	signalInfos []*persistenceblobs.SignalInfo,
	deleteInfo *int64,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(signalInfos) > 0 {
		rows := make([]sqlplugin.SignalInfoMapsRow, len(signalInfos))
		for i, v := range signalInfos {
			blob, err := serialization.SignalInfoToBlob(v)
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
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update signal info. Failed to execute update query. Error: %v", err))
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
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update signal info. Failed to execute delete query. Error: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update signal info. Failed to verify number of rows deleted. Error: %v", err))
	}
	if int(rowsAffected) != 1 {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to update signal info. Deleted %v rows instead of 1", rowsAffected))
	}
	return nil
}

func getSignalInfoMap(
	db sqlplugin.DB,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistenceblobs.SignalInfo, error) {

	rows, err := db.SelectFromSignalInfoMaps(&sqlplugin.SignalInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get signal info. Error: %v", err))
	}

	ret := make(map[int64]*persistenceblobs.SignalInfo)
	for _, v := range rows {
		rowInfo, err := serialization.SignalInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[v.InitiatedID] = rowInfo
	}

	return ret, nil
}

func deleteSignalInfoMap(
	tx sqlplugin.Tx,
	shardID int,
	domainID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromSignalInfoMaps(&sqlplugin.SignalInfoMapsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete signal info map. Error: %v", err))
	}
	return nil
}
