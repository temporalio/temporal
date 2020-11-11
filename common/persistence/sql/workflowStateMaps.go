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

package sql

import (
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

func updateActivityInfos(
	tx sqlplugin.Tx,
	activityInfos []*persistencespb.ActivityInfo,
	deleteInfos []int64,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(activityInfos) > 0 {
		rows := make([]sqlplugin.ActivityInfoMapsRow, len(activityInfos))
		for i, activityInfo := range activityInfos {
			blob, err := serialization.ActivityInfoToBlob(activityInfo)
			if err != nil {
				return err
			}

			rows[i] = sqlplugin.ActivityInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				ScheduleID:   activityInfo.ScheduleId,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			}
		}

		if _, err := tx.ReplaceIntoActivityInfoMaps(rows); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update activity info. Failed to execute update query. Error: %v", err))
		}
	}

	for _, deleteInfoID := range deleteInfos {
		if _, err := tx.DeleteFromActivityInfoMaps(sqlplugin.ActivityInfoMapsDeleteFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			ScheduleID:  &deleteInfoID,
		}); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update activity info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getActivityInfoMap(
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistencespb.ActivityInfo, error) {

	rows, err := db.SelectFromActivityInfoMaps(sqlplugin.ActivityInfoMapsSelectFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get activity info. Error: %v", err))
	}

	ret := make(map[int64]*persistencespb.ActivityInfo)
	for _, row := range rows {
		decoded, err := serialization.ActivityInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[row.ScheduleID] = decoded
	}

	return ret, nil
}

func deleteActivityInfoMap(
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromActivityInfoMaps(sqlplugin.ActivityInfoMapsDeleteFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete activity info map. Error: %v", err))
	}
	return nil
}

func updateTimerInfos(
	tx sqlplugin.Tx,
	timerInfos []*persistencespb.TimerInfo,
	deleteInfos []string,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(timerInfos) > 0 {
		rows := make([]sqlplugin.TimerInfoMapsRow, len(timerInfos))
		for i, timerInfo := range timerInfos {
			blob, err := serialization.TimerInfoToBlob(timerInfo)
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.TimerInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				TimerID:      timerInfo.GetTimerId(),
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			}
		}
		if _, err := tx.ReplaceIntoTimerInfoMaps(rows); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update timer info. Failed to execute update query. Error: %v", err))
		}
	}

	for _, deleteInfoID := range deleteInfos {
		if _, err := tx.DeleteFromTimerInfoMaps(sqlplugin.TimerInfoMapsDeleteFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			TimerID:     &deleteInfoID,
		}); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update timer info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getTimerInfoMap(
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[string]*persistencespb.TimerInfo, error) {

	rows, err := db.SelectFromTimerInfoMaps(sqlplugin.TimerInfoMapsSelectFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get timer info. Error: %v", err))
	}
	ret := make(map[string]*persistencespb.TimerInfo)
	for _, row := range rows {
		info, err := serialization.TimerInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[row.TimerID] = info
	}

	return ret, nil
}

func deleteTimerInfoMap(
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromTimerInfoMaps(sqlplugin.TimerInfoMapsDeleteFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete timer info map. Error: %v", err))
	}
	return nil
}

func updateChildExecutionInfos(
	tx sqlplugin.Tx,
	childExecutionInfos []*persistencespb.ChildExecutionInfo,
	deleteInfos []int64,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(childExecutionInfos) > 0 {
		rows := make([]sqlplugin.ChildExecutionInfoMapsRow, len(childExecutionInfos))
		for i, childExecutionInfo := range childExecutionInfos {
			blob, err := serialization.ChildExecutionInfoToBlob(childExecutionInfo)
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.ChildExecutionInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  childExecutionInfo.InitiatedId,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			}
		}
		if _, err := tx.ReplaceIntoChildExecutionInfoMaps(rows); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err))
		}
	}

	for _, deleteInfoID := range deleteInfos {
		if _, err := tx.DeleteFromChildExecutionInfoMaps(sqlplugin.ChildExecutionInfoMapsDeleteFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			InitiatedID: &deleteInfoID,
		}); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update child execution info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getChildExecutionInfoMap(
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistencespb.ChildExecutionInfo, error) {

	rows, err := db.SelectFromChildExecutionInfoMaps(sqlplugin.ChildExecutionInfoMapsSelectFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get timer info. Error: %v", err))
	}

	ret := make(map[int64]*persistencespb.ChildExecutionInfo)
	for _, row := range rows {
		rowInfo, err := serialization.ChildExecutionInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[row.InitiatedID] = rowInfo
	}

	return ret, nil
}

func deleteChildExecutionInfoMap(
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromChildExecutionInfoMaps(sqlplugin.ChildExecutionInfoMapsDeleteFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete timer info map. Error: %v", err))
	}
	return nil
}

func updateRequestCancelInfos(
	tx sqlplugin.Tx,
	requestCancelInfos []*persistencespb.RequestCancelInfo,
	deleteInfos []int64,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(requestCancelInfos) > 0 {
		rows := make([]sqlplugin.RequestCancelInfoMapsRow, len(requestCancelInfos))
		for i, requestCancelInfo := range requestCancelInfos {
			blob, err := serialization.RequestCancelInfoToBlob(requestCancelInfo)
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.RequestCancelInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  requestCancelInfo.GetInitiatedId(),
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			}
		}

		if _, err := tx.ReplaceIntoRequestCancelInfoMaps(rows); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update request cancel info. Failed to execute update query. Error: %v", err))
		}
	}

	for _, deleteInfoID := range deleteInfos {
		if _, err := tx.DeleteFromRequestCancelInfoMaps(sqlplugin.RequestCancelInfoMapsDeleteFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			InitiatedID: &deleteInfoID,
		}); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update request cancel info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getRequestCancelInfoMap(
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistencespb.RequestCancelInfo, error) {

	rows, err := db.SelectFromRequestCancelInfoMaps(sqlplugin.RequestCancelInfoMapsSelectFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get request cancel info. Error: %v", err))
	}

	ret := make(map[int64]*persistencespb.RequestCancelInfo)
	for _, row := range rows {
		rowInfo, err := serialization.RequestCancelInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[row.InitiatedID] = &persistencespb.RequestCancelInfo{
			Version:               rowInfo.GetVersion(),
			InitiatedId:           row.InitiatedID,
			InitiatedEventBatchId: rowInfo.GetInitiatedEventBatchId(),
			CancelRequestId:       rowInfo.GetCancelRequestId(),
		}
	}

	return ret, nil
}

func deleteRequestCancelInfoMap(
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromRequestCancelInfoMaps(sqlplugin.RequestCancelInfoMapsDeleteFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete request cancel info map. Error: %v", err))
	}
	return nil
}

func updateSignalInfos(
	tx sqlplugin.Tx,
	signalInfos []*persistencespb.SignalInfo,
	deleteInfos []int64,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(signalInfos) > 0 {
		rows := make([]sqlplugin.SignalInfoMapsRow, len(signalInfos))
		for i, signalInfo := range signalInfos {
			blob, err := serialization.SignalInfoToBlob(signalInfo)
			if err != nil {
				return err
			}
			rows[i] = sqlplugin.SignalInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  signalInfo.GetInitiatedId(),
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			}
		}

		if _, err := tx.ReplaceIntoSignalInfoMaps(rows); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update signal info. Failed to execute update query. Error: %v", err))
		}
	}

	for _, deleteInfo := range deleteInfos {
		if _, err := tx.DeleteFromSignalInfoMaps(sqlplugin.SignalInfoMapsDeleteFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			InitiatedID: &deleteInfo,
		}); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update signal info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getSignalInfoMap(
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*persistencespb.SignalInfo, error) {

	rows, err := db.SelectFromSignalInfoMaps(sqlplugin.SignalInfoMapsSelectFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get signal info. Error: %v", err))
	}

	ret := make(map[int64]*persistencespb.SignalInfo)
	for _, row := range rows {
		rowInfo, err := serialization.SignalInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		ret[row.InitiatedID] = rowInfo
	}

	return ret, nil
}

func deleteSignalInfoMap(
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromSignalInfoMaps(sqlplugin.SignalInfoMapsDeleteFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete signal info map. Error: %v", err))
	}
	return nil
}
