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
	"context"
	"database/sql"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/common/persistence"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

func updateActivityInfos(
	ctx context.Context,
	tx sqlplugin.Tx,
	activityInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(activityInfos) > 0 {
		rows := make([]sqlplugin.ActivityInfoMapsRow, 0, len(activityInfos))
		for scheduledEventId, blob := range activityInfos {
			rows = append(rows, sqlplugin.ActivityInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				ScheduleID:   scheduledEventId,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			})
		}

		if _, err := tx.ReplaceIntoActivityInfoMaps(ctx, rows); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update activity info. Failed to execute update query. Error: %v", err))
		}
	}

	if len(deleteIDs) > 0 {
		if _, err := tx.DeleteFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			ScheduleIDs: convert.Int64SetToSlice(deleteIDs),
		}); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update activity info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getActivityInfoMap(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {

	rows, err := db.SelectAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Failed to get activity info. Error: %v", err))
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.ScheduleID] = persistence.NewDataBlob(row.Data, row.DataEncoding)
	}

	return ret, nil
}

func deleteActivityInfoMap(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to delete activity info map. Error: %v", err))
	}
	return nil
}

func updateTimerInfos(
	ctx context.Context,
	tx sqlplugin.Tx,
	timerInfos map[string]*commonpb.DataBlob,
	deleteIDs map[string]struct{},
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(timerInfos) > 0 {
		rows := make([]sqlplugin.TimerInfoMapsRow, 0, len(timerInfos))
		for timerID, blob := range timerInfos {
			rows = append(rows, sqlplugin.TimerInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				TimerID:      timerID,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			})
		}
		if _, err := tx.ReplaceIntoTimerInfoMaps(ctx, rows); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update timer info. Failed to execute update query. Error: %v", err))
		}
	}

	if len(deleteIDs) > 0 {
		if _, err := tx.DeleteFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			TimerIDs:    convert.StringSetToSlice(deleteIDs),
		}); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update timer info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getTimerInfoMap(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[string]*commonpb.DataBlob, error) {

	rows, err := db.SelectAllFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Failed to get timer info. Error: %v", err))
	}
	ret := make(map[string]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.TimerID] = persistence.NewDataBlob(row.Data, row.DataEncoding)
	}

	return ret, nil
}

func deleteTimerInfoMap(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteAllFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to delete timer info map. Error: %v", err))
	}
	return nil
}

func updateChildExecutionInfos(
	ctx context.Context,
	tx sqlplugin.Tx,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(childExecutionInfos) > 0 {
		rows := make([]sqlplugin.ChildExecutionInfoMapsRow, 0, len(childExecutionInfos))
		for initiatedID, blob := range childExecutionInfos {
			rows = append(rows, sqlplugin.ChildExecutionInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  initiatedID,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			})
		}
		if _, err := tx.ReplaceIntoChildExecutionInfoMaps(ctx, rows); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err))
		}
	}

	if len(deleteIDs) > 0 {
		if _, err := tx.DeleteFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsFilter{
			ShardID:      shardID,
			NamespaceID:  namespaceID,
			WorkflowID:   workflowID,
			RunID:        runID,
			InitiatedIDs: convert.Int64SetToSlice(deleteIDs),
		}); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update child execution info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getChildExecutionInfoMap(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {

	rows, err := db.SelectAllFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Failed to get timer info. Error: %v", err))
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.InitiatedID] = persistence.NewDataBlob(row.Data, row.DataEncoding)
	}

	return ret, nil
}

func deleteChildExecutionInfoMap(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteAllFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to delete timer info map. Error: %v", err))
	}
	return nil
}

func updateRequestCancelInfos(
	ctx context.Context,
	tx sqlplugin.Tx,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(requestCancelInfos) > 0 {
		rows := make([]sqlplugin.RequestCancelInfoMapsRow, 0, len(requestCancelInfos))
		for initiatedID, blob := range requestCancelInfos {
			rows = append(rows, sqlplugin.RequestCancelInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  initiatedID,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			})
		}

		if _, err := tx.ReplaceIntoRequestCancelInfoMaps(ctx, rows); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update request cancel info. Failed to execute update query. Error: %v", err))
		}
	}

	if len(deleteIDs) > 0 {
		if _, err := tx.DeleteFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsFilter{
			ShardID:      shardID,
			NamespaceID:  namespaceID,
			WorkflowID:   workflowID,
			RunID:        runID,
			InitiatedIDs: convert.Int64SetToSlice(deleteIDs),
		}); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update request cancel info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getRequestCancelInfoMap(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {

	rows, err := db.SelectAllFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Failed to get request cancel info. Error: %v", err))
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.InitiatedID] = persistence.NewDataBlob(row.Data, row.DataEncoding)
	}

	return ret, nil
}

func deleteRequestCancelInfoMap(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteAllFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to delete request cancel info map. Error: %v", err))
	}
	return nil
}

func updateSignalInfos(
	ctx context.Context,
	tx sqlplugin.Tx,
	signalInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(signalInfos) > 0 {
		rows := make([]sqlplugin.SignalInfoMapsRow, 0, len(signalInfos))
		for initiatedId, blob := range signalInfos {
			rows = append(rows, sqlplugin.SignalInfoMapsRow{
				ShardID:      shardID,
				NamespaceID:  namespaceID,
				WorkflowID:   workflowID,
				RunID:        runID,
				InitiatedID:  initiatedId,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
			})
		}

		if _, err := tx.ReplaceIntoSignalInfoMaps(ctx, rows); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update signal info. Failed to execute update query. Error: %v", err))
		}
	}

	if len(deleteIDs) > 0 {
		if _, err := tx.DeleteFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsFilter{
			ShardID:      shardID,
			NamespaceID:  namespaceID,
			WorkflowID:   workflowID,
			RunID:        runID,
			InitiatedIDs: convert.Int64SetToSlice(deleteIDs),
		}); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update signal info. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getSignalInfoMap(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {

	rows, err := db.SelectAllFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Failed to get signal info. Error: %v", err))
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.InitiatedID] = persistence.NewDataBlob(row.Data, row.DataEncoding)
	}

	return ret, nil
}

func deleteSignalInfoMap(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteAllFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to delete signal info map. Error: %v", err))
	}
	return nil
}
