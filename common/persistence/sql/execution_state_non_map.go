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
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

func updateSignalsRequested(
	ctx context.Context,
	tx sqlplugin.Tx,
	signalRequestedIDs map[string]struct{},
	deleteIDs map[string]struct{},
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(signalRequestedIDs) > 0 {
		rows := make([]sqlplugin.SignalsRequestedSetsRow, 0, len(signalRequestedIDs))
		for signalRequestedID := range signalRequestedIDs {
			rows = append(rows, sqlplugin.SignalsRequestedSetsRow{
				ShardID:     shardID,
				NamespaceID: namespaceID,
				WorkflowID:  workflowID,
				RunID:       runID,
				SignalID:    signalRequestedID,
			})
		}
		if _, err := tx.ReplaceIntoSignalsRequestedSets(ctx, rows); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update signals requested. Failed to execute update query. Error: %v", err))
		}
	}

	if len(deleteIDs) > 0 {
		if _, err := tx.DeleteFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsFilter{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			SignalIDs:   convert.StringSetToSlice(deleteIDs),
		}); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Failed to update signals requested. Failed to execute delete query. Error: %v", err))
		}
	}
	return nil
}

func getSignalsRequested(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) ([]string, error) {

	rows, err := db.SelectAllFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Failed to get signals requested. Error: %v", err))
	}
	var ret = make([]string, len(rows))
	for i, s := range rows {
		ret[i] = s.SignalID
	}
	return ret, nil
}

func deleteSignalsRequestedSet(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteAllFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to delete signals requested set. Error: %v", err))
	}
	return nil
}

func updateBufferedEvents(
	ctx context.Context,
	tx sqlplugin.Tx,
	batch *commonpb.DataBlob,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if batch == nil {
		return nil
	}
	row := sqlplugin.BufferedEventsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		Data:         batch.Data,
		DataEncoding: batch.EncodingType.String(),
	}

	if _, err := tx.InsertIntoBufferedEvents(ctx, []sqlplugin.BufferedEventsRow{row}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateBufferedEvents operation failed. Error: %v", err))
	}
	return nil
}

func getBufferedEvents(
	ctx context.Context,
	db sqlplugin.DB,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) ([]*commonpb.DataBlob, error) {

	rows, err := db.SelectFromBufferedEvents(ctx, sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("getBufferedEvents operation failed. Select failed: %v", err))
	}
	var result []*commonpb.DataBlob
	for _, row := range rows {
		result = append(result, p.NewDataBlob(row.Data, row.DataEncoding))
	}
	return result, nil
}

func deleteBufferedEvents(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromBufferedEvents(ctx, sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateBufferedEvents delete operation failed. Error: %v", err))
	}
	return nil
}
