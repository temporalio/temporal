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

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

func updateSignalsRequested(
	tx sqlplugin.Tx,
	signalRequestedIDs []string,
	deleteSignalRequestID string,
	shardID int,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if len(signalRequestedIDs) > 0 {
		rows := make([]sqlplugin.SignalsRequestedSetsRow, len(signalRequestedIDs))
		for i, v := range signalRequestedIDs {
			rows[i] = sqlplugin.SignalsRequestedSetsRow{
				ShardID:     int32(shardID),
				NamespaceID: namespaceID,
				WorkflowID:  workflowID,
				RunID:       runID,
				SignalID:    v,
			}
		}
		if _, err := tx.ReplaceIntoSignalsRequestedSets(rows); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update signals requested. Failed to execute update query. Error: %v", err))
		}
	}

	if deleteSignalRequestID != "" {
		if _, err := tx.DeleteFromSignalsRequestedSets(&sqlplugin.SignalsRequestedSetsFilter{
			ShardID:     int32(shardID),
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			SignalID:    &deleteSignalRequestID,
		}); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to update signals requested. Failed to execute delete query. Error: %v", err))
		}
	}

	return nil
}

func getSignalsRequested(
	db sqlplugin.DB,
	shardID int,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[string]struct{}, error) {

	rows, err := db.SelectFromSignalsRequestedSets(&sqlplugin.SignalsRequestedSetsFilter{
		ShardID:     int32(shardID),
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to get signals requested. Error: %v", err))
	}
	var ret = make(map[string]struct{})
	for _, s := range rows {
		ret[s.SignalID] = struct{}{}
	}
	return ret, nil
}

func deleteSignalsRequestedSet(
	tx sqlplugin.Tx,
	shardID int,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromSignalsRequestedSets(&sqlplugin.SignalsRequestedSetsFilter{
		ShardID:     int32(shardID),
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to delete signals requested set. Error: %v", err))
	}
	return nil
}

func updateBufferedEvents(
	tx sqlplugin.Tx,
	batch *serialization.DataBlob,
	shardID int,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if batch == nil {
		return nil
	}
	row := sqlplugin.BufferedEventsRow{
		ShardID:      int32(shardID),
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		Data:         batch.Data,
		DataEncoding: batch.Encoding.String(),
	}

	if _, err := tx.InsertIntoBufferedEvents([]sqlplugin.BufferedEventsRow{row}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("updateBufferedEvents operation failed. Error: %v", err))
	}
	return nil
}

func getBufferedEvents(
	db sqlplugin.DB,
	shardID int,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) ([]*serialization.DataBlob, error) {

	rows, err := db.SelectFromBufferedEvents(&sqlplugin.BufferedEventsFilter{
		ShardID:     int32(shardID),
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewInternal(fmt.Sprintf("getBufferedEvents operation failed. Select failed: %v", err))
	}
	var result []*serialization.DataBlob
	for _, row := range rows {
		result = append(result, p.NewDataBlob(row.Data, row.DataEncoding))
	}
	return result, nil
}

func deleteBufferedEvents(
	tx sqlplugin.Tx,
	shardID int,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {

	if _, err := tx.DeleteFromBufferedEvents(&sqlplugin.BufferedEventsFilter{
		ShardID:     int32(shardID),
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("updateBufferedEvents delete operation failed. Error: %v", err))
	}
	return nil
}
