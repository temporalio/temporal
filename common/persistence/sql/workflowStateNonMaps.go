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

	"github.com/uber/cadence/common"

	workflow "github.com/uber/cadence/.gen/go/shared"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

func updateSignalsRequested(
	tx sqlplugin.Tx,
	signalRequestedIDs []string,
	deleteSignalRequestID string,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if len(signalRequestedIDs) > 0 {
		rows := make([]sqlplugin.SignalsRequestedSetsRow, len(signalRequestedIDs))
		for i, v := range signalRequestedIDs {
			rows[i] = sqlplugin.SignalsRequestedSetsRow{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				SignalID:   v,
			}
		}
		if _, err := tx.InsertIntoSignalsRequestedSets(rows); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signals requested. Failed to execute update query. Error: %v", err),
			}
		}
	}

	if deleteSignalRequestID != "" {
		if _, err := tx.DeleteFromSignalsRequestedSets(&sqlplugin.SignalsRequestedSetsFilter{
			ShardID:    int64(shardID),
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			SignalID:   &deleteSignalRequestID,
		}); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signals requested. Failed to execute delete query. Error: %v", err),
			}
		}
	}

	return nil
}

func getSignalsRequested(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) (map[string]struct{}, error) {

	rows, err := db.SelectFromSignalsRequestedSets(&sqlplugin.SignalsRequestedSetsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get signals requested. Error: %v", err),
		}
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
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromSignalsRequestedSets(&sqlplugin.SignalsRequestedSetsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete signals requested set. Error: %v", err),
		}
	}
	return nil
}

func updateBufferedEvents(
	tx sqlplugin.Tx,
	batch *p.DataBlob,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if batch == nil {
		return nil
	}
	row := sqlplugin.BufferedEventsRow{
		ShardID:      shardID,
		DomainID:     domainID,
		WorkflowID:   workflowID,
		RunID:        runID,
		Data:         batch.Data,
		DataEncoding: string(batch.Encoding),
	}

	if _, err := tx.InsertIntoBufferedEvents([]sqlplugin.BufferedEventsRow{row}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("updateBufferedEvents operation failed. Error: %v", err),
		}
	}
	return nil
}

func getBufferedEvents(
	db sqlplugin.DB,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) ([]*p.DataBlob, error) {

	rows, err := db.SelectFromBufferedEvents(&sqlplugin.BufferedEventsFilter{
		ShardID:    shardID,
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("getBufferedEvents operation failed. Select failed: %v", err),
		}
	}
	var result []*p.DataBlob
	for _, row := range rows {
		result = append(result, p.NewDataBlob(row.Data, common.EncodingType(row.DataEncoding)))
	}
	return result, nil
}

func deleteBufferedEvents(
	tx sqlplugin.Tx,
	shardID int,
	domainID sqlplugin.UUID,
	workflowID string,
	runID sqlplugin.UUID,
) error {

	if _, err := tx.DeleteFromBufferedEvents(&sqlplugin.BufferedEventsFilter{
		ShardID:    shardID,
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("updateBufferedEvents delete operation failed. Error: %v", err),
		}
	}
	return nil
}
