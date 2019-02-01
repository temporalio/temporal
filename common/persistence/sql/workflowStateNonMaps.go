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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

func updateSignalsRequested(tx sqldb.Tx,
	signalRequestedIDs []string,
	deleteSignalRequestID string,
	shardID int,
	domainID, workflowID, runID string) error {
	if len(signalRequestedIDs) > 0 {
		rows := make([]sqldb.SignalsRequestedSetsRow, len(signalRequestedIDs))
		for i, v := range signalRequestedIDs {
			rows[i] = sqldb.SignalsRequestedSetsRow{
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
		if _, err := tx.DeleteFromSignalsRequestedSets(&sqldb.SignalsRequestedSetsFilter{
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

func getSignalsRequested(tx sqldb.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[string]struct{}, error) {
	rows, err := tx.SelectFromSignalsRequestedSets(&sqldb.SignalsRequestedSetsFilter{
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

func deleteSignalsRequestedSet(tx sqldb.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.DeleteFromSignalsRequestedSets(&sqldb.SignalsRequestedSetsFilter{
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
