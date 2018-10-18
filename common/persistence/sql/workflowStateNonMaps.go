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

	"github.com/jmoiron/sqlx"
	workflow "github.com/uber/cadence/.gen/go/shared"
)

const (
	deleteSignalsRequestedSetSQLQuery = `DELETE FROM signals_requested_sets
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id
`

	addToSignalsRequestedSetSQLQuery = `INSERT IGNORE INTO signals_requested_sets
(shard_id, domain_id, workflow_id, run_id, signal_id) VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :signal_id)`

	removeFromSignalsRequestedSetSQLQuery = `DELETE FROM signals_requested_sets
WHERE 
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id AND
signal_id = :signal_id`

	getSignalsRequestedSetSQLQuery = `SELECT signal_id FROM signals_requested_sets WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`
)

type (
	signalsRequestedSetsRow struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		SignalID   string
	}
)

func updateSignalsRequested(tx *sqlx.Tx,
	signalRequestedIDs []string,
	deleteSignalRequestID string,
	shardID int,
	domainID, workflowID, runID string) error {
	if len(signalRequestedIDs) > 0 {
		signalsRequestedSetsRows := make([]signalsRequestedSetsRow, len(signalRequestedIDs))
		for i, v := range signalRequestedIDs {
			signalsRequestedSetsRows[i] = signalsRequestedSetsRow{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				SignalID:   v,
			}
		}

		query, args, err := tx.BindNamed(addToSignalsRequestedSetSQLQuery, signalsRequestedSetsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signals requested. Failed to bind query. Error: %v", err),
			}
		}

		if _, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signals requested. Failed to execute update query. Error: %v", err),
			}
		}
	}

	if deleteSignalRequestID != "" {
		if _, err := tx.NamedExec(removeFromSignalsRequestedSetSQLQuery, &signalsRequestedSetsRow{
			ShardID:    int64(shardID),
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			SignalID:   deleteSignalRequestID,
		}); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signals requested. Failed to execute delete query. Error: %v", err),
			}
		}
	}

	return nil
}

func getSignalsRequested(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[string]struct{}, error) {
	var signals []string
	if err := tx.Select(&signals, getSignalsRequestedSetSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get signals requested. Error: %v", err),
		}
	}

	var ret = make(map[string]struct{})
	for _, s := range signals {
		ret[s] = struct{}{}
	}
	return ret, nil
}

func deleteSignalsRequestedSet(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.NamedExec(deleteSignalsRequestedSetSQLQuery, &signalsRequestedSetsRow{
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
