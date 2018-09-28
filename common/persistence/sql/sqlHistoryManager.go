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
	"encoding/json"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
)

type (
	sqlHistoryManager struct {
		db      *sqlx.DB
		shardID int
		logger  bark.Logger
	}

	eventsRow struct {
		DomainID     string
		WorkflowID   string
		RunID        string
		FirstEventID int64
		Data         []byte
		DataEncoding string
		DataVersion  int64

		RangeID int64
		TxID    int64
	}

	historyToken struct {
		LastEventBatchVersion int64
		LastEventID           int64
	}
)

const (
	// ErrDupEntry MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
	// so we don't do the insert and return a ConditionalUpdate error.
	ErrDupEntry = 1062

	appendHistorySQLQuery = `INSERT INTO events (
domain_id,workflow_id,run_id,first_event_id,data,data_encoding,data_version
) VALUES (
:domain_id,:workflow_id,:run_id,:first_event_id,:data,:data_encoding,:data_version
);`

	overwriteHistorySQLQuery = `UPDATE events
SET
domain_id = :domain_id,
workflow_id = :workflow_id,
run_id = :run_id,
first_event_id = :first_event_id,
data = :data,
data_encoding = :data_encoding,
data_version = :data_version
WHERE
domain_id = :domain_id AND 
workflow_id = :workflow_id AND 
run_id = :run_id AND 
first_event_id = :first_event_id`

	pollHistorySQLQuery = `SELECT 1 FROM events WHERE domain_id = :domain_id AND 
workflow_id= :workflow_id AND run_id= :run_id AND first_event_id= :first_event_id`

	getWorkflowExecutionHistorySQLQuery = `SELECT first_event_id, data, data_encoding, data_version
FROM events
WHERE
domain_id = ? AND
workflow_id = ? AND
run_id = ? AND
first_event_id >= ? AND
first_event_id < ?
ORDER BY first_event_id
LIMIT ?`

	deleteWorkflowExecutionHistorySQLQuery = `DELETE FROM events WHERE
domain_id = ? AND workflow_id = ? AND run_id = ?`

	lockRangeIDAndTxIDSQLQuery = `SELECT range_id, tx_id FROM events WHERE
domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id = ?`
)

func (m *sqlHistoryManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

// NewHistoryPersistence creates an instance of HistoryManager
func NewHistoryPersistence(host string, port int, username, password, dbName string, logger bark.Logger) (p.HistoryStore, error) {
	var db, err = newConnection(host, port, username, password, dbName)
	if err != nil {
		return nil, err
	}
	return &sqlHistoryManager{
		db:     db,
		logger: logger,
	}, nil
}

func (m *sqlHistoryManager) AppendHistoryEvents(request *p.InternalAppendHistoryEventsRequest) error {
	arg := &eventsRow{
		DomainID:     request.DomainID,
		WorkflowID:   *request.Execution.WorkflowId,
		RunID:        *request.Execution.RunId,
		FirstEventID: request.FirstEventID,
		Data:         request.Events.Data,
		DataEncoding: string(request.Events.Encoding),
		RangeID:      request.RangeID,
		TxID:         request.TransactionID,
	}

	if request.Overwrite {
		tx, err := m.db.Beginx()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to begin transaction for overwrite. Error: %v", err),
			}
		}
		defer tx.Rollback()

		if err := lockAndCheckRangeIDAndTxID(tx,
			request.RangeID,
			request.TransactionID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			request.FirstEventID); err != nil {
			switch err.(type) {
			case *p.ConditionFailedError:
				return &p.ConditionFailedError{
					Msg: fmt.Sprintf("AppendHistoryEvents operation failed. Overwrite failed. Error: %v", err),
				}
			default:
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to lock row for overwrite. Error: %v", err),
				}
			}
		}
		result, err := tx.NamedExec(overwriteHistorySQLQuery, arg)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Update failed. Error: %v", err),
			}
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to check number of rows updated. Error: %v", err),
			}
		}
		if rowsAffected != 1 {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Updated %v rows instead of one.", rowsAffected),
			}
		}

		if err := tx.Commit(); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to lock row. Error: %v", err),
			}
		}
	} else {
		if _, err := m.db.NamedExec(appendHistorySQLQuery, arg); err != nil {
			// TODO Find another way to do this without inspecting the error message (?)
			if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == ErrDupEntry {
				return &p.ConditionFailedError{
					Msg: fmt.Sprintf("AppendHistoryEvents operaiton failed. Couldn't insert since row already existed. Erorr: %v", err),
				}
			}
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Insert failed. Error: %v", err),
			}
		}
	}

	return nil
}

// TODO: Pagination
func (m *sqlHistoryManager) GetWorkflowExecutionHistory(request *p.InternalGetWorkflowExecutionHistoryRequest) (
	*p.InternalGetWorkflowExecutionHistoryResponse, error) {

	var rows []eventsRow
	if err := m.db.Select(&rows,
		getWorkflowExecutionHistorySQLQuery,
		request.DomainID,
		request.Execution.WorkflowId,
		request.Execution.RunId,
		request.FirstEventID,
		request.NextEventID,
		request.PageSize); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecutionHistory operation failed. Select failed. Error: %v", err),
		}
	}

	if len(rows) == 0 {
		return nil, &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Workflow execution history not found.  WorkflowId: %v, RunId: %v",
				*request.Execution.WorkflowId, *request.Execution.RunId),
		}
	}

	eventBatchVersionPointer := new(int64)
	eventBatchVersion := common.EmptyVersion
	lastEventBatchVersion := request.LastEventBatchVersion

	history := make([]*p.DataBlob, 0)
	for _, v := range rows {
		eventBatch := &p.DataBlob{}
		eventBatch.Data = v.Data
		eventBatch.Encoding = common.EncodingType(v.DataEncoding)

		if eventBatchVersionPointer != nil {
			eventBatchVersion = *eventBatchVersionPointer
		}
		if eventBatchVersion >= lastEventBatchVersion {
			history = append(history, eventBatch)
			lastEventBatchVersion = eventBatchVersion
		}

		eventBatchVersionPointer = new(int64)
		eventBatchVersion = common.EmptyVersion
	}

	response := &p.InternalGetWorkflowExecutionHistoryResponse{
		History:               history,
		LastEventBatchVersion: lastEventBatchVersion,
		NextPageToken:         []byte{},
	}

	return response, nil
}

func (m *sqlHistoryManager) DeleteWorkflowExecutionHistory(request *p.DeleteWorkflowExecutionHistoryRequest) error {
	if _, err := m.db.Exec(deleteWorkflowExecutionHistorySQLQuery, request.DomainID, request.Execution.WorkflowId, request.Execution.RunId); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecutionHistory operation failed. Error: %v", err),
		}
	}
	return nil
}

func lockAndCheckRangeIDAndTxID(tx *sqlx.Tx,
	maxRangeID int64,
	maxTxIDPlusOne int64,
	domainID string,
	workflowID string,
	runID string,
	firstEventID int64) error {
	var row eventsRow
	if err := tx.Get(&row,
		lockRangeIDAndTxIDSQLQuery,
		domainID,
		workflowID,
		runID,
		firstEventID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock range ID and tx ID. Get failed. Error: %v", err),
		}
	}
	if !(row.RangeID <= maxRangeID) {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to lock range ID and tx ID. %v should've been at most %v.", row.RangeID, maxRangeID),
		}
	} else if !(row.TxID < maxTxIDPlusOne) {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to lock range ID and tx ID. %v should've been strictly less than %v.", row.TxID, maxTxIDPlusOne),
		}
	}
	return nil
}

func (m *sqlHistoryManager) serializeToken(token *historyToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, &workflow.InternalServiceError{Message: "Error generating history event token."}
	}
	return data, nil
}

func (m *sqlHistoryManager) deserializeToken(request *p.GetWorkflowExecutionHistoryRequest) (*historyToken, error) {
	token := &historyToken{
		LastEventBatchVersion: common.EmptyVersion,
		LastEventID:           request.FirstEventID - 1,
	}

	if len(request.NextPageToken) == 0 {
		return token, nil
	}

	err := json.Unmarshal(request.NextPageToken, token)
	if err == nil {
		return token, nil
	}
	return token, nil
}
