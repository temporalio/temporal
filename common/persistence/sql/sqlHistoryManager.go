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
	"fmt"

	"database/sql"

	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
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
		BatchVersion int64
		RangeID      int64
		TxID         int64
		Data         []byte
		DataEncoding string
	}

	historyPageToken struct {
		LastEventID int64
	}
)

const (
	// ErrDupEntry MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
	// so we don't do the insert and return a ConditionalUpdate error.
	ErrDupEntry = 1062

	appendHistorySQLQuery = `INSERT INTO events (` +
		`domain_id,workflow_id,run_id,first_event_id,batch_version,range_id,tx_id,data,data_encoding)` +
		`VALUES (:domain_id,:workflow_id,:run_id,:first_event_id,:batch_version,:range_id,:tx_id,:data,:data_encoding);`

	overwriteHistorySQLQuery = `UPDATE events
SET
batch_version = :batch_version,
range_id = :range_id,
tx_id = :tx_id,
data = :data,
data_encoding = :data_encoding
WHERE
domain_id = :domain_id AND 
workflow_id = :workflow_id AND 
run_id = :run_id AND 
first_event_id = :first_event_id`

	getWorkflowExecutionHistorySQLQuery = `SELECT first_event_id, batch_version, data, data_encoding ` +
		`FROM events ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id >= ? AND first_event_id < ? ` +
		`ORDER BY first_event_id LIMIT ?`

	deleteWorkflowExecutionHistorySQLQuery = `DELETE FROM events WHERE domain_id = ? AND workflow_id = ? AND run_id = ?`

	lockEventSQLQuery = `SELECT range_id, tx_id FROM events ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id = ? ` +
		`FOR UPDATE`
)

// newHistoryPersistence creates an instance of HistoryManager
func newHistoryPersistence(cfg config.SQL, logger bark.Logger) (p.HistoryStore, error) {
	var db, err = newConnection(cfg)
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
		BatchVersion: request.EventBatchVersion,
		RangeID:      request.RangeID,
		TxID:         request.TransactionID,
		Data:         request.Events.Data,
		DataEncoding: string(request.Events.Encoding),
	}
	if request.Overwrite {
		return m.overWriteHistoryEvents(request, arg)
	}
	if _, err := m.db.NamedExec(appendHistorySQLQuery, arg); err != nil {
		if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == ErrDupEntry {
			return &p.ConditionFailedError{Msg: fmt.Sprintf("AppendHistoryEvents: event already exist: %v", err)}
		}
		return &workflow.InternalServiceError{Message: fmt.Sprintf("AppendHistoryEvents: %v", err)}
	}
	return nil
}

func (m *sqlHistoryManager) GetWorkflowExecutionHistory(request *p.InternalGetWorkflowExecutionHistoryRequest) (
	*p.InternalGetWorkflowExecutionHistoryResponse, error) {

	token := newHistoryPageToken(request.FirstEventID - 1)
	if request.NextPageToken != nil && len(request.NextPageToken) > 0 {
		if err := token.deserialize(request.NextPageToken); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("invalid next page token %v", request.NextPageToken)}
		}
	}

	var rows []eventsRow
	err := m.db.Select(&rows, getWorkflowExecutionHistorySQLQuery,
		request.DomainID,
		request.Execution.WorkflowId,
		request.Execution.RunId,
		token.LastEventID+1,
		request.NextEventID,
		request.PageSize)

	if err == sql.ErrNoRows || (err == nil && len(rows) == 0) {
		return nil, &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Workflow execution history not found.  WorkflowId: %v, RunId: %v",
				*request.Execution.WorkflowId, *request.Execution.RunId),
		}
	}

	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecutionHistory: %v", err),
		}
	}

	history := make([]*p.DataBlob, 0)
	lastEventBatchVersion := request.LastEventBatchVersion

	for _, v := range rows {
		eventBatch := &p.DataBlob{}
		eventBatchVersion := common.EmptyVersion
		eventBatch.Data = v.Data
		eventBatch.Encoding = common.EncodingType(v.DataEncoding)
		if v.BatchVersion > 0 {
			eventBatchVersion = v.BatchVersion
		}
		if eventBatchVersion >= lastEventBatchVersion {
			history = append(history, eventBatch)
			lastEventBatchVersion = eventBatchVersion
		}
		token.LastEventID = v.FirstEventID
	}

	return &p.InternalGetWorkflowExecutionHistoryResponse{
		History:               history,
		LastEventBatchVersion: lastEventBatchVersion,
		NextPageToken:         token.serialize(),
	}, nil
}

func (m *sqlHistoryManager) DeleteWorkflowExecutionHistory(request *p.DeleteWorkflowExecutionHistoryRequest) error {
	if _, err := m.db.Exec(deleteWorkflowExecutionHistorySQLQuery, request.DomainID, request.Execution.WorkflowId, request.Execution.RunId); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecutionHistory: %v", err),
		}
	}
	return nil
}

func (m *sqlHistoryManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlHistoryManager) overWriteHistoryEvents(request *p.InternalAppendHistoryEventsRequest, row *eventsRow) error {
	return runTransaction("AppendHistoryEvents", m.db, func(tx *sqlx.Tx) error {
		if err := lockEventForUpdate(tx, request); err != nil {
			return err
		}
		result, err := tx.NamedExec(overwriteHistorySQLQuery, row)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return fmt.Errorf("expected 1 row to be affected, got %v", rowsAffected)
		}
		return nil
	})
}

func lockEventForUpdate(tx *sqlx.Tx, req *p.InternalAppendHistoryEventsRequest) error {
	var row eventsRow
	err := tx.Get(&row, lockEventSQLQuery, req.DomainID, *req.Execution.WorkflowId, *req.Execution.RunId, req.FirstEventID)
	if err != nil {
		return err
	}
	if row.RangeID > req.RangeID {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("expected rangedID <=%v, got %v", req.RangeID, row.RangeID),
		}
	}
	if row.TxID >= req.TransactionID {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("expected txID < %v, got %v", req.TransactionID, row.TxID),
		}
	}
	return nil
}

func newHistoryPageToken(eventID int64) *historyPageToken {
	return &historyPageToken{LastEventID: eventID}
}

func (t *historyPageToken) serialize() []byte {
	s := strconv.FormatInt(t.LastEventID, 10)
	return []byte(s)
}

func (t *historyPageToken) deserialize(payload []byte) error {
	eventID, err := strconv.ParseInt(string(payload), 10, 64)
	if err != nil {
		return err
	}
	t.LastEventID = eventID
	return nil
}
