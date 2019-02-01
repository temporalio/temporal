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

	"github.com/go-sql-driver/mysql"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

type sqlHistoryManager struct {
	sqlStore
	shardID int
}

// newHistoryPersistence creates an instance of HistoryManager
func newHistoryPersistence(cfg config.SQL, logger bark.Logger) (p.HistoryStore, error) {
	var db, err = storage.NewSQLDB(&cfg)
	if err != nil {
		return nil, err
	}
	return &sqlHistoryManager{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}

func (m *sqlHistoryManager) AppendHistoryEvents(request *p.InternalAppendHistoryEventsRequest) error {
	row := &sqldb.EventsRow{
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
		return m.overWriteHistoryEvents(request, row)
	}
	_, err := m.db.InsertIntoEvents(row)
	if err != nil {
		if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == ErrDupEntry {
			return &p.ConditionFailedError{Msg: fmt.Sprintf("AppendHistoryEvents: event already exist: %v", err)}
		}
		return &workflow.InternalServiceError{Message: fmt.Sprintf("AppendHistoryEvents: %v", err)}
	}
	return nil
}

func (m *sqlHistoryManager) GetWorkflowExecutionHistory(request *p.InternalGetWorkflowExecutionHistoryRequest) (
	*p.InternalGetWorkflowExecutionHistoryResponse, error) {

	offset := request.FirstEventID - 1
	if request.NextPageToken != nil && len(request.NextPageToken) > 0 {
		var newOffset int64
		var err error
		if newOffset, err = deserializePageToken(request.NextPageToken); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("invalid next page token %v", request.NextPageToken)}
		}
		offset = newOffset
	}

	rows, err := m.db.SelectFromEvents(&sqldb.EventsFilter{
		DomainID:     request.DomainID,
		WorkflowID:   *request.Execution.WorkflowId,
		RunID:        *request.Execution.RunId,
		FirstEventID: common.Int64Ptr(offset + 1),
		NextEventID:  &request.NextEventID,
		PageSize:     &request.PageSize,
	})

	// TODO: Ensure that no last empty page is requested
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
		offset = v.FirstEventID
	}

	nextPageToken := serializePageToken(offset)
	return &p.InternalGetWorkflowExecutionHistoryResponse{
		History:               history,
		LastEventBatchVersion: lastEventBatchVersion,
		NextPageToken:         nextPageToken,
	}, nil
}

func (m *sqlHistoryManager) DeleteWorkflowExecutionHistory(request *p.DeleteWorkflowExecutionHistoryRequest) error {
	_, err := m.db.DeleteFromEvents(&sqldb.EventsFilter{
		DomainID: request.DomainID, WorkflowID: *request.Execution.WorkflowId, RunID: *request.Execution.RunId})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecutionHistory: %v", err),
		}
	}
	return nil
}

func (m *sqlHistoryManager) overWriteHistoryEvents(request *p.InternalAppendHistoryEventsRequest, row *sqldb.EventsRow) error {
	return m.txExecute("AppendHistoryEvents", func(tx sqldb.Tx) error {
		if err := lockEventForUpdate(tx, request); err != nil {
			return err
		}
		result, err := tx.UpdateEvents(row)
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

func lockEventForUpdate(tx sqldb.Tx, req *p.InternalAppendHistoryEventsRequest) error {
	row, err := tx.LockEvents(&sqldb.EventsFilter{
		DomainID:     req.DomainID,
		WorkflowID:   *req.Execution.WorkflowId,
		RunID:        *req.Execution.RunId,
		FirstEventID: &req.FirstEventID,
	})
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
