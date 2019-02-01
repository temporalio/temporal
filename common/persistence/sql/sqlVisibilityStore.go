// Copyright (c) 2017 Uber Technologies, Inc.
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
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

type (
	sqlVisibilityStore struct {
		sqlStore
	}

	visibilityPageToken struct {
		Time  time.Time
		RunID string
	}
)

// NewSQLVisibilityStore creates an instance of ExecutionStore
func NewSQLVisibilityStore(cfg config.SQL, logger bark.Logger) (p.VisibilityManager, error) {
	db, err := storage.NewSQLDB(&cfg)
	if err != nil {
		return nil, err
	}
	return &sqlVisibilityStore{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}

func (s *sqlVisibilityStore) RecordWorkflowExecutionStarted(request *p.RecordWorkflowExecutionStartedRequest) error {
	result, err := s.db.InsertIntoVisibility(&sqldb.VisibilityRow{
		DomainID:         request.DomainUUID,
		WorkflowID:       *request.Execution.WorkflowId,
		RunID:            *request.Execution.RunId,
		StartTime:        time.Unix(0, request.StartTimestamp),
		WorkflowTypeName: request.WorkflowTypeName,
	})
	if err != nil {
		return err
	}
	noRowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("RecordWorkflowExecutionStarted rowsAffected error: %v", err)
	}
	if noRowsAffected != 1 {
		return fmt.Errorf("RecordWorkflowExecutionStarted %v rows updated instead of one", noRowsAffected)
	}
	return nil
}

func (s *sqlVisibilityStore) RecordWorkflowExecutionClosed(request *p.RecordWorkflowExecutionClosedRequest) error {
	closeTime := time.Unix(0, request.CloseTimestamp)
	result, err := s.db.UpdateVisibility(&sqldb.VisibilityRow{
		CloseTime:     &closeTime,
		CloseStatus:   common.Int32Ptr(int32(request.Status)),
		HistoryLength: &request.HistoryLength,
		DomainID:      request.DomainUUID,
		RunID:         *request.Execution.RunId,
	})
	if err != nil {
		return err
	}
	noRowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("RecordWorkflowExecutionStarted rowsAffected error: %v", err)
	}
	if noRowsAffected != 1 {
		return fmt.Errorf("RecordWorkflowExecutionStarted %v rows updated instead of one", noRowsAffected)
	}
	return nil
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutions", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				RunID:        &readLevel.RunID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutions", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				Closed:       true,
				RunID:        &readLevel.RunID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByType", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:         request.DomainUUID,
				MinStartTime:     &minStartTime,
				MaxStartTime:     &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByType", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:         request.DomainUUID,
				MinStartTime:     &minStartTime,
				MaxStartTime:     &readLevel.Time,
				Closed:           true,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByWorkflowID", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				RunID:        &readLevel.RunID,
				WorkflowID:   &request.WorkflowID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByWorkflowID", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				Closed:       true,
				RunID:        &readLevel.RunID,
				WorkflowID:   &request.WorkflowID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByStatus(request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.ListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByStatus", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				Closed:       true,
				RunID:        &readLevel.RunID,
				CloseStatus:  common.Int32Ptr(int32(request.Status)),
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) GetClosedWorkflowExecution(request *p.GetClosedWorkflowExecutionRequest) (*p.GetClosedWorkflowExecutionResponse, error) {
	execution := request.Execution
	rows, err := s.db.SelectFromVisibility(&sqldb.VisibilityFilter{
		DomainID: request.DomainUUID,
		Closed:   true,
		RunID:    execution.RunId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					execution.GetWorkflowId(), execution.GetRunId()),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetClosedWorkflowExecution operation failed. Select failed: %v", err),
		}
	}
	rows[0].DomainID = request.DomainUUID
	rows[0].RunID = execution.GetRunId()
	rows[0].WorkflowID = execution.GetWorkflowId()
	return &p.GetClosedWorkflowExecutionResponse{Execution: rowToInfo(&rows[0])}, nil
}

func rowToInfo(row *sqldb.VisibilityRow) *workflow.WorkflowExecutionInfo {
	info := &workflow.WorkflowExecutionInfo{
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(row.WorkflowID),
			RunId:      common.StringPtr(row.RunID),
		},
		Type: &workflow.WorkflowType{Name: common.StringPtr(row.WorkflowTypeName)},
	}
	if row.CloseStatus != nil {
		status := workflow.WorkflowExecutionCloseStatus(*row.CloseStatus)
		info.CloseStatus = &status
		closeTime := row.CloseTime.Unix()
		info.CloseTime = &closeTime
		info.HistoryLength = row.HistoryLength
	}
	return info
}

func (s *sqlVisibilityStore) listWorkflowExecutions(opName string, pageToken []byte, earliestTime int64, latestTime int64, selectOp func(readLevel *visibilityPageToken) ([]sqldb.VisibilityRow, error)) (*p.ListWorkflowExecutionsResponse, error) {
	var readLevel *visibilityPageToken
	var err error
	if len(pageToken) > 0 {
		readLevel, err = s.deserializePageToken(pageToken)
		if err != nil {
			return nil, err
		}
	} else {
		readLevel = &visibilityPageToken{Time: time.Unix(0, latestTime), RunID: ""}
	}
	rows, err := selectOp(readLevel)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("%v operation failed. Select failed: %v", opName, err),
		}
	}
	if len(rows) == 0 {
		return &p.ListWorkflowExecutionsResponse{}, nil
	}

	var infos = make([]*workflow.WorkflowExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i] = rowToInfo(&row)
	}
	var nextPageToken []byte
	lastRow := rows[len(rows)-1]
	lastStartTime := lastRow.StartTime
	if lastStartTime.Sub(time.Unix(0, earliestTime)).Nanoseconds() > 0 {
		nextPageToken, err = s.serializePageToken(&visibilityPageToken{
			Time:  lastStartTime,
			RunID: lastRow.RunID,
		})
		if err != nil {
			return nil, err
		}
	}
	return &p.ListWorkflowExecutionsResponse{
		Executions:    infos,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *sqlVisibilityStore) deserializePageToken(data []byte) (*visibilityPageToken, error) {
	var token visibilityPageToken
	err := json.Unmarshal(data, &token)
	return &token, err
}

func (s *sqlVisibilityStore) serializePageToken(token *visibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}
