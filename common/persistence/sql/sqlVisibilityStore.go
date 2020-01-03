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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
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
func NewSQLVisibilityStore(cfg config.SQL, logger log.Logger) (p.VisibilityStore, error) {
	db, err := NewSQLDB(&cfg)
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

func (s *sqlVisibilityStore) RecordWorkflowExecutionStarted(request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	_, err := s.db.InsertIntoVisibility(&sqlplugin.VisibilityRow{
		DomainID:         request.DomainUUID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        time.Unix(0, request.StartTimestamp),
		ExecutionTime:    time.Unix(0, request.ExecutionTimestamp),
		WorkflowTypeName: request.WorkflowTypeName,
		Memo:             request.Memo.Data,
		Encoding:         string(request.Memo.GetEncoding()),
	})

	return err
}

func (s *sqlVisibilityStore) RecordWorkflowExecutionClosed(request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	closeTime := time.Unix(0, request.CloseTimestamp)
	result, err := s.db.ReplaceIntoVisibility(&sqlplugin.VisibilityRow{
		DomainID:         request.DomainUUID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        time.Unix(0, request.StartTimestamp),
		ExecutionTime:    time.Unix(0, request.ExecutionTimestamp),
		WorkflowTypeName: request.WorkflowTypeName,
		CloseTime:        &closeTime,
		CloseStatus:      common.Int32Ptr(int32(request.Status)),
		HistoryLength:    &request.HistoryLength,
		Memo:             request.Memo.Data,
		Encoding:         string(request.Memo.GetEncoding()),
	})
	if err != nil {
		return err
	}
	noRowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("RecordWorkflowExecutionClosed rowsAffected error: %v", err)
	}
	if noRowsAffected > 2 { // either adds a new row or deletes old row and adds new row
		return fmt.Errorf("RecordWorkflowExecutionClosed unexpected numRows (%v) updated", noRowsAffected)
	}
	return nil
}

func (s *sqlVisibilityStore) UpsertWorkflowExecution(request *p.InternalUpsertWorkflowExecutionRequest) error {
	if p.IsNopUpsertWorkflowRequest(request) {
		return nil
	}
	return p.NewOperationNotSupportErrorForVis()
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutions", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				RunID:        &readLevel.RunID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutions", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				Closed:       true,
				RunID:        &readLevel.RunID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByType", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
				DomainID:         request.DomainUUID,
				MinStartTime:     &minStartTime,
				MaxStartTime:     &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByType", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
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

func (s *sqlVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByWorkflowID", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
				DomainID:     request.DomainUUID,
				MinStartTime: &minStartTime,
				MaxStartTime: &readLevel.Time,
				RunID:        &readLevel.RunID,
				WorkflowID:   &request.WorkflowID,
				PageSize:     &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByWorkflowID", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
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

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByStatus(request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByStatus", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime)
			return s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
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

func (s *sqlVisibilityStore) GetClosedWorkflowExecution(request *p.GetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {
	execution := request.Execution
	rows, err := s.db.SelectFromVisibility(&sqlplugin.VisibilityFilter{
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
	return &p.InternalGetClosedWorkflowExecutionResponse{Execution: s.rowToInfo(&rows[0])}, nil
}

func (s *sqlVisibilityStore) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	_, err := s.db.DeleteFromVisibility(&sqlplugin.VisibilityFilter{
		DomainID: request.DomainID,
		RunID:    &request.RunID,
	})
	if err != nil {
		return &workflow.InternalServiceError{Message: err.Error()}
	}
	return nil
}

func (s *sqlVisibilityStore) ListWorkflowExecutions(request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {
	return nil, p.NewOperationNotSupportErrorForVis()
}

func (s *sqlVisibilityStore) ScanWorkflowExecutions(request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {
	return nil, p.NewOperationNotSupportErrorForVis()
}

func (s *sqlVisibilityStore) CountWorkflowExecutions(request *p.CountWorkflowExecutionsRequest) (*p.CountWorkflowExecutionsResponse, error) {
	return nil, p.NewOperationNotSupportErrorForVis()
}

func (s *sqlVisibilityStore) rowToInfo(row *sqlplugin.VisibilityRow) *p.VisibilityWorkflowExecutionInfo {
	if row.ExecutionTime.UnixNano() == 0 {
		row.ExecutionTime = row.StartTime
	}
	info := &p.VisibilityWorkflowExecutionInfo{
		WorkflowID:    row.WorkflowID,
		RunID:         row.RunID,
		TypeName:      row.WorkflowTypeName,
		StartTime:     row.StartTime,
		ExecutionTime: row.ExecutionTime,
		Memo:          p.NewDataBlob(row.Memo, common.EncodingType(row.Encoding)),
	}
	if row.CloseStatus != nil {
		status := workflow.WorkflowExecutionCloseStatus(*row.CloseStatus)
		info.Status = &status
		info.CloseTime = *row.CloseTime
		info.HistoryLength = *row.HistoryLength
	}
	return info
}

func (s *sqlVisibilityStore) listWorkflowExecutions(opName string, pageToken []byte, earliestTime int64, latestTime int64, selectOp func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error)) (*p.InternalListWorkflowExecutionsResponse, error) {
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
		return &p.InternalListWorkflowExecutionsResponse{}, nil
	}

	var infos = make([]*p.VisibilityWorkflowExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i] = s.rowToInfo(&row)
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
	return &p.InternalListWorkflowExecutionsResponse{
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
