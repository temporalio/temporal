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
	"encoding/json"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
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
func NewSQLVisibilityStore(db sqlplugin.DB, logger log.Logger) (p.VisibilityStore, error) {
	return &sqlVisibilityStore{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}

func (s *sqlVisibilityStore) RecordWorkflowExecutionStarted(request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	_, err := s.db.InsertIntoVisibility(&sqlplugin.VisibilityRow{
		NamespaceID:      request.NamespaceID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        time.Unix(0, request.StartTimestamp).UTC(),
		ExecutionTime:    time.Unix(0, request.ExecutionTimestamp).UTC(),
		WorkflowTypeName: request.WorkflowTypeName,
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), // Underlying value (1) is hardcoded in SQL queries.
		Memo:             request.Memo.Data,
		Encoding:         request.Memo.Encoding.String(),
	})

	return err
}

func (s *sqlVisibilityStore) RecordWorkflowExecutionClosed(request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	closeTime := time.Unix(0, request.CloseTimestamp).UTC()
	result, err := s.db.ReplaceIntoVisibility(&sqlplugin.VisibilityRow{
		NamespaceID:      request.NamespaceID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        time.Unix(0, request.StartTimestamp).UTC(),
		ExecutionTime:    time.Unix(0, request.ExecutionTimestamp).UTC(),
		WorkflowTypeName: request.WorkflowTypeName,
		CloseTime:        &closeTime,
		Status:           int32(request.Status),
		HistoryLength:    &request.HistoryLength,
		Memo:             request.Memo.Data,
		Encoding:         request.Memo.Encoding.String(),
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

func (s *sqlVisibilityStore) UpsertWorkflowExecution(_ *p.InternalUpsertWorkflowExecutionRequest) error {
	return nil
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutions", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &minStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				PageSize:    &request.PageSize,
				Status:      int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutions", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &minStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				PageSize:    &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByType", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID:      request.NamespaceID,
				MinTime:          &minStartTime,
				MaxTime:          &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
				Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByType", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID:      request.NamespaceID,
				MinTime:          &minStartTime,
				MaxTime:          &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByWorkflowID", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &minStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				WorkflowID:  &request.WorkflowID,
				PageSize:    &request.PageSize,
				Status:      int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByWorkflowID", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &minStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				WorkflowID:  &request.WorkflowID,
				PageSize:    &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) ListClosedWorkflowExecutionsByStatus(request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByStatus", request.NextPageToken, request.EarliestStartTime, request.LatestStartTime, true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			minStartTime := time.Unix(0, request.EarliestStartTime).UTC()
			return s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &minStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				Status:      int32(request.Status),
				PageSize:    &request.PageSize,
			})
		})
}

func (s *sqlVisibilityStore) GetClosedWorkflowExecution(request *p.GetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {
	execution := request.Execution
	rows, err := s.db.SelectFromVisibility(sqlplugin.VisibilitySelectFilter{
		NamespaceID: request.NamespaceID,
		RunID:       &execution.RunId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
				execution.GetWorkflowId(), execution.GetRunId()))
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetClosedWorkflowExecution operation failed. Select failed: %v", err))
	}
	rows[0].NamespaceID = request.NamespaceID
	rows[0].RunID = execution.GetRunId()
	rows[0].WorkflowID = execution.GetWorkflowId()
	return &p.InternalGetClosedWorkflowExecutionResponse{Execution: s.rowToInfo(&rows[0])}, nil
}

func (s *sqlVisibilityStore) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	_, err := s.db.DeleteFromVisibility(sqlplugin.VisibilityDeleteFilter{
		NamespaceID: request.NamespaceID,
		RunID:       request.RunID,
	})
	if err != nil {
		return serviceerror.NewInternal(err.Error())
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
		Memo:          p.NewDataBlob(row.Memo, row.Encoding),
		Status:        enumspb.WorkflowExecutionStatus(row.Status),
	}
	if row.CloseTime != nil {
		info.CloseTime = *row.CloseTime
		info.HistoryLength = *row.HistoryLength
	}
	if row.HistoryLength != nil {
		info.HistoryLength = *row.HistoryLength
	}
	return info
}

func (s *sqlVisibilityStore) listWorkflowExecutions(
	opName string,
	pageToken []byte,
	earliestTime int64,
	latestTime int64,
	closeQuery bool,
	selectOp func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error)) (*p.InternalListWorkflowExecutionsResponse, error) {
	var readLevel *visibilityPageToken
	var err error
	if len(pageToken) > 0 {
		readLevel, err = s.deserializePageToken(pageToken)
		if err != nil {
			return nil, err
		}
	} else {
		readLevel = &visibilityPageToken{Time: time.Unix(0, latestTime).UTC(), RunID: ""}
	}
	rows, err := selectOp(readLevel)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("%v operation failed. Select failed: %v", opName, err))
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
	lastTime := lastRow.StartTime
	if closeQuery {
		lastTime = *lastRow.CloseTime
	}

	if lastTime.Sub(time.Unix(0, earliestTime).UTC()).Nanoseconds() > 0 {
		nextPageToken, err = s.serializePageToken(&visibilityPageToken{
			Time:  lastTime,
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
