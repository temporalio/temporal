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
	"context"
	"encoding/json"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/resolver"
)

const (
	visibilityTimeout = 16 * time.Second
)

type (
	visibilityStore struct {
		sqlStore persistencesql.SqlStore
	}

	visibilityPageToken struct {
		Time  time.Time
		RunID string
	}
)

var _ visibility.VisibilityStore = (*visibilityStore)(nil)

// TODO remove this function when NoSQL & SQL layer all support context timeout
func newVisibilityContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	return context.WithTimeout(ctx, visibilityTimeout)
}

// NewSQLVisibilityStore creates an instance of ExecutionStore
func NewSQLVisibilityStore(
	cfg config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
) (*visibilityStore, error) {
	refDbConn := persistencesql.NewRefCountedDBConn(sqlplugin.DbKindVisibility, &cfg, r)
	db, err := refDbConn.Get()
	if err != nil {
		return nil, err
	}
	return &visibilityStore{
		sqlStore: persistencesql.NewSqlStore(db, logger),
	}, nil
}

func (s *visibilityStore) Close() {
	s.sqlStore.Close()
}

func (s *visibilityStore) GetName() string {
	return s.sqlStore.GetName()
}

func (s *visibilityStore) RecordWorkflowExecutionStarted(
	request *visibility.InternalRecordWorkflowExecutionStartedRequest,
) error {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	_, err := s.sqlStore.Db.InsertIntoVisibility(ctx, &sqlplugin.VisibilityRow{
		NamespaceID:      request.NamespaceID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        request.StartTime,
		ExecutionTime:    request.ExecutionTime,
		WorkflowTypeName: request.WorkflowTypeName,
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), // Underlying value (1) is hardcoded in SQL queries.
		Memo:             request.Memo.Data,
		Encoding:         request.Memo.EncodingType.String(),
	})

	return err
}

func (s *visibilityStore) RecordWorkflowExecutionClosed(request *visibility.InternalRecordWorkflowExecutionClosedRequest) error {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	result, err := s.sqlStore.Db.ReplaceIntoVisibility(ctx, &sqlplugin.VisibilityRow{
		NamespaceID:      request.NamespaceID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        request.StartTime,
		ExecutionTime:    request.ExecutionTime,
		WorkflowTypeName: request.WorkflowTypeName,
		CloseTime:        &request.CloseTime,
		Status:           int32(request.Status),
		HistoryLength:    &request.HistoryLength,
		Memo:             request.Memo.Data,
		Encoding:         request.Memo.EncodingType.String(),
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

func (s *visibilityStore) UpsertWorkflowExecution(
	_ *visibility.InternalUpsertWorkflowExecutionRequest,
) error {
	// Not OperationNotSupportedErr!
	return nil
}

func (s *visibilityStore) ListOpenWorkflowExecutions(
	request *visibility.ListWorkflowExecutionsRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions(
		"ListOpenWorkflowExecutions",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				PageSize:    &request.PageSize,
				Status:      int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *visibilityStore) ListClosedWorkflowExecutions(
	request *visibility.ListWorkflowExecutionsRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions("ListClosedWorkflowExecutions",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				PageSize:    &request.PageSize,
			})
		})
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByType(
	request *visibility.ListWorkflowExecutionsByTypeRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByType",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID:      request.NamespaceID,
				MinTime:          &request.EarliestStartTime,
				MaxTime:          &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
				Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByType(
	request *visibility.ListWorkflowExecutionsByTypeRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByType",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID:      request.NamespaceID,
				MinTime:          &request.EarliestStartTime,
				MaxTime:          &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
			})
		})
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	request *visibility.ListWorkflowExecutionsByWorkflowIDRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByWorkflowID",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				WorkflowID:  &request.WorkflowID,
				PageSize:    &request.PageSize,
				Status:      int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	request *visibility.ListWorkflowExecutionsByWorkflowIDRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByWorkflowID",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				WorkflowID:  &request.WorkflowID,
				PageSize:    &request.PageSize,
			})
		})
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	request *visibility.ListClosedWorkflowExecutionsByStatusRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByStatus",
		request.NextPageToken,
		request.EarliestStartTime,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID,
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				Status:      int32(request.Status),
				PageSize:    &request.PageSize,
			})
		})
}

func (s *visibilityStore) DeleteWorkflowExecution(
	request *visibility.VisibilityDeleteWorkflowExecutionRequest,
) error {
	ctx, cancel := newVisibilityContext()
	defer cancel()
	_, err := s.sqlStore.Db.DeleteFromVisibility(ctx, sqlplugin.VisibilityDeleteFilter{
		NamespaceID: request.NamespaceID,
		RunID:       request.RunID,
	})
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (s *visibilityStore) ListWorkflowExecutions(
	_ *visibility.ListWorkflowExecutionsRequestV2,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	return nil, visibility.OperationNotSupportedErr
}

func (s *visibilityStore) ScanWorkflowExecutions(
	_ *visibility.ListWorkflowExecutionsRequestV2,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	return nil, visibility.OperationNotSupportedErr
}

func (s *visibilityStore) CountWorkflowExecutions(
	_ *visibility.CountWorkflowExecutionsRequest,
) (*visibility.CountWorkflowExecutionsResponse, error) {
	return nil, visibility.OperationNotSupportedErr
}

func (s *visibilityStore) rowToInfo(
	row *sqlplugin.VisibilityRow,
) *visibility.VisibilityWorkflowExecutionInfo {
	if row.ExecutionTime.UnixNano() == 0 {
		row.ExecutionTime = row.StartTime
	}
	info := &visibility.VisibilityWorkflowExecutionInfo{
		WorkflowID:    row.WorkflowID,
		RunID:         row.RunID,
		TypeName:      row.WorkflowTypeName,
		StartTime:     row.StartTime,
		ExecutionTime: row.ExecutionTime,
		Memo:          persistence.NewDataBlob(row.Memo, row.Encoding),
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

func (s *visibilityStore) listWorkflowExecutions(
	opName string,
	pageToken []byte,
	earliestTime time.Time,
	latestTime time.Time,
	closeQuery bool,
	selectOp func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error),
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	var readLevel *visibilityPageToken
	var err error
	if len(pageToken) > 0 {
		readLevel, err = s.deserializePageToken(pageToken)
		if err != nil {
			return nil, err
		}
	} else {
		readLevel = &visibilityPageToken{Time: latestTime, RunID: ""}
	}
	rows, err := selectOp(readLevel)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("%v operation failed. Select failed: %v", opName, err))
	}
	if len(rows) == 0 {
		return &visibility.InternalListWorkflowExecutionsResponse{}, nil
	}

	var infos = make([]*visibility.VisibilityWorkflowExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i] = s.rowToInfo(&row)
	}
	var nextPageToken []byte
	lastRow := rows[len(rows)-1]
	lastTime := lastRow.StartTime
	if closeQuery {
		lastTime = *lastRow.CloseTime
	}

	if lastTime.After(earliestTime) {
		nextPageToken, err = s.serializePageToken(&visibilityPageToken{
			Time:  lastTime,
			RunID: lastRow.RunID,
		})
		if err != nil {
			return nil, err
		}
	}
	return &visibility.InternalListWorkflowExecutionsResponse{
		Executions:    infos,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *visibilityStore) deserializePageToken(
	data []byte,
) (*visibilityPageToken, error) {
	var token visibilityPageToken
	err := json.Unmarshal(data, &token)
	return &token, err
}

func (s *visibilityStore) serializePageToken(
	token *visibilityPageToken,
) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}
