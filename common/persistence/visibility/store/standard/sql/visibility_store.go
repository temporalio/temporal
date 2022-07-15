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
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
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

var _ store.VisibilityStore = (*visibilityStore)(nil)

// NewSQLVisibilityStore creates an instance of VisibilityStore
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
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
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
		TaskQueue:        request.TaskQueue,
	})

	return err
}

func (s *visibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
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
		TaskQueue:        request.TaskQueue,
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
	_ context.Context,
	_ *store.InternalUpsertWorkflowExecutionRequest,
) error {
	// Not OperationNotSupportedErr!
	return nil
}

func (s *visibilityStore) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions(
		"ListOpenWorkflowExecutions",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID.String(),
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				PageSize:    &request.PageSize,
				Status:      int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			})
		})
}

func (s *visibilityStore) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutions",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID.String(),
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				PageSize:    &request.PageSize,
			})
		})
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByType",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID:      request.NamespaceID.String(),
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
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByType",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID:      request.NamespaceID.String(),
				MinTime:          &request.EarliestStartTime,
				MaxTime:          &readLevel.Time,
				RunID:            &readLevel.RunID,
				WorkflowTypeName: &request.WorkflowTypeName,
				PageSize:         &request.PageSize,
			})
		})
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListOpenWorkflowExecutionsByWorkflowID",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		false,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID.String(),
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
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByWorkflowID",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID.String(),
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				WorkflowID:  &request.WorkflowID,
				PageSize:    &request.PageSize,
			})
		})
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.listWorkflowExecutions("ListClosedWorkflowExecutionsByStatus",
		request.NextPageToken,
		request.PageSize,
		request.LatestStartTime,
		true,
		func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error) {
			return s.sqlStore.Db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{
				NamespaceID: request.NamespaceID.String(),
				MinTime:     &request.EarliestStartTime,
				MaxTime:     &readLevel.Time,
				RunID:       &readLevel.RunID,
				Status:      int32(request.Status),
				PageSize:    &request.PageSize,
			})
		})
}

func (s *visibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	_, err := s.sqlStore.Db.DeleteFromVisibility(ctx, sqlplugin.VisibilityDeleteFilter{
		NamespaceID: request.NamespaceID.String(),
		RunID:       request.RunID,
	})
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}
	return nil
}

func (s *visibilityStore) ListWorkflowExecutions(
	_ context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (s *visibilityStore) ScanWorkflowExecutions(
	_ context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (s *visibilityStore) CountWorkflowExecutions(
	_ context.Context,
	_ *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (s *visibilityStore) rowToInfo(
	row *sqlplugin.VisibilityRow,
) *store.InternalWorkflowExecutionInfo {
	if row.ExecutionTime.UnixNano() == 0 {
		row.ExecutionTime = row.StartTime
	}
	info := &store.InternalWorkflowExecutionInfo{
		WorkflowID:    row.WorkflowID,
		RunID:         row.RunID,
		TypeName:      row.WorkflowTypeName,
		StartTime:     row.StartTime,
		ExecutionTime: row.ExecutionTime,
		Memo:          persistence.NewDataBlob(row.Memo, row.Encoding),
		Status:        enumspb.WorkflowExecutionStatus(row.Status),
		TaskQueue:     row.TaskQueue,
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
	pageSize int,
	latestTime time.Time,
	closeQuery bool,
	selectOp func(readLevel *visibilityPageToken) ([]sqlplugin.VisibilityRow, error),
) (*store.InternalListWorkflowExecutionsResponse, error) {
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
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("%v operation failed. Select failed: %v", opName, err))
	}
	if len(rows) == 0 {
		return &store.InternalListWorkflowExecutionsResponse{}, nil
	}

	var infos = make([]*store.InternalWorkflowExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i] = s.rowToInfo(&row)
	}

	var nextPageToken []byte
	if len(rows) == pageSize {
		lastRow := rows[len(rows)-1]
		lastTime := lastRow.StartTime
		if closeQuery {
			lastTime = *lastRow.CloseTime
		}
		nextPageToken, err = s.serializePageToken(&visibilityPageToken{
			Time:  lastTime,
			RunID: lastRow.RunID,
		})
		if err != nil {
			return nil, err
		}
	}
	return &store.InternalListWorkflowExecutionsResponse{
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
