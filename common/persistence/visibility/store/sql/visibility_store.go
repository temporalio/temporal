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
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type (
	VisibilityStore struct {
		sqlStore                       persistencesql.SqlStore
		searchAttributesProvider       searchattribute.Provider
		searchAttributesMapperProvider searchattribute.MapperProvider
	}
)

var _ store.VisibilityStore = (*VisibilityStore)(nil)

var maxTime, _ = time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")

// NewSQLVisibilityStore creates an instance of VisibilityStore
func NewSQLVisibilityStore(
	cfg config.SQL,
	r resolver.ServiceResolver,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*VisibilityStore, error) {
	refDbConn := persistencesql.NewRefCountedDBConn(sqlplugin.DbKindVisibility, &cfg, r, logger, metricsHandler)
	db, err := refDbConn.Get()
	if err != nil {
		return nil, err
	}
	return &VisibilityStore{
		sqlStore:                       persistencesql.NewSqlStore(db, logger),
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
	}, nil
}

func (s *VisibilityStore) Close() {
	s.sqlStore.Close()
}

func (s *VisibilityStore) GetName() string {
	return s.sqlStore.GetName()
}

func (s *VisibilityStore) GetIndexName() string {
	return s.sqlStore.GetDbName()
}

func (s *VisibilityStore) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	return searchAttributes, nil
}

func (s *VisibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	row, err := s.generateVisibilityRow(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	_, err = s.sqlStore.Db.InsertIntoVisibility(ctx, row)
	return err
}

func (s *VisibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	row, err := s.generateVisibilityRow(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	row.CloseTime = &request.CloseTime
	row.HistoryLength = &request.HistoryLength
	row.HistorySizeBytes = &request.HistorySizeBytes
	row.ExecutionDuration = &request.ExecutionDuration
	row.StateTransitionCount = &request.StateTransitionCount

	result, err := s.sqlStore.Db.ReplaceIntoVisibility(ctx, row)
	if err != nil {
		return err
	}
	noRowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("RecordWorkflowExecutionClosed rowsAffected error: %v", err)
	}
	if noRowsAffected > 2 { // either adds a new row or deletes old row and adds new row
		return fmt.Errorf(
			"RecordWorkflowExecutionClosed unexpected numRows (%v) updated",
			noRowsAffected,
		)
	}
	return nil
}

func (s *VisibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *store.InternalUpsertWorkflowExecutionRequest,
) error {
	row, err := s.generateVisibilityRow(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	result, err := s.sqlStore.Db.ReplaceIntoVisibility(ctx, row)
	if err != nil {
		return err
	}
	noRowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if noRowsAffected > 2 { // either adds a new or deletes old row and adds new row
		return fmt.Errorf("UpsertWorkflowExecution unexpected numRows (%v) updates", noRowsAffected)
	}
	return nil
}

func (s *VisibilityStore) DeleteWorkflowExecution(
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

func (s *VisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	converter := NewQueryConverter(
		s.GetName(),
		request.Namespace,
		request.NamespaceID,
		saTypeMap,
		saMapper,
		request.Query,
	)
	selectFilter, err := converter.BuildSelectStmt(request.PageSize, request.NextPageToken)
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	rows, err := s.sqlStore.Db.SelectFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("ListWorkflowExecutions operation failed. Select failed: %v", err))
	}
	if len(rows) == 0 {
		return &store.InternalListWorkflowExecutionsResponse{}, nil
	}

	var infos = make([]*store.InternalWorkflowExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i], err = s.rowToInfo(&row, request.Namespace)
		if err != nil {
			return nil, err
		}
	}

	var nextPageToken []byte
	if len(rows) == request.PageSize {
		lastRow := rows[len(rows)-1]
		closeTime := maxTime
		if lastRow.CloseTime != nil {
			closeTime = *lastRow.CloseTime
		}
		nextPageToken, err = serializePageToken(&pageToken{
			CloseTime: closeTime,
			StartTime: lastRow.StartTime,
			RunID:     lastRow.RunID,
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

func (s *VisibilityStore) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.ListWorkflowExecutions(ctx, request)
}

func (s *VisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	converter := NewQueryConverter(
		s.GetName(),
		request.Namespace,
		request.NamespaceID,
		saTypeMap,
		saMapper,
		request.Query,
	)
	selectFilter, err := converter.BuildCountStmt()
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	if len(selectFilter.GroupBy) > 0 {
		return s.countGroupByWorkflowExecutions(ctx, selectFilter, saTypeMap)
	}

	count, err := s.sqlStore.Db.CountFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountWorkflowExecutions operation failed. Query failed: %v", err))
	}

	return &manager.CountWorkflowExecutionsResponse{Count: count}, nil
}

func (s *VisibilityStore) countGroupByWorkflowExecutions(
	ctx context.Context,
	selectFilter *sqlplugin.VisibilitySelectFilter,
	saTypeMap searchattribute.NameTypeMap,
) (*manager.CountWorkflowExecutionsResponse, error) {
	var err error
	groupByTypes := make([]enumspb.IndexedValueType, len(selectFilter.GroupBy))
	for i, fieldName := range selectFilter.GroupBy {
		groupByTypes[i], err = saTypeMap.GetType(fieldName)
		if err != nil {
			return nil, err
		}
	}

	rows, err := s.sqlStore.Db.CountGroupByFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountWorkflowExecutions operation failed. Query failed: %v", err))
	}
	resp := &manager.CountWorkflowExecutionsResponse{
		Count:  0,
		Groups: make([]*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup, 0, len(rows)),
	}
	for _, row := range rows {
		groupValues := make([]*common.Payload, len(row.GroupValues))
		for i, val := range row.GroupValues {
			groupValues[i], err = searchattribute.EncodeValue(val, groupByTypes[i])
			if err != nil {
				return nil, err
			}
		}
		resp.Groups = append(
			resp.Groups,
			&workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
				GroupValues: groupValues,
				Count:       row.Count,
			},
		)
		resp.Count += row.Count
	}
	return resp, nil
}

func (s *VisibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	row, err := s.sqlStore.Db.GetFromVisibility(ctx, sqlplugin.VisibilityGetFilter{
		NamespaceID: request.NamespaceID.String(),
		RunID:       request.RunID,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("GetWorkflowExecution operation failed. Select failed: %v", err))
	}
	info, err := s.rowToInfo(row, request.Namespace)
	if err != nil {
		return nil, err
	}
	return &store.InternalGetWorkflowExecutionResponse{
		Execution: info,
	}, nil
}

func (s *VisibilityStore) generateVisibilityRow(
	request *store.InternalVisibilityRequestBase,
) (*sqlplugin.VisibilityRow, error) {
	searchAttributes, err := s.prepareSearchAttributesForDb(request)
	if err != nil {
		return nil, err
	}

	return &sqlplugin.VisibilityRow{
		NamespaceID:      request.NamespaceID,
		WorkflowID:       request.WorkflowID,
		RunID:            request.RunID,
		StartTime:        request.StartTime,
		ExecutionTime:    request.ExecutionTime,
		WorkflowTypeName: request.WorkflowTypeName,
		Status:           int32(request.Status),
		Memo:             request.Memo.Data,
		Encoding:         request.Memo.EncodingType.String(),
		TaskQueue:        request.TaskQueue,
		SearchAttributes: searchAttributes,
		ParentWorkflowID: request.ParentWorkflowID,
		ParentRunID:      request.ParentRunID,
		RootWorkflowID:   request.RootWorkflowID,
		RootRunID:        request.RootRunID,
	}, nil
}

func (s *VisibilityStore) prepareSearchAttributesForDb(
	request *store.InternalVisibilityRequestBase,
) (*sqlplugin.VisibilitySearchAttributes, error) {
	if request.SearchAttributes == nil {
		return nil, nil
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(
		s.GetIndexName(),
		false,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attributes types: %v", err))
	}

	var searchAttributes sqlplugin.VisibilitySearchAttributes
	searchAttributes, err = searchattribute.Decode(request.SearchAttributes, &saTypeMap, false)
	if err != nil {
		return nil, err
	}
	// This is to prevent existing tasks to fail indefinitely.
	// If it's only invalid values error, then silently continue without them.
	searchAttributes, err = s.ValidateCustomSearchAttributes(searchAttributes)
	if err != nil {
		if _, ok := err.(*store.VisibilityStoreInvalidValuesError); !ok {
			return nil, err
		}
	}

	for name, value := range searchAttributes {
		if value == nil {
			delete(searchAttributes, name)
			continue
		}
		tp, err := saTypeMap.GetType(name)
		if err != nil {
			return nil, err
		}
		if tp == enumspb.INDEXED_VALUE_TYPE_DATETIME {
			if dt, ok := value.(time.Time); ok {
				searchAttributes[name] = dt.Format(time.RFC3339Nano)
			}
		}
	}
	return &searchAttributes, nil
}

func (s *VisibilityStore) rowToInfo(
	row *sqlplugin.VisibilityRow,
	nsName namespace.Name,
) (*store.InternalWorkflowExecutionInfo, error) {
	if row.ExecutionTime.UnixNano() == 0 {
		row.ExecutionTime = row.StartTime
	}
	info := &store.InternalWorkflowExecutionInfo{
		WorkflowID:     row.WorkflowID,
		RunID:          row.RunID,
		TypeName:       row.WorkflowTypeName,
		StartTime:      row.StartTime,
		ExecutionTime:  row.ExecutionTime,
		Status:         enumspb.WorkflowExecutionStatus(row.Status),
		TaskQueue:      row.TaskQueue,
		RootWorkflowID: row.RootWorkflowID,
		RootRunID:      row.RootRunID,
		Memo:           persistence.NewDataBlob(row.Memo, row.Encoding),
	}
	if row.SearchAttributes != nil && len(*row.SearchAttributes) > 0 {
		searchAttributes, err := s.processRowSearchAttributes(*row.SearchAttributes, nsName)
		if err != nil {
			return nil, err
		}
		info.SearchAttributes = searchAttributes
	}
	if row.CloseTime != nil {
		info.CloseTime = *row.CloseTime
	}
	if row.ExecutionDuration != nil {
		info.ExecutionDuration = *row.ExecutionDuration
	}
	if row.HistoryLength != nil {
		info.HistoryLength = *row.HistoryLength
	}
	if row.HistorySizeBytes != nil {
		info.HistorySizeBytes = *row.HistorySizeBytes
	}
	if row.StateTransitionCount != nil {
		info.StateTransitionCount = *row.StateTransitionCount
	}
	if row.ParentWorkflowID != nil {
		info.ParentWorkflowID = *row.ParentWorkflowID
	}
	if row.ParentRunID != nil {
		info.ParentRunID = *row.ParentRunID
	}
	return info, nil
}

func (s *VisibilityStore) processRowSearchAttributes(
	rowSearchAttributes sqlplugin.VisibilitySearchAttributes,
	nsName namespace.Name,
) (*common.SearchAttributes, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(
		s.GetIndexName(),
		false,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attributes types: %v", err))
	}
	// In SQLite, keyword list can return a string when there's only one element.
	// This changes it into a slice.
	for name, value := range rowSearchAttributes {
		tp, err := saTypeMap.GetType(name)
		if err != nil {
			return nil, err
		}
		if tp == enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
			switch v := value.(type) {
			case []string:
				// no-op
			case string:
				(rowSearchAttributes)[name] = []string{v}
			default:
				return nil, serviceerror.NewInternal(
					fmt.Sprintf("Unexpected data type for keyword list: %T (expected list of strings)", v),
				)
			}
		}
	}
	searchAttributes, err := searchattribute.Encode(rowSearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}
	aliasedSas, err := searchattribute.AliasFields(
		s.searchAttributesMapperProvider,
		searchAttributes,
		nsName.String(),
	)
	if err != nil {
		return nil, err
	}
	return aliasedSas, nil
}
