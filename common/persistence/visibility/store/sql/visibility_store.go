package sql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
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
		chasmRegistry                  *chasm.Registry
		metricsHandler                 metrics.Handler
		logger                         log.Logger

		enableUnifiedQueryConverter dynamicconfig.BoolPropertyFn
	}

	listExecutionsRequestInternal struct {
		NamespaceID   namespace.ID
		Namespace     namespace.Name
		Query         string
		PageSize      int
		NextPageToken []byte
		ArchetypeID   chasm.ArchetypeID
		ChasmMapper   *chasm.VisibilitySearchAttributesMapper
	}
)

var _ store.VisibilityStore = (*VisibilityStore)(nil)

var maxDatetime, _ = time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")

// NewSQLVisibilityStore creates an instance of VisibilityStore
func NewSQLVisibilityStore(
	cfg config.SQL,
	r resolver.ServiceResolver,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	chasmRegistry *chasm.Registry,
	enableUnifiedQueryConverter dynamicconfig.BoolPropertyFn,
	logger log.Logger,
	metricsHandler metrics.Handler,
	serializer serialization.Serializer,
) (*VisibilityStore, error) {
	refDbConn := persistencesql.NewRefCountedDBConn(sqlplugin.DbKindVisibility, &cfg, r, logger, metricsHandler)
	db, err := refDbConn.Get()
	if err != nil {
		return nil, err
	}
	return &VisibilityStore{
		sqlStore:                       persistencesql.NewSQLStore(db, logger, serializer),
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
		chasmRegistry:                  chasmRegistry,
		metricsHandler:                 metricsHandler,
		logger:                         logger,

		enableUnifiedQueryConverter: enableUnifiedQueryConverter,
	}, nil
}

func (s *VisibilityStore) Close() {
	s.sqlStore.Close()
}

func (s *VisibilityStore) GetName() string {
	return s.sqlStore.GetName()
}

func convertSQLError(message string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", message, err)
	}
	return serviceerror.NewUnavailable(fmt.Sprintf("%s: %v", message, err))
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

	_, err = s.sqlStore.DB.InsertIntoVisibility(ctx, row)
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

	result, err := s.sqlStore.DB.ReplaceIntoVisibility(ctx, row)
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

	result, err := s.sqlStore.DB.ReplaceIntoVisibility(ctx, row)
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
	_, err := s.sqlStore.DB.DeleteFromVisibility(ctx, sqlplugin.VisibilityDeleteFilter{
		NamespaceID: request.NamespaceID.String(),
		RunID:       request.RunID,
	})
	if err != nil {
		return convertSQLError("DeleteWorkflowExecution operation failed.", err)
	}
	return nil
}

func (s *VisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	if s.enableUnifiedQueryConverter() {
		return s.listWorkflowExecutions(ctx, request)
	}
	return s.listWorkflowExecutionsLegacy(ctx, request)
}

func (s *VisibilityStore) ListChasmExecutions(
	ctx context.Context,
	request *manager.ListChasmExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown archetype ID: %d", request.ArchetypeID))
	}
	mapper := rc.SearchAttributesMapper()

	requestInternal := &listExecutionsRequestInternal{
		NamespaceID:   request.NamespaceID,
		Namespace:     request.Namespace,
		Query:         request.Query,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
		ChasmMapper:   mapper,
		ArchetypeID:   request.ArchetypeID,
	}

	if s.enableUnifiedQueryConverter() {
		return s.listExecutionsInternal(ctx, requestInternal)
	}

	return s.listExecutionsInternalLegacy(ctx, requestInternal)
}

func (s *VisibilityStore) CountChasmExecutions(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown archetype ID: %d", request.ArchetypeID))
	}
	mapper := rc.SearchAttributesMapper()

	if s.enableUnifiedQueryConverter() {
		return s.countChasmExecutions(ctx, request, mapper)
	}
	return s.countChasmExecutionsLegacy(ctx, request, mapper)
}

func (s *VisibilityStore) countChasmExecutions(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
	mapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalCountExecutionsResponse, error) {
	sqlQC, err := NewSQLQueryConverter(s.GetName())
	if err != nil {
		return nil, err
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	queryParams, err := buildQueryParams(
		request.NamespaceID,
		request.Namespace,
		request.Query,
		sqlQC,
		saTypeMap,
		saMapper,
		mapper,
		request.ArchetypeID,
	)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	selectFilter := s.buildSelectFilterFromQueryParams(queryParams, sqlQC)

	if len(selectFilter.GroupBy) > 0 {
		return s.countGroupByExecutions(ctx, selectFilter, mapper)
	}

	count, err := s.sqlStore.DB.CountFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountChasmExecutions operation failed. Query failed: %v", err))
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *VisibilityStore) countChasmExecutionsLegacy(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
	mapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalCountExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	converter := NewQueryConverterLegacy(
		s.GetName(),
		request.Namespace,
		request.NamespaceID,
		saTypeMap,
		saMapper,
		request.Query,
		mapper,
		request.ArchetypeID,
	)
	selectFilter, err := converter.BuildCountStmt()
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	if len(selectFilter.GroupBy) > 0 {
		return s.countGroupByExecutions(ctx, selectFilter, mapper)
	}

	count, err := s.sqlStore.DB.CountFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("CountChasmExecutions operation failed. Query failed: %v", err))
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *VisibilityStore) listWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	return s.listExecutionsInternal(ctx, &listExecutionsRequestInternal{
		NamespaceID:   request.NamespaceID,
		Namespace:     request.Namespace,
		Query:         request.Query,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
	})
}

func (s *VisibilityStore) listExecutionsInternal(
	ctx context.Context,
	request *listExecutionsRequestInternal,
) (*store.InternalListExecutionsResponse, error) {
	sqlQC, err := NewSQLQueryConverter(s.GetName())
	if err != nil {
		return nil, err
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	queryParams, err := buildQueryParams(
		request.NamespaceID,
		request.Namespace,
		request.Query,
		sqlQC,
		saTypeMap,
		saMapper,
		request.ChasmMapper,
		request.ArchetypeID,
	)
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be
		// only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	pageToken, err := sqlplugin.DeserializeVisibilityPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	sqlQueryString, queryArgs := sqlQC.BuildSelectStmt(
		queryParams,
		request.PageSize,
		pageToken,
	)
	selectFilter := &sqlplugin.VisibilitySelectFilter{
		Query:     sqlQueryString,
		QueryArgs: queryArgs,
	}

	rows, err := s.sqlStore.DB.SelectFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError("ListWorkflowExecutions operation failed.", err)
	}
	if len(rows) == 0 {
		return &store.InternalListExecutionsResponse{}, nil
	}

	var infos = make([]*store.InternalExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i], err = s.rowToInfo(&row, request.ChasmMapper)
		if err != nil {
			return nil, err
		}
	}

	var nextPageTokenResult []byte
	if len(rows) > 0 && len(rows) == request.PageSize {
		lastRow := rows[len(rows)-1]
		closeTime := maxDatetime
		if lastRow.CloseTime != nil {
			closeTime = *lastRow.CloseTime
		}
		nextPageTokenResult, err = sqlplugin.SerializeVisibilityPageToken(&sqlplugin.VisibilityPageToken{
			CloseTime: closeTime,
			StartTime: lastRow.StartTime,
			RunID:     lastRow.RunID,
		})
		if err != nil {
			return nil, err
		}
	}
	return &store.InternalListExecutionsResponse{
		Executions:    infos,
		NextPageToken: nextPageTokenResult,
	}, nil
}

func (s *VisibilityStore) listWorkflowExecutionsLegacy(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	return s.listExecutionsInternalLegacy(ctx, &listExecutionsRequestInternal{
		NamespaceID:   request.NamespaceID,
		Namespace:     request.Namespace,
		Query:         request.Query,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
		ChasmMapper:   nil,
		ArchetypeID:   chasm.UnspecifiedArchetypeID,
	})
}

func (s *VisibilityStore) listExecutionsInternalLegacy(
	ctx context.Context,
	request *listExecutionsRequestInternal,
) (*store.InternalListExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	converter := NewQueryConverterLegacy(
		s.GetName(),
		request.Namespace,
		request.NamespaceID,
		saTypeMap,
		saMapper,
		request.Query,
		request.ChasmMapper,
		request.ArchetypeID,
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

	rows, err := s.sqlStore.DB.SelectFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError("ListWorkflowExecutions operation failed.", err)
	}
	if len(rows) == 0 {
		return &store.InternalListExecutionsResponse{}, nil
	}

	var infos = make([]*store.InternalExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i], err = s.rowToInfo(&row, request.ChasmMapper)
		if err != nil {
			return nil, err
		}
	}

	var nextPageToken []byte
	if len(rows) == request.PageSize {
		lastRow := rows[len(rows)-1]
		closeTime := maxDatetime
		if lastRow.CloseTime != nil {
			closeTime = *lastRow.CloseTime
		}
		nextPageToken, err = serializePageTokenLegacy(&pageTokenLegacy{
			CloseTime: closeTime,
			StartTime: lastRow.StartTime,
			RunID:     lastRow.RunID,
		})
		if err != nil {
			return nil, err
		}
	}
	return &store.InternalListExecutionsResponse{
		Executions:    infos,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *VisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	if s.enableUnifiedQueryConverter() {
		return s.countWorkflowExecutions(ctx, request)
	}
	return s.countWorkflowExecutionsLegacy(ctx, request)
}

func (s *VisibilityStore) countWorkflowExecutionsLegacy(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	converter := NewQueryConverterLegacy(
		s.GetName(),
		request.Namespace,
		request.NamespaceID,
		saTypeMap,
		saMapper,
		request.Query,
		nil,
		chasm.UnspecifiedArchetypeID,
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
		return s.countGroupByExecutions(ctx, selectFilter, nil)
	}

	count, err := s.sqlStore.DB.CountFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError("CountWorkflowExecutions operation failed.", err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *VisibilityStore) countWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	sqlQC, err := NewSQLQueryConverter(s.GetName())
	if err != nil {
		return nil, err
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(request.Namespace)
	if err != nil {
		return nil, err
	}

	queryParams, err := buildQueryParams(
		request.NamespaceID,
		request.Namespace,
		request.Query,
		sqlQC,
		saTypeMap,
		saMapper,
		nil,
		chasm.UnspecifiedArchetypeID,
	)
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be
		// only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	selectFilter := s.buildSelectFilterFromQueryParams(queryParams, sqlQC)

	if len(selectFilter.GroupBy) > 0 {
		return s.countGroupByExecutions(ctx, selectFilter, nil)
	}

	count, err := s.sqlStore.DB.CountFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError("CountWorkflowExecutions operation failed.", err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

// getGroupByFieldTypes resolves the search attribute types for the given field names.
// It handles alias resolution and merges chasm types if a chasmMapper is provided.
func (s *VisibilityStore) getGroupByFieldTypes(
	fieldNames []string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) ([]enumspb.IndexedValueType, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"unable to read search attribute types: %v", err,
		)
	}

	combinedTypeMap := store.CombineTypeMaps(saTypeMap, chasmMapper)

	groupByTypes := make([]enumspb.IndexedValueType, len(fieldNames))
	for i, fieldName := range fieldNames {
		tp, err := combinedTypeMap.GetType(fieldName)
		if err != nil {
			return nil, err
		}
		groupByTypes[i] = tp
	}

	return groupByTypes, nil
}

func (s *VisibilityStore) buildSelectFilterFromQueryParams(
	queryParams *query.QueryParams[sqlparser.Expr],
	sqlQC *SQLQueryConverter,
) *sqlplugin.VisibilitySelectFilter {
	queryString, queryArgs := sqlQC.BuildCountStmt(queryParams)
	groupBy := make([]string, 0, len(queryParams.GroupBy)+1)
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, field.FieldName)
	}

	return &sqlplugin.VisibilitySelectFilter{
		Query:     queryString,
		QueryArgs: queryArgs,
		GroupBy:   groupBy,
	}
}

func (s *VisibilityStore) countGroupByExecutions(
	ctx context.Context,
	selectFilter *sqlplugin.VisibilitySelectFilter,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalCountExecutionsResponse, error) {
	rows, err := s.sqlStore.DB.CountGroupByFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError("CountExecutions operation failed.", err)
	}

	groupByTypes, err := s.getGroupByFieldTypes(selectFilter.GroupBy, chasmMapper)
	if err != nil {
		return nil, err
	}

	resp := &store.InternalCountExecutionsResponse{
		Count:  0,
		Groups: make([]store.InternalAggregationGroup, 0, len(rows)),
	}
	for _, row := range rows {
		groupValues := make([]*commonpb.Payload, len(row.GroupValues))
		for i, val := range row.GroupValues {
			groupValues[i], err = searchattribute.EncodeValue(val, groupByTypes[i])
			if err != nil {
				return nil, err
			}
		}
		resp.Groups = append(
			resp.Groups,
			store.InternalAggregationGroup{
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
	row, err := s.sqlStore.DB.GetFromVisibility(ctx, sqlplugin.VisibilityGetFilter{
		NamespaceID: request.NamespaceID.String(),
		RunID:       request.RunID,
	})
	if err != nil {
		return nil, convertSQLError("GetWorkflowExecution operation failed.", err)
	}
	info, err := s.rowToInfo(row, nil)
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
		Version:          request.TaskID,
	}, nil
}

func (s *VisibilityStore) prepareSearchAttributesForDb(
	request *store.InternalVisibilityRequestBase,
) (*sqlplugin.VisibilitySearchAttributes, error) {
	if request.SearchAttributes == nil {
		return nil, nil
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attributes types: %v", err))
	}

	var searchAttributes sqlplugin.VisibilitySearchAttributes
	searchAttributes, err = searchattribute.Decode(request.SearchAttributes, &saTypeMap, false)
	if err != nil {
		return nil, err
	}
	if len(request.SearchAttributes.GetIndexedFields()) != len(searchAttributes) {
		for name := range request.SearchAttributes.GetIndexedFields() {
			if _, ok := searchAttributes[name]; !ok {
				s.logger.Warn("Skipping unknown search attribute while generating visibility record", tag.String("search-attribute", name))
			}
		}
	}
	// This is to prevent existing tasks to fail indefinitely.
	// If it's only invalid values error, then silently continue without them.
	searchAttributes, err = s.ValidateCustomSearchAttributes(searchAttributes)
	if err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); !ok {
			return nil, err
		}
	}

	for name, value := range searchAttributes {
		if value == nil {
			delete(searchAttributes, name)
			continue
		}
	}
	return &searchAttributes, nil
}

func (s *VisibilityStore) rowToInfo(
	row *sqlplugin.VisibilityRow,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalExecutionInfo, error) {
	if row.ExecutionTime.UnixNano() == 0 {
		row.ExecutionTime = row.StartTime
	}
	info := &store.InternalExecutionInfo{
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
		// Encode all search attributes together (both CHASM and custom)
		encodedSAs, err := s.encodeRowSearchAttributes(*row.SearchAttributes, chasmMapper)
		if err != nil {
			return nil, err
		}
		info.SearchAttributes = encodedSAs
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

func (s *VisibilityStore) encodeRowSearchAttributes(
	rowSearchAttributes sqlplugin.VisibilitySearchAttributes,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*commonpb.SearchAttributes, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attributes types: %v", err))
	}

	combinedTypeMap := store.CombineTypeMaps(saTypeMap, chasmMapper)
	registeredSearchAttributes := sqlplugin.VisibilitySearchAttributes{}

	// Fix SQLite keyword list handling (convert string to []string for keyword lists)
	for name, value := range rowSearchAttributes {
		tp, err := combinedTypeMap.GetType(name)
		if err != nil {
			if errors.Is(err, searchattribute.ErrInvalidName) {
				continue
			}
			return nil, err
		}
		registeredSearchAttributes[name] = value
		if tp == enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
			switch v := value.(type) {
			case []string:
				// no-op
			case string:
				registeredSearchAttributes[name] = []string{v}
			default:
				return nil, serviceerror.NewInternal(
					fmt.Sprintf("Unexpected data type for keyword list: %T (expected list of strings)", v),
				)
			}
		}
	}

	// Encode all search attributes together
	encodedSAs, err := searchattribute.Encode(registeredSearchAttributes, &combinedTypeMap)
	if err != nil {
		return nil, err
	}

	return encodedSAs, nil
}

func (s *VisibilityStore) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	// SQL Visibility does not support modifying schema to add search attributes at this moment.
	return serviceerror.NewUnimplemented("AddSearchAttributes operation not supported in SQL visibility")
}

func buildQueryParams(
	namespaceID namespace.ID,
	namespaceName namespace.Name,
	queryString string,
	sqlQC *SQLQueryConverter,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (*query.QueryParams[sqlparser.Expr], error) {
	c := query.NewQueryConverter(sqlQC, namespaceName, saTypeMap, saMapper).
		WithChasmMapper(chasmMapper).
		WithArchetypeID(archetypeID)

	queryParams, err := c.Convert(queryString)
	if err != nil {
		return nil, err
	}

	nsFilterExpr, err := sqlQC.ConvertComparisonExpr(
		sqlparser.EqualStr,
		query.NamespaceIDSAColumn,
		namespaceID.String(),
	)
	if err != nil {
		return nil, err
	}

	queryParams.QueryExpr, err = sqlQC.BuildAndExpr(nsFilterExpr, queryParams.QueryExpr)
	if err != nil {
		return nil, err
	}

	// ORDER BY is not support in SQL visibility store
	if len(queryParams.OrderBy) > 0 {
		return nil, query.NewConverterError("%s: 'ORDER BY' clause", query.NotSupportedErrMessage)
	}

	return queryParams, nil
}
