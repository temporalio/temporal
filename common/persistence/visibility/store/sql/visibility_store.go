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
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
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
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	VisibilityStore struct {
		sqlStore                       persistencesql.SqlStore
		searchAttributesProvider       searchattribute.Provider
		searchAttributesMapperProvider searchattribute.MapperProvider
		chasmRegistry                  *chasm.Registry
		metricsHandler                 metrics.Handler
		logger                         log.Logger
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

	countExecutionsInternalRequest struct {
		NamespaceID namespace.ID
		Namespace   namespace.Name
		Query       string
		ArchetypeID chasm.ArchetypeID
		ChasmMapper *chasm.VisibilitySearchAttributesMapper
	}

	queryConverterWrapper struct {
		*query.QueryConverter[sqlparser.Expr]
		sqlQueryConverter *SQLQueryConverter

		saTypeMap searchattribute.NameTypeMap
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
	}, nil
}

func (s *VisibilityStore) Close() {
	s.sqlStore.Close()
}

func (s *VisibilityStore) GetName() string {
	return s.sqlStore.GetName()
}

func convertSQLError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s operation failed: %w", operation, err)
	}
	return serviceerror.NewUnavailablef("%s operation failed: %v", operation, err)
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
	row.ExecutionDuration = new(request.ExecutionDuration.Nanoseconds())
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
		return convertSQLError(metrics.VisibilityPersistenceDeleteWorkflowExecutionScope, err)
	}
	return nil
}

func (s *VisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	queryConverter, err := s.newQueryConverter(
		request.Namespace,
		nil, // chasmMapper
		chasm.UnspecifiedArchetypeID,
	)
	if err != nil {
		return nil, err
	}

	return s.listExecutionsInternal(
		ctx,
		&listExecutionsRequestInternal{
			NamespaceID:   request.NamespaceID,
			Namespace:     request.Namespace,
			Query:         request.Query,
			PageSize:      request.PageSize,
			NextPageToken: request.NextPageToken,
			ArchetypeID:   chasm.UnspecifiedArchetypeID,
			ChasmMapper:   nil,
		},
		queryConverter,
		metrics.VisibilityPersistenceListWorkflowExecutionsScope,
	)
}

func (s *VisibilityStore) ListChasmExecutions(
	ctx context.Context,
	request *visibilityservice.ListChasmExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeId)
	if !ok {
		return nil, serviceerror.NewInvalidArgumentf("unknown archetype ID: %d", request.ArchetypeId)
	}
	chasmMapper := rc.SearchAttributesMapper()

	queryConverter, err := s.newQueryConverter(
		namespace.Name(request.Namespace),
		chasmMapper,
		request.ArchetypeId,
	)
	if err != nil {
		return nil, err
	}

	return s.listExecutionsInternal(
		ctx,
		&listExecutionsRequestInternal{
			NamespaceID:   namespace.ID(request.NamespaceId),
			Namespace:     namespace.Name(request.Namespace),
			Query:         request.Query,
			PageSize:      int(request.PageSize),
			NextPageToken: request.NextPageToken,
			ArchetypeID:   request.ArchetypeId,
			ChasmMapper:   chasmMapper,
		},
		queryConverter,
		metrics.VisibilityPersistenceListChasmExecutionsScope,
	)
}

func (s *VisibilityStore) listExecutionsInternal(
	ctx context.Context,
	request *listExecutionsRequestInternal,
	queryConverter *queryConverterWrapper,
	operation string,
) (*store.InternalListExecutionsResponse, error) {
	queryParams, err := buildQueryParams(
		request.NamespaceID,
		queryConverter,
		request.Query,
	)
	if err != nil {
		return nil, err
	}

	pageToken, err := sqlplugin.DeserializeVisibilityPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	sqlQueryString, queryArgs := queryConverter.sqlQueryConverter.BuildSelectStmt(
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
		return nil, convertSQLError(operation, err)
	}
	if len(rows) == 0 {
		return &store.InternalListExecutionsResponse{}, nil
	}

	combinedSATypeMap := store.CombineTypeMaps(queryConverter.saTypeMap, request.ChasmMapper)
	var infos = make([]*store.InternalExecutionInfo, len(rows))
	for i, row := range rows {
		infos[i], err = rowToInfo(&row, combinedSATypeMap)
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

func (s *VisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	queryConverter, err := s.newQueryConverter(
		request.Namespace,
		nil, // chasmMapper
		chasm.UnspecifiedArchetypeID,
	)
	if err != nil {
		return nil, err
	}

	return s.countExecutionsInternal(
		ctx,
		&countExecutionsInternalRequest{
			NamespaceID: request.NamespaceID,
			Namespace:   request.Namespace,
			Query:       request.Query,
		},
		queryConverter,
		metrics.VisibilityPersistenceCountWorkflowExecutionsScope,
	)
}

func (s *VisibilityStore) CountChasmExecutions(
	ctx context.Context,
	request *visibilityservice.CountChasmExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeId)
	if !ok {
		return nil, serviceerror.NewInvalidArgumentf("unknown archetype ID: %d", request.ArchetypeId)
	}
	chasmMapper := rc.SearchAttributesMapper()

	queryConverter, err := s.newQueryConverter(
		namespace.Name(request.Namespace),
		chasmMapper,
		request.ArchetypeId,
	)
	if err != nil {
		return nil, err
	}

	return s.countExecutionsInternal(
		ctx,
		&countExecutionsInternalRequest{
			NamespaceID: namespace.ID(request.NamespaceId),
			Namespace:   namespace.Name(request.Namespace),
			Query:       request.Query,
			ArchetypeID: request.ArchetypeId,
			ChasmMapper: chasmMapper,
		},
		queryConverter,
		metrics.VisibilityPersistenceCountChasmExecutionsScope,
	)
}

func (s *VisibilityStore) countExecutionsInternal(
	ctx context.Context,
	request *countExecutionsInternalRequest,
	queryConverter *queryConverterWrapper,
	operation string,
) (*store.InternalCountExecutionsResponse, error) {
	queryParams, err := buildQueryParams(
		request.NamespaceID,
		queryConverter,
		request.Query,
	)
	if err != nil {
		return nil, err
	}

	queryString, queryArgs := queryConverter.sqlQueryConverter.BuildCountStmt(queryParams)
	groupBy := make([]string, 0, len(queryParams.GroupBy)+1)
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, field.FieldName)
	}

	selectFilter := &sqlplugin.VisibilitySelectFilter{
		Query:     queryString,
		QueryArgs: queryArgs,
		GroupBy:   groupBy,
	}

	if len(selectFilter.GroupBy) > 0 {
		combinedSATypeMap := store.CombineTypeMaps(queryConverter.saTypeMap, request.ChasmMapper)
		return s.countGroupByExecutions(ctx, selectFilter, combinedSATypeMap, operation)
	}

	count, err := s.sqlStore.DB.CountFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError(operation, err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

// getGroupByFieldTypes resolves the search attribute types for the given field names.
func (s *VisibilityStore) getGroupByFieldTypes(
	fieldNames []string,
	saTypeMap searchattribute.NameTypeMap,
) ([]enumspb.IndexedValueType, error) {
	groupByTypes := make([]enumspb.IndexedValueType, len(fieldNames))
	for i, fieldName := range fieldNames {
		tp, err := saTypeMap.GetType(fieldName)
		if err != nil {
			return nil, err
		}
		groupByTypes[i] = tp
	}

	return groupByTypes, nil
}

func (s *VisibilityStore) countGroupByExecutions(
	ctx context.Context,
	selectFilter *sqlplugin.VisibilitySelectFilter,
	saTypeMap searchattribute.NameTypeMap,
	operation string,
) (*store.InternalCountExecutionsResponse, error) {
	rows, err := s.sqlStore.DB.CountGroupByFromVisibility(ctx, *selectFilter)
	if err != nil {
		return nil, convertSQLError(operation, err)
	}

	groupByTypes, err := s.getGroupByFieldTypes(selectFilter.GroupBy, saTypeMap)
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
			groupValues[i], err = sadefs.EncodeValue(val, groupByTypes[i])
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
		return nil, convertSQLError(metrics.VisibilityPersistenceGetWorkflowExecutionScope, err)
	}

	saTypeMap, err := s.getSearchAttributesTypeMap()
	if err != nil {
		return nil, err
	}

	info, err := rowToInfo(row, saTypeMap)
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

	saTypeMap, err := s.getSearchAttributesTypeMap()
	if err != nil {
		return nil, err
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

func (s *VisibilityStore) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	// SQL Visibility does not support modifying schema to add search attributes at this moment.
	return serviceerror.NewUnimplemented("AddSearchAttributes operation not supported in SQL visibility")
}

func (s *VisibilityStore) getSearchAttributesTypeMap() (searchattribute.NameTypeMap, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		err = serviceerror.NewUnavailablef("Unable to read search attributes types: %v", err)
	}
	return saTypeMap, err
}

func (s *VisibilityStore) newQueryConverter(
	namespaceName namespace.Name,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (*queryConverterWrapper, error) {
	sqlQC, err := NewSQLQueryConverter(s.GetName())
	if err != nil {
		return nil, err
	}

	saTypeMap, err := s.getSearchAttributesTypeMap()
	if err != nil {
		return nil, err
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(namespaceName)
	if err != nil {
		return nil, err
	}

	queryConverter := query.NewQueryConverter(
		sqlQC,
		namespaceName,
		saTypeMap,
		saMapper,
		s.metricsHandler,
		s.logger,
	).WithChasmMapper(chasmMapper).
		WithArchetypeID(archetypeID)

	return &queryConverterWrapper{
		QueryConverter:    queryConverter,
		sqlQueryConverter: sqlQC,
		saTypeMap:         saTypeMap,
	}, nil
}

func buildQueryParams(
	namespaceID namespace.ID,
	queryConverter *queryConverterWrapper,
	queryString string,
) (_ *query.QueryParams[sqlparser.Expr], retError error) {
	defer func() {
		if retError != nil {
			// Convert ConverterError to InvalidArgument and pass through any other error
			// (which should be only mapper errors).
			if converterErr, ok := errors.AsType[*query.ConverterError](retError); ok {
				retError = converterErr.ToInvalidArgument()
			}
		}
	}()

	queryParams, err := queryConverter.Convert(queryString)
	if err != nil {
		return nil, err
	}

	sqlQC := queryConverter.sqlQueryConverter
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

func rowToInfo(
	row *sqlplugin.VisibilityRow,
	saTypeMap searchattribute.NameTypeMap,
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
		encodedSAs, err := encodeRowSearchAttributes(*row.SearchAttributes, saTypeMap)
		if err != nil {
			return nil, err
		}
		info.SearchAttributes = encodedSAs
	}
	if row.CloseTime != nil {
		info.CloseTime = *row.CloseTime
	}
	if row.ExecutionDuration != nil {
		info.ExecutionDuration = time.Duration(*row.ExecutionDuration)
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

func encodeRowSearchAttributes(
	rowSearchAttributes sqlplugin.VisibilitySearchAttributes,
	saTypeMap searchattribute.NameTypeMap,
) (*commonpb.SearchAttributes, error) {
	registeredSearchAttributes := sqlplugin.VisibilitySearchAttributes{}

	// Fix SQLite keyword list handling (convert string to []string for keyword lists)
	for name, value := range rowSearchAttributes {
		tp, err := saTypeMap.GetType(name)
		if err != nil {
			if errors.Is(err, sadefs.ErrInvalidName) {
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
				return nil, serviceerror.NewInternalf(
					"Unexpected data type for keyword list: %T (expected list of strings)", v)
			}
		}
	}

	// Encode all search attributes together
	encodedSAs, err := searchattribute.Encode(registeredSearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return encodedSAs, nil
}
