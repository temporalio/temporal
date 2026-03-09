package elasticsearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/util"
)

const (
	PersistenceName = "elasticsearch"

	delimiter = "~"
)

type (
	VisibilityStore struct {
		esClient                       client.Client
		index                          string
		searchAttributesProvider       searchattribute.Provider
		searchAttributesMapperProvider searchattribute.MapperProvider
		chasmRegistry                  *chasm.Registry
		processor                      Processor
		processorAckTimeout            dynamicconfig.DurationPropertyFn
		disableOrderByClause           dynamicconfig.BoolPropertyFnWithNamespaceFilter
		enableManualPagination         dynamicconfig.BoolPropertyFnWithNamespaceFilter
		enableUnifiedQueryConverter    dynamicconfig.BoolPropertyFn
		metricsHandler                 metrics.Handler
		logger                         log.Logger
	}

	visibilityPageToken struct {
		SearchAfter []any
	}

	esQueryParams struct {
		Query   elastic.Query
		Sorter  []elastic.Sorter
		GroupBy []string
	}

	fieldSort struct {
		name          string
		desc          bool
		missing_first bool
	}

	searchParametersInternal struct {
		NamespaceName namespace.Name
		NamespaceID   namespace.ID
		Query         string
		PageSize      int
		NextPageToken []byte
		ChasmMapper   *chasm.VisibilitySearchAttributesMapper
		ArchetypeID   chasm.ArchetypeID
	}
)

var _ store.VisibilityStore = (*VisibilityStore)(nil)

var (
	errUnexpectedJSONFieldType = errors.New("unexpected JSON field type")

	minTime         = time.Unix(0, 0).UTC()
	maxTime         = time.Unix(0, math.MaxInt64).UTC()
	maxStringLength = 32766

	// Default sorter uses the sorting order defined in the index template.
	// It is indirectly built so buildPaginationQuery can have access to
	// the field names to build the page query from the token.
	defaultSorterFields = []fieldSort{
		{sadefs.CloseTime, true, true},
		{sadefs.StartTime, true, true},
	}

	defaultSorter = func() []elastic.Sorter {
		ret := make([]elastic.Sorter, 0, len(defaultSorterFields))
		for _, item := range defaultSorterFields {
			fs := elastic.NewFieldSort(item.name)
			if item.desc {
				fs.Desc()
			}
			if item.missing_first {
				fs.Missing("_first")
			} else {
				fs.Missing("_last")
			}
			ret = append(ret, fs)
		}
		return ret
	}()

	docSorter = []elastic.Sorter{
		elastic.SortByDoc{},
	}
)

// NewVisibilityStore create a visibility store connecting to ElasticSearch
func NewVisibilityStore(
	cfg *client.Config,
	processorConfig *ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	chasmRegistry *chasm.Registry,
	disableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	enableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	enableUnifiedQueryConverter dynamicconfig.BoolPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (*VisibilityStore, error) {
	esHttpClient := cfg.GetHttpClient()
	if esHttpClient == nil {
		var err error
		esHttpClient, err = client.NewAwsHttpClient(cfg.AWSRequestSigning)
		if err != nil {
			return nil, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
		}
	}
	esClient, err := client.NewClient(cfg, esHttpClient, logger)
	if err != nil {
		return nil, fmt.Errorf("unable to create Elasticsearch client (URL = %v, username = %q): %w",
			cfg.URL.Redacted(), cfg.Username, err)
	}
	var (
		processor           Processor
		processorAckTimeout dynamicconfig.DurationPropertyFn
	)
	if processorConfig != nil {
		processor = NewProcessor(processorConfig, esClient, logger, metricsHandler)
		processor.Start()
		processorAckTimeout = processorConfig.ESProcessorAckTimeout
	}
	return &VisibilityStore{
		esClient:                       esClient,
		index:                          cfg.GetVisibilityIndex(),
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
		chasmRegistry:                  chasmRegistry,
		processor:                      processor,
		processorAckTimeout:            processorAckTimeout,
		disableOrderByClause:           disableOrderByClause,
		enableManualPagination:         enableManualPagination,
		enableUnifiedQueryConverter:    enableUnifiedQueryConverter,
		metricsHandler:                 metricsHandler.WithTags(metrics.OperationTag(metrics.ElasticsearchVisibility)),
		logger:                         logger,
	}, nil
}

func (s *VisibilityStore) Close() {
	if s.processor != nil {
		s.processor.Stop()
	}
}

func (s *VisibilityStore) GetName() string {
	return PersistenceName
}

func (s *VisibilityStore) GetIndexName() string {
	return s.index
}

func (s *VisibilityStore) GetEsClient() client.Client {
	return s.esClient
}

func (s *VisibilityStore) GetSearchAttributesProvider() searchattribute.Provider {
	return s.searchAttributesProvider
}

func (s *VisibilityStore) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	validatedSearchAttributes := make(map[string]any, len(searchAttributes))
	var invalidValueErrs []error
	for saName, saValue := range searchAttributes {
		var err error
		switch value := saValue.(type) {
		case time.Time:
			err = validateDatetime(value)
		case []time.Time:
			for _, item := range value {
				if err = validateDatetime(item); err != nil {
					break
				}
			}
		case string:
			err = validateString(value)
		case []string:
			for _, item := range value {
				if err = validateString(item); err != nil {
					break
				}
			}
		}
		if err != nil {
			invalidValueErrs = append(invalidValueErrs, err)
			continue
		}
		validatedSearchAttributes[saName] = saValue
	}
	var retError error
	if len(invalidValueErrs) > 0 {
		retError = store.NewVisibilityStoreInvalidValuesError(invalidValueErrs)
	}
	return validatedSearchAttributes, retError
}

func (s *VisibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	visibilityTaskKey := GetVisibilityTaskKey(request.ShardID, request.TaskID)
	doc, err := s.GenerateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)
	if err != nil {
		return err
	}

	return s.addBulkIndexRequestAndWait(ctx, request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *VisibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	visibilityTaskKey := GetVisibilityTaskKey(request.ShardID, request.TaskID)
	doc, err := s.GenerateClosedESDoc(request, visibilityTaskKey)
	if err != nil {
		return err
	}

	return s.addBulkIndexRequestAndWait(ctx, request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *VisibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *store.InternalUpsertWorkflowExecutionRequest,
) error {
	visibilityTaskKey := GetVisibilityTaskKey(request.ShardID, request.TaskID)
	doc, err := s.GenerateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)
	if err != nil {
		return err
	}

	return s.addBulkIndexRequestAndWait(ctx, request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *VisibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	docID := GetDocID(request.WorkflowID, request.RunID)

	bulkDeleteRequest := &client.BulkableRequest{
		Index:       s.index,
		ID:          docID,
		Version:     request.TaskID,
		RequestType: client.BulkableRequestTypeDelete,
	}

	return s.AddBulkRequestAndWait(ctx, bulkDeleteRequest, docID)
}

func GetDocID(workflowID string, runID string) string {
	// From Elasticsearch doc: _id is limited to 512 bytes in size and larger values will be rejected.
	const maxDocIDLength = 512
	// Generally runID is guid and this should never be the case.
	if len(runID)+len(delimiter) >= maxDocIDLength {
		return util.TruncateUTF8(runID, maxDocIDLength)
	}

	if len(workflowID)+len(runID)+len(delimiter) > maxDocIDLength {
		workflowID = util.TruncateUTF8(workflowID, maxDocIDLength-len(runID)-len(delimiter))
	}

	return workflowID + delimiter + runID
}

func GetVisibilityTaskKey(shardID int32, taskID int64) string {
	return strconv.FormatInt(int64(shardID), 10) + delimiter + strconv.FormatInt(taskID, 10)
}

func (s *VisibilityStore) addBulkIndexRequestAndWait(
	ctx context.Context,
	request *store.InternalVisibilityRequestBase,
	esDoc map[string]any,
	visibilityTaskKey string,
) error {
	bulkIndexRequest := &client.BulkableRequest{
		Index:       s.index,
		ID:          GetDocID(request.WorkflowID, request.RunID),
		Version:     request.TaskID,
		RequestType: client.BulkableRequestTypeIndex,
		Doc:         esDoc,
	}

	return s.AddBulkRequestAndWait(ctx, bulkIndexRequest, visibilityTaskKey)
}

func (s *VisibilityStore) AddBulkRequestAndWait(
	_ context.Context,
	bulkRequest *client.BulkableRequest,
	visibilityTaskKey string,
) error {
	s.checkProcessor()

	// Add method is blocking. If bulk processor is busy flushing previous bulk, request will wait here.
	ackF := s.processor.Add(bulkRequest, visibilityTaskKey)

	// processorAckTimeout is a maximum duration for bulk processor to commit the bulk and unblock the `ackF`.
	// The default value is 30s, and this timeout should never have happened,
	// because Elasticsearch must process a bulk within the 30s.
	// Parent context is not respected here because it has shorter timeout (3s),
	// which might already expired here due to wait at Add method above.
	ctx, cancel := context.WithTimeout(context.Background(), s.processorAckTimeout())
	defer cancel()
	ack, err := ackF.Get(ctx)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return &persistence.TimeoutError{Msg: fmt.Sprintf("visibility task timed out waiting for ACK after %v", s.processorAckTimeout())}
		}
		// Returns non-retryable Internal error here because these errors are unexpected.
		// Visibility task processor retries all errors though; therefore, new request will be generated for the same visibility task.
		return serviceerror.NewInternalf("visibility task received error: %v", err)
	}

	if !ack {
		// Returns retryable Unavailable error here because NACK from bulk processor
		// means that this request wasn't processed successfully and needs to be retried.
		// Visibility task processor retries all errors anyway, therefore, new request will be generated for the same visibility task.
		return serviceerror.NewUnavailable("visibility task received NACK")
	}
	return nil
}

func (s *VisibilityStore) checkProcessor() {
	if s.processor == nil {
		// must be a bug, check history setup
		panic("Elasticsearch processor is nil")
	}
	if s.processorAckTimeout == nil {
		// must be a bug, check history setup
		panic("config.ESProcessorAckTimeout is nil")
	}
}

func (s *VisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	p, err := s.BuildSearchParametersV2(request, s.GetListFieldSorter)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, ConvertElasticsearchClientError("ListWorkflowExecutions failed", err)
	}

	return s.GetListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, nil)
}

func (s *VisibilityStore) ListChasmExecutions(
	ctx context.Context,
	request *manager.ListChasmExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown archetype ID: %d", request.ArchetypeID))
	}
	chasmMapper := rc.SearchAttributesMapper()

	p, err := s.BuildChasmSearchParameters(request, s.GetListFieldSorter, chasmMapper)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, ConvertElasticsearchClientError("ListChasmExecutions failed", err)
	}

	return s.GetListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, chasmMapper)
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

	var queryParams *esQueryParams
	var err error
	if s.enableUnifiedQueryConverter() {
		queryParams, err = s.convertQuery(request.Namespace, request.NamespaceID, request.Query, mapper, request.ArchetypeID)
		if err != nil {
			return nil, err
		}
	} else {
		queryParamsLegacy, err := s.convertQueryLegacy(request.Namespace, request.NamespaceID, request.Query, mapper, request.ArchetypeID)
		if err != nil {
			return nil, err
		}
		queryParams = (*esQueryParams)(queryParamsLegacy)
	}

	if len(queryParams.GroupBy) > 0 {
		return s.countGroupByExecutions(ctx, queryParams, mapper)
	}

	count, err := s.esClient.Count(ctx, s.index, queryParams.Query)
	if err != nil {
		return nil, ConvertElasticsearchClientError("CountChasmExecutions failed", err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *VisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	var queryParams *esQueryParams
	var err error
	if s.enableUnifiedQueryConverter() {
		queryParams, err = s.convertQuery(request.Namespace, request.NamespaceID, request.Query, nil, chasm.UnspecifiedArchetypeID)
		if err != nil {
			return nil, err
		}
	} else {
		queryParamsLegacy, err := s.convertQueryLegacy(request.Namespace, request.NamespaceID, request.Query, nil, chasm.UnspecifiedArchetypeID)
		if err != nil {
			return nil, err
		}
		queryParams = (*esQueryParams)(queryParamsLegacy)
	}

	if len(queryParams.GroupBy) > 0 {
		return s.countGroupByExecutions(ctx, queryParams, nil)
	}

	count, err := s.esClient.Count(ctx, s.index, queryParams.Query)
	if err != nil {
		return nil, ConvertElasticsearchClientError("CountWorkflowExecutions failed", err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *VisibilityStore) countGroupByExecutions(
	ctx context.Context,
	queryParams *esQueryParams,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalCountExecutionsResponse, error) {
	groupByFields := queryParams.GroupBy

	// Elasticsearch aggregation is nested. so need to loop backwards to build it.
	// Example: when grouping by (field1, field2), the object looks like
	// {
	//   "aggs": {
	//     "field1": {
	//       "terms": {
	//         "field": "field1"
	//       },
	//       "aggs": {
	//         "field2": {
	//           "terms": {
	//             "field": "field2"
	//           }
	//         }
	//       }
	//     }
	//   }
	// }
	termsAgg := elastic.NewTermsAggregation().Field(groupByFields[len(groupByFields)-1])
	for i := len(groupByFields) - 2; i >= 0; i-- {
		termsAgg = elastic.NewTermsAggregation().
			Field(groupByFields[i]).
			SubAggregation(groupByFields[i+1], termsAgg)
	}
	esResponse, err := s.esClient.CountGroupBy(
		ctx,
		s.index,
		queryParams.Query,
		groupByFields[0],
		termsAgg,
	)
	if err != nil {
		return nil, ConvertElasticsearchClientError("CountWorkflowExecutions failed", err)
	}
	return s.parseCountGroupByResponse(esResponse, groupByFields, chasmMapper)
}

func (s *VisibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	docID := GetDocID(request.WorkflowID, request.RunID)
	result, err := s.esClient.Get(ctx, s.index, docID)
	if err != nil {
		return nil, ConvertElasticsearchClientError("GetWorkflowExecution failed", err)
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"unable to read search attribute types: %v", err,
		)
	}

	if !result.Found {
		return nil, serviceerror.NewNotFoundf(
			"Workflow execution with RunId %s not found", request.RunID,
		)
	}

	workflowExecutionInfo, err := s.ParseESDoc(result.Id, result.Source, typeMap, request.Namespace, nil)
	if err != nil {
		return nil, err
	}

	return &store.InternalGetWorkflowExecutionResponse{
		Execution: workflowExecutionInfo,
	}, nil
}

func (s *VisibilityStore) BuildSearchParametersV2(
	request *manager.ListWorkflowExecutionsRequestV2,
	getFieldSorter func([]elastic.Sorter) ([]elastic.Sorter, error),
) (*client.SearchParameters, error) {
	return s.buildSearchParametersInternal(&searchParametersInternal{
		NamespaceName: request.Namespace,
		NamespaceID:   request.NamespaceID,
		Query:         request.Query,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
		ChasmMapper:   nil,
		ArchetypeID:   chasm.UnspecifiedArchetypeID,
	})
}

func (s *VisibilityStore) BuildChasmSearchParameters(
	request *manager.ListChasmExecutionsRequest,
	getFieldSorter func([]elastic.Sorter) ([]elastic.Sorter, error),
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*client.SearchParameters, error) {
	return s.buildSearchParametersInternal(&searchParametersInternal{
		NamespaceName: request.Namespace,
		NamespaceID:   request.NamespaceID,
		Query:         request.Query,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
		ChasmMapper:   chasmMapper,
		ArchetypeID:   request.ArchetypeID,
	})
}

func (s *VisibilityStore) buildSearchParametersInternal(
	params *searchParametersInternal,
) (*client.SearchParameters, error) {
	var queryParams *esQueryParams
	var err error
	if s.enableUnifiedQueryConverter() {
		queryParams, err = s.convertQuery(params.NamespaceName, params.NamespaceID, params.Query, params.ChasmMapper, params.ArchetypeID)
		if err != nil {
			return nil, err
		}
	} else {
		queryParamsLegacy, err := s.convertQueryLegacy(
			params.NamespaceName,
			params.NamespaceID,
			params.Query,
			params.ChasmMapper,
			params.ArchetypeID,
		)
		if err != nil {
			return nil, err
		}
		queryParams = (*esQueryParams)(queryParamsLegacy)
	}

	searchParams := &client.SearchParameters{
		Index:    s.index,
		PageSize: params.PageSize,
		Query:    queryParams.Query,
	}

	if len(queryParams.GroupBy) > 0 {
		return nil, serviceerror.NewInvalidArgument("GROUP BY clause is not supported")
	}

	// TODO(rodrigozhou): investigate possible solutions to slow ORDER BY.
	// ORDER BY clause can be slow if there is a large number of documents and
	// using a field that was not indexed by ES. Since slow queries can block
	// writes for unreasonably long, this option forbids the usage of ORDER BY
	// clause to prevent slow down issues.
	if s.disableOrderByClause(params.NamespaceName.String()) && len(queryParams.Sorter) > 0 {
		return nil, serviceerror.NewInvalidArgument("ORDER BY clause is not supported")
	}

	if len(queryParams.Sorter) > 0 {
		// If params.Sorter is not empty, then it's using custom order by.
		s.metricsHandler.WithTags(metrics.NamespaceTag(params.NamespaceName.String())).
			Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Record(1)
	}

	searchParams.Sorter, err = s.GetListFieldSorter(queryParams.Sorter)
	if err != nil {
		return nil, err
	}

	pageToken, err := s.deserializePageToken(params.NextPageToken)
	if err != nil {
		return nil, err
	}
	err = s.processPageToken(searchParams, pageToken, params.NamespaceName)
	if err != nil {
		return nil, err
	}

	return searchParams, nil
}

func (s *VisibilityStore) processPageToken(
	params *client.SearchParameters,
	pageToken *visibilityPageToken,
	namespaceName namespace.Name,
) error {
	if pageToken == nil {
		return nil
	}
	if len(pageToken.SearchAfter) == 0 {
		return nil
	}
	if len(pageToken.SearchAfter) != len(params.Sorter) {
		return serviceerror.NewInvalidArgumentf(
			"invalid page token for given sort fields: expected %d fields, got %d",
			len(params.Sorter),
			len(pageToken.SearchAfter),
		)
	}
	if !s.enableManualPagination(namespaceName.String()) || !isDefaultSorter(params.Sorter) {
		params.SearchAfter = pageToken.SearchAfter
		return nil
	}

	boolQuery, ok := params.Query.(*elastic.BoolQuery)
	if !ok {
		return serviceerror.NewInternalf(
			"unexpected query type: expected %T, got %T",
			&elastic.BoolQuery{},
			params.Query,
		)
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return serviceerror.NewUnavailablef(
			"unable to read search attribute types: %v", err,
		)
	}

	// Build a pagination search query for default sorter.
	shouldQueries, err := buildPaginationQuery(defaultSorterFields, pageToken.SearchAfter, saTypeMap)
	if err != nil {
		return err
	}

	boolQuery.Should(shouldQueries...)
	boolQuery.MinimumNumberShouldMatch(1)
	return nil
}

func (s *VisibilityStore) convertQuery(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (res *esQueryParams, err error) {
	defer func() {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be
		// only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			err = converterErr.ToInvalidArgument()
		}
	}()

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}

	saMapper, err := s.searchAttributesMapperProvider.GetMapper(namespaceName)
	if err != nil {
		return nil, err
	}

	c := query.NewQueryConverter(&queryConverter{}, namespaceName, saTypeMap, saMapper).
		WithChasmMapper(chasmMapper).
		WithArchetypeID(archetypeID)

	queryParams, err := c.Convert(queryString)
	if err != nil {
		return nil, err
	}

	queryParams.QueryExpr = elastic.NewBoolQuery().Filter(
		elastic.NewTermQuery(sadefs.NamespaceID, namespaceID.String()),
		queryParams.QueryExpr,
	)

	orderBy := make([]elastic.Sorter, 0, len(queryParams.OrderBy))
	for _, orderByExpr := range queryParams.OrderBy {
		// query converter is supposed to parse the expression and convert to SAColumn
		colName, ok := orderByExpr.Expr.(*query.SAColumn)
		if !ok {
			return nil, query.NewConverterError(
				"%s: unexpected field in 'ORDER BY' clause: %s",
				query.NotSupportedErrMessage,
				sqlparser.String(orderByExpr),
			)
		}
		fieldSort := elastic.NewFieldSort(colName.FieldName).Missing("_last")
		if orderByExpr.Direction == sqlparser.DescScr {
			fieldSort = fieldSort.Desc()
		}
		orderBy = append(orderBy, fieldSort)
	}

	groupBy := make([]string, 0, len(queryParams.GroupBy))
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, field.FieldName)
	}

	return &esQueryParams{
		Query:   queryParams.QueryExpr,
		Sorter:  orderBy,
		GroupBy: groupBy,
	}, nil
}

func (s *VisibilityStore) convertQueryLegacy(
	namespace namespace.Name,
	namespaceID namespace.ID,
	requestQueryStr string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (*query.QueryParamsLegacy, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}
	nameInterceptor := NewNameInterceptor(namespace, saTypeMap, s.searchAttributesMapperProvider, chasmMapper, archetypeID)
	queryConverter := NewQueryConverterLegacy(
		nameInterceptor,
		NewValuesInterceptor(namespace, saTypeMap, chasmMapper, s.metricsHandler, s.logger),
		saTypeMap,
		chasmMapper,
	)
	queryParams, err := queryConverter.ConvertWhereOrderBy(requestQueryStr)
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}

	// Create a new bool query because a request query might have only "should" (="or") queries.
	namespaceFilterQuery := elastic.NewBoolQuery().Filter(elastic.NewTermQuery(sadefs.NamespaceID, namespaceID.String()))

	// If the query did not explicitly filter on TemporalNamespaceDivision somehow, then add a
	// "must not exist" (i.e. "is null") query for it.
	if !nameInterceptor.seenNamespaceDivision {
		if archetypeID != chasm.UnspecifiedArchetypeID {
			namespaceFilterQuery.Filter(elastic.NewTermQuery(sadefs.TemporalNamespaceDivision, strconv.Itoa(int(archetypeID))))
		} else {
			namespaceFilterQuery.MustNot(elastic.NewExistsQuery(sadefs.TemporalNamespaceDivision))
		}
	}

	if queryParams.Query != nil {
		namespaceFilterQuery.Filter(queryParams.Query)
	}

	queryParams.Query = namespaceFilterQuery
	return queryParams, nil
}

func (s *VisibilityStore) GetListFieldSorter(fieldSorts []elastic.Sorter) ([]elastic.Sorter, error) {
	if len(fieldSorts) == 0 {
		return defaultSorter, nil
	}
	res := make([]elastic.Sorter, len(fieldSorts)+1)
	copy(res, fieldSorts)
	// RunID is explicit tiebreaker.
	res[len(res)-1] = elastic.NewFieldSort(sadefs.RunID).Desc()

	return res, nil
}

func (s *VisibilityStore) GetListWorkflowExecutionsResponse(
	searchResult *elastic.SearchResult,
	namespace namespace.Name,
	pageSize int,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalListExecutionsResponse, error) {
	if searchResult.Hits == nil || len(searchResult.Hits.Hits) == 0 {
		return &store.InternalListExecutionsResponse{}, nil
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}

	response := &store.InternalListExecutionsResponse{
		Executions: make([]*store.InternalExecutionInfo, 0, len(searchResult.Hits.Hits)),
	}
	var lastHitSort []any
	for _, hit := range searchResult.Hits.Hits {
		workflowExecutionInfo, err := s.ParseESDoc(hit.Id, hit.Source, typeMap, namespace, chasmMapper)
		if err != nil {
			return nil, err
		}
		response.Executions = append(response.Executions, workflowExecutionInfo)
		lastHitSort = hit.Sort
	}

	if len(searchResult.Hits.Hits) == pageSize { // this means the response might not the last page
		response.NextPageToken, err = s.serializePageToken(&visibilityPageToken{
			SearchAfter: lastHitSort,
		})
		if err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (s *VisibilityStore) deserializePageToken(data []byte) (*visibilityPageToken, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var token *visibilityPageToken
	dec := json.NewDecoder(bytes.NewReader(data))
	// UseNumber will not lose precision on big int64.
	dec.UseNumber()
	err := dec.Decode(&token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("unable to deserialize page token: %v", err)
	}
	return token, nil
}

func (s *VisibilityStore) serializePageToken(token *visibilityPageToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInternalf("unable to serialize page token: %v", err)
	}
	return data, nil
}

func (s *VisibilityStore) GenerateESDoc(
	request *store.InternalVisibilityRequestBase,
	visibilityTaskKey string,
) (map[string]any, error) {
	doc := map[string]any{
		sadefs.VisibilityTaskKey: visibilityTaskKey,
		sadefs.NamespaceID:       request.NamespaceID,
		sadefs.WorkflowID:        request.WorkflowID,
		sadefs.RunID:             request.RunID,
		sadefs.WorkflowType:      request.WorkflowTypeName,
		sadefs.StartTime:         request.StartTime,
		sadefs.ExecutionTime:     request.ExecutionTime,
		sadefs.ExecutionStatus:   request.Status.String(),
		sadefs.TaskQueue:         request.TaskQueue,
		sadefs.RootWorkflowID:    request.RootWorkflowID,
		sadefs.RootRunID:         request.RootRunID,
	}

	if request.ParentWorkflowID != nil {
		doc[sadefs.ParentWorkflowID] = *request.ParentWorkflowID
	}
	if request.ParentRunID != nil {
		doc[sadefs.ParentRunID] = *request.ParentRunID
	}

	if len(request.Memo.GetData()) > 0 {
		doc[sadefs.Memo] = request.Memo.GetData()
		doc[sadefs.MemoEncoding] = request.Memo.GetEncodingType().String()
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		metrics.ElasticsearchDocumentGenerateFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}

	searchAttributes, err := searchattribute.Decode(request.SearchAttributes, &typeMap, true)
	if err != nil {
		metrics.ElasticsearchDocumentGenerateFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternalf("unable to decode search attributes: %v", err)
	}
	for name := range request.SearchAttributes.GetIndexedFields() {
		if _, ok := searchAttributes[name]; !ok {
			s.logger.Warn("Skipping unknown search attribute while generating visibility record", tag.String("search-attribute", name))
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
	for saName, saValue := range searchAttributes {
		if saValue == nil {
			// If the search attribute value is `nil`, it means that it shouldn't be added to the document.
			// Empty slices are converted to `nil` while decoding.
			continue
		}
		doc[saName] = saValue
	}

	return doc, nil
}

func (s *VisibilityStore) GenerateClosedESDoc(
	request *store.InternalRecordWorkflowExecutionClosedRequest,
	visibilityTaskKey string,
) (map[string]any, error) {
	doc, err := s.GenerateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)
	if err != nil {
		return nil, err
	}

	doc[sadefs.CloseTime] = request.CloseTime
	doc[sadefs.ExecutionDuration] = request.ExecutionDuration
	doc[sadefs.HistoryLength] = request.HistoryLength
	doc[sadefs.StateTransitionCount] = request.StateTransitionCount
	doc[sadefs.HistorySizeBytes] = request.HistorySizeBytes

	return doc, nil
}

//nolint:revive // cyclomatic complexity
func (s *VisibilityStore) ParseESDoc(
	docID string,
	docSource json.RawMessage,
	saTypeMap searchattribute.NameTypeMap,
	namespaceName namespace.Name,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalExecutionInfo, error) {
	logParseError := func(fieldName string, fieldValue any, err error, docID string) error {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return serviceerror.NewInternalf("unable to parse Elasticsearch document(%s) %q field value %q: %v", docID, fieldName, fieldValue, err)
	}

	var sourceMap map[string]any
	d := json.NewDecoder(bytes.NewReader(docSource))
	// Very important line. See finishParseJSONValue bellow.
	d.UseNumber()
	if err := d.Decode(&sourceMap); err != nil {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternalf("unable to unmarshal JSON from Elasticsearch document(%s): %v", docID, err)
	}

	combinedTypeMap := store.CombineTypeMaps(saTypeMap, chasmMapper)

	var (
		isValidType         bool
		memo                []byte
		memoEncoding        string
		allSearchAttributes map[string]any
	)
	record := &store.InternalExecutionInfo{}
	for fieldName, fieldValue := range sourceMap {
		switch fieldName {
		case sadefs.NamespaceID,
			sadefs.VisibilityTaskKey:
			// Ignore these fields.
			continue
		case sadefs.Memo:
			var memoStr string
			if memoStr, isValidType = fieldValue.(string); !isValidType {
				return nil, logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), docID)
			}
			var err error
			if memo, err = base64.StdEncoding.DecodeString(memoStr); err != nil {
				return nil, logParseError(fieldName, memoStr[:10], err, docID)
			}
			continue
		case sadefs.MemoEncoding:
			if memoEncoding, isValidType = fieldValue.(string); !isValidType {
				return nil, logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), docID)
			}
			continue
		}

		fieldType, err := combinedTypeMap.GetType(fieldName)
		if err != nil {
			// Silently ignore ErrInvalidName because it indicates an unknown field in an Elasticsearch document.
			if errors.Is(err, searchattribute.ErrInvalidName) {
				continue
			}
			metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
			return nil, serviceerror.NewInternalf("Unable to get type for Elasticsearch document(%s) field %q: %v", docID, fieldName, err)
		}

		fieldValueParsed, err := finishParseJSONValue(fieldValue, fieldType)
		if err != nil {
			return nil, logParseError(fieldName, fieldValue, err, docID)
		}

		switch fieldName {
		case sadefs.WorkflowID:
			record.WorkflowID = fieldValueParsed.(string)
		case sadefs.RunID:
			record.RunID = fieldValueParsed.(string)
		case sadefs.WorkflowType:
			record.TypeName = fieldValue.(string)
		case sadefs.StartTime:
			record.StartTime = fieldValueParsed.(time.Time)
		case sadefs.ExecutionTime:
			record.ExecutionTime = fieldValueParsed.(time.Time)
		case sadefs.CloseTime:
			record.CloseTime = fieldValueParsed.(time.Time)
		case sadefs.ExecutionDuration:
			record.ExecutionDuration = time.Duration(fieldValueParsed.(int64))
		case sadefs.TaskQueue:
			record.TaskQueue = fieldValueParsed.(string)
		case sadefs.ExecutionStatus:
			status, err := enumspb.WorkflowExecutionStatusFromString(fieldValueParsed.(string))
			if err != nil {
				return nil, logParseError(fieldName, fieldValueParsed.(string), err, docID)
			}
			record.Status = status
		case sadefs.HistoryLength:
			record.HistoryLength = fieldValueParsed.(int64)
		case sadefs.StateTransitionCount:
			record.StateTransitionCount = fieldValueParsed.(int64)
		case sadefs.HistorySizeBytes:
			record.HistorySizeBytes = fieldValueParsed.(int64)
		case sadefs.ParentWorkflowID:
			record.ParentWorkflowID = fieldValueParsed.(string)
		case sadefs.ParentRunID:
			record.ParentRunID = fieldValueParsed.(string)
		case sadefs.RootWorkflowID:
			record.RootWorkflowID = fieldValueParsed.(string)
		case sadefs.RootRunID:
			record.RootRunID = fieldValueParsed.(string)
		default:
			if allSearchAttributes == nil {
				allSearchAttributes = map[string]any{}
			}
			allSearchAttributes[fieldName] = fieldValueParsed
		}
	}

	var err error
	record.SearchAttributes, err = searchattribute.Encode(allSearchAttributes, &combinedTypeMap)
	if err != nil {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternalf(
			"Unable to encode search attributes of Elasticsearch document(%s): %v",
			docID,
			err,
		)
	}

	if memoEncoding != "" {
		record.Memo = persistence.NewDataBlob(memo, memoEncoding)
	} else if memo != nil {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternalf(
			"%q field is missing in Elasticsearch document(%s)",
			sadefs.MemoEncoding,
			docID,
		)
	}

	return record, nil
}

// Elasticsearch aggregation groups are returned as a nested object.
// This function flattens the response into rows.
//
//nolint:revive // cognitive complexity 27 (> max enabled 25)
func (s *VisibilityStore) parseCountGroupByResponse(
	searchResult *elastic.SearchResult,
	groupByFields []string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalCountExecutionsResponse, error) {
	response := &store.InternalCountExecutionsResponse{}
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"unable to read search attribute types: %v", err,
		)
	}

	combinedTypeMap := store.CombineTypeMaps(saTypeMap, chasmMapper)

	groupByTypes := make([]enumspb.IndexedValueType, len(groupByFields))
	for i, saName := range groupByFields {
		tp, err := combinedTypeMap.GetType(saName)
		if err != nil {
			return nil, err
		}
		groupByTypes[i] = tp
	}

	parseJsonNumber := func(val any) (int64, error) {
		numberVal, isNumber := val.(json.Number)
		if !isNumber {
			return 0, fmt.Errorf("%w: expected json.Number, got %T", errUnexpectedJSONFieldType, val)
		}
		return numberVal.Int64()
	}

	var parseInternal func(map[string]any, []*commonpb.Payload) error
	parseInternal = func(aggs map[string]any, bucketValues []*commonpb.Payload) error {
		if len(bucketValues) == len(groupByFields) {
			cnt, err := parseJsonNumber(aggs["doc_count"])
			if err != nil {
				return fmt.Errorf("unable to parse 'doc_count' field: %w", err)
			}
			groupValues := make([]*commonpb.Payload, len(groupByFields))
			copy(groupValues, bucketValues)
			response.Groups = append(
				response.Groups,
				store.InternalAggregationGroup{
					GroupValues: groupValues,
					Count:       cnt,
				},
			)
			response.Count += cnt
			return nil
		}

		index := len(bucketValues)
		fieldName := groupByFields[index]
		buckets := aggs[fieldName].(map[string]any)["buckets"].([]any)
		for i := range buckets {
			bucket := buckets[i].(map[string]any)
			value, err := finishParseJSONValue(bucket["key"], groupByTypes[index])
			if err != nil {
				return fmt.Errorf("unable to parse value %v: %w", bucket["key"], err)
			}
			payload, err := searchattribute.EncodeValue(value, groupByTypes[index])
			if err != nil {
				return fmt.Errorf("unable to encode value %v: %w", value, err)
			}
			err = parseInternal(bucket, append(bucketValues, payload))
			if err != nil {
				return err
			}
		}
		return nil
	}

	var bucketsJson map[string]any
	dec := json.NewDecoder(bytes.NewReader(searchResult.Aggregations[groupByFields[0]]))
	dec.UseNumber()
	if err := dec.Decode(&bucketsJson); err != nil {
		return nil, serviceerror.NewInternalf("unable to unmarshal json response: %v", err)
	}
	if err := parseInternal(map[string]any{groupByFields[0]: bucketsJson}, nil); err != nil {
		return nil, err
	}
	return response, nil
}

// finishParseJSONValue finishes JSON parsing after json.Decode.
// json.Decode returns:
//
//	bool, for JSON booleans
//	json.Number, for JSON numbers (because of d.UseNumber())
//	string, for JSON strings
//	[]interface{}, for JSON arrays
//	map[string]interface{}, for JSON objects (should never be a case)
//	nil for JSON null
func finishParseJSONValue(val any, t enumspb.IndexedValueType) (any, error) {
	// Custom search attributes support array of a particular type.
	if arrayValue, isArray := val.([]any); isArray {
		retArray := make([]any, len(arrayValue))
		var lastErr error
		for i := range retArray {
			retArray[i], lastErr = finishParseJSONValue(arrayValue[i], t)
		}
		return retArray, lastErr
	}

	switch t {
	case enumspb.INDEXED_VALUE_TYPE_TEXT,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		enumspb.INDEXED_VALUE_TYPE_DATETIME:
		stringVal, isString := val.(string)
		if !isString {
			return nil, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, val)
		}
		if t == enumspb.INDEXED_VALUE_TYPE_DATETIME {
			return time.Parse(time.RFC3339Nano, stringVal)
		}
		return stringVal, nil
	case enumspb.INDEXED_VALUE_TYPE_INT, enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		numberVal, isNumber := val.(json.Number)
		if !isNumber {
			return nil, fmt.Errorf("%w: expected json.Number got %T", errUnexpectedJSONFieldType, val)
		}
		if t == enumspb.INDEXED_VALUE_TYPE_INT {
			return numberVal.Int64()
		}
		return numberVal.Float64()
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		boolVal, isBool := val.(bool)
		if !isBool {
			return nil, fmt.Errorf("%w: expected bool got %T", errUnexpectedJSONFieldType, val)
		}
		return boolVal, nil
	}

	panic(fmt.Sprintf("Unknown field type: %v", t))
}

func ConvertElasticsearchClientError(message string, err error) error {
	errMessage := fmt.Sprintf("%s: %s", message, detailedErrorMessage(err))
	var elasticErr *elastic.Error
	switch {
	case errors.As(err, &elasticErr):
		switch elasticErr.Status {
		case 400: // BadRequest
			// Returning InvalidArgument error will prevent retry on a caller side.
			return serviceerror.NewInvalidArgument(errMessage)
		}
		return serviceerror.NewUnavailable(errMessage)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%s: %w", message, err)
	}

	return serviceerror.NewUnavailable(errMessage)
}

func detailedErrorMessage(err error) string {
	var elasticErr *elastic.Error
	if !errors.As(err, &elasticErr) ||
		elasticErr.Details == nil ||
		len(elasticErr.Details.RootCause) == 0 ||
		(len(elasticErr.Details.RootCause) == 1 && elasticErr.Details.RootCause[0].Reason == elasticErr.Details.Reason) {
		return err.Error()
	}

	var sb strings.Builder
	sb.WriteString(elasticErr.Error())
	sb.WriteString(", root causes:")
	for i, rootCause := range elasticErr.Details.RootCause {
		sb.WriteString(fmt.Sprintf(" %s [type=%s]", rootCause.Reason, rootCause.Type))
		if i != len(elasticErr.Details.RootCause)-1 {
			sb.WriteRune(',')
		}
	}
	return sb.String()
}

func isDefaultSorter(sorter []elastic.Sorter) bool {
	if len(sorter) != len(defaultSorter) {
		return false
	}
	for i := range defaultSorter {
		if &sorter[i] != &defaultSorter[i] {
			return false
		}
	}
	return true
}

// buildPaginationQuery builds the Elasticsearch conditions for the next page based on searchAfter.
//
// For example, if sorterFields = [A, B, C] and searchAfter = [lastA, lastB, lastC],
// it will build the following conditions (assuming all values are non-null and orders are desc):
// - k = 0: A < lastA
// - k = 1: A = lastA AND B < lastB
// - k = 2: A = lastA AND B = lastB AND C < lastC
//
//nolint:revive // cyclomatic complexity
func buildPaginationQuery(
	sorterFields []fieldSort,
	searchAfter []any,
	saTypeMap searchattribute.NameTypeMap,
) ([]elastic.Query, error) {
	n := len(sorterFields)
	if len(sorterFields) != len(searchAfter) {
		return nil, serviceerror.NewInvalidArgumentf(
			"invalid page token for given sort fields: expected %d fields, got %d",
			len(sorterFields),
			len(searchAfter),
		)
	}

	parsedSearchAfter := make([]any, n)
	for i := range n {
		tp, err := saTypeMap.GetType(sorterFields[i].name)
		if err != nil {
			return nil, err
		}
		parsedSearchAfter[i], err = parsePageTokenValue(sorterFields[i].name, searchAfter[i], tp)
		if err != nil {
			return nil, err
		}
	}

	// The last field of sorter must be a tiebreaker, and thus cannot contain null value.
	if parsedSearchAfter[len(parsedSearchAfter)-1] == nil {
		return nil, serviceerror.NewInternalf(
			"last field of sorter cannot be a nullable field: %q has null values",
			sorterFields[len(sorterFields)-1].name,
		)
	}

	shouldQueries := make([]elastic.Query, 0, len(sorterFields))
	for k := range sorterFields {
		bq := elastic.NewBoolQuery()
		for i := 0; i <= k; i++ {
			field := sorterFields[i]
			value := parsedSearchAfter[i]
			if i == k {
				if value == nil {
					bq.Filter(elastic.NewExistsQuery(field.name))
				} else if field.desc {
					bq.Filter(elastic.NewRangeQuery(field.name).Lt(value))
				} else {
					bq.Filter(elastic.NewRangeQuery(field.name).Gt(value))
				}
			} else {
				if value == nil {
					bq.MustNot(elastic.NewExistsQuery(field.name))
				} else {
					bq.Filter(elastic.NewTermQuery(field.name, value))
				}
			}
		}
		shouldQueries = append(shouldQueries, bq)
	}
	return shouldQueries, nil
}

// parsePageTokenValue parses the page token values to be used in the search query.
// The page token comes from the `sort` field from the previous response from Elasticsearch.
// Depending on the type of the field, the null value is represented differently:
//   - integer, bool, and datetime: MaxInt64 (desc) or MinInt64 (asc)
//   - double: "Infinity" (desc) or "-Infinity" (asc)
//   - keyword: nil
//
// Furthermore, for bool and datetime, they need to be converted to boolean or the RFC3339Nano
// formats respectively.
//
//nolint:revive // cyclomatic complexity
func parsePageTokenValue(
	fieldName string, jsonValue any,
	tp enumspb.IndexedValueType,
) (any, error) {
	switch tp {
	case enumspb.INDEXED_VALUE_TYPE_INT,
		enumspb.INDEXED_VALUE_TYPE_BOOL,
		enumspb.INDEXED_VALUE_TYPE_DATETIME:
		jsonNumber, ok := jsonValue.(json.Number)
		if !ok {
			return nil, serviceerror.NewInvalidArgumentf(
				"invalid page token: expected interger type, got %q", jsonValue)
		}
		num, err := jsonNumber.Int64()
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf(
				"invalid page token: expected interger type, got %v", jsonValue)
		}
		if num == math.MaxInt64 || num == math.MinInt64 {
			return nil, nil
		}
		if tp == enumspb.INDEXED_VALUE_TYPE_BOOL {
			return num != 0, nil
		}
		if tp == enumspb.INDEXED_VALUE_TYPE_DATETIME {
			return time.Unix(0, num).UTC().Format(time.RFC3339Nano), nil
		}
		return num, nil

	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		switch v := jsonValue.(type) {
		case json.Number:
			num, err := v.Float64()
			if err != nil {
				return nil, serviceerror.NewInvalidArgumentf(
					"invalid page token: expected float type, got %v", jsonValue)
			}
			return num, nil
		case string:
			// it can be the string representation of infinity
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return nil, serviceerror.NewInvalidArgumentf(
					"invalid page token: expected float type, got %q", jsonValue)
			}
			return nil, nil
		default:
			// it should never reach here
			return nil, serviceerror.NewInvalidArgumentf(
				"invalid page token: expected float type, got %#v", jsonValue)
		}

	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		if jsonValue == nil {
			return nil, nil
		}
		if _, ok := jsonValue.(string); !ok {
			return nil, serviceerror.NewInvalidArgumentf(
				"invalid page token: expected string type, got %v", jsonValue)
		}
		return jsonValue, nil

	default:
		return nil, serviceerror.NewInvalidArgumentf(
			"invalid field type in sorter: cannot order by %q",
			fieldName,
		)
	}
}

func validateDatetime(value time.Time) error {
	if value.Before(minTime) || value.After(maxTime) {
		return serviceerror.NewInvalidArgumentf(
			"invalid search attribute date: %v, supported range: [%v, %v]", value, minTime, maxTime,
		)
	}
	return nil
}

func validateString(value string) error {
	if len(value) > maxStringLength {
		return serviceerror.NewInvalidArgumentf(
			"strings with more than %d bytes are not supported (got string of len %d)",
			maxStringLength,
			len(value),
		)
	}
	return nil
}

func (s *VisibilityStore) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	_, err := s.esClient.PutMapping(ctx, s.GetIndexName(), request.SearchAttributes)
	if err != nil {
		return err
	}
	_, err = s.esClient.WaitForYellowStatus(ctx, s.GetIndexName())
	return err
}
