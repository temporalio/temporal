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

package elasticsearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

const (
	PersistenceName = "elasticsearch"

	delimiter                    = "~"
	scrollKeepAliveInterval      = "1m"
	pointInTimeKeepAliveInterval = "1m"
)

type (
	visibilityStore struct {
		esClient                       client.Client
		index                          string
		searchAttributesProvider       searchattribute.Provider
		searchAttributesMapperProvider searchattribute.MapperProvider
		processor                      Processor
		processorAckTimeout            dynamicconfig.DurationPropertyFn
		disableOrderByClause           dynamicconfig.BoolPropertyFnWithNamespaceFilter
		enableManualPagination         dynamicconfig.BoolPropertyFnWithNamespaceFilter
		metricsHandler                 metrics.Handler
	}

	visibilityPageToken struct {
		SearchAfter []interface{}

		// For ScanWorkflowExecutions API.
		// For ES<7.10.0 and "oss" flavor.
		ScrollID string
		// For ES>=7.10.0 and "default" flavor.
		PointInTimeID string
	}

	fieldSort struct {
		name          string
		desc          bool
		missing_first bool
	}
)

var _ store.VisibilityStore = (*visibilityStore)(nil)

var (
	errUnexpectedJSONFieldType = errors.New("unexpected JSON field type")

	minTime         = time.Unix(0, 0).UTC()
	maxTime         = time.Unix(0, math.MaxInt64).UTC()
	maxStringLength = 32766

	// Default sorter uses the sorting order defined in the index template.
	// It is indirectly built so buildPaginationQuery can have access to
	// the fields names to build the page query from the token.
	defaultSorterFields = []fieldSort{
		{searchattribute.CloseTime, true, true},
		{searchattribute.StartTime, true, true},
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
	esClient client.Client,
	index string,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	processor Processor,
	processorAckTimeout dynamicconfig.DurationPropertyFn,
	disableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	enableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	metricsHandler metrics.Handler,
) *visibilityStore {

	return &visibilityStore{
		esClient:                       esClient,
		index:                          index,
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
		processor:                      processor,
		processorAckTimeout:            processorAckTimeout,
		disableOrderByClause:           disableOrderByClause,
		enableManualPagination:         enableManualPagination,
		metricsHandler:                 metricsHandler.WithTags(metrics.OperationTag(metrics.ElasticsearchVisibility)),
	}
}

func (s *visibilityStore) Close() {
	// TODO (alex): visibilityStore shouldn't Stop processor. Processor should be stopped where it is created.
	if s.processor != nil {
		s.processor.Stop()
	}
}

func (s *visibilityStore) GetName() string {
	return PersistenceName
}

func (s *visibilityStore) GetIndexName() string {
	return s.index
}

func (s *visibilityStore) ValidateCustomSearchAttributes(
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

func (s *visibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc, err := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)
	if err != nil {
		return err
	}

	return s.addBulkIndexRequestAndWait(ctx, request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc, err := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)
	if err != nil {
		return err
	}

	doc[searchattribute.CloseTime] = request.CloseTime
	doc[searchattribute.ExecutionDuration] = request.ExecutionDuration
	doc[searchattribute.HistoryLength] = request.HistoryLength
	doc[searchattribute.StateTransitionCount] = request.StateTransitionCount
	doc[searchattribute.HistorySizeBytes] = request.HistorySizeBytes

	return s.addBulkIndexRequestAndWait(ctx, request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *store.InternalUpsertWorkflowExecutionRequest,
) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc, err := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)
	if err != nil {
		return err
	}

	return s.addBulkIndexRequestAndWait(ctx, request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	docID := getDocID(request.WorkflowID, request.RunID)

	bulkDeleteRequest := &client.BulkableRequest{
		Index:       s.index,
		ID:          docID,
		Version:     request.TaskID,
		RequestType: client.BulkableRequestTypeDelete,
	}

	return s.addBulkRequestAndWait(ctx, bulkDeleteRequest, docID)
}

func getDocID(workflowID string, runID string) string {
	// From Elasticsearch doc: _id is limited to 512 bytes in size and larger values will be rejected.
	const maxDocIDLength = 512
	// Generally runID is guid and this should never be the case.
	if len(runID)+len(delimiter) >= maxDocIDLength {
		if len(runID) >= maxDocIDLength {
			return runID[0:maxDocIDLength]
		}
		return runID[0 : maxDocIDLength-len(delimiter)]
	}

	if len(workflowID)+len(runID)+len(delimiter) > maxDocIDLength {
		workflowID = workflowID[0 : maxDocIDLength-len(runID)-len(delimiter)]
	}

	return workflowID + delimiter + runID
}

func getVisibilityTaskKey(shardID int32, taskID int64) string {
	return strconv.FormatInt(int64(shardID), 10) + delimiter + strconv.FormatInt(taskID, 10)
}

func (s *visibilityStore) addBulkIndexRequestAndWait(
	ctx context.Context,
	request *store.InternalVisibilityRequestBase,
	esDoc map[string]interface{},
	visibilityTaskKey string,
) error {
	bulkIndexRequest := &client.BulkableRequest{
		Index:       s.index,
		ID:          getDocID(request.WorkflowID, request.RunID),
		Version:     request.TaskID,
		RequestType: client.BulkableRequestTypeIndex,
		Doc:         esDoc,
	}

	return s.addBulkRequestAndWait(ctx, bulkIndexRequest, visibilityTaskKey)
}

func (s *visibilityStore) addBulkRequestAndWait(
	_ context.Context,
	bulkRequest *client.BulkableRequest,
	visibilityTaskKey string,
) error {
	s.checkProcessor()

	// Add method is blocking. If bulk processor is busy flushing previous bulk, request will wait here.
	ackF := s.processor.Add(bulkRequest, visibilityTaskKey)

	// processorAckTimeout is a maximum duration for bulk processor to commit the bulk and unblock the `ackF`.
	// Default value is 30s and this timeout should never have happened,
	// because Elasticsearch must process a bulk within 30s.
	// Parent context is not respected here because it has shorter timeout (3s),
	// which might already expired here due to wait at Add method above.
	ctx, cancel := context.WithTimeout(context.Background(), s.processorAckTimeout())
	defer cancel()
	ack, err := ackF.Get(ctx)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return &persistence.TimeoutError{Msg: fmt.Sprintf("visibility task %s timed out waiting for ACK after %v", visibilityTaskKey, s.processorAckTimeout())}
		}
		// Returns non-retryable Internal error here because these errors are unexpected.
		// Visibility task processor retries all errors though, therefore new request will be generated for the same visibility task.
		return serviceerror.NewInternal(fmt.Sprintf("visibility task %s received error %v", visibilityTaskKey, err))
	}

	if !ack {
		// Returns retryable Unavailable error here because NACK from bulk processor
		// means that this request wasn't processed successfully and needs to be retried.
		// Visibility task processor retries all errors anyway, therefore new request will be generated for the same visibility task.
		return serviceerror.NewUnavailable(fmt.Sprintf("visibility task %s received NACK", visibilityTaskKey))
	}
	return nil
}

func (s *visibilityStore) checkProcessor() {
	if s.processor == nil {
		// must be bug, check history setup
		panic("Elasticsearch processor is nil")
	}
	if s.processorAckTimeout == nil {
		// must be bug, check history setup
		panic("config.ESProcessorAckTimeout is nil")
	}
}

func (s *visibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	p, err := s.buildSearchParametersV2(request, s.getListFieldSorter)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListWorkflowExecutions failed", err)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize)
}

func (s *visibilityStore) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	// Point in time is only supported in Elasticsearch 7.10+ in default flavor.
	if s.esClient.IsPointInTimeSupported(ctx) {
		return s.scanWorkflowExecutionsWithPit(ctx, request)
	}
	return s.scanWorkflowExecutionsWithScroll(ctx, request)
}

func (s *visibilityStore) scanWorkflowExecutionsWithScroll(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	var (
		searchResult *elastic.SearchResult
		scrollErr    error
	)

	p, err := s.buildSearchParametersV2(request, s.getScanFieldSorter)
	if err != nil {
		return nil, err
	}

	if len(request.NextPageToken) == 0 {
		searchResult, scrollErr = s.esClient.OpenScroll(ctx, p, scrollKeepAliveInterval)
	} else if p.ScrollID != "" {
		searchResult, scrollErr = s.esClient.Scroll(ctx, p.ScrollID, scrollKeepAliveInterval)
	} else {
		return nil, serviceerror.NewInvalidArgument("scrollId must present in pagination token")
	}

	if scrollErr != nil && scrollErr != io.EOF {
		return nil, convertElasticsearchClientError("ScanWorkflowExecutions failed", scrollErr)
	}

	// Both io.IOF and empty hits list indicate that this is a last page.
	if (searchResult.Hits != nil && len(searchResult.Hits.Hits) < request.PageSize) ||
		scrollErr == io.EOF {
		err := s.esClient.CloseScroll(ctx, searchResult.ScrollId)
		if err != nil {
			return nil, convertElasticsearchClientError("Unable to close scroll", err)
		}
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize)
}

func (s *visibilityStore) scanWorkflowExecutionsWithPit(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	p, err := s.buildSearchParametersV2(request, s.getScanFieldSorter)
	if err != nil {
		return nil, err
	}

	// First call doesn't have token with PointInTimeID.
	if len(request.NextPageToken) == 0 {
		pitID, err := s.esClient.OpenPointInTime(ctx, s.index, pointInTimeKeepAliveInterval)
		if err != nil {
			return nil, convertElasticsearchClientError("Unable to create point in time", err)
		}
		p.PointInTime = elastic.NewPointInTimeWithKeepAlive(pitID, pointInTimeKeepAliveInterval)
	} else if p.PointInTime == nil {
		return nil, serviceerror.NewInvalidArgument("pointInTimeId must present in pagination token")
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ScanWorkflowExecutions failed", err)
	}

	// Number hits smaller than the page size indicate that this is the last page.
	if searchResult.Hits != nil && len(searchResult.Hits.Hits) < request.PageSize {
		_, err := s.esClient.ClosePointInTime(ctx, searchResult.PitId)
		if err != nil {
			return nil, convertElasticsearchClientError("Unable to close point in time", err)
		}
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize)
}

func (s *visibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	queryParams, err := s.convertQuery(request.Namespace, request.NamespaceID, request.Query)
	if err != nil {
		return nil, err
	}

	if len(queryParams.GroupBy) > 0 {
		return s.countGroupByWorkflowExecutions(ctx, queryParams)
	}

	count, err := s.esClient.Count(ctx, s.index, queryParams.Query)
	if err != nil {
		return nil, convertElasticsearchClientError("CountWorkflowExecutions failed", err)
	}

	response := &manager.CountWorkflowExecutionsResponse{Count: count}
	return response, nil
}

func (s *visibilityStore) countGroupByWorkflowExecutions(
	ctx context.Context,
	queryParams *query.QueryParams,
) (*manager.CountWorkflowExecutionsResponse, error) {
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
		return nil, err
	}
	return s.parseCountGroupByResponse(esResponse, groupByFields)
}

func (s *visibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	docID := getDocID(request.WorkflowID, request.RunID)
	result, err := s.esClient.Get(ctx, s.index, docID)
	if err != nil {
		return nil, convertElasticsearchClientError("GetWorkflowExecution failed", err)
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attribute types: %v", err),
		)
	}

	if !result.Found {
		return nil, serviceerror.NewNotFound(
			fmt.Sprintf("Workflow execution with run id %s not found.", request.RunID),
		)
	}

	workflowExecutionInfo, err := s.parseESDoc(result.Id, result.Source, typeMap, request.Namespace)
	if err != nil {
		return nil, err
	}

	return &store.InternalGetWorkflowExecutionResponse{
		Execution: workflowExecutionInfo,
	}, nil
}

func (s *visibilityStore) buildSearchParameters(
	request *manager.ListWorkflowExecutionsRequest,
	boolQuery *elastic.BoolQuery,
	overStartTime bool,
) (*client.SearchParameters, error) {

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery.Filter(elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID.String()))

	if request.NamespaceDivision == "" {
		boolQuery.MustNot(elastic.NewExistsQuery(searchattribute.TemporalNamespaceDivision))
	} else {
		boolQuery.Filter(elastic.NewTermQuery(searchattribute.TemporalNamespaceDivision, request.NamespaceDivision))
	}

	if !request.EarliestStartTime.IsZero() || !request.LatestStartTime.IsZero() {
		var rangeQuery *elastic.RangeQuery
		if overStartTime {
			rangeQuery = elastic.NewRangeQuery(searchattribute.StartTime)
		} else {
			rangeQuery = elastic.NewRangeQuery(searchattribute.CloseTime)
		}

		if !request.EarliestStartTime.IsZero() {
			rangeQuery = rangeQuery.Gte(request.EarliestStartTime)
		}

		if !request.LatestStartTime.IsZero() {
			rangeQuery = rangeQuery.Lte(request.LatestStartTime)
		}
		boolQuery.Filter(rangeQuery)
	}

	params := &client.SearchParameters{
		Index:    s.index,
		Query:    boolQuery,
		PageSize: request.PageSize,
		Sorter:   defaultSorter,
	}

	if token != nil && len(token.SearchAfter) > 0 {
		params.SearchAfter = token.SearchAfter
	}

	return params, nil
}

func (s *visibilityStore) buildSearchParametersV2(
	request *manager.ListWorkflowExecutionsRequestV2,
	getFieldSorter func([]elastic.Sorter) ([]elastic.Sorter, error),
) (*client.SearchParameters, error) {
	queryParams, err := s.convertQuery(
		request.Namespace,
		request.NamespaceID,
		request.Query,
	)
	if err != nil {
		return nil, err
	}

	searchParams := &client.SearchParameters{
		Index:    s.index,
		PageSize: request.PageSize,
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
	if s.disableOrderByClause(request.Namespace.String()) && len(queryParams.Sorter) > 0 {
		return nil, serviceerror.NewInvalidArgument("ORDER BY clause is not supported")
	}

	if len(queryParams.Sorter) > 0 {
		// If params.Sorter is not empty, then it's using custom order by.
		s.metricsHandler.WithTags(metrics.NamespaceTag(request.Namespace.String())).
			Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Record(1)
	}

	searchParams.Sorter, err = getFieldSorter(queryParams.Sorter)
	if err != nil {
		return nil, err
	}

	pageToken, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}
	err = s.processPageToken(searchParams, pageToken, request.Namespace)
	if err != nil {
		return nil, err
	}

	return searchParams, nil
}

func (s *visibilityStore) processPageToken(
	params *client.SearchParameters,
	pageToken *visibilityPageToken,
	namespaceName namespace.Name,
) error {
	if pageToken == nil {
		return nil
	}
	if pageToken.ScrollID != "" {
		params.ScrollID = pageToken.ScrollID
		return nil
	}
	if len(pageToken.SearchAfter) == 0 {
		return nil
	}
	if pageToken.PointInTimeID != "" {
		params.SearchAfter = pageToken.SearchAfter
		params.PointInTime = elastic.NewPointInTimeWithKeepAlive(
			pageToken.PointInTimeID,
			pointInTimeKeepAliveInterval,
		)
		return nil
	}
	if len(pageToken.SearchAfter) != len(params.Sorter) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf(
			"Invalid page token for given sort fields: expected %d fields, got %d",
			len(params.Sorter),
			len(pageToken.SearchAfter),
		))
	}
	if !s.enableManualPagination(namespaceName.String()) || !isDefaultSorter(params.Sorter) {
		params.SearchAfter = pageToken.SearchAfter
		return nil
	}

	boolQuery, ok := params.Query.(*elastic.BoolQuery)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf(
			"Unexpected query type: expected *elastic.BoolQuery, got %T",
			params.Query,
		))
	}

	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attribute types: %v", err),
		)
	}

	// build pagination search query for default sorter
	shouldQueries, err := buildPaginationQuery(defaultSorterFields, pageToken.SearchAfter, saTypeMap)
	if err != nil {
		return err
	}

	boolQuery.Should(shouldQueries...)
	boolQuery.MinimumNumberShouldMatch(1)
	return nil
}

func (s *visibilityStore) convertQuery(
	namespace namespace.Name,
	namespaceID namespace.ID,
	requestQueryStr string,
) (*query.QueryParams, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Unable to read search attribute types: %v", err))
	}
	nameInterceptor := newNameInterceptor(namespace, s.index, saTypeMap, s.searchAttributesMapperProvider)
	queryConverter := NewQueryConverter(
		nameInterceptor,
		NewValuesInterceptor(namespace, saTypeMap, s.searchAttributesMapperProvider),
		saTypeMap,
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

	// Create new bool query because request query might have only "should" (="or") queries.
	namespaceFilterQuery := elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.NamespaceID, namespaceID.String()))

	// If the query did not explicitly filter on TemporalNamespaceDivision somehow, then add a
	// "must not exist" (i.e. "is null") query for it.
	if !nameInterceptor.seenNamespaceDivision {
		namespaceFilterQuery.MustNot(elastic.NewExistsQuery(searchattribute.TemporalNamespaceDivision))
	}

	if queryParams.Query != nil {
		namespaceFilterQuery.Filter(queryParams.Query)
	}

	queryParams.Query = namespaceFilterQuery
	return queryParams, nil
}

func (s *visibilityStore) getScanFieldSorter(fieldSorts []elastic.Sorter) ([]elastic.Sorter, error) {
	// custom order is not supported by Scan API
	if len(fieldSorts) > 0 {
		return nil, serviceerror.NewInvalidArgument("ORDER BY clause is not supported")
	}

	return docSorter, nil
}

func (s *visibilityStore) getListFieldSorter(fieldSorts []elastic.Sorter) ([]elastic.Sorter, error) {
	if len(fieldSorts) == 0 {
		return defaultSorter, nil
	}
	res := make([]elastic.Sorter, len(fieldSorts)+1)
	for i, fs := range fieldSorts {
		res[i] = fs
	}
	// RunID is explicit tiebreaker.
	res[len(res)-1] = elastic.NewFieldSort(searchattribute.RunID).Desc()

	return res, nil
}

func (s *visibilityStore) getListWorkflowExecutionsResponse(
	searchResult *elastic.SearchResult,
	namespace namespace.Name,
	pageSize int,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	if searchResult.Hits == nil || len(searchResult.Hits.Hits) == 0 {
		return &store.InternalListWorkflowExecutionsResponse{}, nil
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Unable to read search attribute types: %v", err))
	}

	response := &store.InternalListWorkflowExecutionsResponse{
		Executions: make([]*store.InternalWorkflowExecutionInfo, 0, len(searchResult.Hits.Hits)),
	}
	var lastHitSort []interface{}
	for _, hit := range searchResult.Hits.Hits {
		workflowExecutionInfo, err := s.parseESDoc(hit.Id, hit.Source, typeMap, namespace)
		if err != nil {
			return nil, err
		}
		response.Executions = append(response.Executions, workflowExecutionInfo)
		lastHitSort = hit.Sort
	}

	if len(searchResult.Hits.Hits) == pageSize { // this means the response might not the last page
		response.NextPageToken, err = s.serializePageToken(&visibilityPageToken{
			SearchAfter:   lastHitSort,
			ScrollID:      searchResult.ScrollId,
			PointInTimeID: searchResult.PitId,
		})
		if err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (s *visibilityStore) deserializePageToken(data []byte) (*visibilityPageToken, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var token *visibilityPageToken
	dec := json.NewDecoder(bytes.NewReader(data))
	// UseNumber will not lose precision on big int64.
	dec.UseNumber()
	err := dec.Decode(&token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to deserialize page token: %v", err))
	}
	return token, nil
}

func (s *visibilityStore) serializePageToken(token *visibilityPageToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("unable to serialize page token: %v", err))
	}
	return data, nil
}

func (s *visibilityStore) generateESDoc(
	request *store.InternalVisibilityRequestBase,
	visibilityTaskKey string,
) (map[string]interface{}, error) {
	doc := map[string]interface{}{
		searchattribute.VisibilityTaskKey: visibilityTaskKey,
		searchattribute.NamespaceID:       request.NamespaceID,
		searchattribute.WorkflowID:        request.WorkflowID,
		searchattribute.RunID:             request.RunID,
		searchattribute.WorkflowType:      request.WorkflowTypeName,
		searchattribute.StartTime:         request.StartTime,
		searchattribute.ExecutionTime:     request.ExecutionTime,
		searchattribute.ExecutionStatus:   request.Status.String(),
		searchattribute.TaskQueue:         request.TaskQueue,
		searchattribute.RootWorkflowID:    request.RootWorkflowID,
		searchattribute.RootRunID:         request.RootRunID,
	}

	if request.ParentWorkflowID != nil {
		doc[searchattribute.ParentWorkflowID] = *request.ParentWorkflowID
	}
	if request.ParentRunID != nil {
		doc[searchattribute.ParentRunID] = *request.ParentRunID
	}

	if len(request.Memo.GetData()) > 0 {
		doc[searchattribute.Memo] = request.Memo.GetData()
		doc[searchattribute.MemoEncoding] = request.Memo.GetEncodingType().String()
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		metrics.ElasticsearchDocumentGenerateFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Unable to read search attribute types: %v", err))
	}

	searchAttributes, err := searchattribute.Decode(request.SearchAttributes, &typeMap, true)
	if err != nil {
		metrics.ElasticsearchDocumentGenerateFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to decode search attributes: %v", err))
	}
	// This is to prevent existing tasks to fail indefinitely.
	// If it's only invalid values error, then silently continue without them.
	searchAttributes, err = s.ValidateCustomSearchAttributes(searchAttributes)
	if err != nil {
		if _, ok := err.(*store.VisibilityStoreInvalidValuesError); !ok {
			return nil, err
		}
	}
	for saName, saValue := range searchAttributes {
		if saValue == nil {
			// If search attribute value is `nil`, it means that it shouldn't be added to the document.
			// Empty slices are converted to `nil` while decoding.
			continue
		}
		doc[saName] = saValue
	}

	return doc, nil
}

//nolint:revive // cyclomatic complexity
func (s *visibilityStore) parseESDoc(
	docID string,
	docSource json.RawMessage,
	saTypeMap searchattribute.NameTypeMap,
	namespaceName namespace.Name,
) (*store.InternalWorkflowExecutionInfo, error) {
	logParseError := func(fieldName string, fieldValue interface{}, err error, docID string) error {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return serviceerror.NewInternal(fmt.Sprintf("Unable to parse Elasticsearch document(%s) %q field value %q: %v", docID, fieldName, fieldValue, err))
	}

	var sourceMap map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(docSource))
	// Very important line. See finishParseJSONValue bellow.
	d.UseNumber()
	if err := d.Decode(&sourceMap); err != nil {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to unmarshal JSON from Elasticsearch document(%s): %v", docID, err))
	}

	var (
		isValidType            bool
		memo                   []byte
		memoEncoding           string
		customSearchAttributes map[string]interface{}
	)
	record := &store.InternalWorkflowExecutionInfo{}
	for fieldName, fieldValue := range sourceMap {
		switch fieldName {
		case searchattribute.NamespaceID,
			searchattribute.VisibilityTaskKey:
			// Ignore these fields.
			continue
		case searchattribute.Memo:
			var memoStr string
			if memoStr, isValidType = fieldValue.(string); !isValidType {
				return nil, logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), docID)
			}
			var err error
			if memo, err = base64.StdEncoding.DecodeString(memoStr); err != nil {
				return nil, logParseError(fieldName, memoStr[:10], err, docID)
			}
			continue
		case searchattribute.MemoEncoding:
			if memoEncoding, isValidType = fieldValue.(string); !isValidType {
				return nil, logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), docID)
			}
			continue
		}

		fieldType, err := saTypeMap.GetType(fieldName)
		if err != nil {
			// Silently ignore ErrInvalidName because it indicates unknown field in Elasticsearch document.
			if errors.Is(err, searchattribute.ErrInvalidName) {
				continue
			}
			metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to get type for Elasticsearch document(%s) field %q: %v", docID, fieldName, err))
		}

		fieldValueParsed, err := finishParseJSONValue(fieldValue, fieldType)
		if err != nil {
			return nil, logParseError(fieldName, fieldValue, err, docID)
		}

		switch fieldName {
		case searchattribute.WorkflowID:
			record.WorkflowID = fieldValueParsed.(string)
		case searchattribute.RunID:
			record.RunID = fieldValueParsed.(string)
		case searchattribute.WorkflowType:
			record.TypeName = fieldValue.(string)
		case searchattribute.StartTime:
			record.StartTime = fieldValueParsed.(time.Time)
		case searchattribute.ExecutionTime:
			record.ExecutionTime = fieldValueParsed.(time.Time)
		case searchattribute.CloseTime:
			record.CloseTime = fieldValueParsed.(time.Time)
		case searchattribute.ExecutionDuration:
			record.ExecutionDuration = time.Duration(fieldValueParsed.(int64))
		case searchattribute.TaskQueue:
			record.TaskQueue = fieldValueParsed.(string)
		case searchattribute.ExecutionStatus:
			status, err := enumspb.WorkflowExecutionStatusFromString(fieldValueParsed.(string))
			if err != nil {
				return nil, logParseError(fieldName, fieldValueParsed.(string), err, docID)
			}
			record.Status = status
		case searchattribute.HistoryLength:
			record.HistoryLength = fieldValueParsed.(int64)
		case searchattribute.StateTransitionCount:
			record.StateTransitionCount = fieldValueParsed.(int64)
		case searchattribute.HistorySizeBytes:
			record.HistorySizeBytes = fieldValueParsed.(int64)
		case searchattribute.ParentWorkflowID:
			record.ParentWorkflowID = fieldValueParsed.(string)
		case searchattribute.ParentRunID:
			record.ParentRunID = fieldValueParsed.(string)
		case searchattribute.RootWorkflowID:
			record.RootWorkflowID = fieldValueParsed.(string)
		case searchattribute.RootRunID:
			record.RootRunID = fieldValueParsed.(string)
		default:
			// All custom and predefined search attributes are handled here.
			if customSearchAttributes == nil {
				customSearchAttributes = map[string]interface{}{}
			}
			customSearchAttributes[fieldName] = fieldValueParsed
		}
	}

	if customSearchAttributes != nil {
		var err error
		record.SearchAttributes, err = searchattribute.Encode(customSearchAttributes, &saTypeMap)
		if err != nil {
			metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
			return nil, serviceerror.NewInternal(
				fmt.Sprintf(
					"Unable to encode custom search attributes of Elasticsearch document(%s): %v",
					docID,
					err,
				),
			)
		}
		aliasedSas, err := searchattribute.AliasFields(
			s.searchAttributesMapperProvider,
			record.SearchAttributes,
			namespaceName.String(),
		)
		if err != nil {
			return nil, err
		}

		if aliasedSas != record.SearchAttributes {
			record.SearchAttributes = aliasedSas
		}
	}

	if memoEncoding != "" {
		record.Memo = persistence.NewDataBlob(memo, memoEncoding)
	} else if memo != nil {
		metrics.ElasticsearchDocumentParseFailuresCount.With(s.metricsHandler).Record(1)
		return nil, serviceerror.NewInternal(
			fmt.Sprintf(
				"%q field is missing in Elasticsearch document(%s)",
				searchattribute.MemoEncoding,
				docID,
			),
		)
	}

	return record, nil
}

// Elasticsearch aggregation groups are returned as nested object.
// This function flattens the response into rows.
//
//nolint:revive // cognitive complexity 27 (> max enabled 25)
func (s *visibilityStore) parseCountGroupByResponse(
	searchResult *elastic.SearchResult,
	groupByFields []string,
) (*manager.CountWorkflowExecutionsResponse, error) {
	response := &manager.CountWorkflowExecutionsResponse{}
	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("Unable to read search attribute types: %v", err),
		)
	}
	groupByTypes := make([]enumspb.IndexedValueType, len(groupByFields))
	for i, saName := range groupByFields {
		tp, err := typeMap.GetType(saName)
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
				return fmt.Errorf("Unable to parse 'doc_count' field: %w", err)
			}
			groupValues := make([]*commonpb.Payload, len(groupByFields))
			for i := range bucketValues {
				groupValues[i] = bucketValues[i]
			}
			response.Groups = append(
				response.Groups,
				&workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
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
				return fmt.Errorf("Failed to parse value %v: %w", bucket["key"], err)
			}
			payload, err := searchattribute.EncodeValue(value, groupByTypes[index])
			if err != nil {
				return fmt.Errorf("Failed to encode value %v: %w", value, err)
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
		return nil, serviceerror.NewInternal(fmt.Sprintf("unable to unmarshal json response: %v", err))
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
func finishParseJSONValue(val interface{}, t enumspb.IndexedValueType) (interface{}, error) {
	// Custom search attributes support array of particular type.
	if arrayValue, isArray := val.([]interface{}); isArray {
		retArray := make([]interface{}, len(arrayValue))
		var lastErr error
		for i := 0; i < len(retArray); i++ {
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

func convertElasticsearchClientError(message string, err error) error {
	errMessage := fmt.Sprintf("%s: %s", message, detailedErrorMessage(err))
	switch e := err.(type) {
	case *elastic.Error:
		switch e.Status {
		case 400: // BadRequest
			// Returning InvalidArgument error will prevent retry on a caller side.
			return serviceerror.NewInvalidArgument(errMessage)
		}
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
	for i := 0; i < len(defaultSorter); i++ {
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
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"Invalid page token for given sort fields: expected %d fields, got %d",
			len(sorterFields),
			len(searchAfter),
		))
	}

	parsedSearchAfter := make([]any, n)
	for i := 0; i < n; i++ {
		tp, err := saTypeMap.GetType(sorterFields[i].name)
		if err != nil {
			return nil, err
		}
		parsedSearchAfter[i], err = parsePageTokenValue(sorterFields[i].name, searchAfter[i], tp)
		if err != nil {
			return nil, err
		}
	}

	// Last field of sorter must be a tie breaker, and thus cannot contain null value.
	if parsedSearchAfter[len(parsedSearchAfter)-1] == nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"Last field of sorter cannot be a nullable field: %q has null values",
			sorterFields[len(sorterFields)-1].name,
		))
	}

	shouldQueries := make([]elastic.Query, 0, len(sorterFields))
	for k := 0; k < len(sorterFields); k++ {
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
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"Invalid page token: expected interger type, got %q", jsonValue))
		}
		num, err := jsonNumber.Int64()
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"Invalid page token: expected interger type, got %v", jsonValue))
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
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
					"Invalid page token: expected float type, got %v", jsonValue))
			}
			return num, nil
		case string:
			// it can be the string representation of infinity
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
					"Invalid page token: expected float type, got %q", jsonValue))
			}
			return nil, nil
		default:
			// it should never reach here
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"Invalid page token: expected float type, got %#v", jsonValue))
		}

	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		if jsonValue == nil {
			return nil, nil
		}
		if _, ok := jsonValue.(string); !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"Invalid page token: expected string type, got %v", jsonValue))
		}
		return jsonValue, nil

	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"Invalid field type in sorter: cannot order by %q",
			fieldName,
		))
	}
}

func validateDatetime(value time.Time) error {
	if value.Before(minTime) || value.After(maxTime) {
		return serviceerror.NewInvalidArgument(
			fmt.Sprintf("Date not supported in Elasticsearch: %v", value),
		)
	}
	return nil
}

func validateString(value string) error {
	if len(value) > maxStringLength {
		return serviceerror.NewInvalidArgument(
			fmt.Sprintf(
				"Strings with more than %d bytes are not supported in Elasticsearch (got %s)",
				maxStringLength,
				value,
			),
		)
	}
	return nil
}
