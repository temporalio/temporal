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
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
)

const (
	persistenceName = "elasticsearch"

	delimiter                    = "~"
	pointInTimeKeepAliveInterval = "1m"
	scrollKeepAliveInterval      = "1m"
)

// Default sort by uses the sorting order defined in the index template, so no
// additional sorting is needed during query.
var defaultSorter = []elastic.Sorter{
	elastic.NewFieldSort(searchattribute.CloseTime).Desc().Missing("_first"),
	elastic.NewFieldSort(searchattribute.StartTime).Desc().Missing("_first"),
	elastic.NewFieldSort(searchattribute.RunID).Desc().Missing("_first"),
}

type (
	visibilityStore struct {
		esClient                 client.Client
		index                    string
		searchAttributesProvider searchattribute.Provider
		searchAttributesMapper   searchattribute.Mapper
		processor                Processor
		processorAckTimeout      dynamicconfig.DurationPropertyFn
		metricsClient            metrics.Client
	}

	visibilityPageToken struct {
		SearchAfter []interface{}

		// For ScanWorkflowExecutions API.
		// For ES<7.10.0 and "oss" flavor.
		ScrollID string
		// For ES>=7.10.0 and "default" flavor.
		PointInTimeID string
	}
)

var _ store.VisibilityStore = (*visibilityStore)(nil)

var (
	errUnexpectedJSONFieldType = errors.New("unexpected JSON field type")
)

// NewVisibilityStore create a visibility store connecting to ElasticSearch
func NewVisibilityStore(
	esClient client.Client,
	index string,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapper searchattribute.Mapper,
	processor Processor,
	processorAckTimeout dynamicconfig.DurationPropertyFn,
	metricsClient metrics.Client,
) *visibilityStore {

	return &visibilityStore{
		esClient:                 esClient,
		index:                    index,
		searchAttributesProvider: searchAttributesProvider,
		searchAttributesMapper:   searchAttributesMapper,
		processor:                processor,
		processorAckTimeout:      processorAckTimeout,
		metricsClient:            metricsClient,
	}
}

func (s *visibilityStore) Close() {
	// TODO (alex): visibilityStore shouldn't Stop processor. Processor should be stopped where it is created.
	if s.processor != nil {
		s.processor.Stop()
	}
}

func (s *visibilityStore) GetName() string {
	return persistenceName
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
	doc[searchattribute.ExecutionDuration] = request.CloseTime.Sub(request.ExecutionTime).Nanoseconds()
	doc[searchattribute.HistoryLength] = request.HistoryLength
	doc[searchattribute.StateTransitionCount] = request.StateTransitionCount

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
	ctx context.Context,
	bulkRequest *client.BulkableRequest,
	visibilityTaskKey string,
) error {
	s.checkProcessor()

	// Add method is blocking. If bulk processor is busy flushing previous bulk, request will wait here.
	// Therefore, ackTimeoutTimer in fact wait for request to be committed after it was added to bulk processor.
	// TODO: this also means ctx is not respected if bulk processor is busy. Shall we make Add non-blocking or
	// respecting the context?
	future := s.processor.Add(bulkRequest, visibilityTaskKey)

	ackTimeout := s.processorAckTimeout()
	if deadline, ok := ctx.Deadline(); ok {
		ackTimeout = util.Min(ackTimeout, time.Until(deadline))
	}
	subCtx, subCtxCancelFn := context.WithTimeout(context.Background(), ackTimeout)
	defer subCtxCancelFn()

	ack, err := future.Get(subCtx)

	if errors.Is(err, context.DeadlineExceeded) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("visibility task %s timedout waiting for ACK after %v", visibilityTaskKey, s.processorAckTimeout())}
	}

	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("visibility task %s received error %v", visibilityTaskKey, err))
	}

	if !ack {
		// Returns non-retryable Internal error here because NACK from bulk processor means that this request can't be processed.
		// Visibility task processor retries all errors though, therefore new request will be generated for the same task.
		return serviceerror.NewInternal(fmt.Sprintf("visibility task %s received NACK", visibilityTaskKey))
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

func (s *visibilityStore) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))

	p, err := s.buildSearchParameters(request, boolQuery, true)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListOpenWorkflowExecutions failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))

	p, err := s.buildSearchParameters(request, boolQuery, false)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListClosedWorkflowExecutions failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery(searchattribute.WorkflowType, request.WorkflowTypeName),
			elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))

	p, err := s.buildSearchParameters(request.ListWorkflowExecutionsRequest, boolQuery, true)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListOpenWorkflowExecutionsByType failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.WorkflowType, request.WorkflowTypeName)).
		MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))

	p, err := s.buildSearchParameters(request.ListWorkflowExecutionsRequest, boolQuery, false)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListClosedWorkflowExecutionsByType failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery(searchattribute.WorkflowID, request.WorkflowID),
			elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))

	p, err := s.buildSearchParameters(request.ListWorkflowExecutionsRequest, boolQuery, true)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListOpenWorkflowExecutionsByWorkflowID failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.WorkflowID, request.WorkflowID)).
		MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))

	p, err := s.buildSearchParameters(request.ListWorkflowExecutionsRequest, boolQuery, false)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListClosedWorkflowExecutionsByWorkflowID failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, request.Status.String()))

	p, err := s.buildSearchParameters(request.ListWorkflowExecutionsRequest, boolQuery, false)
	if err != nil {
		return nil, err
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListClosedWorkflowExecutionsByStatus failed", err)
	}

	isRecordValid := func(rec *store.InternalWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	p, err := s.buildSearchParametersV2(request)
	if err != nil {
		return nil, err
	}

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	if token != nil && len(token.SearchAfter) > 0 {
		p.SearchAfter = token.SearchAfter
	}

	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ListWorkflowExecutions failed", err)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, nil)
}

func (s *visibilityStore) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	if esClientV7, isV7 := s.esClient.(client.ClientV7); isV7 {
		// Elasticsearch 7.10+ can use "point in time" (PIT) instead of scroll to scan over all workflows without skipping or duplicating them.
		// https://www.elastic.co/guide/en/elasticsearch/reference/7.10/point-in-time-api.html
		if esClientV7.IsPointInTimeSupported(ctx) {
			return s.scanWorkflowExecutionsWithPit(ctx, request, esClientV7)
		}
	}

	return s.scanWorkflowExecutionsWithScroll(ctx, request)
}

func (s *visibilityStore) scanWorkflowExecutionsWithPit(ctx context.Context, request *manager.ListWorkflowExecutionsRequestV2, esClient client.ClientV7) (*store.InternalListWorkflowExecutionsResponse, error) {
	p, err := s.buildSearchParametersV2(request)
	if err != nil {
		return nil, err
	}

	// First call doesn't have token with PointInTimeID.
	if len(request.NextPageToken) == 0 {
		pitID, err := esClient.OpenPointInTime(ctx, s.index, pointInTimeKeepAliveInterval)
		if err != nil {
			return nil, convertElasticsearchClientError("Unable to create point in time", err)
		}
		p.PointInTime = elastic.NewPointInTimeWithKeepAlive(pitID, pointInTimeKeepAliveInterval)
	} else {
		token, err := s.deserializePageToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		if token.PointInTimeID == "" {
			return nil, serviceerror.NewInvalidArgument("pointInTimeId must present in pagination token")
		}
		p.SearchAfter = token.SearchAfter
		p.PointInTime = elastic.NewPointInTimeWithKeepAlive(token.PointInTimeID, pointInTimeKeepAliveInterval)
	}

	searchResult, err := esClient.Search(ctx, p)
	if err != nil {
		return nil, convertElasticsearchClientError("ScanWorkflowExecutions failed", err)
	}

	// Empty hits list indicate that this is a last page.
	if searchResult.Hits != nil && len(searchResult.Hits.Hits) < request.PageSize {
		_, err = esClient.ClosePointInTime(ctx, searchResult.PitId)
		if err != nil {
			return nil, convertElasticsearchClientError("Unable to close point in time", err)
		}
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, nil)
}

func (s *visibilityStore) scanWorkflowExecutionsWithScroll(ctx context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*store.InternalListWorkflowExecutionsResponse, error) {
	var (
		searchResult *elastic.SearchResult
		scrollErr    error
	)

	// First call doesn't have token with ScrollID.
	if len(request.NextPageToken) == 0 {
		// First page.
		p, err := s.buildSearchParametersV2(request)
		if err != nil {
			return nil, err
		}
		searchResult, scrollErr = s.esClient.OpenScroll(ctx, p, scrollKeepAliveInterval)
	} else {
		token, err := s.deserializePageToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		if token.ScrollID == "" {
			return nil, serviceerror.NewInvalidArgument("scrollId must present in pagination token")
		}
		searchResult, scrollErr = s.esClient.Scroll(ctx, token.ScrollID, scrollKeepAliveInterval)
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

	return s.getListWorkflowExecutionsResponse(searchResult, request.Namespace, request.PageSize, nil)
}

func (s *visibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	boolQuery, _, err := s.convertQuery(request.Namespace, request.NamespaceID, request.Query)
	if err != nil {
		return nil, err
	}

	count, err := s.esClient.Count(ctx, s.index, boolQuery)
	if err != nil {
		return nil, convertElasticsearchClientError("CountWorkflowExecutions failed", err)
	}

	response := &manager.CountWorkflowExecutionsResponse{Count: count}
	return response, nil
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
) (*client.SearchParameters, error) {

	boolQuery, fieldSorts, err := s.convertQuery(request.Namespace, request.NamespaceID, request.Query)
	if err != nil {
		return nil, err
	}

	params := &client.SearchParameters{
		Index:    s.index,
		Query:    boolQuery,
		PageSize: request.PageSize,
		Sorter:   s.setDefaultFieldSort(fieldSorts),
	}

	return params, nil
}
func (s *visibilityStore) convertQuery(namespace namespace.Name, namespaceID namespace.ID, requestQueryStr string) (*elastic.BoolQuery, []*elastic.FieldSort, error) {
	saTypeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		return nil, nil, serviceerror.NewUnavailable(fmt.Sprintf("Unable to read search attribute types: %v", err))
	}
	queryConverter := newQueryConverter(
		newNameInterceptor(namespace, s.index, saTypeMap, s.searchAttributesMapper),
		NewValuesInterceptor(),
	)
	requestQuery, fieldSorts, err := queryConverter.ConvertWhereOrderBy(requestQueryStr)
	if err != nil {
		// Convert ConverterError to InvalidArgument and pass through all other errors (which should be only mapper errors).
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, nil, converterErr.ToInvalidArgument()
		}
		return nil, nil, err
	}

	// Create new bool query because request query might have only "should" (="or") queries.
	namespaceFilterQuery := elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.NamespaceID, namespaceID.String()))
	if requestQuery != nil {
		namespaceFilterQuery.Filter(requestQuery)
	}

	return namespaceFilterQuery, fieldSorts, nil
}

func (s *visibilityStore) setDefaultFieldSort(fieldSorts []*elastic.FieldSort) []elastic.Sorter {
	if len(fieldSorts) == 0 {
		return defaultSorter
	}

	res := make([]elastic.Sorter, len(fieldSorts)+1)
	for i, fs := range fieldSorts {
		res[i] = fs
	}
	// RunID is explicit tiebreaker.
	res[len(res)-1] = elastic.NewFieldSort(searchattribute.RunID).Desc()

	return res
}

func (s *visibilityStore) getListWorkflowExecutionsResponse(
	searchResult *elastic.SearchResult,
	namespace namespace.Name,
	pageSize int,
	isRecordValid func(rec *store.InternalWorkflowExecutionInfo) bool,
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
		workflowExecutionInfo, err := s.parseESDoc(hit, typeMap, namespace)
		if err != nil {
			return nil, err
		}
		// ES6 uses "date" data type not "date_nanos". It truncates dates using milliseconds and might return extra rows.
		// For example: 2021-06-12T00:21:43.159739259Z fits 2021-06-12T00:21:43.158Z...2021-06-12T00:21:43.159Z range lte/gte query.
		// Therefore, these records need to be filtered out on the client side to support nanos precision.
		// After ES6 deprecation isRecordValid can be removed.
		if isRecordValid == nil || isRecordValid(workflowExecutionInfo) {
			response.Executions = append(response.Executions, workflowExecutionInfo)
			lastHitSort = hit.Sort
		}
	}

	if len(searchResult.Hits.Hits) == pageSize && lastHitSort != nil { // this means the response is not the last page
		response.NextPageToken, err = s.serializePageToken(&visibilityPageToken{
			SearchAfter:   lastHitSort,
			PointInTimeID: searchResult.PitId,
			ScrollID:      searchResult.ScrollId,
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

func (s *visibilityStore) generateESDoc(request *store.InternalVisibilityRequestBase, visibilityTaskKey string) (map[string]interface{}, error) {
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
	}

	if len(request.Memo.GetData()) > 0 {
		doc[searchattribute.Memo] = request.Memo.GetData()
		doc[searchattribute.MemoEncoding] = request.Memo.GetEncodingType().String()
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentGenerateFailuresCount)
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("Unable to read search attribute types: %v", err))
	}

	searchAttributes, err := searchattribute.Decode(request.SearchAttributes, &typeMap)
	if err != nil {
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentGenerateFailuresCount)
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to decode search attributes: %v", err))
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

func (s *visibilityStore) parseESDoc(hit *elastic.SearchHit, saTypeMap searchattribute.NameTypeMap, namespace namespace.Name) (*store.InternalWorkflowExecutionInfo, error) {
	logParseError := func(fieldName string, fieldValue interface{}, err error, docID string) error {
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentParseFailuresCount)
		return serviceerror.NewInternal(fmt.Sprintf("Unable to parse Elasticsearch document(%s) %q field value %q: %v", docID, fieldName, fieldValue, err))
	}

	var sourceMap map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(hit.Source))
	// Very important line. See finishParseJSONValue bellow.
	d.UseNumber()
	if err := d.Decode(&sourceMap); err != nil {
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentParseFailuresCount)
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to unmarshal JSON from Elasticsearch document(%s): %v", hit.Id, err))
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
			searchattribute.ExecutionDuration,
			searchattribute.VisibilityTaskKey:
			// Ignore these fields.
			continue
		case searchattribute.Memo:
			var memoStr string
			if memoStr, isValidType = fieldValue.(string); !isValidType {
				return nil, logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), hit.Id)
			}
			var err error
			if memo, err = base64.StdEncoding.DecodeString(memoStr); err != nil {
				return nil, logParseError(fieldName, memoStr[:10], err, hit.Id)
			}
			continue
		case searchattribute.MemoEncoding:
			if memoEncoding, isValidType = fieldValue.(string); !isValidType {
				return nil, logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), hit.Id)
			}
			continue
		}

		fieldType, err := saTypeMap.GetType(fieldName)
		if err != nil {
			// Silently ignore ErrInvalidName because it indicates unknown field in Elasticsearch document.
			if errors.Is(err, searchattribute.ErrInvalidName) {
				continue
			}
			s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentParseFailuresCount)
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to get type for Elasticsearch document(%s) field %q: %v", hit.Id, fieldName, err))
		}

		fieldValueParsed, err := finishParseJSONValue(fieldValue, fieldType)
		if err != nil {
			return nil, logParseError(fieldName, fieldValue, err, hit.Id)
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
		case searchattribute.TaskQueue:
			record.TaskQueue = fieldValueParsed.(string)
		case searchattribute.ExecutionStatus:
			record.Status = enumspb.WorkflowExecutionStatus(enumspb.WorkflowExecutionStatus_value[fieldValueParsed.(string)])
		case searchattribute.HistoryLength:
			record.HistoryLength = fieldValueParsed.(int64)
		case searchattribute.StateTransitionCount:
			record.StateTransitionCount = fieldValueParsed.(int64)
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
			s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentParseFailuresCount)
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to encode custom search attributes of Elasticsearch document(%s): %v", hit.Id, err))
		}
		err = searchattribute.ApplyAliases(s.searchAttributesMapper, record.SearchAttributes, namespace.String())
		if err != nil {
			return nil, err
		}
	}

	if memoEncoding != "" {
		record.Memo = persistence.NewDataBlob(memo, memoEncoding)
	} else if memo != nil {
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentParseFailuresCount)
		return nil, serviceerror.NewInternal(fmt.Sprintf("%q field is missing in Elasticsearch document(%s)", searchattribute.MemoEncoding, hit.Id))
	}

	return record, nil
}

// finishParseJSONValue finishes JSON parsing after json.Decode.
// json.Decode returns:
//     bool, for JSON booleans
//     json.Number, for JSON numbers (because of d.UseNumber())
//     string, for JSON strings
//     []interface{}, for JSON arrays
//     map[string]interface{}, for JSON objects (should never be a case)
//     nil for JSON null
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
	case enumspb.INDEXED_VALUE_TYPE_TEXT, enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_DATETIME:
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
			// Returning Internal error will prevent retry on a caller side.
			return serviceerror.NewInternal(errMessage)
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
