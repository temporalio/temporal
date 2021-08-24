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
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/elasticsearch/client"
	esclient "go.temporal.io/server/common/persistence/visibility/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/elasticsearch/query"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
)

const (
	persistenceName = "elasticsearch"

	delimiter                    = "~"
	pointInTimeKeepAliveInterval = "1m"
)

type (
	visibilityStore struct {
		esClient                 esclient.Client
		index                    string
		searchAttributesProvider searchattribute.Provider
		logger                   log.Logger
		config                   *config.VisibilityConfig
		metricsClient            metrics.Client
		processor                Processor
		queryConverter           *query.Converter
	}

	visibilityPageToken struct {
		SearchAfter []interface{}

		// For ScanWorkflowExecutions API.
		// Deprecated. Remove after ES v6 support removal.
		ScrollID string
		// Not supported in ES v6.
		PointInTimeID string
	}
)

var _ visibility.VisibilityStore = (*visibilityStore)(nil)

var (
	ErrInvalidDuration         = errors.New("invalid duration format")
	errUnexpectedJSONFieldType = errors.New("unexpected JSON field type")
)

// NewVisibilityStore create a visibility store connecting to ElasticSearch
func NewVisibilityStore(
	esClient esclient.Client,
	index string,
	searchAttributesProvider searchattribute.Provider,
	processor Processor,
	cfg *config.VisibilityConfig,
	logger log.Logger,
	metricsClient metrics.Client,
) *visibilityStore {

	return &visibilityStore{
		esClient:                 esClient,
		index:                    index,
		searchAttributesProvider: searchAttributesProvider,
		processor:                processor,
		logger:                   log.With(logger, tag.ComponentESVisibilityManager),
		config:                   cfg,
		metricsClient:            metricsClient,
		queryConverter: query.NewConverter(
			newNameInterceptor(index, searchAttributesProvider),
			newValuesInterceptor(),
		),
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

func (s *visibilityStore) RecordWorkflowExecutionStarted(request *visibility.InternalRecordWorkflowExecutionStartedRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	return s.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) RecordWorkflowExecutionClosed(request *visibility.InternalRecordWorkflowExecutionClosedRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	doc[searchattribute.CloseTime] = request.CloseTime
	doc[searchattribute.ExecutionDuration] = request.CloseTime.Sub(request.ExecutionTime).Nanoseconds()
	doc[searchattribute.HistoryLength] = request.HistoryLength
	doc[searchattribute.StateTransitionCount] = request.StateTransitionCount

	return s.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) UpsertWorkflowExecution(request *visibility.InternalUpsertWorkflowExecutionRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	return s.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) DeleteWorkflowExecution(request *visibility.VisibilityDeleteWorkflowExecutionRequest) error {
	docID := getDocID(request.WorkflowID, request.RunID)

	bulkDeleteRequest := &esclient.BulkableRequest{
		Index:       s.index,
		ID:          docID,
		Version:     request.TaskID,
		RequestType: esclient.BulkableRequestTypeDelete,
	}

	return s.addBulkRequestAndWait(bulkDeleteRequest, docID)
}

func getDocID(workflowID string, runID string) string {
	return fmt.Sprintf("%s%s%s", workflowID, delimiter, runID)
}

func getVisibilityTaskKey(shardID int32, taskID int64) string {
	return fmt.Sprintf("%d%s%d", shardID, delimiter, taskID)
}

func (s *visibilityStore) addBulkIndexRequestAndWait(
	request *visibility.InternalVisibilityRequestBase,
	esDoc map[string]interface{},
	visibilityTaskKey string,
) error {
	bulkIndexRequest := &esclient.BulkableRequest{
		Index:       s.index,
		ID:          getDocID(request.WorkflowID, request.RunID),
		Version:     request.TaskID,
		RequestType: esclient.BulkableRequestTypeIndex,
		Doc:         esDoc,
	}

	return s.addBulkRequestAndWait(bulkIndexRequest, visibilityTaskKey)
}

func (s *visibilityStore) addBulkRequestAndWait(bulkRequest *esclient.BulkableRequest, visibilityTaskKey string) error {
	s.checkProcessor()

	ackCh := s.processor.Add(bulkRequest, visibilityTaskKey)
	ackTimeoutTimer := time.NewTimer(s.config.ESProcessorAckTimeout())
	defer ackTimeoutTimer.Stop()

	select {
	case ack := <-ackCh:
		if !ack {
			return newVisibilityTaskNAckError(visibilityTaskKey)
		}
		return nil
	case <-ackTimeoutTimer.C:
		return newVisibilityTaskAckTimeoutError(visibilityTaskKey, s.config.ESProcessorAckTimeout())
	}
}

func (s *visibilityStore) checkProcessor() {
	if s.processor == nil {
		// must be bug, check history setup
		panic("elastic search processor is nil")
	}
	if s.config.ESProcessorAckTimeout == nil {
		// must be bug, check history setup
		panic("config.ESProcessorAckTimeout is nil")
	}
}

func (s *visibilityStore) ListOpenWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))
	searchResult, err := s.getSearchResult(request, token, boolQuery, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutions failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	executionStatusQuery := elastic.NewBoolQuery().
		MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))
	searchResult, err := s.getSearchResult(request, token, executionStatusQuery, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutions failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByType(request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request.ListWorkflowExecutionsRequest)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery(searchattribute.WorkflowType, request.WorkflowTypeName),
			elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))
	searchResult, err := s.getSearchResult(request.ListWorkflowExecutionsRequest, token, boolQuery, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByType failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByType(request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request.ListWorkflowExecutionsRequest)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.WorkflowType, request.WorkflowTypeName)).
		MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))
	searchResult, err := s.getSearchResult(request.ListWorkflowExecutionsRequest, token, boolQuery, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByType failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request.ListWorkflowExecutionsRequest)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery(searchattribute.WorkflowID, request.WorkflowID),
			elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))
	searchResult, err := s.getSearchResult(request.ListWorkflowExecutionsRequest, token, boolQuery, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request.ListWorkflowExecutionsRequest)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.WorkflowID, request.WorkflowID)).
		MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String()))
	searchResult, err := s.getSearchResult(request.ListWorkflowExecutionsRequest, token, boolQuery, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByStatus(request *visibility.ListClosedWorkflowExecutionsByStatusRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSize(request.ListWorkflowExecutionsRequest)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	boolQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, request.Status.String()))
	searchResult, err := s.getSearchResult(request.ListWorkflowExecutionsRequest, token, boolQuery, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByStatus failed. Error: %s", detailedErrorMessage(err)))
	}

	isRecordValid := func(rec *visibility.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequestV2) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSizeV2(request)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	bq, fs, err := s.queryConverter.ConvertWhereOrderBy(request.Query)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to parse query: %v", err))
	}
	bq.Filter(elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID))

	p := &esclient.SearchParameters{
		Index:       s.index,
		Query:       bq,
		PageSize:    request.PageSize,
		Sorter:      s.setDefaultFieldSort(fs),
		SearchAfter: token.SearchAfter,
	}

	ctx := context.Background()
	searchResult, err := s.esClient.Search(ctx, p)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListWorkflowExecutions failed. Error: %s", detailedErrorMessage(err)))
	}

	return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, nil)
}

func (s *visibilityStore) ScanWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequestV2) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	s.setDefaultPageSizeV2(request)

	token, err := s.deserializePageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	switch esClient := s.esClient.(type) {
	case client.ClientV7:
		// Elasticsearch V7 uses "point in time" (PIT) instead of scroll to scan over all workflows without skipping them.
		// https://www.elastic.co/guide/en/elasticsearch/reference/7.13/point-in-time-api.html

		// First call doesn't have PointInTimeID.
		if token.PointInTimeID == "" {
			token.PointInTimeID, err = esClient.OpenPointInTime(ctx, s.index, pointInTimeKeepAliveInterval)
			if err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to create point in time: %s", detailedErrorMessage(err)))
			}
		}

		bq, fs, err := s.queryConverter.ConvertWhereOrderBy(request.Query)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to parse query: %v", err))
		}
		bq.Filter(elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID))

		p := &esclient.SearchParameters{
			Query:       bq,
			PageSize:    request.PageSize,
			Sorter:      s.setDefaultFieldSort(fs),
			SearchAfter: token.SearchAfter,
			PointInTime: elastic.NewPointInTime(token.PointInTimeID, pointInTimeKeepAliveInterval),
		}

		searchResult, err := esClient.Search(ctx, p)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("ScanWorkflowExecutions failed. Error: %s", detailedErrorMessage(err)))
		}

		if len(searchResult.Hits.Hits) < request.PageSize {
			// It is the last page, close PIT.
			_, err = esClient.ClosePointInTime(ctx, token.PointInTimeID)
			if err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to close point in time: %s", detailedErrorMessage(err)))
			}
		}
		return s.getListWorkflowExecutionsResponse(searchResult, request.PageSize, nil)
	case client.ClientV6:
		var searchResult *elastic.SearchResult
		var scrollService esclient.ScrollService
		if len(token.ScrollID) == 0 { // first call
			bq, fs, err := s.queryConverter.ConvertWhereOrderBy(request.Query)
			if err != nil {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to parse query: %v", err))
			}
			bq.Filter(elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID))

			p := &esclient.SearchParameters{
				Index:       s.index,
				Query:       bq,
				PageSize:    request.PageSize,
				Sorter:      s.setDefaultFieldSort(fs),
				SearchAfter: token.SearchAfter,
			}

			searchResult, scrollService, err = esClient.ScrollFirstPage(ctx, p)
		} else {
			searchResult, scrollService, err = esClient.Scroll(ctx, token.ScrollID)
		}

		isLastPage := false
		if err == io.EOF { // no more result
			isLastPage = true
			_ = scrollService.Clear(context.Background())
		} else if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("ScanWorkflowExecutions failed. Error: %s", detailedErrorMessage(err)))
		}
		return s.getScrollWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, searchResult.ScrollId, isLastPage)
	default:
		panic("esClient has unsupported type")
	}
}

func (s *visibilityStore) CountWorkflowExecutions(request *visibility.CountWorkflowExecutionsRequest) (
	*visibility.CountWorkflowExecutionsResponse, error) {

	bq, _, err := s.queryConverter.ConvertWhereOrderBy(request.Query)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to parse query: %v", err))
	}
	bq.Filter(elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID))

	p := &esclient.SearchParameters{
		Index: s.index,
		Query: bq,
	}

	ctx := context.Background()
	count, err := s.esClient.Count(ctx, p)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CountWorkflowExecutions failed. Error: %s", detailedErrorMessage(err)))
	}

	response := &visibility.CountWorkflowExecutionsResponse{Count: count}
	return response, nil
}

func (s *visibilityStore) setDefaultFieldSort(fieldSorts []*elastic.FieldSort) []elastic.Sorter {
	var res []elastic.Sorter
	if len(fieldSorts) == 0 {
		// set default sorting by StartTime desc.
		res = []elastic.Sorter{elastic.NewFieldSort(searchattribute.StartTime).Desc()}
	} else { // user provide sorting using order by
		for _, fs := range fieldSorts {
			res = append(res, fs)
		}
	}
	// Add RunID as tiebreaker.
	return append(res, elastic.NewFieldSort(searchattribute.RunID).Desc())
}

func (s *visibilityStore) setDefaultPageSize(request *visibility.ListWorkflowExecutionsRequest) {
	if request.PageSize == 0 {
		request.PageSize = 1000
	}
}

func (s *visibilityStore) setDefaultPageSizeV2(request *visibility.ListWorkflowExecutionsRequestV2) {
	if request.PageSize == 0 {
		request.PageSize = 1000
	}
}

func (s *visibilityStore) getSearchResult(
	request *visibility.ListWorkflowExecutionsRequest,
	token *visibilityPageToken,
	boolQuery *elastic.BoolQuery,
	overStartTime bool,
) (*elastic.SearchResult, error) {

	if boolQuery == nil {
		boolQuery = elastic.NewBoolQuery()
	}

	boolQuery.Filter(elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID))

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

	ctx := context.Background()
	params := &esclient.SearchParameters{
		Index:       s.index,
		Query:       boolQuery,
		PageSize:    request.PageSize,
		SearchAfter: token.SearchAfter,
	}
	if overStartTime {
		params.Sorter = append(params.Sorter, elastic.NewFieldSort(searchattribute.StartTime).Desc())
	} else {
		params.Sorter = append(params.Sorter, elastic.NewFieldSort(searchattribute.CloseTime).Desc())
	}
	params.Sorter = append(params.Sorter, elastic.NewFieldSort(searchattribute.RunID).Desc())

	return s.esClient.Search(ctx, params)
}

func (s *visibilityStore) getScrollWorkflowExecutionsResponse(
	searchHits *elastic.SearchHits,
	token *visibilityPageToken,
	pageSize int,
	scrollID string,
	isLastPage bool,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
		return nil, err
	}

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, len(searchHits.Hits))
	for i := 0; i < len(searchHits.Hits); i++ {
		response.Executions[i] = s.parseESDoc(searchHits.Hits[i], typeMap)
	}

	if len(searchHits.Hits) == pageSize && !isLastPage {
		nextPageToken, err := s.serializePageToken(&visibilityPageToken{ScrollID: scrollID})
		if err != nil {
			return nil, err
		}
		response.NextPageToken = make([]byte, len(nextPageToken))
		copy(response.NextPageToken, nextPageToken)
	}

	return response, nil
}

func (s *visibilityStore) getListWorkflowExecutionsResponse(
	searchResult *elastic.SearchResult,
	pageSize int,
	isRecordValid func(rec *visibility.VisibilityWorkflowExecutionInfo) bool,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
		return nil, err
	}

	response := &visibility.InternalListWorkflowExecutionsResponse{
		Executions: make([]*visibility.VisibilityWorkflowExecutionInfo, 0, len(searchResult.Hits.Hits)),
	}
	var lastHitSort []interface{}
	for _, hit := range searchResult.Hits.Hits {
		workflowExecutionInfo := s.parseESDoc(hit, typeMap)
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
		})
		if err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (s *visibilityStore) deserializePageToken(data []byte) (*visibilityPageToken, error) {
	if len(data) == 0 {
		return &visibilityPageToken{}, nil
	}

	var token *visibilityPageToken
	err := json.Unmarshal(data, &token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to deserialize page token: %v", err))
	}
	return token, nil
}

func (s *visibilityStore) serializePageToken(token *visibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to serialize page token: %v", err))
	}
	return data, nil
}

func (s *visibilityStore) generateESDoc(request *visibility.InternalVisibilityRequestBase, visibilityTaskKey string) map[string]interface{} {
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
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
	}

	searchAttributes, err := searchattribute.Decode(request.SearchAttributes, &typeMap)
	if err != nil {
		s.logger.Error("Unable to decode search attributes.", tag.Error(err))
	}
	for saName, saValue := range searchAttributes {
		doc[saName] = saValue
	}

	return doc
}

func (s *visibilityStore) parseESDoc(hit *elastic.SearchHit, saTypeMap searchattribute.NameTypeMap) *visibility.VisibilityWorkflowExecutionInfo {
	logParseError := func(fieldName string, fieldValue interface{}, err error, docID string) {
		s.logger.Error("Unable to parse Elasticsearch document field.", tag.Name(fieldName), tag.Value(fieldValue), tag.Error(err), tag.ESDocID(docID))
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchInvalidSearchAttributeCount)
	}

	var sourceMap map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(hit.Source))
	// Very important line. See finishParseJSONValue bellow.
	d.UseNumber()
	if err := d.Decode(&sourceMap); err != nil {
		s.logger.Error("Unable to JSON unmarshal Elasticsearch SearchHit.Source.", tag.Error(err), tag.ESDocID(hit.Id))
		return nil
	}

	var isValidType bool
	var memo []byte
	var memoEncoding string
	record := &visibility.VisibilityWorkflowExecutionInfo{}
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
				logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), hit.Id)
			}
			var err error
			if memo, err = base64.StdEncoding.DecodeString(memoStr); err != nil {
				logParseError(fieldName, memoStr[:10], err, hit.Id)
			}
			continue
		case searchattribute.MemoEncoding:
			if memoEncoding, isValidType = fieldValue.(string); !isValidType {
				logParseError(fieldName, fieldValue, fmt.Errorf("%w: expected string got %T", errUnexpectedJSONFieldType, fieldValue), hit.Id)
			}
			continue
		}

		fieldType, err := saTypeMap.GetType(fieldName)
		if err != nil {
			// Silently ignore ErrInvalidName because it indicates unknown field in Elasticsearch document.
			if !errors.Is(err, searchattribute.ErrInvalidName) {
				s.logger.Error("Unable to get type for Elasticsearch document field.", tag.Name(fieldName), tag.Error(err), tag.ESDocID(hit.Id))
			}
			continue
		}

		fieldValueParsed, err := finishParseJSONValue(fieldValue, fieldType)
		if err != nil {
			logParseError(fieldName, fieldValue, err, hit.Id)
			continue
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
			// All custom search attributes are handled here.
			if record.SearchAttributes == nil {
				record.SearchAttributes = map[string]interface{}{}
			}
			record.SearchAttributes[fieldName] = fieldValueParsed
		}
	}

	if memoEncoding != "" {
		record.Memo = persistence.NewDataBlob(memo, memoEncoding)
	} else if memo != nil {
		s.logger.Error("Field is missing in Elasticsearch document.", tag.Name(searchattribute.MemoEncoding), tag.ESDocID(hit.Id))
		s.metricsClient.IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchInvalidSearchAttributeCount)
	}

	return record
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
	case enumspb.INDEXED_VALUE_TYPE_STRING, enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_DATETIME:
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
