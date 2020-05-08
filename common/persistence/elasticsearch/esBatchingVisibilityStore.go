// Copyright (c) 2017 Uber Technologies, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic"
	"github.com/valyala/fastjson"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/definition"
	es "github.com/temporalio/temporal/common/elasticsearch"
	esbatcher "github.com/temporalio/temporal/common/elasticsearch/batcher"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/config"
)

type (
	esBatchingVisibilityStore struct {
		esClient    es.Client
		index       string
		logger      log.Logger
		config      *config.VisibilityConfig
		esProcessor *esbatcher.ESProcessor
	}
)

var _ p.VisibilityStore = (*esBatchingVisibilityStore)(nil)

// NewElasticSearchBatchingVisibilityStore create a visibility store connecting to ElasticSearch
func NewElasticSearchBatchingVisibilityStore(esClient es.Client, esProcessor *esbatcher.ESProcessor, index string, config *config.VisibilityConfig, logger log.Logger) p.VisibilityStore {
	return &esBatchingVisibilityStore{
		esClient:    esClient,
		index:       index,
		logger:      logger.WithTags(tag.ComponentESVisibilityManager),
		config:      config,
		esProcessor: esProcessor,
	}
}

func (v *esBatchingVisibilityStore) Close() {}

func (v *esBatchingVisibilityStore) GetName() string {
	return esPersistenceName
}

func (v *esBatchingVisibilityStore) RecordWorkflowExecutionStarted(request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	return (*v.esProcessor).AddUpsert(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.StartTimestamp,
		request.ExecutionTimestamp,
		request.TaskID,
		request.Memo.Data,
		request.Memo.Encoding,
		request.SearchAttributes,
	)
}

func (v *esBatchingVisibilityStore) UpsertWorkflowExecution(request *p.InternalUpsertWorkflowExecutionRequest) error {
	return (*v.esProcessor).AddUpsert(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.StartTimestamp,
		request.ExecutionTimestamp,
		request.TaskID,
		request.Memo.Data,
		request.Memo.Encoding,
		request.SearchAttributes,
	)
}

func (v *esBatchingVisibilityStore) RecordWorkflowExecutionClosed(request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	return (*v.esProcessor).AddDelete(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.TaskID,
	)
}

func (v *esBatchingVisibilityStore) ListOpenWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := true
	searchResult, err := v.getSearchResult(request, token, nil, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutions failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		startTime := rec.StartTime.UnixNano()
		return request.EarliestStartTime <= startTime && startTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) ListClosedWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	searchResult, err := v.getSearchResult(request, token, nil, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutions failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) ListOpenWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := true
	matchQuery := elastic.NewMatchQuery(es.WorkflowType, request.WorkflowTypeName)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByType failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		startTime := rec.StartTime.UnixNano()
		return request.EarliestStartTime <= startTime && startTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) ListClosedWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	matchQuery := elastic.NewMatchQuery(es.WorkflowType, request.WorkflowTypeName)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByType failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := true
	matchQuery := elastic.NewMatchQuery(es.WorkflowID, request.WorkflowID)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		startTime := rec.StartTime.UnixNano()
		return request.EarliestStartTime <= startTime && startTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	matchQuery := elastic.NewMatchQuery(es.WorkflowID, request.WorkflowID)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) ListClosedWorkflowExecutionsByStatus(
	request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	matchQuery := elastic.NewMatchQuery(es.CloseStatus, int32(request.Status))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByStatus failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esBatchingVisibilityStore) GetClosedWorkflowExecution(
	request *p.GetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {

	matchDomainQuery := elastic.NewMatchQuery(es.DomainID, request.DomainUUID)
	existClosedStatusQuery := elastic.NewExistsQuery(es.CloseStatus)
	matchWorkflowIDQuery := elastic.NewMatchQuery(es.WorkflowID, request.Execution.GetWorkflowId())
	boolQuery := elastic.NewBoolQuery().Must(matchDomainQuery).Must(existClosedStatusQuery).Must(matchWorkflowIDQuery)
	rid := request.Execution.GetRunId()
	if rid != "" {
		matchRunIDQuery := elastic.NewMatchQuery(es.RunID, rid)
		boolQuery = boolQuery.Must(matchRunIDQuery)
	}

	ctx := context.Background()
	params := &es.SearchParameters{
		Index: v.index,
		Query: boolQuery,
	}
	searchResult, err := v.esClient.Search(ctx, params)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetClosedWorkflowExecution failed. Error: %v", err))
	}

	response := &p.InternalGetClosedWorkflowExecutionResponse{}
	actualHits := searchResult.Hits.Hits
	if len(actualHits) == 0 {
		return response, nil
	}
	response.Execution = v.convertSearchResultToVisibilityRecord(actualHits[0])

	return response, nil
}

func (v *esBatchingVisibilityStore) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	// TODO becker
	return nil
}

func (v *esBatchingVisibilityStore) ListWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {

	checkPageSize(request)

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	queryDSL, err := v.getESQueryDSL(request, token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Error when parse query: %v", err))
	}

	ctx := context.Background()
	searchResult, err := v.esClient.SearchWithDSL(ctx, v.index, queryDSL)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListWorkflowExecutions failed. Error: %v", err))
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, nil)
}

func (v *esBatchingVisibilityStore) ScanWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {

	checkPageSize(request)

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	var searchResult *elastic.SearchResult
	var scrollService es.ScrollService
	if len(token.ScrollID) == 0 { // first call
		var queryDSL string
		queryDSL, err = getESQueryDSLForScan(request)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Error when parse query: %v", err))
		}
		searchResult, scrollService, err = v.esClient.ScrollFirstPage(ctx, v.index, queryDSL)
	} else {
		searchResult, scrollService, err = v.esClient.Scroll(ctx, token.ScrollID)
	}

	isLastPage := false
	if err == io.EOF { // no more result
		isLastPage = true
		scrollService.Clear(context.Background()) //nolint:errcheck
	} else if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ScanWorkflowExecutions failed. Error: %v", err))
	}

	return v.getScanWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, searchResult.ScrollId, isLastPage)
}

func (v *esBatchingVisibilityStore) CountWorkflowExecutions(request *p.CountWorkflowExecutionsRequest) (
	*p.CountWorkflowExecutionsResponse, error) {

	queryDSL, err := getESQueryDSLForCount(request)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Error when parse query: %v", err))
	}

	ctx := context.Background()
	count, err := v.esClient.Count(ctx, v.index, queryDSL)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CountWorkflowExecutions failed. Error: %v", err))
	}

	response := &p.CountWorkflowExecutionsResponse{Count: count}
	return response, nil
}

func (v *esBatchingVisibilityStore) getESQueryDSL(request *p.ListWorkflowExecutionsRequestV2, token *esVisibilityPageToken) (string, error) {
	sql := getSQLFromListRequest(request)
	dsl, err := getCustomizedDSLFromSQL(sql, request.DomainUUID)
	if err != nil {
		return "", err
	}

	sortField, err := v.processSortField(dsl)
	if err != nil {
		return "", err
	}

	if shouldSearchAfter(token) {
		valueOfSearchAfter, err := v.getValueOfSearchAfterInJSON(token, sortField)
		if err != nil {
			return "", err
		}
		dsl.Set(dslFieldSearchAfter, fastjson.MustParse(valueOfSearchAfter))
	} else { // use from+size
		dsl.Set(dslFieldFrom, fastjson.MustParse(strconv.Itoa(token.From)))
	}

	dslStr := cleanDSL(dsl.String())

	return dslStr, nil
}

func (v *esBatchingVisibilityStore) processSortField(dsl *fastjson.Value) (string, error) {
	isSorted := dsl.Exists(dslFieldSort)
	var sortField string

	if !isSorted { // set default sorting by StartTime desc
		dsl.Set(dslFieldSort, fastjson.MustParse(jsonSortForOpen))
		sortField = definition.StartTime
	} else { // user provide sorting using order by
		// sort validation on length
		if len(dsl.GetArray(dslFieldSort)) > 1 {
			return "", errors.New("only one field can be used to sort")
		}
		// sort validation to exclude IndexedValueTypeString
		obj, _ := dsl.GetArray(dslFieldSort)[0].Object()
		obj.Visit(func(k []byte, v *fastjson.Value) { // visit is only way to get object key in fastjson
			sortField = string(k)
		})
		if v.getFieldType(sortField) == enums.IndexedValueTypeString {
			return "", errors.New("not able to sort by IndexedValueTypeString field, use IndexedValueTypeKeyword field")
		}
		// add RunID as tie-breaker
		dsl.Get(dslFieldSort).Set("1", fastjson.MustParse(jsonSortWithTieBreaker))
	}

	return sortField, nil
}

func (v *esBatchingVisibilityStore) getFieldType(fieldName string) enums.IndexedValueType {
	if strings.HasPrefix(fieldName, definition.Attr) {
		fieldName = fieldName[len(definition.Attr)+1:] // remove prefix
	}
	validMap := v.config.ValidSearchAttributes()
	fieldType, ok := validMap[fieldName]
	if !ok {
		v.logger.Error("Unknown fieldName, validation should be done in frontend already", tag.Value(fieldName))
	}
	return common.ConvertIndexedValueTypeToProtoType(fieldType, v.logger)
}

func (v *esBatchingVisibilityStore) getValueOfSearchAfterInJSON(token *esVisibilityPageToken, sortField string) (string, error) {
	var sortVal interface{}
	var err error
	switch v.getFieldType(sortField) {
	case enums.IndexedValueTypeInt, enums.IndexedValueTypeDatetime, enums.IndexedValueTypeBool:
		sortVal, err = token.SortValue.(json.Number).Int64()
		if err != nil {
			err, ok := err.(*strconv.NumError) // field not present, ES will return big int +-9223372036854776000
			if !ok {
				return "", err
			}
			if err.Num[0] == '-' { // desc
				sortVal = math.MinInt64
			} else { // asc
				sortVal = math.MaxInt64
			}
		}
	case enums.IndexedValueTypeDouble:
		switch token.SortValue.(type) {
		case json.Number:
			sortVal, err = token.SortValue.(json.Number).Float64()
			if err != nil {
				return "", err
			}
		case string: // field not present, ES will return "-Infinity" or "Infinity"
			sortVal = fmt.Sprintf(`"%s"`, token.SortValue.(string))
		}
	case enums.IndexedValueTypeKeyword:
		if token.SortValue != nil {
			sortVal = fmt.Sprintf(`"%s"`, token.SortValue.(string))
		} else { // field not present, ES will return null (so token.SortValue is nil)
			sortVal = "null"
		}
	default:
		sortVal = token.SortValue
	}

	return fmt.Sprintf(`[%v, "%s"]`, sortVal, token.TieBreaker), nil
}

func (v *esBatchingVisibilityStore) getNextPageToken(token []byte) (*esVisibilityPageToken, error) {
	var result *esVisibilityPageToken
	var err error
	if len(token) > 0 {
		result, err = v.deserializePageToken(token)
		if err != nil {
			return nil, err
		}
	} else {
		result = &esVisibilityPageToken{}
	}
	return result, nil
}

func (v *esBatchingVisibilityStore) getSearchResult(request *p.ListWorkflowExecutionsRequest, token *esVisibilityPageToken,
	matchQuery *elastic.MatchQuery, isOpen bool) (*elastic.SearchResult, error) {

	matchDomainQuery := elastic.NewMatchQuery(es.DomainID, request.DomainUUID)
	existClosedStatusQuery := elastic.NewExistsQuery(es.CloseStatus)
	var rangeQuery *elastic.RangeQuery
	if isOpen {
		rangeQuery = elastic.NewRangeQuery(es.StartTime)
	} else {
		rangeQuery = elastic.NewRangeQuery(es.CloseTime)
	}
	// ElasticSearch v6 is unable to precisely compare time, have to manually add resolution 1ms to time range.
	// Also has to use string instead of int64 to avoid data conversion issue,
	// 9223372036854775807 to 9223372036854776000 (long overflow)
	if request.LatestStartTime > math.MaxInt64-oneMilliSecondInNano { // prevent latestTime overflow
		request.LatestStartTime = math.MaxInt64 - oneMilliSecondInNano
	}
	if request.EarliestStartTime < math.MinInt64+oneMilliSecondInNano { // prevent earliestTime overflow
		request.EarliestStartTime = math.MinInt64 + oneMilliSecondInNano
	}
	earliestTimeStr := strconv.FormatInt(request.EarliestStartTime-oneMilliSecondInNano, 10)
	latestTimeStr := strconv.FormatInt(request.LatestStartTime+oneMilliSecondInNano, 10)
	rangeQuery = rangeQuery.
		Gte(earliestTimeStr).
		Lte(latestTimeStr)

	boolQuery := elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery)
	if matchQuery != nil {
		boolQuery = boolQuery.Must(matchQuery)
	}
	if isOpen {
		boolQuery = boolQuery.MustNot(existClosedStatusQuery)
	} else {
		boolQuery = boolQuery.Must(existClosedStatusQuery)
	}

	ctx := context.Background()
	params := &es.SearchParameters{
		Index:    v.index,
		Query:    boolQuery,
		From:     token.From,
		PageSize: request.PageSize,
	}
	if isOpen {
		params.Sorter = append(params.Sorter, elastic.NewFieldSort(es.StartTime).Desc())
	} else {
		params.Sorter = append(params.Sorter, elastic.NewFieldSort(es.CloseTime).Desc())
	}
	params.Sorter = append(params.Sorter, elastic.NewFieldSort(es.RunID).Desc())

	if shouldSearchAfter(token) {
		params.SearchAfter = []interface{}{token.SortValue, token.TieBreaker}
	}

	return v.esClient.Search(ctx, params)
}

func (v *esBatchingVisibilityStore) getScanWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
	token *esVisibilityPageToken, pageSize int, scrollID string, isLastPage bool) (
	*p.InternalListWorkflowExecutionsResponse, error) {

	response := &p.InternalListWorkflowExecutionsResponse{}
	actualHits := searchHits.Hits
	numOfActualHits := len(actualHits)

	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	for i := 0; i < numOfActualHits; i++ {
		workflowExecutionInfo := v.convertSearchResultToVisibilityRecord(actualHits[i])
		response.Executions = append(response.Executions, workflowExecutionInfo)
	}

	if numOfActualHits == pageSize && !isLastPage {
		nextPageToken, err := v.serializePageToken(&esVisibilityPageToken{ScrollID: scrollID})
		if err != nil {
			return nil, err
		}
		response.NextPageToken = make([]byte, len(nextPageToken))
		copy(response.NextPageToken, nextPageToken)
	}

	return response, nil
}

func (v *esBatchingVisibilityStore) getListWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
	token *esVisibilityPageToken, pageSize int, isRecordValid func(rec *p.VisibilityWorkflowExecutionInfo) bool) (*p.InternalListWorkflowExecutionsResponse, error) {

	response := &p.InternalListWorkflowExecutionsResponse{}
	actualHits := searchHits.Hits
	numOfActualHits := len(actualHits)

	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	for i := 0; i < numOfActualHits; i++ {
		workflowExecutionInfo := v.convertSearchResultToVisibilityRecord(actualHits[i])
		if isRecordValid == nil || isRecordValid(workflowExecutionInfo) {
			// for old APIs like ListOpenWorkflowExecutions, we added 1 ms to range query to overcome ES limitation
			// (see getSearchResult function), but manually dropped records beyond request range here.
			response.Executions = append(response.Executions, workflowExecutionInfo)
		}
	}

	if numOfActualHits == pageSize { // this means the response is not the last page
		var nextPageToken []byte
		var err error

		// ES Search API support pagination using From and PageSize, but has limit that From+PageSize cannot exceed a threshold
		// to retrieve deeper pages, use ES SearchAfter
		if searchHits.TotalHits <= int64(v.config.ESIndexMaxResultWindow()-pageSize) { // use ES Search From+Size
			nextPageToken, err = v.serializePageToken(&esVisibilityPageToken{From: token.From + numOfActualHits})
		} else { // use ES Search After
			var sortVal interface{}
			sortVals := actualHits[len(response.Executions)-1].Sort
			sortVal = sortVals[0]
			tieBreaker := sortVals[1].(string)

			nextPageToken, err = v.serializePageToken(&esVisibilityPageToken{SortValue: sortVal, TieBreaker: tieBreaker})
		}
		if err != nil {
			return nil, err
		}

		response.NextPageToken = make([]byte, len(nextPageToken))
		copy(response.NextPageToken, nextPageToken)
	}

	return response, nil
}

func (v *esBatchingVisibilityStore) deserializePageToken(data []byte) (*esVisibilityPageToken, error) {
	var token esVisibilityPageToken
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err := dec.Decode(&token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to deserialize page token. err: %v", err))
	}
	return &token, nil
}

func (v *esBatchingVisibilityStore) serializePageToken(token *esVisibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to serialize page token. err: %v", err))
	}
	return data, nil
}

func (v *esBatchingVisibilityStore) convertSearchResultToVisibilityRecord(hit *elastic.SearchHit) *p.VisibilityWorkflowExecutionInfo {
	var source *visibilityRecord
	err := json.Unmarshal(*hit.Source, &source)
	if err != nil { // log and skip error
		v.logger.Error("unable to unmarshal search hit source",
			tag.Error(err), tag.ESDocID(hit.Id))
		return nil
	}

	record := &p.VisibilityWorkflowExecutionInfo{
		WorkflowID:       source.WorkflowID,
		RunID:            source.RunID,
		TypeName:         source.WorkflowType,
		StartTime:        time.Unix(0, source.StartTime),
		ExecutionTime:    time.Unix(0, source.ExecutionTime),
		Memo:             p.NewDataBlob(source.Memo, common.EncodingType(source.Encoding)),
		SearchAttributes: source.Attr,
	}
	if source.CloseTime != 0 {
		record.CloseTime = time.Unix(0, source.CloseTime)
		record.Status = &source.CloseStatus
		record.HistoryLength = source.HistoryLength
	}

	return record
}
