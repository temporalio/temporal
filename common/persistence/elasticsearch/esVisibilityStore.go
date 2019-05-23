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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cch123/elasticsql"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/uber/cadence/.gen/go/indexer"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
	"github.com/valyala/fastjson"
)

const (
	esPersistenceName = "elasticsearch"
)

type (
	esVisibilityStore struct {
		esClient es.Client
		index    string
		producer messaging.Producer
		logger   log.Logger
		config   *config.VisibilityConfig
	}

	esVisibilityPageToken struct {
		// for ES API From+Size
		From int
		// for ES API searchAfter
		SortTime   int64  // startTime or closeTime
		TieBreaker string // runID
		// for ES scroll API
		ScrollID string
	}

	visibilityRecord struct {
		WorkflowID    string
		RunID         string
		WorkflowType  string
		StartTime     int64
		ExecutionTime int64
		CloseTime     int64
		CloseStatus   workflow.WorkflowExecutionCloseStatus
		HistoryLength int64
		Memo          []byte
		Encoding      string
		Attr          map[string]interface{}
	}
)

var _ p.VisibilityStore = (*esVisibilityStore)(nil)

var (
	errOperationNotSupported = errors.New("operation not support")

	oneMilliSecondInNano = int64(1000)
)

// NewElasticSearchVisibilityStore create a visibility store connecting to ElasticSearch
func NewElasticSearchVisibilityStore(esClient es.Client, index string, producer messaging.Producer, config *config.VisibilityConfig, logger log.Logger) p.VisibilityStore {
	return &esVisibilityStore{
		esClient: esClient,
		index:    index,
		producer: producer,
		logger:   logger.WithTags(tag.ComponentESVisibilityManager),
		config:   config,
	}
}

func (v *esVisibilityStore) Close() {}

func (v *esVisibilityStore) GetName() string {
	return esPersistenceName
}

func (v *esVisibilityStore) RecordWorkflowExecutionStarted(request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	v.checkProducer()
	msg := getVisibilityMessageForOpenExecution(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.StartTimestamp,
		request.ExecutionTimestamp,
		request.TaskID,
		request.Memo.Data,
		request.Memo.GetEncoding(),
		request.SearchAttributes,
	)
	return v.producer.Publish(msg)
}

func (v *esVisibilityStore) RecordWorkflowExecutionClosed(request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	v.checkProducer()
	msg := getVisibilityMessageForCloseExecution(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.StartTimestamp,
		request.ExecutionTimestamp,
		request.CloseTimestamp,
		request.Status,
		request.HistoryLength,
		request.TaskID,
		request.Memo.Data,
		request.Memo.GetEncoding(),
		request.SearchAttributes,
	)
	return v.producer.Publish(msg)
}

func (v *esVisibilityStore) ListOpenWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := true
	searchResult, err := v.getSearchResult(request, token, nil, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutions failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	searchResult, err := v.getSearchResult(request, token, nil, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutions failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ListOpenWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := true
	matchQuery := elastic.NewMatchQuery(es.WorkflowType, request.WorkflowTypeName)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutionsByType failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	matchQuery := elastic.NewMatchQuery(es.WorkflowType, request.WorkflowTypeName)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutionsByType failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := true
	matchQuery := elastic.NewMatchQuery(es.WorkflowID, request.WorkflowID)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	matchQuery := elastic.NewMatchQuery(es.WorkflowID, request.WorkflowID)
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutionsByStatus(
	request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	isOpen := false
	matchQuery := elastic.NewMatchQuery(es.CloseStatus, int32(request.Status))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, matchQuery, isOpen)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutionsByStatus failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) GetClosedWorkflowExecution(
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
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetClosedWorkflowExecution failed. Error: %v", err),
		}
	}

	response := &p.InternalGetClosedWorkflowExecutionResponse{}
	actualHits := searchResult.Hits.Hits
	if len(actualHits) == 0 {
		return response, nil
	}
	response.Execution = v.convertSearchResultToVisibilityRecord(actualHits[0], false)

	return response, nil
}

func (v *esVisibilityStore) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	v.checkProducer()
	msg := getVisibilityMessageForDeletion(
		request.DomainID,
		request.WorkflowID,
		request.RunID,
		request.TaskID,
	)
	return v.producer.Publish(msg)
}

func (v *esVisibilityStore) ListWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {

	checkPageSize(request)

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	queryDSL, isOpen, err := getESQueryDSL(request, token)
	if err != nil {
		return nil, &workflow.BadRequestError{Message: fmt.Sprintf("Error when parse query: %v", err)}
	}

	ctx := context.Background()
	searchResult, err := v.esClient.SearchWithDSL(ctx, v.index, queryDSL)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListWorkflowExecutions failed. Error: %v", err),
		}
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, isOpen, request.PageSize)
}

func (v *esVisibilityStore) ScanWorkflowExecutions(
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
		queryDSL, _, err := getESQueryDSLForScan(request, token)
		if err != nil {
			return nil, &workflow.BadRequestError{Message: fmt.Sprintf("Error when parse query: %v", err)}
		}
		searchResult, scrollService, err = v.esClient.ScrollFirstPage(ctx, v.index, queryDSL)
	} else {
		searchResult, scrollService, err = v.esClient.Scroll(ctx, token.ScrollID)
	}

	isLastPage := false
	if err == io.EOF { // no more result
		isLastPage = true
		scrollService.Clear(context.Background())
	} else if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ScanWorkflowExecutions failed. Error: %v", err),
		}
	}

	return v.getScanWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, searchResult.ScrollId, isLastPage)
}

func (v *esVisibilityStore) CountWorkflowExecutions(request *p.CountWorkflowExecutionsRequest) (
	*p.CountWorkflowExecutionsResponse, error) {

	queryDSL, err := getESQueryDSLForCount(request)
	if err != nil {
		return nil, &workflow.BadRequestError{Message: fmt.Sprintf("Error when parse query: %v", err)}
	}

	ctx := context.Background()
	count, err := v.esClient.Count(ctx, v.index, queryDSL)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CountWorkflowExecutions failed. Error: %v", err),
		}
	}

	response := &p.CountWorkflowExecutionsResponse{Count: count}
	return response, nil
}

const (
	jsonMissingCloseTime     = `{"missing":{"field":"CloseTime"}}`
	jsonRangeOnExecutionTime = `{"range":{"ExecutionTime":`
	jsonSortForOpen          = `[{"StartTime":"desc"},{"WorkflowID":"desc"}]`
	jsonSortForClose         = `[{"CloseTime":"desc"},{"WorkflowID":"desc"}]`
	jsonSortWithTieBreaker   = `{"WorkflowID":"desc"}`

	dslFieldSort        = "sort"
	dslFieldSearchAfter = "search_after"
	dslFieldFrom        = "from"
	dslFieldSize        = "size"
	dslFieldMust        = "must"

	defaultDateTimeFormat = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
)

var (
	timeKeys = map[string]bool{
		"StartTime":     true,
		"CloseTime":     true,
		"ExecutionTime": true,
	}
	rangeKeys = map[string]bool{
		"from": true,
		"to":   true,
		"gt":   true,
		"lt":   true,
	}
)

func getESQueryDSLForScan(request *p.ListWorkflowExecutionsRequestV2, token *esVisibilityPageToken) (string, bool, error) {
	return getESQueryDSLHelper(request, token, true)
}

func getESQueryDSL(request *p.ListWorkflowExecutionsRequestV2, token *esVisibilityPageToken) (string, bool, error) {
	return getESQueryDSLHelper(request, token, false)
}

func getESQueryDSLForCount(request *p.CountWorkflowExecutionsRequest) (string, error) {
	sql := getSQLFromCountRequest(request)
	dsl, _, err := getCustomizedDSLFromSQL(sql, request.DomainUUID)
	if err != nil {
		return "", err
	}

	// remove not needed fields
	dsl.Del(dslFieldFrom)
	dsl.Del(dslFieldSize)
	dsl.Del(dslFieldSort)

	return dsl.String(), nil
}

func getESQueryDSLHelper(request *p.ListWorkflowExecutionsRequestV2, token *esVisibilityPageToken, isScan bool) (string, bool, error) {
	sql := getSQLFromListRequest(request)
	dsl, isOpen, err := getCustomizedDSLFromSQL(sql, request.DomainUUID)
	if err != nil {
		return "", false, err
	}

	if isScan { // no need to sort for scan
		dsl.Del(dslFieldSort)
		return dsl.String(), isOpen, nil
	}

	isSorted := dsl.Exists(dslFieldSort)
	if !isSorted { // set default sorting
		if isOpen {
			dsl.Set(dslFieldSort, fastjson.MustParse(jsonSortForOpen))
		} else {
			dsl.Set(dslFieldSort, fastjson.MustParse(jsonSortForClose))
		}
	} else {
		// add WorkflowID as tie-breaker
		dsl.Get(dslFieldSort).Set("1", fastjson.MustParse(jsonSortWithTieBreaker))
	}

	if shouldSearchAfter(token) {
		dsl.Set(dslFieldSearchAfter, fastjson.MustParse(getValueOfSearchAfterInJSON(token)))
	} else { // use from+size
		dsl.Set(dslFieldFrom, fastjson.MustParse(strconv.Itoa(token.From)))
	}
	return dsl.String(), isOpen, nil
}

func getSQLFromListRequest(request *p.ListWorkflowExecutionsRequestV2) string {
	var sql string
	if strings.TrimSpace(request.Query) == "" {
		sql = fmt.Sprintf("select * from dumy limit %d", request.PageSize)
	} else {
		sql = fmt.Sprintf("select * from dumy where %s limit %d", request.Query, request.PageSize)
	}
	return sql
}

func getSQLFromCountRequest(request *p.CountWorkflowExecutionsRequest) string {
	var sql string
	if strings.TrimSpace(request.Query) == "" {
		sql = "select * from dumy"
	} else {
		sql = fmt.Sprintf("select * from dumy where %s", request.Query)
	}
	return sql
}

func getCustomizedDSLFromSQL(sql string, domainID string) (*fastjson.Value, bool, error) {
	dslStr, _, err := elasticsql.Convert(sql)
	if err != nil {
		return nil, false, err
	}
	dsl := fastjson.MustParse(dslStr) // dsl.String() will be a compact json without spaces

	dslStr = dsl.String()
	isOpen := strings.Contains(dslStr, jsonMissingCloseTime)
	if isOpen {
		dsl = replaceQueryForOpen(dsl)
	}
	if strings.Contains(dslStr, jsonRangeOnExecutionTime) {
		addQueryForExecutionTime(dsl)
	}
	addDomainToQuery(dsl, domainID)
	if err := processAllValuesForKey(dsl, timeKeyFilter, timeProcessFunc); err != nil {
		return nil, false, err
	}
	return dsl, isOpen, nil
}

// ES v6 only accepts "must_not exists" query instead of "missing" query, but elasticsql will produce "missing",
// so use this func to replace.
// Note it also means a temp limitation that we cannot support field missing search
func replaceQueryForOpen(dsl *fastjson.Value) *fastjson.Value {
	re := regexp.MustCompile(`(,)?` + jsonMissingCloseTime + `(,)?`)
	newDslStr := re.ReplaceAllString(dsl.String(), "")
	dsl = fastjson.MustParse(newDslStr)
	dsl.Get("query").Get("bool").Set("must_not", fastjson.MustParse(`{"exists": {"field": "CloseTime"}}`))
	return dsl
}

func addQueryForExecutionTime(dsl *fastjson.Value) {
	executionTimeQueryString := `{"range" : {"ExecutionTime" : {"gt" : "0"}}}`
	addMustQuery(dsl, executionTimeQueryString)
}

func addDomainToQuery(dsl *fastjson.Value, domainID string) {
	if len(domainID) == 0 {
		return
	}

	domainQueryString := fmt.Sprintf(`{"match_phrase":{"DomainID":{"query":"%s"}}}`, domainID)
	addMustQuery(dsl, domainQueryString)
}

func addMustQuery(dsl *fastjson.Value, queryString string) {
	valOfBool := dsl.Get("query", "bool")
	if valOfMust := valOfBool.Get(dslFieldMust); valOfMust == nil {
		valOfBool.Set(dslFieldMust, fastjson.MustParse(fmt.Sprintf("[%s]", queryString)))
	} else {
		valOfMust.SetArrayItem(len(valOfMust.GetArray()), fastjson.MustParse(queryString))
	}
}

func shouldSearchAfter(token *esVisibilityPageToken) bool {
	return token.SortTime != 0 && token.TieBreaker != ""
}

func getValueOfSearchAfterInJSON(token *esVisibilityPageToken) string {
	return fmt.Sprintf(`[%v, "%s"]`, token.SortTime, token.TieBreaker)
}

func (v *esVisibilityStore) checkProducer() {
	if v.producer == nil {
		// must be bug, check history setup
		panic("message producer is nil")
	}
}

func (v *esVisibilityStore) getNextPageToken(token []byte) (*esVisibilityPageToken, error) {
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

func (v *esVisibilityStore) getSearchResult(request *p.ListWorkflowExecutionsRequest, token *esVisibilityPageToken,
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
	rangeQuery = rangeQuery.
		Gte(request.EarliestStartTime - oneMilliSecondInNano).
		Lte(request.LatestStartTime + oneMilliSecondInNano)

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
		params.SearchAfter = []interface{}{token.SortTime, token.TieBreaker}
	}

	return v.esClient.Search(ctx, params)
}

func (v *esVisibilityStore) getScanWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
	token *esVisibilityPageToken, pageSize int, scrollID string, isLastPage bool) (
	*p.InternalListWorkflowExecutionsResponse, error) {

	response := &p.InternalListWorkflowExecutionsResponse{}
	actualHits := searchHits.Hits
	numOfActualHits := len(actualHits)

	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	for i := 0; i < numOfActualHits; i++ {
		workflowExecutionInfo := v.convertSearchResultToVisibilityRecord(actualHits[i], false)
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

func (v *esVisibilityStore) getListWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
	token *esVisibilityPageToken, isOpen bool, pageSize int) (*p.InternalListWorkflowExecutionsResponse, error) {

	response := &p.InternalListWorkflowExecutionsResponse{}
	actualHits := searchHits.Hits
	numOfActualHits := len(actualHits)

	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	for i := 0; i < numOfActualHits; i++ {
		workflowExecutionInfo := v.convertSearchResultToVisibilityRecord(actualHits[i], isOpen)
		response.Executions = append(response.Executions, workflowExecutionInfo)
	}

	if numOfActualHits == pageSize { // this means the response is not the last page
		var nextPageToken []byte
		var err error

		// ES Search API support pagination using From and PageSize, but has limit that From+PageSize cannot exceed a threshold
		// to retrieve deeper pages, use ES SearchAfter
		if searchHits.TotalHits <= int64(v.config.ESIndexMaxResultWindow()) { // use ES Search From+Size
			nextPageToken, err = v.serializePageToken(&esVisibilityPageToken{From: token.From + numOfActualHits})
		} else { // use ES Search After
			lastExecution := response.Executions[len(response.Executions)-1]
			var sortTime int64
			if isOpen {
				sortTime = lastExecution.StartTime.UnixNano()
			} else {
				sortTime = lastExecution.CloseTime.UnixNano()
			}
			nextPageToken, err = v.serializePageToken(&esVisibilityPageToken{SortTime: sortTime, TieBreaker: lastExecution.RunID})
		}
		if err != nil {
			return nil, err
		}

		response.NextPageToken = make([]byte, len(nextPageToken))
		copy(response.NextPageToken, nextPageToken)
	}

	return response, nil
}

func (v *esVisibilityStore) deserializePageToken(data []byte) (*esVisibilityPageToken, error) {
	var token esVisibilityPageToken
	err := json.Unmarshal(data, &token)
	if err != nil {
		return nil, &workflow.BadRequestError{
			Message: fmt.Sprintf("unable to deserialize page token. err: %v", err),
		}
	}
	return &token, nil
}

func (v *esVisibilityStore) serializePageToken(token *esVisibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, &workflow.BadRequestError{
			Message: fmt.Sprintf("unable to serialize page token. err: %v", err),
		}
	}
	return data, nil
}

func (v *esVisibilityStore) convertSearchResultToVisibilityRecord(hit *elastic.SearchHit, isOpen bool) *p.VisibilityWorkflowExecutionInfo {
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
	if !isOpen {
		record.CloseTime = time.Unix(0, source.CloseTime)
		record.Status = &source.CloseStatus
		record.HistoryLength = source.HistoryLength
	}

	return record
}

func getVisibilityMessageForOpenExecution(domainID string, wid, rid string, workflowTypeName string,
	startTimeUnixNano, executionTimeUnixNano int64, taskID int64, memo []byte, encoding common.EncodingType,
	searchAttributes map[string][]byte) *indexer.Message {

	msgType := indexer.MessageTypeIndex
	fields := map[string]*indexer.Field{
		es.WorkflowType:  {Type: &es.FieldTypeString, StringData: common.StringPtr(workflowTypeName)},
		es.StartTime:     {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(startTimeUnixNano)},
		es.ExecutionTime: {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(executionTimeUnixNano)},
	}
	if len(memo) != 0 {
		fields[es.Memo] = &indexer.Field{Type: &es.FieldTypeBinary, BinaryData: memo}
		fields[es.Encoding] = &indexer.Field{Type: &es.FieldTypeString, StringData: common.StringPtr(string(encoding))}
	}
	for k, v := range searchAttributes {
		fields[k] = &indexer.Field{Type: &es.FieldTypeBinary, BinaryData: v}
	}

	msg := &indexer.Message{
		MessageType: &msgType,
		DomainID:    common.StringPtr(domainID),
		WorkflowID:  common.StringPtr(wid),
		RunID:       common.StringPtr(rid),
		Version:     common.Int64Ptr(taskID),
		Fields:      fields,
	}
	return msg
}

func getVisibilityMessageForCloseExecution(domainID string, wid, rid string, workflowTypeName string,
	startTimeUnixNano int64, executionTimeUnixNano int64, endTimeUnixNano int64, closeStatus workflow.WorkflowExecutionCloseStatus,
	historyLength int64, taskID int64, memo []byte, encoding common.EncodingType,
	searchAttributes map[string][]byte) *indexer.Message {

	msgType := indexer.MessageTypeIndex
	fields := map[string]*indexer.Field{
		es.WorkflowType:  {Type: &es.FieldTypeString, StringData: common.StringPtr(workflowTypeName)},
		es.StartTime:     {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(startTimeUnixNano)},
		es.ExecutionTime: {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(executionTimeUnixNano)},
		es.CloseTime:     {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(endTimeUnixNano)},
		es.CloseStatus:   {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(int64(closeStatus))},
		es.HistoryLength: {Type: &es.FieldTypeInt, IntData: common.Int64Ptr(historyLength)},
	}
	if len(memo) != 0 {
		fields[es.Memo] = &indexer.Field{Type: &es.FieldTypeBinary, BinaryData: memo}
		fields[es.Encoding] = &indexer.Field{Type: &es.FieldTypeString, StringData: common.StringPtr(string(encoding))}
	}
	for k, v := range searchAttributes {
		fields[k] = &indexer.Field{Type: &es.FieldTypeBinary, BinaryData: v}
	}

	msg := &indexer.Message{
		MessageType: &msgType,
		DomainID:    common.StringPtr(domainID),
		WorkflowID:  common.StringPtr(wid),
		RunID:       common.StringPtr(rid),
		Version:     common.Int64Ptr(taskID),
		Fields:      fields,
	}
	return msg
}

func getVisibilityMessageForDeletion(domainID, workflowID, runID string, docVersion int64) *indexer.Message {
	msgType := indexer.MessageTypeDelete
	msg := &indexer.Message{
		MessageType: &msgType,
		DomainID:    common.StringPtr(domainID),
		WorkflowID:  common.StringPtr(workflowID),
		RunID:       common.StringPtr(runID),
		Version:     common.Int64Ptr(docVersion),
	}
	return msg
}

func checkPageSize(request *p.ListWorkflowExecutionsRequestV2) {
	if request.PageSize == 0 {
		request.PageSize = 1000
	}
}

func processAllValuesForKey(dsl *fastjson.Value, keyFilter func(k string) bool,
	processFunc func(obj *fastjson.Object, key string, v *fastjson.Value) error,
) error {
	switch dsl.Type() {
	case fastjson.TypeArray:
		for _, val := range dsl.GetArray() {
			if err := processAllValuesForKey(val, keyFilter, processFunc); err != nil {
				return err
			}
		}
	case fastjson.TypeObject:
		objectVal := dsl.GetObject()
		keys := []string{}
		objectVal.Visit(func(key []byte, val *fastjson.Value) {
			keys = append(keys, string(key))
		})

		for _, key := range keys {
			var err error
			val := objectVal.Get(key)
			if keyFilter(key) {
				err = processFunc(objectVal, key, val)
			} else {
				err = processAllValuesForKey(val, keyFilter, processFunc)
			}
			if err != nil {
				return err
			}
		}
	default:
		// do nothing, since there's no key
	}
	return nil
}

func timeKeyFilter(key string) bool {
	return timeKeys[key]
}

func timeProcessFunc(obj *fastjson.Object, key string, value *fastjson.Value) error {
	return processAllValuesForKey(value, func(key string) bool {
		return rangeKeys[key]
	}, func(obj *fastjson.Object, key string, v *fastjson.Value) error {
		timeStr := string(v.GetStringBytes())

		// first check if already in int64 format
		if _, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			return nil
		}

		// try to parse time
		parsedTime, err := time.Parse(defaultDateTimeFormat, timeStr)
		if err != nil {
			return err
		}

		obj.Set(key, fastjson.MustParse(fmt.Sprintf(`"%v"`, parsedTime.UnixNano())))
		return nil
	})
}
