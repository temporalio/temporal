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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cch123/elasticsql"
	"github.com/olivere/elastic/v7"
	"github.com/valyala/fastjson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	es "go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/service/config"
)

const (
	persistenceName = "elasticsearch"

	delimiter = "~"
)

type (
	esVisibilityStore struct {
		esClient      es.Client
		index         string
		logger        log.Logger
		config        *config.VisibilityConfig
		metricsClient metrics.Client
		processor     Processor
	}

	esVisibilityPageToken struct {
		// for ES API From+Size
		From int
		// for ES API searchAfter
		SortValue  interface{}
		TieBreaker string // runID
		// for ES scroll API
		ScrollID string
	}

	visibilityRecord struct {
		WorkflowID      string
		RunID           string
		WorkflowType    string
		StartTime       int64
		ExecutionTime   int64
		CloseTime       int64
		ExecutionStatus enumspb.WorkflowExecutionStatus
		HistoryLength   int64
		Memo            []byte
		Encoding        string
		TaskQueue       string
		Attr            map[string]interface{}
	}
)

var _ p.VisibilityStore = (*esVisibilityStore)(nil)

var (
	oneMilliSecondInNano = int64(1000)
)

// NewElasticSearchVisibilityStore create a visibility store connecting to ElasticSearch
func NewElasticSearchVisibilityStore(
	esClient es.Client,
	index string,
	processor Processor,
	cfg *config.VisibilityConfig,
	logger log.Logger,
	metricsClient metrics.Client,
) *esVisibilityStore {

	return &esVisibilityStore{
		esClient:      esClient,
		index:         index,
		processor:     processor,
		logger:        logger.WithTags(tag.ComponentESVisibilityManager),
		config:        cfg,
		metricsClient: metricsClient,
	}
}

func (v *esVisibilityStore) Close() {
	// TODO (alex): esVisibilityStore shouldn't Stop processor. Processor should be stopped where it is created.
	if v.processor != nil {
		v.processor.Stop()
	}
}

func (v *esVisibilityStore) GetName() string {
	return persistenceName
}

func (v *esVisibilityStore) RecordWorkflowExecutionStarted(request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := v.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	return v.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (v *esVisibilityStore) RecordWorkflowExecutionClosed(request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := v.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	doc[searchattribute.CloseTime] = request.CloseTimestamp
	doc[searchattribute.HistoryLength] = request.HistoryLength

	return v.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (v *esVisibilityStore) UpsertWorkflowExecution(request *p.InternalUpsertWorkflowExecutionRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := v.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	return v.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (v *esVisibilityStore) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	docID := getDocID(request.WorkflowID, request.RunID)

	bulkDeleteRequest := &es.BulkableRequest{
		Index:       v.index,
		ID:          docID,
		Version:     request.TaskID,
		RequestType: es.BulkableRequestTypeDelete,
	}

	return v.addBulkRequestAndWait(bulkDeleteRequest, docID)
}

func getDocID(workflowID string, runID string) string {
	return fmt.Sprintf("%s%s%s", workflowID, delimiter, runID)
}

func getVisibilityTaskKey(shardID int32, taskID int64) string {
	return fmt.Sprintf("%d%s%d", shardID, delimiter, taskID)
}

func (v *esVisibilityStore) addBulkIndexRequestAndWait(
	request *p.InternalVisibilityRequestBase,
	esDoc map[string]interface{},
	visibilityTaskKey string,
) error {
	bulkIndexRequest := &es.BulkableRequest{
		Index:       v.index,
		ID:          getDocID(request.WorkflowID, request.RunID),
		Version:     request.TaskID,
		RequestType: es.BulkableRequestTypeIndex,
		Doc:         esDoc,
	}

	return v.addBulkRequestAndWait(bulkIndexRequest, visibilityTaskKey)
}

func (v *esVisibilityStore) addBulkRequestAndWait(bulkRequest *es.BulkableRequest, visibilityTaskKey string) error {
	v.checkProcessor()

	ackCh := make(chan bool, 1)
	v.processor.Add(bulkRequest, visibilityTaskKey, ackCh)
	ackTimeoutTimer := time.NewTimer(v.config.ESProcessorAckTimeout())
	defer ackTimeoutTimer.Stop()

	select {
	case ack := <-ackCh:
		if !ack {
			return newVisibilityTaskNAckError(visibilityTaskKey)
		}
		return nil
	case <-ackTimeoutTimer.C:
		return newVisibilityTaskAckTimeoutError(visibilityTaskKey, v.config.ESProcessorAckTimeout())
	}
}

func (v *esVisibilityStore) generateESDoc(request *p.InternalVisibilityRequestBase, visibilityTaskKey string) map[string]interface{} {
	doc := map[string]interface{}{
		searchattribute.VisibilityTaskKey: visibilityTaskKey,
		searchattribute.NamespaceID:       request.NamespaceID,
		searchattribute.WorkflowID:        request.WorkflowID,
		searchattribute.RunID:             request.RunID,
		searchattribute.WorkflowType:      request.WorkflowTypeName,
		searchattribute.StartTime:         request.StartTimestamp,
		searchattribute.ExecutionTime:     request.ExecutionTimestamp,
		searchattribute.ExecutionStatus:   request.Status,
		searchattribute.TaskQueue:         request.TaskQueue,
	}

	if len(request.Memo.GetData()) > 0 {
		doc[searchattribute.Memo] = request.Memo.GetData()
		doc[searchattribute.Encoding] = request.Memo.GetEncodingType().String()
	}

	typeMap, err := searchattribute.BuildTypeMap(v.config.ValidSearchAttributes)
	if err != nil {
		v.logger.Error("Unable to parse search attributes from config.", tag.Error(err))
		v.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}

	attr, err := searchattribute.Decode(request.SearchAttributes, typeMap)
	if err != nil {
		v.logger.Error("Unable to decode search attributes.", tag.Error(err))
		v.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}
	doc[searchattribute.Attr] = attr

	return doc
}

func (v *esVisibilityStore) ListOpenWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := v.getSearchResult(request, token, query, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutions failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		startTime := rec.StartTime.UnixNano()
		return request.EarliestStartTime <= startTime && startTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	executionStatusQuery := elastic.NewBoolQuery().MustNot(elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := v.getSearchResult(request, token, executionStatusQuery, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutions failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) ListOpenWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(es.WorkflowType, request.WorkflowTypeName)).
		Must(elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByType failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		startTime := rec.StartTime.UnixNano()
		return request.EarliestStartTime <= startTime && startTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(es.WorkflowType, request.WorkflowTypeName)).
		MustNot(elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByType failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(es.WorkflowID, request.WorkflowID)).
		Must(elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		startTime := rec.StartTime.UnixNano()
		return request.EarliestStartTime <= startTime && startTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(es.WorkflowID, request.WorkflowID)).
		MustNot(elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) ListClosedWorkflowExecutionsByStatus(
	request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {

	token, err := v.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(es.ExecutionStatus, int32(request.Status)))
	searchResult, err := v.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByStatus failed. Error: %v", err))
	}

	isRecordValid := func(rec *p.VisibilityWorkflowExecutionInfo) bool {
		closeTime := rec.CloseTime.UnixNano()
		return request.EarliestStartTime <= closeTime && closeTime <= request.LatestStartTime
	}

	return v.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (v *esVisibilityStore) GetClosedWorkflowExecution(
	request *p.GetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {

	matchNamespaceQuery := elastic.NewMatchQuery(es.NamespaceID, request.NamespaceID)
	executionStatusQuery := elastic.NewMatchQuery(es.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	matchWorkflowIDQuery := elastic.NewMatchQuery(es.WorkflowID, request.Execution.GetWorkflowId())
	boolQuery := elastic.NewBoolQuery().Must(matchNamespaceQuery).MustNot(executionStatusQuery).Must(matchWorkflowIDQuery)
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

func (v *esVisibilityStore) ListWorkflowExecutions(
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
		_ = scrollService.Clear(context.Background())
	} else if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ScanWorkflowExecutions failed. Error: %v", err))
	}

	return v.getScanWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, searchResult.ScrollId, isLastPage)
}

func (v *esVisibilityStore) CountWorkflowExecutions(request *p.CountWorkflowExecutionsRequest) (
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

const (
	jsonMissingCloseTime     = `{"missing":{"field":"CloseTime"}}`
	jsonRangeOnExecutionTime = `{"range":{"ExecutionTime":`
	jsonSortForOpen          = `[{"StartTime":"desc"},{"RunId":"desc"}]`
	jsonSortWithTieBreaker   = `{"RunId":"desc"}`

	dslFieldSort        = "sort"
	dslFieldSearchAfter = "search_after"
	dslFieldFrom        = "from"
	dslFieldSize        = "size"

	defaultDateTimeFormat = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
)

var (
	timeKeys = map[string]bool{
		"StartTime":     true,
		"CloseTime":     true,
		"ExecutionTime": true,
	}
	rangeKeys = map[string]bool{
		"from":  true,
		"to":    true,
		"gt":    true,
		"lt":    true,
		"query": true,
	}
)

func getESQueryDSLForScan(request *p.ListWorkflowExecutionsRequestV2) (string, error) {
	sql := getSQLFromListRequest(request)
	dsl, err := getCustomizedDSLFromSQL(sql, request.NamespaceID)
	if err != nil {
		return "", err
	}

	// remove not needed fields
	dsl.Del(dslFieldSort)
	return dsl.String(), nil
}

func getESQueryDSLForCount(request *p.CountWorkflowExecutionsRequest) (string, error) {
	sql := getSQLFromCountRequest(request)
	dsl, err := getCustomizedDSLFromSQL(sql, request.NamespaceID)
	if err != nil {
		return "", err
	}

	// remove not needed fields
	dsl.Del(dslFieldFrom)
	dsl.Del(dslFieldSize)
	dsl.Del(dslFieldSort)

	return dsl.String(), nil
}

func (v *esVisibilityStore) getESQueryDSL(request *p.ListWorkflowExecutionsRequestV2, token *esVisibilityPageToken) (string, error) {
	sql := getSQLFromListRequest(request)
	dsl, err := getCustomizedDSLFromSQL(sql, request.NamespaceID)
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

func getSQLFromListRequest(request *p.ListWorkflowExecutionsRequestV2) string {
	var sql string
	query := strings.TrimSpace(request.Query)
	if query == "" {
		sql = fmt.Sprintf("select * from dummy limit %d", request.PageSize)
	} else if common.IsJustOrderByClause(query) {
		sql = fmt.Sprintf("select * from dummy %s limit %d", request.Query, request.PageSize)
	} else {
		sql = fmt.Sprintf("select * from dummy where %s limit %d", request.Query, request.PageSize)
	}
	return sql
}

func getSQLFromCountRequest(request *p.CountWorkflowExecutionsRequest) string {
	var sql string
	if strings.TrimSpace(request.Query) == "" {
		sql = "select * from dummy"
	} else {
		sql = fmt.Sprintf("select * from dummy where %s", request.Query)
	}
	return sql
}

func getCustomizedDSLFromSQL(sql string, namespaceID string) (*fastjson.Value, error) {
	dslStr, _, err := elasticsql.Convert(sql)
	if err != nil {
		return nil, err
	}
	dsl, err := fastjson.Parse(dslStr) // dsl.String() will be a compact json without spaces
	if err != nil {
		return nil, err
	}
	dslStr = dsl.String()
	if strings.Contains(dslStr, jsonMissingCloseTime) { // isOpen
		dsl = replaceQueryForOpen(dsl)
	}
	if strings.Contains(dslStr, jsonRangeOnExecutionTime) {
		addQueryForExecutionTime(dsl)
	}
	addNamespaceToQuery(dsl, namespaceID)
	if err := processAllValuesForKey(dsl, timeKeyFilter, timeProcessFunc); err != nil {
		return nil, err
	}
	return dsl, nil
}

// ES v6 only accepts "must_not exists" query instead of "missing" query, but elasticsql produces "missing",
// so use this func to replace.
// Note it also means a temp limitation that we cannot support field missing search
func replaceQueryForOpen(dsl *fastjson.Value) *fastjson.Value {
	re := regexp.MustCompile(jsonMissingCloseTime)
	newDslStr := re.ReplaceAllString(dsl.String(), `{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}`)
	dsl = fastjson.MustParse(newDslStr)
	return dsl
}

func addQueryForExecutionTime(dsl *fastjson.Value) {
	executionTimeQueryString := `{"range" : {"ExecutionTime" : {"gt" : "0"}}}`
	addMustQuery(dsl, executionTimeQueryString)
}

func addNamespaceToQuery(dsl *fastjson.Value, namespaceID string) {
	if len(namespaceID) == 0 {
		return
	}

	namespaceQueryString := fmt.Sprintf(`{"match_phrase":{"NamespaceId":{"query":"%s"}}}`, namespaceID)
	addMustQuery(dsl, namespaceQueryString)
}

// addMustQuery is wrapping bool query with new bool query with must,
// reason not making a flat bool query is to ensure "should (or)" query works correctly in query context.
func addMustQuery(dsl *fastjson.Value, queryString string) {
	valOfTopQuery := dsl.Get("query")
	valOfBool := dsl.Get("query", "bool")
	newValOfBool := fmt.Sprintf(`{"must":[%s,{"bool":%s}]}`, queryString, valOfBool.String())
	valOfTopQuery.Set("bool", fastjson.MustParse(newValOfBool))
}

func (v *esVisibilityStore) processSortField(dsl *fastjson.Value) (string, error) {
	isSorted := dsl.Exists(dslFieldSort)
	var sortField string

	if !isSorted { // set default sorting by StartTime desc
		dsl.Set(dslFieldSort, fastjson.MustParse(jsonSortForOpen))
		sortField = searchattribute.StartTime
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
		if v.getFieldType(sortField) == enumspb.INDEXED_VALUE_TYPE_STRING {
			return "", errors.New("unable to sort by field of String type, use field of type Keyword")
		}
		// add RunID as tie-breaker
		dsl.Get(dslFieldSort).Set("1", fastjson.MustParse(jsonSortWithTieBreaker))
	}

	return sortField, nil
}

func (v *esVisibilityStore) getFieldType(fieldName string) enumspb.IndexedValueType {
	if strings.HasPrefix(fieldName, searchattribute.Attr) {
		fieldName = fieldName[len(searchattribute.Attr)+1:] // remove prefix
	}
	typeMap, err := searchattribute.BuildTypeMap(v.config.ValidSearchAttributes)
	if err != nil {
		v.logger.Error("Unable to parse search attributes from config.", tag.Error(err))
	}

	fieldType, err := searchattribute.GetType(fieldName, typeMap)
	if err != nil {
		v.logger.Error("Unable to get search attribute type.", tag.Value(fieldName), tag.Error(err))
	}
	return fieldType
}

func shouldSearchAfter(token *esVisibilityPageToken) bool {
	return token.TieBreaker != ""
}

func (v *esVisibilityStore) getValueOfSearchAfterInJSON(token *esVisibilityPageToken, sortField string) (string, error) {
	var sortVal interface{}
	var err error
	switch v.getFieldType(sortField) {
	case enumspb.INDEXED_VALUE_TYPE_INT, enumspb.INDEXED_VALUE_TYPE_DATETIME, enumspb.INDEXED_VALUE_TYPE_BOOL:
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
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		switch token.SortValue.(type) {
		case json.Number:
			sortVal, err = token.SortValue.(json.Number).Float64()
			if err != nil {
				return "", err
			}
		case string: // field not present, ES will return "-Infinity" or "Infinity"
			sortVal = fmt.Sprintf(`"%s"`, token.SortValue.(string))
		}
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
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

func (v *esVisibilityStore) checkProcessor() {
	if v.processor == nil {
		// must be bug, check history setup
		panic("elastic search processor is nil")
	}
	if v.config.ESProcessorAckTimeout == nil {
		// must be bug, check history setup
		panic("config.ESProcessorAckTimeout is nil")
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
	boolQuery *elastic.BoolQuery, overStartTime bool) (*elastic.SearchResult, error) {

	matchNamespaceQuery := elastic.NewMatchQuery(es.NamespaceID, request.NamespaceID)
	var rangeQuery *elastic.RangeQuery
	if overStartTime {
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
	earliestTimeStr := convert.Int64ToString(request.EarliestStartTime - oneMilliSecondInNano)
	latestTimeStr := convert.Int64ToString(request.LatestStartTime + oneMilliSecondInNano)
	rangeQuery = rangeQuery.
		Gte(earliestTimeStr).
		Lte(latestTimeStr)

	query := elastic.NewBoolQuery()
	if boolQuery != nil {
		*query = *boolQuery
	}

	query = query.Must(matchNamespaceQuery).Filter(rangeQuery)

	ctx := context.Background()
	params := &es.SearchParameters{
		Index:    v.index,
		Query:    query,
		From:     token.From,
		PageSize: request.PageSize,
	}
	if overStartTime {
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

func (v *esVisibilityStore) getScanWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
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

func (v *esVisibilityStore) getListWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
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
		if searchHits.TotalHits.Value <= int64(v.config.ESIndexMaxResultWindow()-pageSize) { // use ES Search From+Size
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

func (v *esVisibilityStore) deserializePageToken(data []byte) (*esVisibilityPageToken, error) {
	var token esVisibilityPageToken
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err := dec.Decode(&token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to deserialize page token. err: %v", err))
	}
	return &token, nil
}

func (v *esVisibilityStore) serializePageToken(token *esVisibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to serialize page token. err: %v", err))
	}
	return data, nil
}

func (v *esVisibilityStore) convertSearchResultToVisibilityRecord(hit *elastic.SearchHit) *p.VisibilityWorkflowExecutionInfo {
	var source *visibilityRecord
	// TODO (alex): use hit.Fields instead
	err := json.Unmarshal(hit.Source, &source)
	if err != nil { // log and skip error
		v.logger.Error("unable to unmarshal search hit source",
			tag.Error(err), tag.ESDocID(hit.Id))
		return nil
	}

	record := &p.VisibilityWorkflowExecutionInfo{
		WorkflowID:       source.WorkflowID,
		RunID:            source.RunID,
		TypeName:         source.WorkflowType,
		StartTime:        time.Unix(0, source.StartTime).UTC(),
		ExecutionTime:    time.Unix(0, source.ExecutionTime).UTC(),
		Memo:             p.NewDataBlob(source.Memo, source.Encoding),
		TaskQueue:        source.TaskQueue,
		SearchAttributes: source.Attr,
		Status:           source.ExecutionStatus,
	}
	if source.CloseTime != 0 {
		record.CloseTime = time.Unix(0, source.CloseTime).UTC()
		record.HistoryLength = source.HistoryLength
	}

	return record
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

// elasticsql may transfer `Attr.Name` to "`Attr.Name`" instead of "Attr.Name" in dsl in some operator like "between and"
// this function is used to clean up
func cleanDSL(input string) string {
	var re = regexp.MustCompile("(`)(Attr.\\w+)(`)")
	result := re.ReplaceAllString(input, `$2`)
	return result
}
