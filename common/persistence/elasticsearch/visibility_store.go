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
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

const (
	persistenceName = "elasticsearch"

	delimiter = "~"
)

type (
	visibilityStore struct {
		esClient                 client.Client
		index                    string
		searchAttributesProvider searchattribute.Provider
		logger                   log.Logger
		config                   *config.VisibilityConfig
		metricsClient            metrics.Client
		processor                Processor
	}

	visibilityPageToken struct {
		// for ES API From+Size
		From int
		// for ES API searchAfter
		SortValue  interface{}
		TieBreaker string // runID
		// for ES scroll API
		ScrollID string
	}
)

var _ persistence.VisibilityStore = (*visibilityStore)(nil)

// NewVisibilityStore create a visibility store connecting to ElasticSearch
func NewVisibilityStore(
	esClient client.Client,
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

func (s *visibilityStore) RecordWorkflowExecutionStarted(request *persistence.InternalRecordWorkflowExecutionStartedRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	return s.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) RecordWorkflowExecutionClosed(request *persistence.InternalRecordWorkflowExecutionClosedRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	doc[searchattribute.CloseTime] = request.CloseTimestamp
	doc[searchattribute.HistoryLength] = request.HistoryLength

	return s.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) UpsertWorkflowExecution(request *persistence.InternalUpsertWorkflowExecutionRequest) error {
	visibilityTaskKey := getVisibilityTaskKey(request.ShardID, request.TaskID)
	doc := s.generateESDoc(request.InternalVisibilityRequestBase, visibilityTaskKey)

	return s.addBulkIndexRequestAndWait(request.InternalVisibilityRequestBase, doc, visibilityTaskKey)
}

func (s *visibilityStore) DeleteWorkflowExecution(request *persistence.VisibilityDeleteWorkflowExecutionRequest) error {
	docID := getDocID(request.WorkflowID, request.RunID)

	bulkDeleteRequest := &client.BulkableRequest{
		Index:       s.index,
		ID:          docID,
		Version:     request.TaskID,
		RequestType: client.BulkableRequestTypeDelete,
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
	request *persistence.InternalVisibilityRequestBase,
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

	return s.addBulkRequestAndWait(bulkIndexRequest, visibilityTaskKey)
}

func (s *visibilityStore) addBulkRequestAndWait(bulkRequest *client.BulkableRequest, visibilityTaskKey string) error {
	s.checkProcessor()

	ackCh := make(chan bool, 1)
	s.processor.Add(bulkRequest, visibilityTaskKey, ackCh)
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

func (s *visibilityStore) ListOpenWorkflowExecutions(
	request *persistence.ListWorkflowExecutionsRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {
	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := s.getSearchResult(request, token, query, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutions failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutions(
	request *persistence.ListWorkflowExecutionsRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	executionStatusQuery := elastic.NewBoolQuery().MustNot(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := s.getSearchResult(request, token, executionStatusQuery, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutions failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByType(
	request *persistence.ListWorkflowExecutionsByTypeRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(searchattribute.WorkflowType, request.WorkflowTypeName)).
		Must(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := s.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByType failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByType(
	request *persistence.ListWorkflowExecutionsByTypeRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(searchattribute.WorkflowType, request.WorkflowTypeName)).
		MustNot(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := s.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByType failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	request *persistence.ListWorkflowExecutionsByWorkflowIDRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(searchattribute.WorkflowID, request.WorkflowID)).
		Must(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := s.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, true)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.StartTime.Before(request.EarliestStartTime) && !rec.StartTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	request *persistence.ListWorkflowExecutionsByWorkflowIDRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(searchattribute.WorkflowID, request.WorkflowID)).
		MustNot(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)))
	searchResult, err := s.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	request *persistence.ListClosedWorkflowExecutionsByStatusRequest) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	query := elastic.NewBoolQuery().Must(elastic.NewMatchQuery(searchattribute.ExecutionStatus, int32(request.Status)))
	searchResult, err := s.getSearchResult(&request.ListWorkflowExecutionsRequest, token, query, false)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListClosedWorkflowExecutionsByStatus failed. Error: %v", err))
	}

	isRecordValid := func(rec *persistence.VisibilityWorkflowExecutionInfo) bool {
		return !rec.CloseTime.Before(request.EarliestStartTime) && !rec.CloseTime.After(request.LatestStartTime)
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, isRecordValid)
}

func (s *visibilityStore) GetClosedWorkflowExecution(
	request *persistence.GetClosedWorkflowExecutionRequest) (*persistence.InternalGetClosedWorkflowExecutionResponse, error) {

	matchNamespaceQuery := elastic.NewMatchQuery(searchattribute.NamespaceID, request.NamespaceID)
	executionStatusQuery := elastic.NewMatchQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	matchWorkflowIDQuery := elastic.NewMatchQuery(searchattribute.WorkflowID, request.Execution.GetWorkflowId())
	boolQuery := elastic.NewBoolQuery().Must(matchNamespaceQuery).MustNot(executionStatusQuery).Must(matchWorkflowIDQuery)
	rid := request.Execution.GetRunId()
	if rid != "" {
		matchRunIDQuery := elastic.NewMatchQuery(searchattribute.RunID, rid)
		boolQuery = boolQuery.Must(matchRunIDQuery)
	}

	ctx := context.Background()
	params := &client.SearchParameters{
		Index: s.index,
		Query: boolQuery,
	}
	searchResult, err := s.esClient.Search(ctx, params)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetClosedWorkflowExecution failed. Error: %v", err))
	}

	response := &persistence.InternalGetClosedWorkflowExecutionResponse{}
	if len(searchResult.Hits.Hits) == 0 {
		return response, nil
	}

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
		return nil, err
	}

	response.Execution = s.parseESDoc(searchResult.Hits.Hits[0], typeMap)
	return response, nil
}

func (s *visibilityStore) ListWorkflowExecutions(
	request *persistence.ListWorkflowExecutionsRequestV2) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	checkPageSize(request)

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	queryDSL, err := s.getESQueryDSL(request, token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Error when parse query: %v", err))
	}

	ctx := context.Background()
	searchResult, err := s.esClient.SearchWithDSL(ctx, s.index, queryDSL)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ListWorkflowExecutions failed. Error: %v", err))
	}

	return s.getListWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, nil)
}

func (s *visibilityStore) ScanWorkflowExecutions(
	request *persistence.ListWorkflowExecutionsRequestV2) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	checkPageSize(request)

	token, err := s.getNextPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	var searchResult *elastic.SearchResult
	var scrollService client.ScrollService
	if len(token.ScrollID) == 0 { // first call
		var queryDSL string
		queryDSL, err = getESQueryDSLForScan(request)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Error when parse query: %v", err))
		}
		searchResult, scrollService, err = s.esClient.ScrollFirstPage(ctx, s.index, queryDSL)
	} else {
		searchResult, scrollService, err = s.esClient.Scroll(ctx, token.ScrollID)
	}

	isLastPage := false
	if err == io.EOF { // no more result
		isLastPage = true
		_ = scrollService.Clear(context.Background())
	} else if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ScanWorkflowExecutions failed. Error: %v", err))
	}

	return s.getScanWorkflowExecutionsResponse(searchResult.Hits, token, request.PageSize, searchResult.ScrollId, isLastPage)
}

func (s *visibilityStore) CountWorkflowExecutions(request *persistence.CountWorkflowExecutionsRequest) (
	*persistence.CountWorkflowExecutionsResponse, error) {

	queryDSL, err := getESQueryDSLForCount(request)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Error when parse query: %v", err))
	}

	ctx := context.Background()
	count, err := s.esClient.Count(ctx, s.index, queryDSL)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CountWorkflowExecutions failed. Error: %v", err))
	}

	response := &persistence.CountWorkflowExecutionsResponse{Count: count}
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

func getESQueryDSLForScan(request *persistence.ListWorkflowExecutionsRequestV2) (string, error) {
	sql := getSQLFromListRequest(request)
	dsl, err := getCustomizedDSLFromSQL(sql, request.NamespaceID)
	if err != nil {
		return "", err
	}

	// remove not needed fields
	dsl.Del(dslFieldSort)
	return dsl.String(), nil
}

func getESQueryDSLForCount(request *persistence.CountWorkflowExecutionsRequest) (string, error) {
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

func (s *visibilityStore) getESQueryDSL(request *persistence.ListWorkflowExecutionsRequestV2, token *visibilityPageToken) (string, error) {
	sql := getSQLFromListRequest(request)
	dsl, err := getCustomizedDSLFromSQL(sql, request.NamespaceID)
	if err != nil {
		return "", err
	}

	sortField, err := s.processSortField(dsl)
	if err != nil {
		return "", err
	}

	if shouldSearchAfter(token) {
		valueOfSearchAfter, err := s.getValueOfSearchAfterInJSON(token, sortField)
		if err != nil {
			return "", err
		}
		dsl.Set(dslFieldSearchAfter, fastjson.MustParse(valueOfSearchAfter))
	} else { // use from+size
		dsl.Set(dslFieldFrom, fastjson.MustParse(strconv.Itoa(token.From)))
	}

	return dsl.String(), nil
}

func getSQLFromListRequest(request *persistence.ListWorkflowExecutionsRequestV2) string {
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

func getSQLFromCountRequest(request *persistence.CountWorkflowExecutionsRequest) string {
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

func (s *visibilityStore) processSortField(dsl *fastjson.Value) (string, error) {
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
		if s.getFieldType(sortField) == enumspb.INDEXED_VALUE_TYPE_STRING {
			return "", errors.New("unable to sort by field of String type, use field of type Keyword")
		}
		// add RunID as tie-breaker
		dsl.Get(dslFieldSort).Set("1", fastjson.MustParse(jsonSortWithTieBreaker))
	}

	return sortField, nil
}

func (s *visibilityStore) getFieldType(fieldName string) enumspb.IndexedValueType {
	searchAttributes, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
	}
	fieldType, _ := searchAttributes.GetType(fieldName)
	return fieldType
}

func shouldSearchAfter(token *visibilityPageToken) bool {
	return token.TieBreaker != ""
}

func (s *visibilityStore) getValueOfSearchAfterInJSON(token *visibilityPageToken, sortField string) (string, error) {
	var sortVal interface{}
	var err error
	switch s.getFieldType(sortField) {
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

func (s *visibilityStore) getNextPageToken(token []byte) (*visibilityPageToken, error) {
	var result *visibilityPageToken
	var err error
	if len(token) > 0 {
		result, err = s.deserializePageToken(token)
		if err != nil {
			return nil, err
		}
	} else {
		result = &visibilityPageToken{}
	}
	return result, nil
}

func (s *visibilityStore) getSearchResult(request *persistence.ListWorkflowExecutionsRequest, token *visibilityPageToken,
	boolQuery *elastic.BoolQuery, overStartTime bool) (*elastic.SearchResult, error) {

	matchNamespaceQuery := elastic.NewMatchQuery(searchattribute.NamespaceID, request.NamespaceID)
	var rangeQuery *elastic.RangeQuery
	if overStartTime {
		rangeQuery = elastic.NewRangeQuery(searchattribute.StartTime)
	} else {
		rangeQuery = elastic.NewRangeQuery(searchattribute.CloseTime)
	}

	rangeQuery = rangeQuery.
		Gte(request.EarliestStartTime).
		Lte(request.LatestStartTime)

	query := elastic.NewBoolQuery()
	if boolQuery != nil {
		*query = *boolQuery
	}

	query = query.Must(matchNamespaceQuery).Filter(rangeQuery)

	ctx := context.Background()
	params := &client.SearchParameters{
		Index:    s.index,
		Query:    query,
		From:     token.From,
		PageSize: request.PageSize,
	}
	if overStartTime {
		params.Sorter = append(params.Sorter, elastic.NewFieldSort(searchattribute.StartTime).Desc())
	} else {
		params.Sorter = append(params.Sorter, elastic.NewFieldSort(searchattribute.CloseTime).Desc())
	}
	params.Sorter = append(params.Sorter, elastic.NewFieldSort(searchattribute.RunID).Desc())

	if shouldSearchAfter(token) {
		params.SearchAfter = []interface{}{token.SortValue, token.TieBreaker}
	}

	return s.esClient.Search(ctx, params)
}

func (s *visibilityStore) getScanWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
	token *visibilityPageToken, pageSize int, scrollID string, isLastPage bool) (
	*persistence.InternalListWorkflowExecutionsResponse, error) {

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
		return nil, err
	}

	response := &persistence.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*persistence.VisibilityWorkflowExecutionInfo, len(searchHits.Hits))
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

func (s *visibilityStore) getListWorkflowExecutionsResponse(searchHits *elastic.SearchHits,
	token *visibilityPageToken, pageSize int, isRecordValid func(rec *persistence.VisibilityWorkflowExecutionInfo) bool) (*persistence.InternalListWorkflowExecutionsResponse, error) {

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
		return nil, err
	}

	response := &persistence.InternalListWorkflowExecutionsResponse{}

	response.Executions = make([]*persistence.VisibilityWorkflowExecutionInfo, 0, len(searchHits.Hits))
	for _, hit := range searchHits.Hits {
		workflowExecutionInfo := s.parseESDoc(hit, typeMap)
		if isRecordValid == nil || isRecordValid(workflowExecutionInfo) {
			// Elasticsearch truncates dates using milliseconds and might return extra rows.
			// For example: 2021-06-12T00:21:43.159739259Z fits 2021-06-12T00:21:43.158Z...2021-06-12T00:21:43.159Z range lte/gte query.
			// Therefore these records needs to be filtered out on the client side to support nanos precision.
			response.Executions = append(response.Executions, workflowExecutionInfo)
		}
	}

	if len(searchHits.Hits) == pageSize { // this means the response is not the last page
		var nextPageToken []byte
		var err error

		// ES Search API support pagination using From and PageSize, but has limit that From+PageSize cannot exceed a threshold
		// to retrieve deeper pages, use ES SearchAfter
		if searchHits.TotalHits.Value <= int64(s.config.ESIndexMaxResultWindow()-pageSize) { // use ES Search From+Size
			nextPageToken, err = s.serializePageToken(&visibilityPageToken{From: token.From + len(searchHits.Hits)})
		} else { // use ES Search After
			var sortVal interface{}
			sortVals := searchHits.Hits[len(response.Executions)-1].Sort
			sortVal = sortVals[0]
			tieBreaker := sortVals[1].(string)

			nextPageToken, err = s.serializePageToken(&visibilityPageToken{SortValue: sortVal, TieBreaker: tieBreaker})
		}
		if err != nil {
			return nil, err
		}

		response.NextPageToken = make([]byte, len(nextPageToken))
		copy(response.NextPageToken, nextPageToken)
	}

	return response, nil
}

func (s *visibilityStore) deserializePageToken(data []byte) (*visibilityPageToken, error) {
	var token visibilityPageToken
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err := dec.Decode(&token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to deserialize page token. err: %v", err))
	}
	return &token, nil
}

func (s *visibilityStore) serializePageToken(token *visibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unable to serialize page token. err: %v", err))
	}
	return data, nil
}

func (s *visibilityStore) generateESDoc(request *persistence.InternalVisibilityRequestBase, visibilityTaskKey string) map[string]interface{} {
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

	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.index, false)
	if err != nil {
		s.logger.Error("Unable to read search attribute types.", tag.Error(err))
	}

	searchAttributes, err := searchattribute.Decode(request.SearchAttributes, &typeMap)
	if err != nil {
		s.logger.Error("Unable to decode search attributes.", tag.Error(err))
		s.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}
	for saName, saValue := range searchAttributes {
		doc[saName] = saValue
	}

	return doc
}

func (s *visibilityStore) parseESDoc(hit *elastic.SearchHit, saTypeMap searchattribute.NameTypeMap) *persistence.VisibilityWorkflowExecutionInfo {
	logUnexpectedType := func(fieldName string, fieldValue interface{}, docID string) {
		s.logger.Error("Unexpected field type while parsing Elasticsearch document.", tag.Name(fieldName), tag.ValueType(fieldValue), tag.ESDocID(docID))
		s.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}
	logNumberParseError := func(fieldName string, fieldValue json.Number, err error, docID string) {
		s.logger.Error("Unable to parse JSON number while parsing Elasticsearch document.", tag.Name(fieldName), tag.ValueType(fieldValue), tag.Error(err), tag.ESDocID(docID))
		s.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}
	logDateParseError := func(fieldName string, fieldValue string, err error, docID string) {
		s.logger.Error("Unable to parse JSON date while parsing Elasticsearch document.", tag.Name(fieldName), tag.ValueType(fieldValue), tag.Error(err), tag.ESDocID(docID))
		s.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}

	var sourceMap map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(hit.Source))
	d.UseNumber()
	if err := d.Decode(&sourceMap); err != nil {
		s.logger.Error("Unable to JSON unmarshal Elasticsearch SearchHit.Source.", tag.Error(err), tag.ESDocID(hit.Id))
		return nil
	}

	var isValidType bool
	var memo []byte
	var memoEncoding string
	record := &persistence.VisibilityWorkflowExecutionInfo{}
	for fieldName, fieldValue := range sourceMap {
		switch fieldName {
		case searchattribute.NamespaceID:
			// Ignore NamespaceID
		case searchattribute.WorkflowID:
			if record.WorkflowID, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
		case searchattribute.RunID:
			if record.RunID, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
		case searchattribute.WorkflowType:
			if record.TypeName, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
		case searchattribute.StartTime:
			var startTime string
			if startTime, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
			var err error
			record.StartTime, err = time.Parse(time.RFC3339Nano, startTime)
			if err != nil {
				logDateParseError(fieldName, startTime, err, hit.Id)
			}
		case searchattribute.ExecutionTime:
			var executionTime string
			if executionTime, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
			var err error
			record.ExecutionTime, err = time.Parse(time.RFC3339Nano, executionTime)
			if err != nil {
				logDateParseError(fieldName, executionTime, err, hit.Id)
			}
		case searchattribute.CloseTime:
			var closeTime string
			if closeTime, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
			var err error
			record.CloseTime, err = time.Parse(time.RFC3339Nano, closeTime)
			if err != nil {
				logDateParseError(fieldName, closeTime, err, hit.Id)
			}
		case searchattribute.Memo:
			if memo, isValidType = fieldValue.([]byte); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
		case searchattribute.Encoding:
			if memoEncoding, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
		case searchattribute.TaskQueue:
			if record.TaskQueue, isValidType = fieldValue.(string); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
		case searchattribute.ExecutionStatus:
			var executionStatus json.Number
			if executionStatus, isValidType = fieldValue.(json.Number); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
			executionStatusInt64, err := executionStatus.Int64()
			if err != nil {
				logNumberParseError(fieldName, executionStatus, err, hit.Id)
			}
			record.Status = enumspb.WorkflowExecutionStatus(executionStatusInt64)
		case searchattribute.HistoryLength:
			var historyLength json.Number
			if historyLength, isValidType = fieldValue.(json.Number); !isValidType {
				logUnexpectedType(fieldName, fieldValue, hit.Id)
			}
			historyLengthInt64, err := historyLength.Int64()
			if err != nil {
				logNumberParseError(fieldName, historyLength, err, hit.Id)
			}
			record.HistoryLength = historyLengthInt64
		default:
			// All custom search attributes are handled here.
			// Add only defined search attributes and ignore all unknown fields.
			if saTypeMap.IsDefined(fieldName) {
				if record.SearchAttributes == nil {
					record.SearchAttributes = map[string]interface{}{}
				}
				record.SearchAttributes[fieldName] = fieldValue
			}
		}
	}

	if memoEncoding != "" {
		record.Memo = persistence.NewDataBlob(memo, memoEncoding)
	} else if memo != nil {
		s.logger.Error("Encoding field is missing in Elasticsearch document.", tag.ESDocID(hit.Id))
		s.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESInvalidSearchAttribute)
	}

	return record
}

func checkPageSize(request *persistence.ListWorkflowExecutionsRequestV2) {
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

		// ES6 use milliseconds for times and doesn't support "date_nanos" type (available in ES7 only).
		// Temporal historically uses nanos for timestamps which need to be converted to milliseconds.
		// After ES6 deprecation this func can be removed but "date" columns need to be reindexed to "data_nanos"
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/date_nanos.html
		if nanos, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			milliseconds := nanos / int64(time.Millisecond)
			obj.Set(key, fastjson.MustParse(strconv.FormatInt(milliseconds, 10)))
		}

		return nil
	})
}
