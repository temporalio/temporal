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
	"time"

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

	if token.SortTime != 0 && token.TieBreaker != "" {
		params.SearchAfter = []interface{}{token.SortTime, token.TieBreaker}
	}

	return v.esClient.Search(ctx, params)
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
		WorkflowID:    source.WorkflowID,
		RunID:         source.RunID,
		TypeName:      source.WorkflowType,
		StartTime:     time.Unix(0, source.StartTime),
		ExecutionTime: time.Unix(0, source.ExecutionTime),
		Memo:          p.NewDataBlob(source.Memo, common.EncodingType(source.Encoding)),
	}
	if !isOpen {
		record.CloseTime = time.Unix(0, source.CloseTime)
		record.Status = &source.CloseStatus
		record.HistoryLength = source.HistoryLength
	}

	return record
}

func getVisibilityMessageForOpenExecution(domainID string, wid, rid string, workflowTypeName string,
	startTimeUnixNano, executionTimeUnixNano int64, taskID int64, memo []byte, encoding common.EncodingType) *indexer.Message {

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
	historyLength int64, taskID int64, memo []byte, encoding common.EncodingType) *indexer.Message {

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
