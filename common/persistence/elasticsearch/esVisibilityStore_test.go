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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/olivere/elastic"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/valyala/fastjson"

	"github.com/uber/cadence/.gen/go/indexer"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	es "github.com/uber/cadence/common/elasticsearch"
	esMocks "github.com/uber/cadence/common/elasticsearch/mocks"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type ESVisibilitySuite struct {
	suite.Suite
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	visibilityStore *esVisibilityStore
	mockESClient    *esMocks.Client
	mockProducer    *mocks.KafkaProducer
}

var (
	testIndex        = "test-index"
	testDomain       = "test-domain"
	testDomainID     = "bfd5c907-f899-4baf-a7b2-2ab85e623ebd"
	testPageSize     = 5
	testEarliestTime = int64(1547596872371000000)
	testLatestTime   = int64(2547596872371000000)
	testWorkflowType = "test-wf-type"
	testWorkflowID   = "test-wid"
	testRunID        = "1601da05-4db9-4eeb-89e4-da99481bdfc9"
	testCloseStatus  = 1

	testRequest = &p.ListWorkflowExecutionsRequest{
		DomainUUID:        testDomainID,
		Domain:            testDomain,
		PageSize:          testPageSize,
		EarliestStartTime: testEarliestTime,
		LatestStartTime:   testLatestTime,
	}
	testSearchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{},
	}
	errTestESSearch = errors.New("ES error")

	filterOpen     = "must_not:map[exists:map[field:CloseStatus]]"
	filterClose    = "map[exists:map[field:CloseStatus]]"
	filterByType   = fmt.Sprintf("map[match:map[WorkflowType:map[query:%s]]]", testWorkflowType)
	filterByWID    = fmt.Sprintf("map[match:map[WorkflowID:map[query:%s]]]", testWorkflowID)
	filterByRunID  = fmt.Sprintf("map[match:map[RunID:map[query:%s]]]", testRunID)
	filterByStatus = fmt.Sprintf("map[match:map[CloseStatus:map[query:%v]]]", testCloseStatus)
)

func TestESVisibilitySuite(t *testing.T) {
	suite.Run(t, new(ESVisibilitySuite))
}

func (s *ESVisibilitySuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	s.mockESClient = &esMocks.Client{}
	config := &config.VisibilityConfig{
		ESIndexMaxResultWindow: dynamicconfig.GetIntPropertyFn(3),
		ValidSearchAttributes:  dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
	}

	s.mockProducer = &mocks.KafkaProducer{}
	mgr := NewElasticSearchVisibilityStore(s.mockESClient, testIndex, s.mockProducer, config, loggerimpl.NewNopLogger())
	s.visibilityStore = mgr.(*esVisibilityStore)
}

func (s *ESVisibilitySuite) TearDownTest() {
	s.mockESClient.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted() {
	// test non-empty request fields match
	request := &p.InternalRecordWorkflowExecutionStartedRequest{}
	request.DomainUUID = "domainID"
	request.WorkflowID = "wid"
	request.RunID = "rid"
	request.WorkflowTypeName = "wfType"
	request.StartTimestamp = int64(123)
	request.ExecutionTimestamp = int64(321)
	request.TaskID = int64(111)
	memoBytes := []byte(`test bytes`)
	request.Memo = p.NewDataBlob(memoBytes, common.EncodingTypeThriftRW)
	s.mockProducer.On("Publish", mock.MatchedBy(func(input *indexer.Message) bool {
		fields := input.Fields
		s.Equal(request.DomainUUID, input.GetDomainID())
		s.Equal(request.WorkflowID, input.GetWorkflowID())
		s.Equal(request.RunID, input.GetRunID())
		s.Equal(request.TaskID, input.GetVersion())
		s.Equal(request.WorkflowTypeName, fields[es.WorkflowType].GetStringData())
		s.Equal(request.StartTimestamp, fields[es.StartTime].GetIntData())
		s.Equal(request.ExecutionTimestamp, fields[es.ExecutionTime].GetIntData())
		s.Equal(memoBytes, fields[es.Memo].GetBinaryData())
		s.Equal(string(common.EncodingTypeThriftRW), fields[es.Encoding].GetStringData())
		return true
	})).Return(nil).Once()
	err := s.visibilityStore.RecordWorkflowExecutionStarted(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted_EmptyRequest() {
	// test empty request
	request := &p.InternalRecordWorkflowExecutionStartedRequest{
		Memo: &p.DataBlob{},
	}
	s.mockProducer.On("Publish", mock.MatchedBy(func(input *indexer.Message) bool {
		s.Equal(indexer.MessageTypeIndex, input.GetMessageType())
		_, ok := input.Fields[es.Memo]
		s.False(ok)
		_, ok = input.Fields[es.Encoding]
		s.False(ok)
		return true
	})).Return(nil).Once()
	err := s.visibilityStore.RecordWorkflowExecutionStarted(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed() {
	// test non-empty request fields match
	request := &p.InternalRecordWorkflowExecutionClosedRequest{}
	request.DomainUUID = "domainID"
	request.WorkflowID = "wid"
	request.RunID = "rid"
	request.WorkflowTypeName = "wfType"
	request.StartTimestamp = int64(123)
	request.ExecutionTimestamp = int64(321)
	request.TaskID = int64(111)
	memoBytes := []byte(`test bytes`)
	request.Memo = p.NewDataBlob(memoBytes, common.EncodingTypeThriftRW)
	request.CloseTimestamp = int64(999)
	request.Status = workflow.WorkflowExecutionCloseStatusTerminated
	request.HistoryLength = int64(20)
	s.mockProducer.On("Publish", mock.MatchedBy(func(input *indexer.Message) bool {
		fields := input.Fields
		s.Equal(request.DomainUUID, input.GetDomainID())
		s.Equal(request.WorkflowID, input.GetWorkflowID())
		s.Equal(request.RunID, input.GetRunID())
		s.Equal(request.TaskID, input.GetVersion())
		s.Equal(request.WorkflowTypeName, fields[es.WorkflowType].GetStringData())
		s.Equal(request.StartTimestamp, fields[es.StartTime].GetIntData())
		s.Equal(request.ExecutionTimestamp, fields[es.ExecutionTime].GetIntData())
		s.Equal(memoBytes, fields[es.Memo].GetBinaryData())
		s.Equal(string(common.EncodingTypeThriftRW), fields[es.Encoding].GetStringData())
		s.Equal(request.CloseTimestamp, fields[es.CloseTime].GetIntData())
		s.Equal(int64(request.Status), fields[es.CloseStatus].GetIntData())
		s.Equal(request.HistoryLength, fields[es.HistoryLength].GetIntData())
		return true
	})).Return(nil).Once()
	err := s.visibilityStore.RecordWorkflowExecutionClosed(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed_EmptyRequest() {
	// test empty request
	request := &p.InternalRecordWorkflowExecutionClosedRequest{
		Memo: &p.DataBlob{},
	}
	s.mockProducer.On("Publish", mock.MatchedBy(func(input *indexer.Message) bool {
		s.Equal(indexer.MessageTypeIndex, input.GetMessageType())
		_, ok := input.Fields[es.Memo]
		s.False(ok)
		_, ok = input.Fields[es.Encoding]
		s.False(ok)
		return true
	})).Return(nil).Once()
	err := s.visibilityStore.RecordWorkflowExecutionClosed(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutions() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
		return true
	})).Return(testSearchResult, nil).Once()
	_, err := s.visibilityStore.ListOpenWorkflowExecutions(testRequest)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListOpenWorkflowExecutions(testRequest)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListOpenWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutions() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
		return true
	})).Return(testSearchResult, nil).Once()
	_, err := s.visibilityStore.ListClosedWorkflowExecutions(testRequest)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListClosedWorkflowExecutions(testRequest)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutionsByType() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByType))
		return true
	})).Return(testSearchResult, nil).Once()

	request := &p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityStore.ListOpenWorkflowExecutionsByType(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListOpenWorkflowExecutionsByType(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListOpenWorkflowExecutionsByType failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutionsByType() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByType))
		return true
	})).Return(testSearchResult, nil).Once()

	request := &p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByType(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByType(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByType failed"))
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutionsByWorkflowID() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
		return true
	})).Return(testSearchResult, nil).Once()

	request := &p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityStore.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListOpenWorkflowExecutionsByWorkflowID failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutionsByWorkflowID() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
		return true
	})).Return(testSearchResult, nil).Once()

	request := &p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByWorkflowID failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutionsByStatus() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByStatus))
		return true
	})).Return(testSearchResult, nil).Once()

	request := &p.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		Status:                        workflow.WorkflowExecutionCloseStatus(testCloseStatus),
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByStatus(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByStatus(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByStatus failed"))
}

func (s *ESVisibilitySuite) TestGetClosedWorkflowExecution() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByRunID))
		return true
	})).Return(testSearchResult, nil).Once()
	request := &p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
			RunId:      common.StringPtr(testRunID),
		},
	}
	_, err := s.visibilityStore.GetClosedWorkflowExecution(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.GetClosedWorkflowExecution(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "GetClosedWorkflowExecution failed"))
}

func (s *ESVisibilitySuite) TestGetClosedWorkflowExecution_NoRunID() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
		s.False(strings.Contains(fmt.Sprintf("%v", source), filterByRunID))
		return true
	})).Return(testSearchResult, nil).Once()
	request := &p.GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
		},
	}
	_, err := s.visibilityStore.GetClosedWorkflowExecution(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestGetNextPageToken() {
	token, err := s.visibilityStore.getNextPageToken([]byte{})
	s.Equal(0, token.From)
	s.NoError(err)

	from := 5
	input, err := s.visibilityStore.serializePageToken(&esVisibilityPageToken{From: from})
	s.NoError(err)
	token, err = s.visibilityStore.getNextPageToken(input)
	s.Equal(from, token.From)
	s.NoError(err)

	badInput := []byte("bad input")
	token, err = s.visibilityStore.getNextPageToken(badInput)
	s.Nil(token)
	s.Error(err)
}

func (s *ESVisibilitySuite) TestGetSearchResult() {
	request := testRequest
	from := 1
	token := &esVisibilityPageToken{From: from}

	matchDomainQuery := elastic.NewMatchQuery(es.DomainID, request.DomainUUID)
	existClosedStatusQuery := elastic.NewExistsQuery(es.CloseStatus)
	tieBreakerSorter := elastic.NewFieldSort(es.RunID).Desc()

	earliestTime := strconv.FormatInt(request.EarliestStartTime-oneMilliSecondInNano, 10)
	latestTime := strconv.FormatInt(request.LatestStartTime+oneMilliSecondInNano, 10)

	// test for open
	isOpen := true
	rangeQuery := elastic.NewRangeQuery(es.StartTime).Gte(earliestTime).Lte(latestTime)
	boolQuery := elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery).MustNot(existClosedStatusQuery)
	params := &es.SearchParameters{
		Index:    testIndex,
		Query:    boolQuery,
		From:     from,
		PageSize: testPageSize,
		Sorter:   []elastic.Sorter{elastic.NewFieldSort(es.StartTime).Desc(), tieBreakerSorter},
	}
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	_, err := s.visibilityStore.getSearchResult(request, token, nil, isOpen)
	s.NoError(err)

	// test request latestTime overflow
	request.LatestStartTime = math.MaxInt64
	rangeQuery1 := elastic.NewRangeQuery(es.StartTime).Gte(earliestTime).Lte(strconv.FormatInt(request.LatestStartTime, 10))
	boolQuery1 := elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery1).MustNot(existClosedStatusQuery)
	param1 := &es.SearchParameters{
		Index:    testIndex,
		Query:    boolQuery1,
		From:     from,
		PageSize: testPageSize,
		Sorter:   []elastic.Sorter{elastic.NewFieldSort(es.StartTime).Desc(), tieBreakerSorter},
	}
	s.mockESClient.On("Search", mock.Anything, param1).Return(nil, nil).Once()
	_, err = s.visibilityStore.getSearchResult(request, token, nil, isOpen)
	s.NoError(err)
	request.LatestStartTime = testLatestTime // revert

	// test for closed
	isOpen = false
	rangeQuery = elastic.NewRangeQuery(es.CloseTime).Gte(earliestTime).Lte(latestTime)
	boolQuery = elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery).Must(existClosedStatusQuery)
	params.Query = boolQuery
	params.Sorter = []elastic.Sorter{elastic.NewFieldSort(es.CloseTime).Desc(), tieBreakerSorter}
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	_, err = s.visibilityStore.getSearchResult(request, token, nil, isOpen)
	s.NoError(err)

	// test for additional matchQuery
	matchQuery := elastic.NewMatchQuery(es.CloseStatus, int32(0))
	boolQuery = elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery).Must(matchQuery).Must(existClosedStatusQuery)
	params.Query = boolQuery
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	_, err = s.visibilityStore.getSearchResult(request, token, matchQuery, isOpen)
	s.NoError(err)

	// test for search after
	runID := "runID"
	token = &esVisibilityPageToken{
		SortValue:  latestTime,
		TieBreaker: runID,
	}
	params.From = 0
	params.SearchAfter = []interface{}{token.SortValue, token.TieBreaker}
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	_, err = s.visibilityStore.getSearchResult(request, token, matchQuery, isOpen)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	token := &esVisibilityPageToken{From: 0}

	// test for empty hits
	searchHits := &elastic.SearchHits{}
	resp, err := s.visibilityStore.getListWorkflowExecutionsResponse(searchHits, token, 1, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(0, len(resp.Executions))

	// test for one hits
	data := []byte(`{"CloseStatus": 0,
          "CloseTime": 1547596872817380000,
          "DomainID": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "KafkaKey": "7-619",
          "RunID": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": 1547596872371000000,
          "WorkflowID": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"}`)
	source := (*json.RawMessage)(&data)
	searchHit := &elastic.SearchHit{
		Source: source,
		Sort:   []interface{}{1547596872371000000, "e481009e-14b3-45ae-91af-dce6e2a88365"},
	}
	searchHits.Hits = []*elastic.SearchHit{searchHit}
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchHits, token, 1, nil)
	s.NoError(err)
	serializedToken, _ := s.visibilityStore.serializePageToken(&esVisibilityPageToken{From: 1})
	s.Equal(serializedToken, resp.NextPageToken)
	s.Equal(1, len(resp.Executions))

	// test for last page hits
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchHits, token, 2, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(1, len(resp.Executions))

	// test for search after
	token = &esVisibilityPageToken{}
	searchHits.Hits = []*elastic.SearchHit{}
	searchHits.TotalHits = int64(s.visibilityStore.config.ESIndexMaxResultWindow() + 1)
	for i := int64(0); i < searchHits.TotalHits; i++ {
		searchHits.Hits = append(searchHits.Hits, searchHit)
	}
	numOfHits := len(searchHits.Hits)
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchHits, token, numOfHits, nil)
	s.NoError(err)
	s.Equal(numOfHits, len(resp.Executions))
	nextPageToken, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	resultSortValue, err := nextPageToken.SortValue.(json.Number).Int64()
	s.NoError(err)
	s.Equal(int64(1547596872371000000), resultSortValue)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", nextPageToken.TieBreaker)
	s.Equal(0, nextPageToken.From)
	// for last page
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchHits, token, numOfHits+1, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(numOfHits, len(resp.Executions))
}

func (s *ESVisibilitySuite) TestDeserializePageToken() {
	token := &esVisibilityPageToken{From: 0}
	data, _ := s.visibilityStore.serializePageToken(token)
	result, err := s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	s.Equal(token, result)

	badInput := []byte("bad input")
	result, err = s.visibilityStore.deserializePageToken(badInput)
	s.Error(err)
	s.Nil(result)
	err, ok := err.(*workflow.BadRequestError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "unable to deserialize page token"))

	token = &esVisibilityPageToken{SortValue: int64(64), TieBreaker: "unique"}
	data, _ = s.visibilityStore.serializePageToken(token)
	result, err = s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	resultSortValue, err := result.SortValue.(json.Number).Int64()
	s.NoError(err)
	s.Equal(token.SortValue.(int64), resultSortValue)
}

func (s *ESVisibilitySuite) TestSerializePageToken() {
	data, err := s.visibilityStore.serializePageToken(nil)
	s.NoError(err)
	s.True(len(data) > 0)
	token, err := s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	s.Equal(0, token.From)
	s.Equal(nil, token.SortValue)
	s.Equal("", token.TieBreaker)

	newToken := &esVisibilityPageToken{From: 5}
	data, err = s.visibilityStore.serializePageToken(newToken)
	s.NoError(err)
	s.True(len(data) > 0)
	token, err = s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	s.Equal(newToken, token)

	sortTime := int64(123)
	tieBreaker := "unique"
	newToken = &esVisibilityPageToken{SortValue: sortTime, TieBreaker: tieBreaker}
	data, err = s.visibilityStore.serializePageToken(newToken)
	s.NoError(err)
	s.True(len(data) > 0)
	token, err = s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	resultSortValue, err := token.SortValue.(json.Number).Int64()
	s.NoError(err)
	s.Equal(newToken.SortValue, resultSortValue)
	s.Equal(newToken.TieBreaker, token.TieBreaker)
}

func (s *ESVisibilitySuite) TestConvertSearchResultToVisibilityRecord() {
	data := []byte(`{"CloseStatus": 0,
          "CloseTime": 1547596872817380000,
          "DomainID": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "KafkaKey": "7-619",
          "RunID": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": 1547596872371000000,
          "WorkflowID": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "TestWorkflowExecute"}`)
	source := (*json.RawMessage)(&data)
	searchHit := &elastic.SearchHit{
		Source: source,
	}

	// test for open
	info := s.visibilityStore.convertSearchResultToVisibilityRecord(searchHit)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	s.Equal("TestWorkflowExecute", info.TypeName)
	s.Equal(int64(1547596872371000000), info.StartTime.UnixNano())

	// test for close
	info = s.visibilityStore.convertSearchResultToVisibilityRecord(searchHit)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	s.Equal("TestWorkflowExecute", info.TypeName)
	s.Equal(int64(1547596872371000000), info.StartTime.UnixNano())
	s.Equal(int64(1547596872817380000), info.CloseTime.UnixNano())
	s.Equal(workflow.WorkflowExecutionCloseStatusCompleted, *info.Status)
	s.Equal(int64(29), info.HistoryLength)

	// test for error case
	badData := []byte(`corrupted data`)
	source = (*json.RawMessage)(&badData)
	searchHit = &elastic.SearchHit{
		Source: source,
	}
	info = s.visibilityStore.convertSearchResultToVisibilityRecord(searchHit)
	s.Nil(info)
}

func (s *ESVisibilitySuite) TestShouldSearchAfter() {
	token := &esVisibilityPageToken{}
	s.False(shouldSearchAfter(token))

	token.TieBreaker = "a"
	s.True(shouldSearchAfter(token))
}

//nolint
func (s *ESVisibilitySuite) TestGetESQueryDSL() {
	request := &p.ListWorkflowExecutionsRequestV2{
		DomainUUID: testDomainID,
		PageSize:   10,
	}
	token := &esVisibilityPageToken{}

	v := s.visibilityStore

	request.Query = ""
	dsl, err := v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = "invaild query"
	dsl, err = v.getESQueryDSL(request, token)
	s.NotNil(err)
	s.Equal("", dsl)

	request.Query = `WorkflowID = 'wid'`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `WorkflowID = 'wid' or WorkflowID = 'another-wid'`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"should":[{"match_phrase":{"WorkflowID":{"query":"wid"}}},{"match_phrase":{"WorkflowID":{"query":"another-wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `WorkflowID = 'wid' order by StartTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `WorkflowID = 'wid' and CloseTime = missing`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `WorkflowID = 'wid' or CloseTime = missing`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"should":[{"match_phrase":{"WorkflowID":{"query":"wid"}}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `CloseTime = missing order by CloseTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}},"from":0,"size":10,"sort":[{"CloseTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `StartTime = "2018-06-07T15:04:05-08:00"`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"StartTime":{"query":"1528412645000000000"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `WorkflowID = 'wid' and StartTime > "2018-06-07T15:04:05+00:00"`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}},{"range":{"StartTime":{"gt":"1528383845000000000"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `ExecutionTime < 1000`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"gt":"0"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"lt":"1000"}}}]}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `ExecutionTime < 1000 or ExecutionTime > 2000`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"gt":"0"}}},{"bool":{"should":[{"range":{"ExecutionTime":{"lt":"1000"}}},{"range":{"ExecutionTime":{"gt":"2000"}}}]}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `order by ExecutionTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}},"from":0,"size":10,"sort":[{"ExecutionTime":"desc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `order by StartTime desc, CloseTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Equal(errors.New("only one field can be used to sort"), err)

	request.Query = `order by CustomStringField desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Equal(errors.New("not able to sort by IndexedValueTypeString field, use IndexedValueTypeKeyword field"), err)

	request.Query = `order by CustomIntField asc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}},"from":0,"size":10,"sort":[{"CustomIntField":"asc"},{"RunID":"desc"}]}`, dsl)

	request.Query = `ExecutionTime < "unable to parse"`
	_, err = v.getESQueryDSL(request, token)
	s.Error(err)

	token = s.getTokenHelper(1)
	request.Query = `WorkflowID = 'wid'`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}],"search_after":[1,"t"]}`, dsl)

	// invalid union injection
	request.Query = `WorkflowID = 'wid' union select * from dummy`
	_, err = v.getESQueryDSL(request, token)
	s.NotNil(err)
}

func (s *ESVisibilitySuite) TestGetESQueryDSLForScan() {
	request := &p.ListWorkflowExecutionsRequestV2{
		DomainUUID: testDomainID,
		PageSize:   10,
	}

	request.Query = `WorkflowID = 'wid' order by StartTime desc`
	dsl, err := getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}}]}}]}},"from":0,"size":10}`, dsl)

	request.Query = `WorkflowID = 'wid'`
	dsl, err = getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}}]}}]}},"from":0,"size":10}`, dsl)

	request.Query = `CloseTime = missing and (ExecutionTime >= "2019-08-27T15:04:05+00:00" or StartTime <= "2018-06-07T15:04:05+00:00")`
	dsl, err = getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"gt":"0"}}},{"bool":{"must":[{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}},{"bool":{"should":[{"range":{"ExecutionTime":{"from":"1566918245000000000"}}},{"range":{"StartTime":{"to":"1528383845000000000"}}}]}}]}}]}}]}},"from":0,"size":10}`, dsl)

	request.Query = `ExecutionTime < 1000 and ExecutionTime > 500`
	dsl, err = getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"gt":"0"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"lt":"1000"}}},{"range":{"ExecutionTime":{"gt":"500"}}}]}}]}}]}},"from":0,"size":10}`, dsl)
}

func (s *ESVisibilitySuite) TestGetESQueryDSLForCount() {
	request := &p.CountWorkflowExecutionsRequest{
		DomainUUID: testDomainID,
	}

	// empty query
	dsl, err := getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}}}`, dsl)

	request.Query = `WorkflowID = 'wid' order by StartTime desc`
	dsl, err = getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowID":{"query":"wid"}}}]}}]}}}`, dsl)

	request.Query = `CloseTime < "2018-06-07T15:04:05+07:00" and StartTime > "2018-05-04T16:00:00+07:00" and ExecutionTime >= "2018-05-05T16:00:00+07:00"`
	dsl, err = getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"gt":"0"}}},{"bool":{"must":[{"range":{"CloseTime":{"lt":"1528358645000000000"}}},{"range":{"StartTime":{"gt":"1525424400000000000"}}},{"range":{"ExecutionTime":{"from":"1525510800000000000"}}}]}}]}}]}}}`, dsl)

	request.Query = `ExecutionTime < 1000`
	dsl, err = getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"gt":"0"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"lt":"1000"}}}]}}]}}]}}}`, dsl)
}

func (s *ESVisibilitySuite) TestAddDomainToQuery() {
	dsl := fastjson.MustParse(`{}`)
	dslStr := dsl.String()
	addDomainToQuery(dsl, "")
	s.Equal(dslStr, dsl.String())

	dsl = fastjson.MustParse(`{"query":{"bool":{"must":[{"match_all":{}}]}}}`)
	addDomainToQuery(dsl, testDomainID)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}}}`, dsl.String())
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions() {
	s.mockESClient.On("SearchWithDSL", mock.Anything, mock.Anything, mock.MatchedBy(func(input string) bool {
		s.True(strings.Contains(input, `{"match_phrase":{"CloseStatus":{"query":"5"}}}`))
		return true
	})).Return(testSearchResult, nil).Once()

	request := &p.ListWorkflowExecutionsRequestV2{
		DomainUUID: testDomainID,
		Domain:     testDomain,
		PageSize:   10,
		Query:      `CloseStatus = 5`,
	}
	_, err := s.visibilityStore.ListWorkflowExecutions(request)
	s.NoError(err)

	s.mockESClient.On("SearchWithDSL", mock.Anything, mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ListWorkflowExecutions(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListWorkflowExecutions failed"))

	request.Query = `invalid query`
	_, err = s.visibilityStore.ListWorkflowExecutions(request)
	s.Error(err)
	_, ok = err.(*workflow.BadRequestError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "Error when parse query"))
}

func (s *ESVisibilitySuite) TestScanWorkflowExecutions() {
	// test first page
	s.mockESClient.On("ScrollFirstPage", mock.Anything, testIndex, mock.MatchedBy(func(input string) bool {
		s.True(strings.Contains(input, `{"match_phrase":{"CloseStatus":{"query":"5"}}}`))
		return true
	})).Return(testSearchResult, nil, nil).Once()

	request := &p.ListWorkflowExecutionsRequestV2{
		DomainUUID: testDomainID,
		Domain:     testDomain,
		PageSize:   10,
		Query:      `CloseStatus = 5`,
	}
	_, err := s.visibilityStore.ScanWorkflowExecutions(request)
	s.NoError(err)

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.Error(err)
	_, ok := err.(*workflow.BadRequestError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "Error when parse query"))

	// test scroll
	scrollID := "scrollID-1"
	s.mockESClient.On("Scroll", mock.Anything, scrollID).Return(testSearchResult, nil, nil).Once()

	token := &esVisibilityPageToken{ScrollID: scrollID}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
	request.NextPageToken = tokenBytes
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.NoError(err)

	// test last page
	mockScroll := &esMocks.ScrollService{}
	s.mockESClient.On("Scroll", mock.Anything, scrollID).Return(testSearchResult, mockScroll, io.EOF).Once()
	mockScroll.On("Clear", mock.Anything).Return(nil).Once()
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.NoError(err)
	mockScroll.AssertExpectations(s.T())

	// test internal error
	s.mockESClient.On("Scroll", mock.Anything, scrollID).Return(nil, nil, errTestESSearch).Once()
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.Error(err)
	_, ok = err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ScanWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestCountWorkflowExecutions() {
	s.mockESClient.On("Count", mock.Anything, testIndex, mock.MatchedBy(func(input string) bool {
		s.True(strings.Contains(input, `{"match_phrase":{"CloseStatus":{"query":"5"}}}`))
		return true
	})).Return(int64(1), nil).Once()

	request := &p.CountWorkflowExecutionsRequest{
		DomainUUID: testDomainID,
		Domain:     testDomain,
		Query:      `CloseStatus = 5`,
	}
	resp, err := s.visibilityStore.CountWorkflowExecutions(request)
	s.NoError(err)
	s.Equal(int64(1), resp.Count)

	// test internal error
	s.mockESClient.On("Count", mock.Anything, testIndex, mock.Anything).Return(int64(0), errTestESSearch).Once()

	_, err = s.visibilityStore.CountWorkflowExecutions(request)
	s.Error(err)
	_, ok := err.(*workflow.InternalServiceError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "CountWorkflowExecutions failed"))

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.CountWorkflowExecutions(request)
	s.Error(err)
	_, ok = err.(*workflow.BadRequestError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "Error when parse query"))
}

func (s *ESVisibilitySuite) TestTimeProcessFunc() {
	cases := []struct {
		key   string
		value string
	}{
		{key: "from", value: "1528358645000000000"},
		{key: "to", value: "2018-06-07T15:04:05+07:00"},
		{key: "gt", value: "some invalid time string"},
		{key: "unrelatedKey", value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: `"1528358645000000000"`, returnErr: false},
		{value: `"1528358645000000000"`},
		{value: "", returnErr: true},
		{value: `"should not be modified"`, returnErr: false},
	}

	for i, testCase := range cases {
		value := fastjson.MustParse(fmt.Sprintf(`{"%s": "%s"}`, testCase.key, testCase.value))
		err := timeProcessFunc(nil, "", value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.Equal(expected[i].value, value.Get(testCase.key).String())
	}
}

func (s *ESVisibilitySuite) TestProcessAllValuesForKey() {
	testJSONStr := `{
		"arrayKey": [
			{"testKey1": "value1"},
			{"testKey2": "value2"},
			{"key3": "value3"}
		],
		"key4": {
			"testKey5": "value5",
			"key6": "value6"
		},
		"testArrayKey": [
			{"testKey7": "should not be processed"}
		],
		"testKey8": "value8"
	}`
	dsl := fastjson.MustParse(testJSONStr)
	testKeyFilter := func(key string) bool {
		return strings.HasPrefix(key, "test")
	}
	processedValue := make(map[string]struct{})
	testProcessFunc := func(obj *fastjson.Object, key string, value *fastjson.Value) error {
		s.Equal(obj.Get(key), value)
		processedValue[value.String()] = struct{}{}
		return nil
	}
	s.NoError(processAllValuesForKey(dsl, testKeyFilter, testProcessFunc))

	expectedProcessedValue := map[string]struct{}{
		`"value1"`: struct{}{},
		`"value2"`: struct{}{},
		`"value5"`: struct{}{},
		`[{"testKey7":"should not be processed"}]`: struct{}{},
		`"value8"`: struct{}{},
	}
	s.Equal(expectedProcessedValue, processedValue)
}

func (s *ESVisibilitySuite) TestGetFieldType() {
	s.Equal(workflow.IndexedValueTypeInt, s.visibilityStore.getFieldType("StartTime"))
	s.Equal(workflow.IndexedValueTypeDatetime, s.visibilityStore.getFieldType("Attr.CustomDatetimeField"))
}

func (s *ESVisibilitySuite) TestGetValueOfSearchAfterInJSON() {
	v := s.visibilityStore

	// Int field
	token := s.getTokenHelper(123)
	sortField := definition.CustomIntField
	res, err := v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`[123, "t"]`, res)

	jsonData := `{"SortValue": -9223372036854776000, "TieBreaker": "t"}`
	dec := json.NewDecoder(strings.NewReader(jsonData))
	dec.UseNumber()
	err = dec.Decode(&token)
	s.Nil(err)
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`[-9223372036854775808, "t"]`, res)

	jsonData = `{"SortValue": 9223372036854776000, "TieBreaker": "t"}`
	dec = json.NewDecoder(strings.NewReader(jsonData))
	dec.UseNumber()
	err = dec.Decode(&token)
	s.Nil(err)
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`[9223372036854775807, "t"]`, res)

	// Double field
	token = s.getTokenHelper(1.11)
	sortField = definition.CustomDoubleField
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`[1.11, "t"]`, res)

	jsonData = `{"SortValue": "-Infinity", "TieBreaker": "t"}`
	dec = json.NewDecoder(strings.NewReader(jsonData))
	dec.UseNumber()
	err = dec.Decode(&token)
	s.Nil(err)
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`["-Infinity", "t"]`, res)

	// Keyword field
	token = s.getTokenHelper("keyword")
	sortField = definition.CustomKeywordField
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`["keyword", "t"]`, res)

	token = s.getTokenHelper(nil)
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`[null, "t"]`, res)
}

func (s *ESVisibilitySuite) getTokenHelper(sortValue interface{}) *esVisibilityPageToken {
	v := s.visibilityStore
	token := &esVisibilityPageToken{
		SortValue:  sortValue,
		TieBreaker: "t",
	}
	encoded, _ := v.serializePageToken(token) // necessary, otherwise token is fake and not json decoded
	token, _ = v.deserializePageToken(encoded)
	return token
}

func (s *ESVisibilitySuite) TestCleanDSL() {
	// dsl without `field`
	dsl := `{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"2b8344db-0ed6-47a4-92fd-bdeb6ead93e3"}}},{"bool":{"must":[{"match_phrase":{"Attr.CustomIntField":{"query":"1"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`
	res := cleanDSL(dsl)
	s.Equal(dsl, res)

	// dsl with `field`
	dsl = `{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"2b8344db-0ed6-47a4-92fd-bdeb6ead93e3"}}},{"bool":{"must":[{"range":{"` + "`Attr.CustomIntField`" + `":{"from":"1","to":"5"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`
	res = cleanDSL(dsl)
	expected := `{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"2b8344db-0ed6-47a4-92fd-bdeb6ead93e3"}}},{"bool":{"must":[{"range":{"Attr.CustomIntField":{"from":"1","to":"5"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`
	s.Equal(expected, res)

	// dsl with mixed
	dsl = `{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"2b8344db-0ed6-47a4-92fd-bdeb6ead93e3"}}},{"bool":{"must":[{"range":{"` + "`Attr.CustomIntField`" + `":{"from":"1","to":"5"}}},{"range":{"` + "`Attr.CustomDoubleField`" + `":{"from":"1.0","to":"2.0"}}},{"range":{"StartTime":{"gt":"0"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`
	res = cleanDSL(dsl)
	expected = `{"query":{"bool":{"must":[{"match_phrase":{"DomainID":{"query":"2b8344db-0ed6-47a4-92fd-bdeb6ead93e3"}}},{"bool":{"must":[{"range":{"Attr.CustomIntField":{"from":"1","to":"5"}}},{"range":{"Attr.CustomDoubleField":{"from":"1.0","to":"2.0"}}},{"range":{"StartTime":{"gt":"0"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunID":"desc"}]}`
	s.Equal(expected, res)
}
