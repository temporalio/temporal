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

package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	es "github.com/uber/cadence/common/elasticsearch"
	esMocks "github.com/uber/cadence/common/elasticsearch/mocks"
	"strings"
	"testing"
)

type ESVisibilitySuite struct {
	suite.Suite
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	visibilityMgr *esVisibilityManager
	mockESClient  *esMocks.Client
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

	testRequest = &ListWorkflowExecutionsRequest{
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
	mgr := NewElasticSearchVisibilityManager(s.mockESClient, testIndex, bark.NewNopLogger())
	s.visibilityMgr = mgr.(*esVisibilityManager)
}

func (s *ESVisibilitySuite) TearDownTest() {
	s.mockESClient.AssertExpectations(s.T())
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionStarted() {
	s.Equal(errOperationNotSupported, s.visibilityMgr.RecordWorkflowExecutionStarted(nil))
}

func (s *ESVisibilitySuite) TestRecordWorkflowExecutionClosed() {
	s.Equal(errOperationNotSupported, s.visibilityMgr.RecordWorkflowExecutionClosed(nil))
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutions() {
	s.mockESClient.On("Search", mock.Anything, mock.MatchedBy(func(input *es.SearchParameters) bool {
		source, _ := input.Query.Source()
		s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
		return true
	})).Return(testSearchResult, nil).Once()
	_, err := s.visibilityMgr.ListOpenWorkflowExecutions(testRequest)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListOpenWorkflowExecutions(testRequest)
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
	_, err := s.visibilityMgr.ListClosedWorkflowExecutions(testRequest)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListClosedWorkflowExecutions(testRequest)
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

	request := &ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityMgr.ListOpenWorkflowExecutionsByType(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListOpenWorkflowExecutionsByType(request)
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

	request := &ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityMgr.ListClosedWorkflowExecutionsByType(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListClosedWorkflowExecutionsByType(request)
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

	request := &ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityMgr.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListOpenWorkflowExecutionsByWorkflowID(request)
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

	request := &ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityMgr.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListClosedWorkflowExecutionsByWorkflowID(request)
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

	request := &ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		Status:                        workflow.WorkflowExecutionCloseStatus(testCloseStatus),
	}
	_, err := s.visibilityMgr.ListClosedWorkflowExecutionsByStatus(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.ListClosedWorkflowExecutionsByStatus(request)
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
	request := &GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
			RunId:      common.StringPtr(testRunID),
		},
	}
	_, err := s.visibilityMgr.GetClosedWorkflowExecution(request)
	s.NoError(err)

	s.mockESClient.On("Search", mock.Anything, mock.Anything).Return(nil, errTestESSearch).Once()
	_, err = s.visibilityMgr.GetClosedWorkflowExecution(request)
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
	request := &GetClosedWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
		},
	}
	_, err := s.visibilityMgr.GetClosedWorkflowExecution(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestGetNextPageToken() {
	token, err := s.visibilityMgr.getNextPageToken([]byte{})
	s.Equal(0, token.From)
	s.NoError(err)

	from := 5
	input, err := s.visibilityMgr.serializePageToken(&esVisibilityPageToken{From: from})
	s.NoError(err)
	token, err = s.visibilityMgr.getNextPageToken(input)
	s.Equal(from, token.From)
	s.NoError(err)

	badInput := []byte("bad input")
	token, err = s.visibilityMgr.getNextPageToken(badInput)
	s.Nil(token)
	s.Error(err)
}

func (s *ESVisibilitySuite) TestGetSearchResult() {
	request := testRequest
	from := 1
	token := &esVisibilityPageToken{From: from}

	matchDomainQuery := elastic.NewMatchQuery(es.DomainID, request.DomainUUID)
	existClosedStatusQuery := elastic.NewExistsQuery(es.CloseStatus)

	// test for open
	isOpen := true
	rangeQuery := elastic.NewRangeQuery(es.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery := elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery).MustNot(existClosedStatusQuery)
	params := &es.SearchParameters{
		Index:    testIndex,
		Query:    boolQuery,
		From:     from,
		PageSize: testPageSize,
		Sorter:   []elastic.Sorter{elastic.NewFieldSort(es.StartTime).Desc()},
	}
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	s.visibilityMgr.getSearchResult(request, token, nil, isOpen)

	// test for closed
	isOpen = false
	rangeQuery = elastic.NewRangeQuery(es.CloseTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery = elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery).Must(existClosedStatusQuery)
	params.Query = boolQuery
	params.Sorter = []elastic.Sorter{elastic.NewFieldSort(es.CloseTime).Desc()}
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	s.visibilityMgr.getSearchResult(request, token, nil, isOpen)

	// test for additional matchQuery
	matchQuery := elastic.NewMatchQuery(es.CloseStatus, int32(0))
	boolQuery = elastic.NewBoolQuery().Must(matchDomainQuery).Filter(rangeQuery).Must(matchQuery).Must(existClosedStatusQuery)
	params.Query = boolQuery
	s.mockESClient.On("Search", mock.Anything, params).Return(nil, nil).Once()
	s.visibilityMgr.getSearchResult(request, token, matchQuery, isOpen)
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	isOpen := true
	token := &esVisibilityPageToken{From: 0}
	serializedToken, _ := s.visibilityMgr.serializePageToken(token)

	// test for empty hits
	searchHits := &elastic.SearchHits{}
	resp, err := s.visibilityMgr.getListWorkflowExecutionsResponse(searchHits, token, isOpen)
	s.NoError(err)
	s.Equal(serializedToken, resp.NextPageToken)
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
	}
	searchHits.Hits = []*elastic.SearchHit{searchHit}
	resp, err = s.visibilityMgr.getListWorkflowExecutionsResponse(searchHits, token, isOpen)
	s.NoError(err)
	serializedToken, _ = s.visibilityMgr.serializePageToken(&esVisibilityPageToken{From: 1})
	s.Equal(serializedToken, resp.NextPageToken)
	s.Equal(1, len(resp.Executions))
}

func (s *ESVisibilitySuite) TestDeserializePageToken() {
	token := &esVisibilityPageToken{From: 0}
	data, _ := s.visibilityMgr.serializePageToken(token)
	result, err := s.visibilityMgr.deserializePageToken(data)
	s.NoError(err)
	s.Equal(token, result)

	badInput := []byte("bad input")
	result, err = s.visibilityMgr.deserializePageToken(badInput)
	s.Error(err)
	s.Nil(result)
	err, ok := err.(*workflow.BadRequestError)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "unable to deserialize page token"))
}

func (s *ESVisibilitySuite) TestSerializePageToken() {
	data, err := s.visibilityMgr.serializePageToken(nil)
	s.NoError(err)
	s.True(len(data) > 0)
	token, err := s.visibilityMgr.deserializePageToken(data)
	s.NoError(err)
	s.Equal(0, token.From)

	newToken := &esVisibilityPageToken{From: 5}
	data, err = s.visibilityMgr.serializePageToken(newToken)
	s.NoError(err)
	s.True(len(data) > 0)
	token, err = s.visibilityMgr.deserializePageToken(data)
	s.NoError(err)
	s.Equal(newToken, token)
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
	isOpen := true
	info := s.visibilityMgr.convertSearchResultToVisibilityRecord(searchHit, isOpen)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.Execution.GetWorkflowId())
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.Execution.GetRunId())
	s.Equal("TestWorkflowExecute", info.GetType().GetName())
	s.Equal(int64(1547596872371000000), info.GetStartTime())

	// test for close
	isOpen = false
	info = s.visibilityMgr.convertSearchResultToVisibilityRecord(searchHit, isOpen)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.Execution.GetWorkflowId())
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.Execution.GetRunId())
	s.Equal("TestWorkflowExecute", info.GetType().GetName())
	s.Equal(int64(1547596872371000000), info.GetStartTime())
	s.Equal(int64(1547596872817380000), info.GetCloseTime())
	s.Equal(workflow.WorkflowExecutionCloseStatusCompleted, info.GetCloseStatus())
	s.Equal(int64(29), info.GetHistoryLength())

	// test for error case
	badData := []byte(`corrupted data`)
	source = (*json.RawMessage)(&badData)
	searchHit = &elastic.SearchHit{
		Source: source,
	}
	info = s.visibilityMgr.convertSearchResultToVisibilityRecord(searchHit, isOpen)
	s.Nil(info)
}
