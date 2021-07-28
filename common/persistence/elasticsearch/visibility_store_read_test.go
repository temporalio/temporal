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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/valyala/fastjson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

type (
	ESVisibilitySuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		*require.Assertions
		controller        *gomock.Controller
		visibilityStore   *visibilityStore
		mockESClient      *client.MockClient
		mockProcessor     *MockProcessor
		mockMetricsClient *metrics.MockClient
	}
)

var (
	testIndex        = "test-index"
	testNamespace    = "test-namespace"
	testNamespaceID  = "bfd5c907-f899-4baf-a7b2-2ab85e623ebd"
	testPageSize     = 5
	testEarliestTime = time.Unix(0, 1547596872371000000).UTC()
	testLatestTime   = time.Unix(0, 2547596872371000000).UTC()
	testWorkflowType = "test-wf-type"
	testWorkflowID   = "test-wid"
	testRunID        = "1601da05-4db9-4eeb-89e4-da99481bdfc9"
	testStatus       = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED

	testSearchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{},
	}
	errTestESSearch = errors.New("ES error")

	filterOpen              = fmt.Sprintf("map[term:map[ExecutionStatus:%s]", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String())
	filterClose             = fmt.Sprintf("must_not:map[term:map[ExecutionStatus:%s]]", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String())
	filterByType            = fmt.Sprintf("map[term:map[WorkflowType:%s]", testWorkflowType)
	filterByWID             = fmt.Sprintf("map[term:map[WorkflowId:%s]", testWorkflowID)
	filterByRunID           = fmt.Sprintf("map[term:map[RunId:%s]", testRunID)
	filterByExecutionStatus = fmt.Sprintf("map[term:map[ExecutionStatus:%s]", testStatus.String())
)

func createTestRequest() *persistence.ListWorkflowExecutionsRequest {
	return &persistence.ListWorkflowExecutionsRequest{
		NamespaceID:       testNamespaceID,
		Namespace:         testNamespace,
		PageSize:          testPageSize,
		EarliestStartTime: testEarliestTime,
		LatestStartTime:   testLatestTime,
	}
}

func TestESVisibilitySuite(t *testing.T) {
	suite.Run(t, new(ESVisibilitySuite))
}

func (s *ESVisibilitySuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	cfg := &config.VisibilityConfig{
		ESProcessorAckTimeout: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}

	s.controller = gomock.NewController(s.T())
	s.mockMetricsClient = metrics.NewMockClient(s.controller)
	s.mockProcessor = NewMockProcessor(s.controller)
	s.mockESClient = client.NewMockClient(s.controller)
	s.visibilityStore = NewVisibilityStore(s.mockESClient, testIndex, searchattribute.NewTestProvider(), s.mockProcessor, cfg, log.NewNoopLogger(), s.mockMetricsClient)
}

func (s *ESVisibilitySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutions() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
			return testSearchResult, nil
		})
	_, err := s.visibilityStore.ListOpenWorkflowExecutions(createTestRequest())
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListOpenWorkflowExecutions(createTestRequest())
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListOpenWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutions() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
			return testSearchResult, nil
		})
	_, err := s.visibilityStore.ListClosedWorkflowExecutions(createTestRequest())
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutions(createTestRequest())
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutionsByType() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterByType))
			return testSearchResult, nil
		})

	testRequest := createTestRequest()
	request := &persistence.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityStore.ListOpenWorkflowExecutionsByType(request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListOpenWorkflowExecutionsByType(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListOpenWorkflowExecutionsByType failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutionsByType() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterByType))
			return testSearchResult, nil
		})

	testRequest := createTestRequest()
	request := &persistence.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByType(request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByType(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByType failed"))
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutionsByWorkflowID() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterOpen))
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
			return testSearchResult, nil
		})

	testRequest := createTestRequest()
	request := &persistence.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityStore.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListOpenWorkflowExecutionsByWorkflowID(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListOpenWorkflowExecutionsByWorkflowID failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutionsByWorkflowID() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
			return testSearchResult, nil
		})

	testRequest := createTestRequest()
	request := &persistence.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByWorkflowID(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByWorkflowID failed"))
}

func (s *ESVisibilitySuite) TestListClosedWorkflowExecutionsByStatus() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterByExecutionStatus))
			return testSearchResult, nil
		})

	testRequest := createTestRequest()
	request := &persistence.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: *testRequest,
		Status:                        testStatus,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByStatus(request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByStatus(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByStatus failed"))
}

func (s *ESVisibilitySuite) TestGetClosedWorkflowExecution() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			sourceStr := fmt.Sprintf("%v", source)
			s.True(strings.Contains(sourceStr, filterClose))
			s.True(strings.Contains(sourceStr, filterByWID))
			s.True(strings.Contains(sourceStr, filterByRunID))
			return testSearchResult, nil
		})
	request := &persistence.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	_, err := s.visibilityStore.GetClosedWorkflowExecution(request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.GetClosedWorkflowExecution(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "GetClosedWorkflowExecution failed"))
}

func (s *ESVisibilitySuite) TestGetClosedWorkflowExecution_NoRunID() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterClose))
			s.True(strings.Contains(fmt.Sprintf("%v", source), filterByWID))
			s.False(strings.Contains(fmt.Sprintf("%v", source), filterByRunID))
			return testSearchResult, nil
		})
	request := &persistence.GetClosedWorkflowExecutionRequest{
		NamespaceID: testNamespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
		},
	}
	_, err := s.visibilityStore.GetClosedWorkflowExecution(request)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestGetNextPageToken() {
	token, err := s.visibilityStore.getNextPageToken([]byte{})
	s.NoError(err)
	s.Nil(token.SortValue)
	s.Empty(token.TieBreaker)

	input, err := s.visibilityStore.serializePageToken(&visibilityPageToken{SortValue: 123, TieBreaker: "qwe"})
	s.NoError(err)
	token, err = s.visibilityStore.getNextPageToken(input)
	s.NoError(err)
	tokenSortValue, err := token.SortValue.(json.Number).Int64()
	s.NoError(err)
	s.Equal(int64(123), tokenSortValue)
	s.Equal("qwe", token.TieBreaker)
	s.NoError(err)

	badInput := []byte("bad input")
	token, err = s.visibilityStore.getNextPageToken(badInput)
	s.Nil(token)
	s.Error(err)
}

func (s *ESVisibilitySuite) TestGetSearchResult() {
	request := createTestRequest()
	token := &visibilityPageToken{SortValue: 123, TieBreaker: "qwe"}

	matchNamespaceQuery := elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID)
	runningQuery := elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	tieBreakerSorter := elastic.NewFieldSort(searchattribute.RunID).Desc()

	// test for open
	rangeQuery := elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery := elastic.NewBoolQuery().Filter(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	params := &client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: []interface{}{123, "qwe"},
		PageSize:    testPageSize,
		Sorter:      []elastic.Sorter{elastic.NewFieldSort(searchattribute.StartTime).Desc(), tieBreakerSorter},
	}
	s.mockESClient.EXPECT().Search(gomock.Any(), params).Return(nil, nil)
	_, err := s.visibilityStore.getSearchResult(request, token, elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))), true)
	s.NoError(err)

	// test request latestTime overflow
	request.LatestStartTime = time.Unix(0, math.MaxInt64).UTC()
	rangeQuery1 := elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery1 := elastic.NewBoolQuery().Filter(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery1)
	param1 := &client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery1,
		SearchAfter: []interface{}{123, "qwe"},
		PageSize:    testPageSize,
		Sorter:      []elastic.Sorter{elastic.NewFieldSort(searchattribute.StartTime).Desc(), tieBreakerSorter},
	}
	s.mockESClient.EXPECT().Search(gomock.Any(), param1).Return(nil, nil)
	_, err = s.visibilityStore.getSearchResult(request, token, elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))), true)
	s.NoError(err)
	request = createTestRequest() // revert

	// test for closed
	rangeQuery = elastic.NewRangeQuery(searchattribute.CloseTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery = elastic.NewBoolQuery().MustNot(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	params.Query = boolQuery
	params.Sorter = []elastic.Sorter{elastic.NewFieldSort(searchattribute.CloseTime).Desc(), tieBreakerSorter}
	s.mockESClient.EXPECT().Search(gomock.Any(), params).Return(nil, nil)
	_, err = s.visibilityStore.getSearchResult(request, token, elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))), false)
	s.NoError(err)

	// test for additional boolQuery
	matchQuery := elastic.NewTermQuery(searchattribute.ExecutionStatus, int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	boolQuery = elastic.NewBoolQuery().Filter(matchQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	params.Query = boolQuery
	s.mockESClient.EXPECT().Search(gomock.Any(), params).Return(nil, nil)
	_, err = s.visibilityStore.getSearchResult(request, token, elastic.NewBoolQuery().Filter(matchQuery), false)
	s.NoError(err)

	// test for search after
	token = &visibilityPageToken{
		SortValue:  request.LatestStartTime,
		TieBreaker: "qwe",
	}
	params.SearchAfter = []interface{}{token.SortValue, token.TieBreaker}
	s.mockESClient.EXPECT().Search(gomock.Any(), params).Return(nil, nil)
	_, err = s.visibilityStore.getSearchResult(request, token, elastic.NewBoolQuery().Filter(matchQuery), false)
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	// test for empty hits
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			TotalHits: &elastic.TotalHits{},
		}}
	resp, err := s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, 1, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(0, len(resp.Executions))

	// test for one hits
	data := []byte(`{"ExecutionStatus": "Running",
          "CloseTime": "2021-06-11T16:04:07.980-07:00",
          "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "StateTransitionCount": 22,
          "VisibilityTaskKey": "7-619",
          "RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": "2021-06-11T15:04:07.980-07:00",
          "WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "basic.stressWorkflowExecute"}`)
	source := json.RawMessage(data)
	searchHit := &elastic.SearchHit{
		Source: source,
		Sort:   []interface{}{1547596872371000000, "e481009e-14b3-45ae-91af-dce6e2a88365"},
	}
	searchResult.Hits.Hits = []*elastic.SearchHit{searchHit}
	searchResult.Hits.TotalHits.Value = 1
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, 1, nil)
	s.NoError(err)
	serializedToken, _ := s.visibilityStore.serializePageToken(&visibilityPageToken{SortValue: 1547596872371000000, TieBreaker: "e481009e-14b3-45ae-91af-dce6e2a88365"})
	s.Equal(serializedToken, resp.NextPageToken)
	s.Equal(1, len(resp.Executions))

	// test for last page hits
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, 2, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(1, len(resp.Executions))

	// test for search after
	searchResult.Hits.Hits = []*elastic.SearchHit{}
	for i := int64(0); i < searchResult.Hits.TotalHits.Value; i++ {
		searchResult.Hits.Hits = append(searchResult.Hits.Hits, searchHit)
	}
	numOfHits := len(searchResult.Hits.Hits)
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, numOfHits, nil)
	s.NoError(err)
	s.Equal(numOfHits, len(resp.Executions))
	nextPageToken, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	resultSortValue, err := nextPageToken.SortValue.(json.Number).Int64()
	s.NoError(err)
	s.Equal(int64(1547596872371000000), resultSortValue)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", nextPageToken.TieBreaker)
	// for last page
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, numOfHits+1, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(numOfHits, len(resp.Executions))
}

func (s *ESVisibilitySuite) TestDeserializePageToken() {
	badInput := []byte("bad input")
	result, err := s.visibilityStore.deserializePageToken(badInput)
	s.Error(err)
	s.Nil(result)
	err, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "unable to deserialize page token"))

	token := &visibilityPageToken{SortValue: int64(64), TieBreaker: "unique"}
	data, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
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
	s.Nil(token.SortValue)
	s.Empty(token.TieBreaker)

	sortTime := int64(123)
	tieBreaker := "unique"
	newToken := &visibilityPageToken{SortValue: sortTime, TieBreaker: tieBreaker}
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

func (s *ESVisibilitySuite) TestParseESDoc() {
	searchHit := &elastic.SearchHit{
		Source: []byte(`{"ExecutionStatus": "Running",
          "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "StateTransitionCount": 10,
          "VisibilityTaskKey": "7-619",
          "RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": "2021-06-11T15:04:07.980-07:00",
          "WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "TestWorkflowExecute"}`),
	}
	// test for open
	info := s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	s.Equal("TestWorkflowExecute", info.TypeName)
	s.Equal(int64(10), info.StateTransitionCount)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.Status)
	expectedStartTime, err := time.Parse(time.RFC3339Nano, "2021-06-11T15:04:07.980-07:00")
	s.NoError(err)
	s.Equal(expectedStartTime, info.StartTime)
	s.Nil(info.SearchAttributes)

	// test for close
	searchHit = &elastic.SearchHit{
		Source: []byte(`{"ExecutionStatus": "Completed",
          "CloseTime": "2021-06-11T16:04:07Z",
          "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "StateTransitionCount": 20,
          "VisibilityTaskKey": "7-619",
          "RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": "2021-06-11T15:04:07.980-07:00",
          "WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "TestWorkflowExecute"}`),
	}
	info = s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	s.Equal("TestWorkflowExecute", info.TypeName)
	s.Equal(int64(20), info.StateTransitionCount)
	expectedStartTime, err = time.Parse(time.RFC3339Nano, "2021-06-11T15:04:07.980-07:00")
	s.NoError(err)
	expectedCloseTime, err := time.Parse(time.RFC3339Nano, "2021-06-11T16:04:07Z")
	s.NoError(err)
	s.Equal(expectedStartTime, info.StartTime)
	s.Equal(expectedCloseTime, info.CloseTime)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)
	s.Equal(int64(29), info.HistoryLength)
	s.Nil(info.SearchAttributes)

	// test for error case
	searchHit = &elastic.SearchHit{
		Source: []byte(`corrupted data`),
	}
	info = s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap)
	s.Nil(info)
}

func (s *ESVisibilitySuite) TestParseESDoc_SearchAttributes() {
	searchHit := &elastic.SearchHit{
		Source: []byte(`{"TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomStringField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`),
	}
	// test for open
	info := s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap)
	s.NotNil(info)
	s.Equal([]interface{}{"ver1", "ver2"}, info.SearchAttributes["TemporalChangeVersion"])

	s.Equal("bfd5c907-f899-4baf-a7b2-2ab85e623ebd", info.SearchAttributes["CustomKeywordField"])

	s.Equal("text text", info.SearchAttributes["CustomStringField"])

	date1, err := time.Parse(time.RFC3339Nano, "2014-08-28T03:15:00.000-07:00")
	s.NoError(err)
	date2, err := time.Parse(time.RFC3339Nano, "2016-04-21T05:00:00.000-07:00")
	s.NoError(err)
	s.Equal([]interface{}{date1, date2}, info.SearchAttributes["CustomDatetimeField"])

	s.Equal([]interface{}{1234.1234, 5678.5678}, info.SearchAttributes["CustomDoubleField"])

	s.Equal(true, info.SearchAttributes["CustomBoolField"])

	s.Equal([]interface{}{int64(111), int64(222)}, info.SearchAttributes["CustomIntField"])

	_, ok := info.SearchAttributes["UnknownField"]
	s.False(ok)
}

// nolint
func (s *ESVisibilitySuite) TestGetESQueryDSL() {
	request := &persistence.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		PageSize:    10,
	}
	token := &visibilityPageToken{}

	v := s.visibilityStore

	request.Query = ""
	dsl, err := v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = "invaild query"
	dsl, err = v.getESQueryDSL(request, token)
	s.NotNil(err)
	s.Equal("", dsl)

	request.Query = `WorkflowId = 'wid'`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `WorkflowId = 'wid' or WorkflowId = 'another-wid'`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"should":[{"match_phrase":{"WorkflowId":{"query":"wid"}}},{"match_phrase":{"WorkflowId":{"query":"another-wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `WorkflowId = 'wid' order by StartTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `WorkflowId = 'wid' and CloseTime = missing`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `WorkflowId = 'wid' or CloseTime = missing`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"should":[{"match_phrase":{"WorkflowId":{"query":"wid"}}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `CloseTime = missing order by CloseTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}},"from":0,"size":10,"sort":[{"CloseTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `StartTime = "2018-06-07T15:04:05-08:00"`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"StartTime":{"query":"2018-06-07T15:04:05-08:00"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `WorkflowId = 'wid' and StartTime > "2018-06-07T15:04:05+00:00"`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}},{"range":{"StartTime":{"gt":"2018-06-07T15:04:05+00:00"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `ExecutionTime < 1000000`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"lt":"1970-01-01T00:00:00.001Z"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `ExecutionTime < 1000000 or ExecutionTime > 2000000`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"should":[{"range":{"ExecutionTime":{"lt":"1970-01-01T00:00:00.001Z"}}},{"range":{"ExecutionTime":{"gt":"1970-01-01T00:00:00.002Z"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `order by ExecutionTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}},"from":0,"size":10,"sort":[{"ExecutionTime":"desc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `order by StartTime desc, CloseTime desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Equal(errors.New("only one field can be used to sort"), err)

	request.Query = `order by CustomStringField desc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Equal(errors.New("unable to sort by field of String type, use field of type Keyword"), err)

	request.Query = `order by CustomIntField asc`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}},"from":0,"size":10,"sort":[{"CustomIntField":"asc"},{"RunId":"desc"}]}`, dsl)

	request.Query = `ExecutionTime < "unable to parse"`
	_, err = v.getESQueryDSL(request, token)
	// Wrong dates goes directly to Elasticsearch and it return an error.
	s.NoError(err)

	token = s.getTokenHelper(1)
	request.Query = `WorkflowId = 'wid'`
	dsl, err = v.getESQueryDSL(request, token)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}}]}}]}},"from":0,"size":10,"sort":[{"StartTime":"desc"},{"RunId":"desc"}],"search_after":[1,"t"]}`, dsl)

	// invalid union injection
	request.Query = `WorkflowId = 'wid' union select * from dummy`
	_, err = v.getESQueryDSL(request, token)
	s.NotNil(err)
}

func (s *ESVisibilitySuite) TestGetESQueryDSLForScan() {
	request := &persistence.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		PageSize:    10,
	}

	request.Query = `WorkflowId = 'wid' order by StartTime desc`
	dsl, err := getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}}]}}]}},"from":0,"size":10}`, dsl)

	request.Query = `WorkflowId = 'wid'`
	dsl, err = getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}}]}}]}},"from":0,"size":10}`, dsl)

	request.Query = `CloseTime = missing and (ExecutionTime >= "2019-08-27T15:04:05+00:00" or StartTime <= "2018-06-07T15:04:05+00:00")`
	dsl, err = getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}},{"bool":{"should":[{"range":{"ExecutionTime":{"from":"2019-08-27T15:04:05+00:00"}}},{"range":{"StartTime":{"to":"2018-06-07T15:04:05+00:00"}}}]}}]}}]}},"from":0,"size":10}`, dsl)

	request.Query = `ExecutionTime < 1000000 and ExecutionTime > 5000000`
	dsl, err = getESQueryDSLForScan(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"lt":"1970-01-01T00:00:00.001Z"}}},{"range":{"ExecutionTime":{"gt":"1970-01-01T00:00:00.005Z"}}}]}}]}},"from":0,"size":10}`, dsl)
}

func (s *ESVisibilitySuite) TestGetESQueryDSLForCount() {
	request := &persistence.CountWorkflowExecutionsRequest{
		NamespaceID: testNamespaceID,
	}

	// empty query
	dsl, err := getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}}}`, dsl)

	request.Query = `WorkflowId = 'wid' order by StartTime desc`
	dsl, err = getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_phrase":{"WorkflowId":{"query":"wid"}}}]}}]}}}`, dsl)

	request.Query = `CloseTime < "2018-06-07T15:04:05+07:00" and StartTime > "2018-05-04T16:00:00+07:00" and ExecutionTime >= "2018-05-05T16:00:00+07:00"`
	dsl, err = getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"CloseTime":{"lt":"2018-06-07T15:04:05+07:00"}}},{"range":{"StartTime":{"gt":"2018-05-04T16:00:00+07:00"}}},{"range":{"ExecutionTime":{"from":"2018-05-05T16:00:00+07:00"}}}]}}]}}}`, dsl)

	request.Query = `ExecutionTime < 1000000`
	dsl, err = getESQueryDSLForCount(request)
	s.Nil(err)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"range":{"ExecutionTime":{"lt":"1970-01-01T00:00:00.001Z"}}}]}}]}}}`, dsl)
}

func (s *ESVisibilitySuite) TestAddNamespaceToQuery() {
	dsl := fastjson.MustParse(`{}`)
	dslStr := dsl.String()
	addNamespaceToQuery(dsl, "")
	s.Equal(dslStr, dsl.String())

	dsl = fastjson.MustParse(`{"query":{"bool":{"must":[{"match_all":{}}]}}}`)
	addNamespaceToQuery(dsl, testNamespaceID)
	s.Equal(`{"query":{"bool":{"must":[{"match_phrase":{"NamespaceId":{"query":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}},{"bool":{"must":[{"match_all":{}}]}}]}}}`, dsl.String())
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions() {
	s.mockESClient.EXPECT().SearchWithDSL(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, index, input string) (*elastic.SearchResult, error) {
			s.True(strings.Contains(input, `{"match_phrase":{"ExecutionStatus":{"query":"Terminated"}}}`))
			return testSearchResult, nil
		})

	request := &persistence.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    10,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	_, err := s.visibilityStore.ListWorkflowExecutions(request)
	s.NoError(err)

	s.mockESClient.EXPECT().SearchWithDSL(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListWorkflowExecutions(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListWorkflowExecutions failed"))

	request.Query = `invalid query`
	_, err = s.visibilityStore.ListWorkflowExecutions(request)
	s.Error(err)
	_, ok = err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "Error when parse query"))
}

func (s *ESVisibilitySuite) TestScanWorkflowExecutions() {
	// test first page
	s.mockESClient.EXPECT().ScrollFirstPage(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index, input string) (*elastic.SearchResult, client.ScrollService, error) {
			s.True(strings.Contains(input, `{"match_phrase":{"ExecutionStatus":{"query":"Terminated"}}}`))
			return testSearchResult, nil, nil
		})

	request := &persistence.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    10,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	_, err := s.visibilityStore.ScanWorkflowExecutions(request)
	s.NoError(err)

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "Error when parse query"))

	// test scroll
	scrollID := "scrollID-1"
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID).Return(testSearchResult, nil, nil)

	token := &visibilityPageToken{ScrollID: scrollID}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
	request.NextPageToken = tokenBytes
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.NoError(err)

	// test last page
	mockScroll := client.NewMockScrollService(s.controller)
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID).Return(testSearchResult, mockScroll, io.EOF)
	mockScroll.EXPECT().Clear(gomock.Any()).Return(nil)
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.NoError(err)

	// test internal error
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID).Return(nil, nil, errTestESSearch)
	_, err = s.visibilityStore.ScanWorkflowExecutions(request)
	s.Error(err)
	_, ok = err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ScanWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestCountWorkflowExecutions() {
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index, input string) (int64, error) {
			s.True(strings.Contains(input, `{"match_phrase":{"ExecutionStatus":{"query":"Terminated"}}}`))
			return int64(1), nil
		})

	request := &persistence.CountWorkflowExecutionsRequest{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	resp, err := s.visibilityStore.CountWorkflowExecutions(request)
	s.NoError(err)
	s.Equal(int64(1), resp.Count)

	// test internal error
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).Return(int64(0), errTestESSearch)

	_, err = s.visibilityStore.CountWorkflowExecutions(request)
	s.Error(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "CountWorkflowExecutions failed"))

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.CountWorkflowExecutions(request)
	s.Error(err)
	_, ok = err.(*serviceerror.InvalidArgument)
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
		{value: `"2018-06-07T08:04:05Z"`, returnErr: false},
		{value: `"2018-06-07T15:04:05+07:00"`, returnErr: false},
		{value: `"some invalid time string"`, returnErr: false},
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

func (s *ESVisibilitySuite) TestStatusProcessFunc() {
	cases := []struct {
		key   string
		value string
	}{
		{key: "query", value: "Completed"},
		{key: "query", value: "1"},
		{key: "query", value: "100"},
		{key: "query", value: "BadStatus"},
		{key: "unrelatedKey", value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: `"Completed"`, returnErr: false},
		{value: `"Running"`, returnErr: false},
		{value: `""`, returnErr: false},
		{value: `"BadStatus"`, returnErr: false},
		{value: `"should not be modified"`, returnErr: false},
	}

	for i, testCase := range cases {
		value := fastjson.MustParse(fmt.Sprintf(`{"%s": "%s"}`, testCase.key, testCase.value))
		err := statusProcessFunc(nil, "", value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.Equal(expected[i].value, value.Get(testCase.key).String())
	}
}

func (s *ESVisibilitySuite) TestDurationProcessFunc() {
	cases := []struct {
		key   string
		value string
	}{
		{key: "query", value: "1"},
		{key: "query", value: "5h3m"},
		{key: "query", value: "00:00:01"},
		{key: "query", value: "00:00:61"},
		{key: "query", value: "bad value"},
		{key: "unrelatedKey", value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: `"1"`, returnErr: false},
		{value: `18180000000000`, returnErr: false},
		{value: `1000000000`, returnErr: false},
		{value: `"0"`, returnErr: true},
		{value: `"bad value"`, returnErr: false},
		{value: `"should not be modified"`, returnErr: false},
	}

	for i, testCase := range cases {
		value := fastjson.MustParse(fmt.Sprintf(`{"%s": "%s"}`, testCase.key, testCase.value))
		err := durationProcessFunc(nil, "", value)
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

func (s *ESVisibilitySuite) TestGetValueOfSearchAfterInJSON() {
	v := s.visibilityStore

	// Int field
	token := s.getTokenHelper(123)
	sortField := "CustomIntField"
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
	sortField = "CustomDoubleField"
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
	sortField = "CustomKeywordField"
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`["keyword", "t"]`, res)

	token = s.getTokenHelper(nil)
	res, err = v.getValueOfSearchAfterInJSON(token, sortField)
	s.Nil(err)
	s.Equal(`[null, "t"]`, res)
}

func (s *ESVisibilitySuite) getTokenHelper(sortValue interface{}) *visibilityPageToken {
	v := s.visibilityStore
	token := &visibilityPageToken{
		SortValue:  sortValue,
		TieBreaker: "t",
	}
	encoded, _ := v.serializePageToken(token) // necessary, otherwise token is fake and not json decoded
	token, _ = v.deserializePageToken(encoded)
	return token
}

func (s *ESVisibilitySuite) Test_detailedErrorMessage() {
	err := errors.New("test message")
	s.Equal("test message", detailedErrorMessage(err))

	err = &elastic.Error{
		Status: 500,
	}
	s.Equal("elastic: Error 500 (Internal Server Error)", detailedErrorMessage(err))

	err = &elastic.Error{
		Status: 500,
		Details: &elastic.ErrorDetails{
			Type:   "some type",
			Reason: "some reason",
		},
	}
	s.Equal("elastic: Error 500 (Internal Server Error): some reason [type=some type]", detailedErrorMessage(err))

	err = &elastic.Error{
		Status: 500,
		Details: &elastic.ErrorDetails{
			Type:   "some type",
			Reason: "some reason",
			RootCause: []*elastic.ErrorDetails{
				{
					Type:   "some type",
					Reason: "some reason",
				},
			},
		},
	}
	s.Equal("elastic: Error 500 (Internal Server Error): some reason [type=some type]", detailedErrorMessage(err))

	err = &elastic.Error{
		Status: 500,
		Details: &elastic.ErrorDetails{
			Type:   "some type",
			Reason: "some reason",
			RootCause: []*elastic.ErrorDetails{
				{
					Type:   "some other type1",
					Reason: "some other reason1",
				},
				{
					Type:   "some other type2",
					Reason: "some other reason2",
				},
			},
		},
	}
	s.Equal("elastic: Error 500 (Internal Server Error): some reason [type=some type], root causes: some other reason1 [type=some other type1], some other reason2 [type=some other type2]", detailedErrorMessage(err))
}
