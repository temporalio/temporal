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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

type (
	ESVisibilitySuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		*require.Assertions
		controller                 *gomock.Controller
		visibilityStore            *visibilityStore
		mockESClient               *client.MockClientV7
		mockProcessor              *MockProcessor
		mockMetricsClient          *metrics.MockClient
		mockSearchAttributesMapper *searchattribute.MockMapper
	}
)

var (
	testIndex        = "test-index"
	testNamespace    = namespace.Name("test-namespace")
	testNamespaceID  = namespace.ID("bfd5c907-f899-4baf-a7b2-2ab85e623ebd")
	testPageSize     = 5
	testEarliestTime = time.Unix(0, 1547596872371000000).UTC()
	testLatestTime   = time.Unix(0, 2547596872371000000).UTC()
	testWorkflowType = "test-wf-type"
	testWorkflowID   = "test-wid"
	testStatus       = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED

	testSearchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{},
	}
	errTestESSearch = errors.New("ES error")

	filterOpen              = fmt.Sprintf("map[term:map[ExecutionStatus:%s]", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String())
	filterClose             = fmt.Sprintf("must_not:map[term:map[ExecutionStatus:%s]]", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String())
	filterByType            = fmt.Sprintf("map[term:map[WorkflowType:%s]", testWorkflowType)
	filterByWID             = fmt.Sprintf("map[term:map[WorkflowId:%s]", testWorkflowID)
	filterByExecutionStatus = fmt.Sprintf("map[term:map[ExecutionStatus:%s]", testStatus.String())
)

func createTestRequest() *manager.ListWorkflowExecutionsRequest {
	return &manager.ListWorkflowExecutionsRequest{
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

	esProcessorAckTimeout := dynamicconfig.GetDurationPropertyFn(1 * time.Minute)

	s.controller = gomock.NewController(s.T())
	s.mockMetricsClient = metrics.NewMockClient(s.controller)
	s.mockProcessor = NewMockProcessor(s.controller)
	s.mockESClient = client.NewMockClientV7(s.controller)
	s.mockSearchAttributesMapper = searchattribute.NewMockMapper(s.controller)
	s.visibilityStore = NewVisibilityStore(s.mockESClient, testIndex, searchattribute.NewTestProvider(), nil, s.mockProcessor, esProcessorAckTimeout, s.mockMetricsClient)
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
	_, err := s.visibilityStore.ListOpenWorkflowExecutions(context.Background(), createTestRequest())
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListOpenWorkflowExecutions(context.Background(), createTestRequest())
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
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
	_, err := s.visibilityStore.ListClosedWorkflowExecutions(context.Background(), createTestRequest())
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutions(context.Background(), createTestRequest())
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
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
	request := &manager.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityStore.ListOpenWorkflowExecutionsByType(context.Background(), request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListOpenWorkflowExecutionsByType(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
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
	request := &manager.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: testRequest,
		WorkflowTypeName:              testWorkflowType,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByType(context.Background(), request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByType(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
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
	request := &manager.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityStore.ListOpenWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListOpenWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
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
	request := &manager.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: testRequest,
		WorkflowID:                    testWorkflowID,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByWorkflowID(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
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
	request := &manager.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: testRequest,
		Status:                        testStatus,
	}
	_, err := s.visibilityStore.ListClosedWorkflowExecutionsByStatus(context.Background(), request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListClosedWorkflowExecutionsByStatus(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListClosedWorkflowExecutionsByStatus failed"))
}

func (s *ESVisibilitySuite) TestBuildSearchParameters() {
	request := createTestRequest()

	matchNamespaceQuery := elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID.String())
	runningQuery := elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	token := &visibilityPageToken{SearchAfter: []interface{}{1528358645123456789, "qwe"}}
	var err error
	request.NextPageToken, err = s.visibilityStore.serializePageToken(token)
	s.NoError(err)

	// test for open
	rangeQuery := elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery := elastic.NewBoolQuery().Filter(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	p, err := s.visibilityStore.buildSearchParameters(request, elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))), true)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: []interface{}{json.Number("1528358645123456789"), "qwe"},
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)

	// test request latestTime overflow
	request.LatestStartTime = time.Unix(0, math.MaxInt64).UTC()
	rangeQuery = elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery = elastic.NewBoolQuery().Filter(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	p, err = s.visibilityStore.buildSearchParameters(request, elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))), true)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: []interface{}{json.Number("1528358645123456789"), "qwe"},
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request = createTestRequest() // revert

	// test for closed
	rangeQuery = elastic.NewRangeQuery(searchattribute.CloseTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery = elastic.NewBoolQuery().MustNot(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	p, err = s.visibilityStore.buildSearchParameters(request, elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(searchattribute.ExecutionStatus, int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))), false)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)

	// test for additional boolQuery
	rangeQuery = elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	matchQuery := elastic.NewTermQuery(searchattribute.ExecutionStatus, int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	boolQuery = elastic.NewBoolQuery().Filter(matchQuery).Filter(matchNamespaceQuery).Filter(rangeQuery)
	p, err = s.visibilityStore.buildSearchParameters(request, elastic.NewBoolQuery().Filter(matchQuery), true)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)

	// test for search after
	token = &visibilityPageToken{SearchAfter: []interface{}{json.Number("1528358645123456789"), "qwe"}}
	request.NextPageToken, err = s.visibilityStore.serializePageToken(token)
	s.NoError(err)

	rangeQuery = elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery).Filter(rangeQuery)
	p, err = s.visibilityStore.buildSearchParameters(request, elastic.NewBoolQuery(), true)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: token.SearchAfter,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request = createTestRequest() // revert

	// test for nil token
	rangeQuery = elastic.NewRangeQuery(searchattribute.StartTime).Gte(request.EarliestStartTime).Lte(request.LatestStartTime)
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery).Filter(rangeQuery)
	request.NextPageToken = nil
	p, err = s.visibilityStore.buildSearchParameters(request, elastic.NewBoolQuery(), true)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		PageSize:    testPageSize,
		SearchAfter: nil,
		Sorter:      defaultSorter,
	}, p)
}

func (s *ESVisibilitySuite) TestBuildSearchParametersV2() {
	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    testPageSize,
	}

	matchNamespaceQuery := elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID.String())

	// test for open
	request.Query = `WorkflowId="guid-2208"`
	filterQuery := elastic.NewBoolQuery().Filter(elastic.NewMatchQuery(searchattribute.WorkflowID, "guid-2208"))
	boolQuery := elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery)
	p, err := s.visibilityStore.buildSearchParametersV2(request)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PointInTime: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request.Query = ""

	// test custom sort
	request.Query = `Order bY WorkflowId`
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery)
	p, err = s.visibilityStore.buildSearchParametersV2(request)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PointInTime: nil,
		PageSize:    testPageSize,
		Sorter: []elastic.Sorter{
			elastic.NewFieldSort(searchattribute.WorkflowID).Asc(),
			elastic.NewFieldSort(searchattribute.RunID).Desc(),
		},
	}, p)
	request.Query = ""

	// test for wrong query
	request.Query = "invalid query"
	p, err = s.visibilityStore.buildSearchParametersV2(request)
	s.Nil(p)
	s.Error(err)
	request.Query = ""
}

func (s *ESVisibilitySuite) queryToJSON(q elastic.Query) string {
	m, err := q.Source()
	s.NoError(err)
	b, err := json.Marshal(m)
	s.NoError(err)
	return string(b)
}

func (s *ESVisibilitySuite) sorterToJSON(sorters []*elastic.FieldSort) string {
	var ms []interface{}
	for _, sorter := range sorters {
		m, err := sorter.Source()
		s.NoError(err)
		ms = append(ms, m)
	}
	b, err := json.Marshal(ms)
	s.NoError(err)
	return string(b)
}

func (s *ESVisibilitySuite) Test_convertQuery() {
	query := `WorkflowId = 'wid'`
	qry, srt, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"WorkflowId":{"query":"wid"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `WorkflowId = 'wid' or WorkflowId = 'another-wid'`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"match":{"WorkflowId":{"query":"wid"}}},{"match":{"WorkflowId":{"query":"another-wid"}}}]}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `WorkflowId = 'wid' order by StartTime desc`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"WorkflowId":{"query":"wid"}}}}}]}}`, s.queryToJSON(qry))
	s.Equal(`[{"StartTime":{"order":"desc"}}]`, s.sorterToJSON(srt))

	query = `WorkflowId = 'wid' and CloseTime is null`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"match":{"WorkflowId":{"query":"wid"}}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `WorkflowId = 'wid' or CloseTime is null`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"match":{"WorkflowId":{"query":"wid"}}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `CloseTime is null order by CloseTime desc`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}`, s.queryToJSON(qry))
	s.Equal(`[{"CloseTime":{"order":"desc"}}]`, s.sorterToJSON(srt))

	query = `StartTime = "2018-06-07T15:04:05.123456789-08:00"`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"StartTime":{"query":"2018-06-07T15:04:05.123456789-08:00"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `WorkflowId = 'wid' and StartTime > "2018-06-07T15:04:05+00:00"`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"match":{"WorkflowId":{"query":"wid"}}},{"range":{"StartTime":{"from":"2018-06-07T15:04:05+00:00","include_lower":false,"include_upper":true,"to":null}}}]}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `ExecutionTime < 1000000`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `ExecutionTime between 1 and 2`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.000000001Z","include_lower":true,"include_upper":true,"to":"1970-01-01T00:00:00.000000002Z"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `ExecutionTime < 1000000 or ExecutionTime > 2000000`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}},{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.002Z","include_lower":false,"include_upper":true,"to":null}}}]}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `order by ExecutionTime`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}}}`, s.queryToJSON(qry))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(srt))

	query = `order by StartTime desc, CloseTime asc`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}}}`, s.queryToJSON(qry))
	s.Equal(`[{"StartTime":{"order":"desc"}},{"CloseTime":{"order":"asc"}}]`, s.sorterToJSON(srt))

	query = `order by CustomTextField desc`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal(err.(*serviceerror.InvalidArgument).Error(), "invalid query: unable to convert 'order by' column name: unable to sort by field of Text type, use field of type Keyword")

	query = `order by CustomIntField asc`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}}}`, s.queryToJSON(qry))
	s.Equal(`[{"CustomIntField":{"order":"asc"}}]`, s.sorterToJSON(srt))

	query = `ExecutionTime < "unable to parse"`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	// Wrong dates goes directly to Elasticsearch, and it returns an error.
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"unable to parse"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	// invalid union injection
	query = `WorkflowId = 'wid' union select * from dummy`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.Nil(qry)
	s.Nil(srt)
}

func (s *ESVisibilitySuite) Test_convertQuery_Mapper() {
	s.mockSearchAttributesMapper.EXPECT().GetFieldName(gomock.Any(), testNamespace.String()).DoAndReturn(
		func(alias string, namespace string) (string, error) {
			if strings.HasPrefix(alias, "AliasFor") {
				return strings.TrimPrefix(alias, "AliasFor"), nil
			}
			return "", serviceerror.NewInvalidArgument("mapper error")
		}).AnyTimes()

	s.visibilityStore.searchAttributesMapper = s.mockSearchAttributesMapper

	query := `WorkflowId = 'wid'`
	qry, srt, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"WorkflowId":{"query":"wid"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `AliasForCustomKeywordField = 'pid'`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"CustomKeywordField":{"query":"pid"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `CustomKeywordField = 'pid'`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `AliasForUnknownField = 'pid'`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert filter expression: unable to convert left part of comparison expression: invalid search attribute: AliasForUnknownField")

	query = `order by ExecutionTime`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}}}`, s.queryToJSON(qry))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(srt))

	query = `order by AliasForCustomKeywordField asc`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}}}`, s.queryToJSON(qry))
	s.Equal(`[{"CustomKeywordField":{"order":"asc"}}]`, s.sorterToJSON(srt))

	query = `order by CustomKeywordField asc`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `order by AliasForUnknownField asc`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert 'order by' column name: invalid search attribute: AliasForUnknownField")
	s.visibilityStore.searchAttributesMapper = nil
}

func (s *ESVisibilitySuite) Test_convertQuery_Mapper_Error() {
	s.mockSearchAttributesMapper.EXPECT().GetFieldName(gomock.Any(), testNamespace.String()).DoAndReturn(
		func(fieldName string, namespace string) (string, error) {
			return "", serviceerror.NewInvalidArgument("mapper error")
		}).AnyTimes()

	s.visibilityStore.searchAttributesMapper = s.mockSearchAttributesMapper

	query := `WorkflowId = 'wid'`
	qry, srt, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"WorkflowId":{"query":"wid"}}}}}]}}`, s.queryToJSON(qry))
	s.Nil(srt)

	query = `ProductId = 'pid'`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `order by ExecutionTime`
	qry, srt, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}}}}`, s.queryToJSON(qry))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(srt))

	query = `order by CustomIntField asc`
	_, _, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	s.visibilityStore.searchAttributesMapper = nil
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	// test for empty hits
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			TotalHits: &elastic.TotalHits{},
		}}
	resp, err := s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, 1, nil)
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
		Sort:   []interface{}{1547596872371234567, "e481009e-14b3-45ae-91af-dce6e2a88365"},
	}
	searchResult.Hits.Hits = []*elastic.SearchHit{searchHit}
	searchResult.Hits.TotalHits.Value = 1
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, 1, nil)
	s.NoError(err)
	serializedToken, _ := s.visibilityStore.serializePageToken(&visibilityPageToken{SearchAfter: []interface{}{1547596872371234567, "e481009e-14b3-45ae-91af-dce6e2a88365"}})
	s.Equal(serializedToken, resp.NextPageToken)
	s.Equal(1, len(resp.Executions))

	// test for last page hits
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, 2, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(1, len(resp.Executions))

	// test for search after
	searchResult.Hits.Hits = []*elastic.SearchHit{}
	for i := int64(0); i < searchResult.Hits.TotalHits.Value; i++ {
		searchResult.Hits.Hits = append(searchResult.Hits.Hits, searchHit)
	}
	numOfHits := len(searchResult.Hits.Hits)
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits, nil)
	s.NoError(err)
	s.Equal(numOfHits, len(resp.Executions))
	nextPageToken, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	resultSortValue, err := nextPageToken.SearchAfter[0].(json.Number).Int64()
	s.NoError(err)
	s.Equal(int64(1547596872371234567), resultSortValue)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", nextPageToken.SearchAfter[1])
	// for last page
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits+1, nil)
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

	result, err = s.visibilityStore.deserializePageToken(nil)
	s.NoError(err)
	s.Nil(result)

	token := &visibilityPageToken{SearchAfter: []interface{}{int64(1629936710090695939), "unique"}}
	data, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
	result, err = s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	resultSortValue, err := result.SearchAfter[0].(json.Number).Int64()
	s.NoError(err)
	s.Equal(token.SearchAfter[0].(int64), resultSortValue)
}

func (s *ESVisibilitySuite) TestSerializePageToken() {
	data, err := s.visibilityStore.serializePageToken(nil)
	s.NoError(err)
	s.Nil(data)
	token, err := s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	s.Nil(token)

	sortTime := int64(123)
	tieBreaker := "unique"
	newToken := &visibilityPageToken{SearchAfter: []interface{}{sortTime, tieBreaker}}
	data, err = s.visibilityStore.serializePageToken(newToken)
	s.NoError(err)
	s.True(len(data) > 0)
	token, err = s.visibilityStore.deserializePageToken(data)
	s.NoError(err)
	resultSortValue, err := token.SearchAfter[0].(json.Number).Int64()
	s.NoError(err)
	s.Equal(newToken.SearchAfter[0], resultSortValue)
	s.Equal(newToken.SearchAfter[1], token.SearchAfter[1])
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
	info, err := s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap, testNamespace)
	s.NoError(err)
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
	info, err = s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap, testNamespace)
	s.NoError(err)
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
	s.mockMetricsClient.EXPECT().IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchDocumentParseFailuresCount)
	info, err = s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap, testNamespace)
	s.Error(err)
	s.Nil(info)
}

func (s *ESVisibilitySuite) TestParseESDoc_SearchAttributes() {
	searchHit := &elastic.SearchHit{
		Source: []byte(`{"ExecutionStatus": "Completed",
          "TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomTextField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`),
	}
	info, err := s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap, testNamespace)
	s.NoError(err)
	s.NotNil(info)
	customSearchAttributes, err := searchattribute.Decode(info.SearchAttributes, &searchattribute.TestNameTypeMap)
	s.NoError(err)

	s.Len(customSearchAttributes, 7)

	s.Equal([]string{"ver1", "ver2"}, customSearchAttributes["TemporalChangeVersion"])

	s.Equal("bfd5c907-f899-4baf-a7b2-2ab85e623ebd", customSearchAttributes["CustomKeywordField"])

	s.Equal("text text", customSearchAttributes["CustomTextField"])

	date1, err := time.Parse(time.RFC3339Nano, "2014-08-28T03:15:00.000-07:00")
	s.NoError(err)
	date2, err := time.Parse(time.RFC3339Nano, "2016-04-21T05:00:00.000-07:00")
	s.NoError(err)
	s.Equal([]time.Time{date1, date2}, customSearchAttributes["CustomDatetimeField"])

	s.Equal([]float64{1234.1234, 5678.5678}, customSearchAttributes["CustomDoubleField"])

	s.Equal(true, customSearchAttributes["CustomBoolField"])

	s.Equal([]int64{int64(111), int64(222)}, customSearchAttributes["CustomIntField"])

	_, ok := customSearchAttributes["UnknownField"]
	s.False(ok)

	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)
}

func (s *ESVisibilitySuite) TestParseESDoc_SearchAttributes_WithMapper() {
	searchHit := &elastic.SearchHit{
		Source: []byte(`{"ExecutionStatus": "Completed",
          "TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomTextField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`),
	}
	s.visibilityStore.searchAttributesMapper = s.mockSearchAttributesMapper

	s.mockSearchAttributesMapper.EXPECT().GetAlias(gomock.Any(), testNamespace.String()).DoAndReturn(
		func(fieldName string, namespace string) (string, error) {
			return "AliasOf" + fieldName, nil
		}).Times(6)

	info, err := s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap, testNamespace)
	s.NoError(err)
	s.NotNil(info)

	s.Len(info.SearchAttributes.GetIndexedFields(), 7)
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalChangeVersion")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasOfCustomKeywordField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasOfCustomTextField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasOfCustomDatetimeField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasOfCustomDoubleField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasOfCustomBoolField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasOfCustomIntField")
	s.NotContains(info.SearchAttributes.GetIndexedFields(), "UnknownField")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)

	s.mockSearchAttributesMapper.EXPECT().GetAlias(gomock.Any(), testNamespace.String()).DoAndReturn(
		func(fieldName string, namespace string) (string, error) {
			return "", serviceerror.NewUnavailable("error")
		})
	info, err = s.visibilityStore.parseESDoc(searchHit, searchattribute.TestNameTypeMap, testNamespace)
	s.Error(err)
	s.Nil(info)

	s.visibilityStore.searchAttributesMapper = nil
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewMatchQuery("ExecutionStatus", "Terminated"))),
				p.Query,
			)
			return testSearchResult, nil
		})

	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    10,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	_, err := s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ListWorkflowExecutions failed"))

	request.Query = `invalid query`
	_, err = s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"))
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions_Error() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			return nil, &elastic.Error{
				Status: 400,
				Details: &elastic.ErrorDetails{
					Reason: "error reason",
				},
			}
		})

	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    10,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	_, err := s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.Equal("ListWorkflowExecutions failed: elastic: Error 400 (Bad Request): error reason [type=]", internalErr.Message)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			return nil, &elastic.Error{
				Status: 500,
				Details: &elastic.ErrorDetails{
					Reason: "error reason",
				},
			}
		})
	_, err = s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	var unavailableErr *serviceerror.Unavailable
	s.ErrorAs(err, &unavailableErr)
	s.Equal("ListWorkflowExecutions failed: elastic: Error 500 (Internal Server Error): error reason [type=]", unavailableErr.Message)
}

func (s *ESVisibilitySuite) TestScanWorkflowExecutionsV6() {
	// Set v6 client for test.
	mockESClientV6 := client.NewMockClient(s.controller)
	s.visibilityStore.esClient = mockESClientV6

	// test first page
	mockESClientV6.EXPECT().OpenScroll(gomock.Any(), gomock.Any(), "1m").DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters, keepAliveInterval string) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewMatchQuery("ExecutionStatus", "Terminated"))),
				p.Query,
			)
			return testSearchResult, nil
		})
	mockESClientV6.EXPECT().CloseScroll(gomock.Any(), gomock.Any()).Return(nil)

	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    10,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	_, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"))

	// test scroll
	scrollID := "scrollID-1"
	testSearchResult.ScrollId = scrollID
	mockESClientV6.EXPECT().Scroll(gomock.Any(), scrollID, "1m").Return(testSearchResult, nil)
	mockESClientV6.EXPECT().CloseScroll(gomock.Any(), gomock.Any()).Return(nil)

	token := &visibilityPageToken{ScrollID: scrollID}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
	request.NextPageToken = tokenBytes
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test io.EOF error
	mockESClientV6.EXPECT().Scroll(gomock.Any(), scrollID, "1m").Return(testSearchResult, io.EOF)
	mockESClientV6.EXPECT().CloseScroll(gomock.Any(), gomock.Any()).Return(nil)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test unavailable error
	mockESClientV6.EXPECT().Scroll(gomock.Any(), scrollID, "1m").Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.Unavailable)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ScanWorkflowExecutions failed"))

	// Restore v7 client.
	s.visibilityStore.esClient = s.mockESClient
	testSearchResult.ScrollId = ""
}

func (s *ESVisibilitySuite) TestScanWorkflowExecutionsV7_Scroll() {
	// test first page
	s.mockESClient.EXPECT().OpenScroll(gomock.Any(), gomock.Any(), "1m").DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters, keepAliveInterval string) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewMatchQuery("ExecutionStatus", "Terminated"))),
				p.Query,
			)
			return testSearchResult, nil
		})
	s.mockESClient.EXPECT().CloseScroll(gomock.Any(), gomock.Any()).Return(nil)
	s.mockESClient.EXPECT().IsPointInTimeSupported(gomock.Any()).Return(false).AnyTimes()

	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    10,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	_, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"))

	// test scroll
	scrollID := "scrollID-1"
	testSearchResult.ScrollId = scrollID
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID, "1m").Return(testSearchResult, nil)
	s.mockESClient.EXPECT().CloseScroll(gomock.Any(), gomock.Any()).Return(nil)

	token := &visibilityPageToken{ScrollID: scrollID}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
	request.NextPageToken = tokenBytes
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test io.EOF error
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID, "1m").Return(testSearchResult, io.EOF)
	s.mockESClient.EXPECT().CloseScroll(gomock.Any(), gomock.Any()).Return(nil)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test unavailable error
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID, "1m").Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.Unavailable)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ScanWorkflowExecutions failed"))

}

func (s *ESVisibilitySuite) TestScanWorkflowExecutionsV7_PIT() {
	// test first page
	pitID := "pitID"

	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    1,
		Query:       `ExecutionStatus = "Terminated"`,
	}

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
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			Hits: []*elastic.SearchHit{
				{
					Source: source,
					Sort:   []interface{}{json.Number("123"), "runId"},
				},
			},
		},
	}
	s.mockESClient.EXPECT().IsPointInTimeSupported(gomock.Any()).Return(true).AnyTimes()
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(searchResult, nil)
	s.mockESClient.EXPECT().OpenPointInTime(gomock.Any(), testIndex, gomock.Any()).Return(pitID, nil)
	_, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"))

	// test search
	request.Query = `ExecutionStatus = "Terminated"`
	searchResult.PitId = pitID
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(searchResult, nil)

	token := &visibilityPageToken{PointInTimeID: pitID, SearchAfter: []interface{}{json.Number("1528358645123456789"), "qwe"}}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)
	request.NextPageToken = tokenBytes
	result, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	responseToken, err := s.visibilityStore.deserializePageToken(result.NextPageToken)
	s.NoError(err)
	s.Equal([]interface{}{json.Number("123"), "runId"}, responseToken.SearchAfter)
	s.Equal(pitID, responseToken.PointInTimeID)
	searchResult.PitId = ""

	// test last page
	searchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			Hits: []*elastic.SearchHit{},
		},
		PitId: pitID,
	}
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(searchResult, nil)
	s.mockESClient.EXPECT().ClosePointInTime(gomock.Any(), pitID).Return(true, nil)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)

	// test unavailable error
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.Unavailable)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "ScanWorkflowExecutions failed"))
}

func (s *ESVisibilitySuite) TestCountWorkflowExecutions() {
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewMatchQuery("ExecutionStatus", "Terminated"))),
				query,
			)
			return int64(1), nil
		})

	request := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		Query:       `ExecutionStatus = "Terminated"`,
	}
	resp, err := s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	s.Equal(int64(1), resp.Count)

	// test unavailable error
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewMatchQuery("ExecutionStatus", "Terminated"))),
				query,
			)
			return int64(0), errTestESSearch
		})

	_, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
	s.True(ok)
	s.True(strings.Contains(err.Error(), "CountWorkflowExecutions failed"))

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"), err.Error())
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
