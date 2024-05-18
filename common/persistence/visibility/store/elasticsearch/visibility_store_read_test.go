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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/api/temporalproto"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
)

type (
	ESVisibilitySuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		controller                         *gomock.Controller
		visibilityStore                    *visibilityStore
		mockESClient                       *client.MockClient
		mockProcessor                      *MockProcessor
		mockMetricsHandler                 *metrics.MockHandler
		mockSearchAttributesMapperProvider *searchattribute.MockMapperProvider
	}
)

var (
	testIndex        = "test-index"
	testNamespace    = namespace.Name("test-namespace")
	testNamespaceID  = namespace.ID("bfd5c907-f899-4baf-a7b2-2ab85e623ebd")
	testNSDivision   = "hidden-stuff"
	testPageSize     = 5
	testEarliestTime = time.Unix(0, 1547596872371000000).UTC()
	testLatestTime   = time.Unix(0, 2547596872371000000).UTC()
	testWorkflowType = "test-wf-type"
	testWorkflowID   = "test-wid"
	testRunID        = "test-rid"
	testStatus       = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED

	testSearchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{},
	}
	errTestESSearch = errors.New("ES error")

	filterOpen              = fmt.Sprintf("map[term:map[ExecutionStatus:%s]", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	filterCloseRE           = fmt.Sprintf(`must_not:\[?map\[term:map\[ExecutionStatus:%s\]\]`, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	filterByType            = fmt.Sprintf("map[term:map[WorkflowType:%s]", testWorkflowType)
	filterByWID             = fmt.Sprintf("map[term:map[WorkflowId:%s]", testWorkflowID)
	filterByExecutionStatus = fmt.Sprintf("map[term:map[ExecutionStatus:%s]", testStatus.String())
	filterByNSDivision      = fmt.Sprintf("map[term:map[TemporalNamespaceDivision:%s]", testNSDivision)

	namespaceDivisionExists = elastic.NewExistsQuery(searchattribute.TemporalNamespaceDivision)
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

func createTestRequestWithNSDivision() *manager.ListWorkflowExecutionsRequestV2 {
	return &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    testPageSize,
		Query: fmt.Sprintf("%s = '%s' AND %s = '%s'",
			searchattribute.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			searchattribute.TemporalNamespaceDivision,
			testNSDivision,
		),
	}
}

func TestESVisibilitySuite(t *testing.T) {
	suite.Run(t, new(ESVisibilitySuite))
}

func (s *ESVisibilitySuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	esProcessorAckTimeout := dynamicconfig.GetDurationPropertyFn(1 * time.Minute * debug.TimeoutMultiplier)
	visibilityDisableOrderByClause := dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	visibilityEnableManualPagination := dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	s.controller = gomock.NewController(s.T())
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ElasticsearchVisibility)).Return(s.mockMetricsHandler).AnyTimes()
	s.mockProcessor = NewMockProcessor(s.controller)
	s.mockESClient = client.NewMockClient(s.controller)
	s.mockSearchAttributesMapperProvider = searchattribute.NewMockMapperProvider(s.controller)
	s.visibilityStore = NewVisibilityStore(
		s.mockESClient,
		testIndex,
		searchattribute.NewTestProvider(),
		searchattribute.NewTestMapperProvider(nil),
		s.mockProcessor,
		esProcessorAckTimeout,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		s.mockMetricsHandler,
	)
}

func (s *ESVisibilitySuite) TearDownTest() {
	s.controller.Finish()
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
	boolQuery := elastic.NewBoolQuery().Filter(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery).MustNot(namespaceDivisionExists)
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
	boolQuery = elastic.NewBoolQuery().Filter(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery).MustNot(namespaceDivisionExists)
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
	boolQuery = elastic.NewBoolQuery().MustNot(runningQuery).Filter(matchNamespaceQuery).Filter(rangeQuery).MustNot(namespaceDivisionExists)
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
	boolQuery = elastic.NewBoolQuery().Filter(matchQuery).Filter(matchNamespaceQuery).Filter(rangeQuery).MustNot(namespaceDivisionExists)
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
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery).Filter(rangeQuery).MustNot(namespaceDivisionExists)
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
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery).Filter(rangeQuery).MustNot(namespaceDivisionExists)
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

func (s *ESVisibilitySuite) TestGetListFieldSorter() {

	// test defaultSorter is returned when fieldSorts is empty
	fieldSorts := make([]elastic.Sorter, 0)
	sorter, err := s.visibilityStore.getListFieldSorter(fieldSorts)
	s.NoError(err)
	s.Equal(defaultSorter, sorter)

	// test passing non-empty fieldSorts
	testFieldSorts := []elastic.Sorter{elastic.NewFieldSort("_test"), elastic.NewFieldSort("_second_tes")}
	sorter, err = s.visibilityStore.getListFieldSorter(testFieldSorts[:])
	expectedSorter := make([]elastic.Sorter, len(testFieldSorts)+1)
	expectedSorter[0] = testFieldSorts[0]
	expectedSorter[1] = testFieldSorts[1]
	expectedSorter[2] = elastic.NewFieldSort(searchattribute.RunID).Desc()
	s.NoError(err)
	s.Equal(expectedSorter, sorter)

}

func (s *ESVisibilitySuite) TestGetScanFieldSorter() {
	// test docSorter is returned when fieldSorts is empty
	fieldSorts := make([]elastic.Sorter, 0)
	sorter, err := s.visibilityStore.getScanFieldSorter(fieldSorts)
	s.NoError(err)
	s.Equal(docSorter, sorter)

	// test error is returned if fieldSorts is not empty
	testFieldSorts := []elastic.Sorter{elastic.NewFieldSort("_test"), elastic.NewFieldSort("_second_tes")}
	sorter, err = s.visibilityStore.getScanFieldSorter(testFieldSorts[:])
	s.Error(err)
	s.Nil(sorter)
}

func (s *ESVisibilitySuite) TestBuildSearchParametersV2() {
	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    testPageSize,
	}

	matchNamespaceQuery := elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID.String())
	matchNSDivision := elastic.NewTermQuery(searchattribute.TemporalNamespaceDivision, "hidden-stuff")

	// test for open
	request.Query = `WorkflowId="guid-2208"`
	filterQuery := elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.WorkflowID, "guid-2208"))
	boolQuery := elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery).MustNot(namespaceDivisionExists)
	p, err := s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getListFieldSorter)
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

	// test for open with namespace division
	request.Query = `WorkflowId="guid-2208" and TemporalNamespaceDivision="hidden-stuff"`
	// note namespace division appears in the filterQuery, not the boolQuery like the negative version
	filterQuery = elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.WorkflowID, "guid-2208"), matchNSDivision)
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery)
	p, err = s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getListFieldSorter)
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
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery).MustNot(namespaceDivisionExists)
	s.mockMetricsHandler.EXPECT().WithTags(metrics.NamespaceTag(request.Namespace.String())).Return(s.mockMetricsHandler)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Return(metrics.NoopCounterMetricFunc)
	p, err = s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getListFieldSorter)
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

	// test with Scan API
	request.Query = `WorkflowId="guid-2208"`
	filterQuery = elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.WorkflowID, "guid-2208"))
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery).MustNot(namespaceDivisionExists)
	p, err = s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getScanFieldSorter)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PointInTime: nil,
		PageSize:    testPageSize,
		Sorter:      docSorter,
	}, p)
	request.Query = ""

	// test with Scan API with custom sort
	request.Query = `Order bY WorkflowId`
	s.mockMetricsHandler.EXPECT().WithTags(metrics.NamespaceTag(request.Namespace.String())).Return(s.mockMetricsHandler)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Return(metrics.NoopCounterMetricFunc)
	p, err = s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getScanFieldSorter)
	s.Error(err)
	s.Nil(p)
	request.Query = ""

	// test for wrong query
	request.Query = "invalid query"
	p, err = s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getListFieldSorter)
	s.Nil(p)
	s.Error(err)
	request.Query = ""
}

func (s *ESVisibilitySuite) TestBuildSearchParametersV2DisableOrderByClause() {
	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    testPageSize,
	}

	matchNamespaceQuery := elastic.NewTermQuery(searchattribute.NamespaceID, request.NamespaceID.String())

	// disable ORDER BY clause
	s.visibilityStore.disableOrderByClause = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	// test valid query
	request.Query = `WorkflowId="guid-2208"`
	filterQuery := elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.WorkflowID, "guid-2208"))
	boolQuery := elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery).MustNot(namespaceDivisionExists)
	p, err := s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getListFieldSorter)
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

	// test invalid query with ORDER BY
	request.Query = `ORDER BY WorkflowId`
	p, err = s.visibilityStore.buildSearchParametersV2(request, s.visibilityStore.getListFieldSorter)
	s.Nil(p)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "ORDER BY clause is not supported")
	request.Query = ""
}

func (s *ESVisibilitySuite) queryToJSON(q elastic.Query) string {
	m, err := q.Source()
	s.NoError(err)
	b, err := json.Marshal(m)
	s.NoError(err)
	return string(b)
}

func (s *ESVisibilitySuite) sorterToJSON(sorters []elastic.Sorter) string {
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
	queryParams, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' or WorkflowId = 'another-wid'`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"term":{"WorkflowId":"wid"}},{"term":{"WorkflowId":"another-wid"}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' order by StartTime desc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"StartTime":{"order":"desc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `WorkflowId = 'wid' and CloseTime is null`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' or CloseTime is null`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"term":{"WorkflowId":"wid"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `CloseTime is null order by CloseTime desc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"CloseTime":{"order":"desc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `StartTime = "2018-06-07T15:04:05.123456789-08:00"`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"StartTime":{"query":"2018-06-07T15:04:05.123456789-08:00"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' and StartTime > "2018-06-07T15:04:05+00:00"`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"range":{"StartTime":{"from":"2018-06-07T15:04:05+00:00","include_lower":false,"include_upper":true,"to":null}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ExecutionTime < 1000000`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ExecutionTime between 1 and 2`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.000000001Z","include_lower":true,"include_upper":true,"to":"1970-01-01T00:00:00.000000002Z"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ExecutionTime < 1000000 or ExecutionTime > 2000000`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}},{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.002Z","include_lower":false,"include_upper":true,"to":null}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by StartTime desc, CloseTime asc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"StartTime":{"order":"desc"}},{"CloseTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomTextField desc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal(err.(*serviceerror.InvalidArgument).Error(), "invalid query: unable to convert 'order by' column name: unable to sort by field of Text type, use field of type Keyword")

	query = `order by CustomIntField asc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"CustomIntField":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `ExecutionTime < "unable to parse"`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal(err.Error(), "invalid query: unable to convert filter expression: unable to convert values of comparison expression: invalid value for search attribute ExecutionTime of type Datetime: \"unable to parse\"")

	// invalid union injection
	query = `WorkflowId = 'wid' union select * from dummy`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.Nil(queryParams)
}

func (s *ESVisibilitySuite) Test_convertQuery_Mapper() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(testNamespace).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	s.visibilityStore.searchAttributesMapperProvider = s.mockSearchAttributesMapperProvider

	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = "`AliasForCustomKeywordField` = 'pid'"
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = "`AliasWithHyphenFor-CustomKeywordField` = 'pid'"
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `CustomKeywordField = 'pid'`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `AliasForUnknownField = 'pid'`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert filter expression: unable to convert left side of \"AliasForUnknownField = 'pid'\": invalid search attribute: AliasForUnknownField")

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by AliasForCustomKeywordField asc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"CustomKeywordField":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomKeywordField asc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `order by AliasForUnknownField asc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert 'order by' column name: invalid search attribute: AliasForUnknownField")
	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) Test_convertQuery_Mapper_Error() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(testNamespace).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	s.visibilityStore.searchAttributesMapperProvider = s.mockSearchAttributesMapperProvider

	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ProductId = 'pid'`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomIntField asc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	// test for empty hits
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			TotalHits: &elastic.TotalHits{},
		}}
	resp, err := s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, 1)
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
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, 1)
	s.NoError(err)
	serializedToken, _ := s.visibilityStore.serializePageToken(&visibilityPageToken{SearchAfter: []interface{}{1547596872371234567, "e481009e-14b3-45ae-91af-dce6e2a88365"}})
	s.Equal(serializedToken, resp.NextPageToken)
	s.Equal(1, len(resp.Executions))

	// test for last page hits
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, 2)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(1, len(resp.Executions))

	// test for search after
	searchResult.Hits.Hits = []*elastic.SearchHit{}
	for i := int64(0); i < searchResult.Hits.TotalHits.Value; i++ {
		searchResult.Hits.Hits = append(searchResult.Hits.Hits, searchHit)
	}
	numOfHits := len(searchResult.Hits.Hits)
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits)
	s.NoError(err)
	s.Equal(numOfHits, len(resp.Executions))
	nextPageToken, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	resultSortValue, err := nextPageToken.SearchAfter[0].(json.Number).Int64()
	s.NoError(err)
	s.Equal(int64(1547596872371234567), resultSortValue)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", nextPageToken.SearchAfter[1])
	// for last page
	resp, err = s.visibilityStore.getListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits+1)
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
	s.Contains(err.Error(), "unable to deserialize page token")

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
	docSource := []byte(`{"ExecutionStatus": "Running",
          "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "StateTransitionCount": 10,
          "VisibilityTaskKey": "7-619",
          "RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": "2021-06-11T15:04:07.980-07:00",
          "WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "TestWorkflowExecute"}`)
	// test for open
	info, err := s.visibilityStore.parseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
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
	docSource = []byte(`{"ExecutionStatus": "Completed",
          "CloseTime": "2021-06-11T16:04:07Z",
          "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "HistoryLength": 29,
          "StateTransitionCount": 20,
          "VisibilityTaskKey": "7-619",
          "RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
          "StartTime": "2021-06-11T15:04:07.980-07:00",
          "WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
          "WorkflowType": "TestWorkflowExecute"}`)
	info, err = s.visibilityStore.parseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
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
	docSource = []byte(`corrupted data`)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchDocumentParseFailuresCount.Name()).Return(metrics.NoopCounterMetricFunc)
	info, err = s.visibilityStore.parseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	s.Error(err)
	s.Nil(info)
}

func (s *ESVisibilitySuite) TestParseESDoc_SearchAttributes() {
	docSource := []byte(`{"ExecutionStatus": "Completed",
          "TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomTextField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`)
	info, err := s.visibilityStore.parseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	s.NoError(err)
	s.NotNil(info)
	customSearchAttributes, err := searchattribute.Decode(info.SearchAttributes, &searchattribute.TestNameTypeMap, true)
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
	docSource := []byte(`{"ExecutionStatus": "Completed",
          "TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomTextField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`)
	s.visibilityStore.searchAttributesMapperProvider = s.mockSearchAttributesMapperProvider

	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(testNamespace).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	info, err := s.visibilityStore.parseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	s.NoError(err)
	s.NotNil(info)

	s.Len(info.SearchAttributes.GetIndexedFields(), 7)
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalChangeVersion")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasForCustomKeywordField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasForCustomTextField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasForCustomDatetimeField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasForCustomDoubleField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasForCustomBoolField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "AliasForCustomIntField")
	s.NotContains(info.SearchAttributes.GetIndexedFields(), "UnknownField")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)

	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewTermQuery("ExecutionStatus", "Terminated")),
				).MustNot(namespaceDivisionExists),
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
	s.Contains(err.Error(), "ListWorkflowExecutions failed")

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
	var invalidArgErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgErr)
	s.Equal("ListWorkflowExecutions failed: elastic: Error 400 (Bad Request): error reason [type=]", invalidArgErr.Message)

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

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutionsWithNamespaceDivision() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			s.Contains(fmt.Sprintf("%v", source), filterOpen)
			s.Contains(fmt.Sprintf("%v", source), filterByNSDivision)
			return testSearchResult, nil
		})
	_, err := s.visibilityStore.ListWorkflowExecutions(context.Background(), createTestRequestWithNSDivision())
	s.NoError(err)
}

func (s *ESVisibilitySuite) TestScanWorkflowExecutions_Scroll() {
	scrollID := "scrollID"
	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    1,
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
				},
			},
		},
		ScrollId: scrollID,
	}

	s.mockESClient.EXPECT().IsPointInTimeSupported(gomock.Any()).Return(false).AnyTimes()

	// test bad request
	request.Query = `invalid query`
	_, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"))

	// test search
	request.Query = `ExecutionStatus = "Running"`
	s.mockESClient.EXPECT().OpenScroll(
		gomock.Any(),
		&client.SearchParameters{
			Index: testIndex,
			Query: elastic.NewBoolQuery().
				Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(
						elastic.NewTermQuery(
							searchattribute.ExecutionStatus,
							enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
						),
					),
				).
				MustNot(elastic.NewExistsQuery(searchattribute.TemporalNamespaceDivision)),
			PageSize: 1,
			Sorter:   docSorter,
		},
		gomock.Any(),
	).Return(searchResult, nil)

	token := &visibilityPageToken{ScrollID: scrollID}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)

	result, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	s.Equal(tokenBytes, result.NextPageToken)

	// test last page
	request.NextPageToken = tokenBytes
	searchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			Hits: []*elastic.SearchHit{},
		},
		ScrollId: scrollID,
	}
	s.mockESClient.EXPECT().Scroll(gomock.Any(), scrollID, gomock.Any()).Return(searchResult, nil)
	s.mockESClient.EXPECT().CloseScroll(gomock.Any(), scrollID).Return(nil)
	result, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	s.Nil(result.NextPageToken)

	// test unavailable error
	request.NextPageToken = nil
	s.mockESClient.EXPECT().OpenScroll(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.Unavailable)
	s.True(ok)
	s.Contains(err.Error(), "ScanWorkflowExecutions failed")
}

func (s *ESVisibilitySuite) TestScanWorkflowExecutions_Pit() {
	pitID := "pitID"
	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    1,
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
	searchAfter := []any{json.Number("123")}
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			Hits: []*elastic.SearchHit{
				{
					Source: source,
					Sort:   searchAfter,
				},
			},
		},
		PitId: pitID,
	}

	s.mockESClient.EXPECT().IsPointInTimeSupported(gomock.Any()).Return(true).AnyTimes()

	// test bad request
	request.Query = `invalid query`
	_, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"))

	request.Query = `ExecutionStatus = "Running"`
	s.mockESClient.EXPECT().OpenPointInTime(gomock.Any(), testIndex, gomock.Any()).Return(pitID, nil)
	s.mockESClient.EXPECT().Search(
		gomock.Any(),
		&client.SearchParameters{
			Index: testIndex,
			Query: elastic.NewBoolQuery().
				Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(
						elastic.NewTermQuery(
							searchattribute.ExecutionStatus,
							enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
						),
					),
				).
				MustNot(elastic.NewExistsQuery(searchattribute.TemporalNamespaceDivision)),
			PageSize:    1,
			Sorter:      docSorter,
			PointInTime: elastic.NewPointInTimeWithKeepAlive(pitID, pointInTimeKeepAliveInterval),
		},
	).Return(searchResult, nil)

	token := &visibilityPageToken{
		SearchAfter:   searchAfter,
		PointInTimeID: pitID,
	}
	tokenBytes, err := s.visibilityStore.serializePageToken(token)
	s.NoError(err)

	result, err := s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	s.Equal(tokenBytes, result.NextPageToken)

	// test last page
	request.NextPageToken = tokenBytes
	searchResult = &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			Hits: []*elastic.SearchHit{},
		},
		PitId: pitID,
	}
	s.mockESClient.EXPECT().ClosePointInTime(gomock.Any(), pitID).Return(true, nil)
	s.mockESClient.EXPECT().Search(
		gomock.Any(),
		&client.SearchParameters{
			Index: testIndex,
			Query: elastic.NewBoolQuery().
				Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(
						elastic.NewTermQuery(
							searchattribute.ExecutionStatus,
							enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
						),
					),
				).
				MustNot(elastic.NewExistsQuery(searchattribute.TemporalNamespaceDivision)),
			PageSize:    1,
			Sorter:      docSorter,
			SearchAfter: token.SearchAfter,
			PointInTime: elastic.NewPointInTimeWithKeepAlive(pitID, pointInTimeKeepAliveInterval),
		},
	).Return(searchResult, nil)
	result, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	s.Nil(result.NextPageToken)

	// test unavailable error
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ScanWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.Unavailable)
	s.True(ok)
	s.Contains(err.Error(), "ScanWorkflowExecutions failed")
}

func (s *ESVisibilitySuite) TestCountWorkflowExecutions() {
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewTermQuery("ExecutionStatus", "Terminated")),
				).MustNot(namespaceDivisionExists),
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
					elastic.NewBoolQuery().Filter(elastic.NewTermQuery("ExecutionStatus", "Terminated")),
				).MustNot(namespaceDivisionExists),
				query,
			)
			return int64(0), errTestESSearch
		})

	_, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
	s.True(ok)
	s.Contains(err.Error(), "CountWorkflowExecutions failed")

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.Error(err)
	_, ok = err.(*serviceerror.InvalidArgument)
	s.True(ok)
	s.True(strings.HasPrefix(err.Error(), "invalid query"), err.Error())
}

func (s *ESVisibilitySuite) TestCountWorkflowExecutions_GroupBy() {
	request := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		Query:       "GROUP BY ExecutionStatus",
	}
	s.mockESClient.EXPECT().
		CountGroupBy(
			gomock.Any(),
			testIndex,
			elastic.NewBoolQuery().
				Filter(elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String())).
				MustNot(namespaceDivisionExists),
			searchattribute.ExecutionStatus,
			elastic.NewTermsAggregation().Field(searchattribute.ExecutionStatus),
		).
		Return(
			&elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					searchattribute.ExecutionStatus: json.RawMessage(
						`{"buckets":[{"key":"Completed","doc_count":100},{"key":"Running","doc_count":10}]}`,
					),
				},
			},
			nil,
		)
	resp, err := s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	payload1, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	payload2, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	s.True(temporalproto.DeepEqual(
		&manager.CountWorkflowExecutionsResponse{
			Count: 110,
			Groups: []*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
				{
					GroupValues: []*commonpb.Payload{payload1},
					Count:       100,
				},
				{
					GroupValues: []*commonpb.Payload{payload2},
					Count:       10,
				},
			},
		},
		resp),
	)

	// test only allowed to group by a single field
	request.Query = "GROUP BY ExecutionStatus, WorkflowType"
	resp, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.Error(err)
	s.Contains(err.Error(), "'group by' clause supports only a single field")
	s.Nil(resp)

	// test only allowed to group by ExecutionStatus
	request.Query = "GROUP BY WorkflowType"
	resp, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.Error(err)
	s.Contains(err.Error(), "'group by' clause is only supported for ExecutionStatus search attribute")
	s.Nil(resp)
}

func (s *ESVisibilitySuite) TestCountGroupByWorkflowExecutions() {
	statusCompletedPayload, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	statusRunningPayload, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	wfType1Payload, _ := searchattribute.EncodeValue("wf-type-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	wfType2Payload, _ := searchattribute.EncodeValue("wf-type-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	wfId1Payload, _ := searchattribute.EncodeValue("wf-id-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	wfId2Payload, _ := searchattribute.EncodeValue("wf-id-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	wfId3Payload, _ := searchattribute.EncodeValue("wf-id-3", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	wfId4Payload, _ := searchattribute.EncodeValue("wf-id-4", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	wfId5Payload, _ := searchattribute.EncodeValue("wf-id-5", enumspb.INDEXED_VALUE_TYPE_KEYWORD)

	testCases := []struct {
		name         string
		groupBy      []string
		aggName      string
		agg          elastic.Aggregation
		mockResponse *elastic.SearchResult
		response     *manager.CountWorkflowExecutionsResponse
	}{
		{
			name:    "group by one field",
			groupBy: []string{searchattribute.ExecutionStatus},
			aggName: searchattribute.ExecutionStatus,
			agg:     elastic.NewTermsAggregation().Field(searchattribute.ExecutionStatus),
			mockResponse: &elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					searchattribute.ExecutionStatus: json.RawMessage(
						`{
							"buckets":[
								{
									"key": "Completed",
									"doc_count": 100
								},
								{
									"key": "Running",
									"doc_count": 10
								}
							]
						}`,
					),
				},
			},
			response: &manager.CountWorkflowExecutionsResponse{
				Count: 110,
				Groups: []*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
					{
						GroupValues: []*commonpb.Payload{statusCompletedPayload},
						Count:       100,
					},
					{
						GroupValues: []*commonpb.Payload{statusRunningPayload},
						Count:       10,
					},
				},
			},
		},

		{
			name:    "group by two fields",
			groupBy: []string{searchattribute.ExecutionStatus, searchattribute.WorkflowType},
			aggName: searchattribute.ExecutionStatus,
			agg: elastic.NewTermsAggregation().Field(searchattribute.ExecutionStatus).SubAggregation(
				searchattribute.WorkflowType,
				elastic.NewTermsAggregation().Field(searchattribute.WorkflowType),
			),
			mockResponse: &elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					searchattribute.ExecutionStatus: json.RawMessage(
						`{
							"buckets":[
								{
									"key": "Completed",
									"doc_count": 100,
									"WorkflowType": {
										"buckets": [
											{
												"key": "wf-type-1",
												"doc_count": 75
											},
											{
												"key": "wf-type-2",
												"doc_count": 25
											}
										]
									}
								},
								{
									"key": "Running",
									"doc_count": 10,
									"WorkflowType": {
										"buckets": [
											{
												"key": "wf-type-1",
												"doc_count": 7
											},
											{
												"key": "wf-type-2",
												"doc_count": 3
											}
										]
									}
								}
							]
						}`,
					),
				},
			},
			response: &manager.CountWorkflowExecutionsResponse{
				Count: 110,
				Groups: []*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
					{
						GroupValues: []*commonpb.Payload{statusCompletedPayload, wfType1Payload},
						Count:       75,
					},
					{
						GroupValues: []*commonpb.Payload{statusCompletedPayload, wfType2Payload},
						Count:       25,
					},
					{
						GroupValues: []*commonpb.Payload{statusRunningPayload, wfType1Payload},
						Count:       7,
					},
					{
						GroupValues: []*commonpb.Payload{statusRunningPayload, wfType2Payload},
						Count:       3,
					},
				},
			},
		},

		{
			name: "group by three fields",
			groupBy: []string{
				searchattribute.ExecutionStatus,
				searchattribute.WorkflowType,
				searchattribute.WorkflowID,
			},
			aggName: searchattribute.ExecutionStatus,
			agg: elastic.NewTermsAggregation().Field(searchattribute.ExecutionStatus).SubAggregation(
				searchattribute.WorkflowType,
				elastic.NewTermsAggregation().Field(searchattribute.WorkflowType).SubAggregation(
					searchattribute.WorkflowID,
					elastic.NewTermsAggregation().Field(searchattribute.WorkflowID),
				),
			),
			mockResponse: &elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					searchattribute.ExecutionStatus: json.RawMessage(
						`{
							"buckets":[
								{
									"key": "Completed",
									"doc_count": 100,
									"WorkflowType": {
										"buckets": [
											{
												"key": "wf-type-1",
												"doc_count": 75,
												"WorkflowId": {
													"buckets": [
														{
															"key": "wf-id-1",
															"doc_count": 75
														}
													]
												}
											},
											{
												"key": "wf-type-2",
												"doc_count": 25,
												"WorkflowId": {
													"buckets": [
														{
															"key": "wf-id-2",
															"doc_count": 20
														},
														{
															"key": "wf-id-3",
															"doc_count": 5
														}
													]
												}
											}
										]
									}
								},
								{
									"key": "Running",
									"doc_count": 10,
									"WorkflowType": {
										"buckets": [
											{
												"key": "wf-type-1",
												"doc_count": 7,
												"WorkflowId": {
													"buckets": [
														{
															"key": "wf-id-4",
															"doc_count": 7
														}
													]
												}
											},
											{
												"key": "wf-type-2",
												"doc_count": 3,
												"WorkflowId": {
													"buckets": [
														{
															"key": "wf-id-5",
															"doc_count": 3
														}
													]
												}
											}
										]
									}
								}
							]
						}`,
					),
				},
			},
			response: &manager.CountWorkflowExecutionsResponse{
				Count: 110,
				Groups: []*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
					{
						GroupValues: []*commonpb.Payload{statusCompletedPayload, wfType1Payload, wfId1Payload},
						Count:       75,
					},
					{
						GroupValues: []*commonpb.Payload{statusCompletedPayload, wfType2Payload, wfId2Payload},
						Count:       20,
					},
					{
						GroupValues: []*commonpb.Payload{statusCompletedPayload, wfType2Payload, wfId3Payload},
						Count:       5,
					},
					{
						GroupValues: []*commonpb.Payload{statusRunningPayload, wfType1Payload, wfId4Payload},
						Count:       7,
					},
					{
						GroupValues: []*commonpb.Payload{statusRunningPayload, wfType2Payload, wfId5Payload},
						Count:       3,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			searchParams := &query.QueryParams{
				Query: elastic.NewBoolQuery().
					Filter(elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String())).
					MustNot(namespaceDivisionExists),
				GroupBy: tc.groupBy,
			}
			s.mockESClient.EXPECT().
				CountGroupBy(
					gomock.Any(),
					testIndex,
					elastic.NewBoolQuery().
						Filter(elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String())).
						MustNot(namespaceDivisionExists),
					tc.aggName,
					tc.agg,
				).
				Return(tc.mockResponse, nil)
			resp, err := s.visibilityStore.countGroupByWorkflowExecutions(context.Background(), searchParams)
			s.NoError(err)
			s.True(temporalproto.DeepEqual(tc.response, resp))
		})
	}
}

func (s *ESVisibilitySuite) TestGetWorkflowExecution() {
	s.mockESClient.EXPECT().Get(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, docID string) (*elastic.GetResult, error) {
			s.Equal(testIndex, index)
			s.Equal(testWorkflowID+delimiter+testRunID, docID)
			data := map[string]interface{}{
				"ExecutionStatus":      "Running",
				"NamespaceId":          testNamespaceID.String(),
				"StateTransitionCount": 22,
				"VisibilityTaskKey":    "7-619",
				"RunId":                testRunID,
				"StartTime":            "2021-06-11T15:04:07.980-07:00",
				"WorkflowId":           testWorkflowID,
				"WorkflowType":         "basic.stressWorkflowExecute",
			}
			source, _ := json.Marshal(data)
			return &elastic.GetResult{Found: true, Source: source}, nil
		})

	request := &manager.GetWorkflowExecutionRequest{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
	}
	_, err := s.visibilityStore.GetWorkflowExecution(context.Background(), request)
	s.NoError(err)

	// test unavailable error
	s.mockESClient.EXPECT().Get(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, docID string) (*elastic.GetResult, error) {
			s.Equal(testIndex, index)
			s.Equal(testWorkflowID+delimiter+testRunID, docID)
			return nil, errTestESSearch
		})

	_, err = s.visibilityStore.GetWorkflowExecution(context.Background(), request)
	s.Error(err)
	_, ok := err.(*serviceerror.Unavailable)
	s.True(ok)
	s.Contains(err.Error(), "GetWorkflowExecution failed")
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

func (s *ESVisibilitySuite) TestProcessPageToken() {
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-1 * time.Minute)
	baseQuery := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery(searchattribute.NamespaceID, testNamespace.String()))

	testCases := []struct {
		name             string
		manualPagination bool
		sorter           []elastic.Sorter
		pageToken        *visibilityPageToken
		resSearchAfter   []any
		resQuery         elastic.Query
		resError         error
	}{
		{
			name:             "nil page token",
			manualPagination: false,
			sorter:           docSorter,
			pageToken:        nil,
			resSearchAfter:   nil,
			resQuery:         baseQuery,
			resError:         nil,
		},
		{
			name:             "empty page token",
			manualPagination: false,
			sorter:           docSorter,
			pageToken:        &visibilityPageToken{SearchAfter: []any{}},
			resSearchAfter:   nil,
			resQuery:         baseQuery,
			resError:         nil,
		},
		{
			name:             "page token doesn't match sorter size",
			manualPagination: false,
			sorter:           docSorter,
			pageToken:        &visibilityPageToken{SearchAfter: []any{"foo", "bar"}},
			resSearchAfter:   nil,
			resQuery:         baseQuery,
			resError:         serviceerror.NewInvalidArgument("Invalid page token for given sort fields: expected 1 fields, got 2"),
		},
		{
			name:             "not using default sorter",
			manualPagination: false,
			sorter:           docSorter,
			pageToken:        &visibilityPageToken{SearchAfter: []any{123}},
			resSearchAfter:   []any{123},
			resQuery:         baseQuery,
			resError:         nil,
		},
		{
			name:             "default sorter without manual pagination",
			manualPagination: false,
			sorter:           defaultSorter,
			pageToken: &visibilityPageToken{
				SearchAfter: []any{
					json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
					json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
				},
			},
			resSearchAfter: []any{
				json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
			},
			resQuery: baseQuery,
			resError: nil,
		},
		{
			name:             "default sorter with manual pagination",
			manualPagination: true,
			sorter:           defaultSorter,
			pageToken: &visibilityPageToken{
				SearchAfter: []any{
					json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
					json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
				},
			},
			resSearchAfter: nil,
			resQuery: baseQuery.MinimumNumberShouldMatch(1).Should(
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(searchattribute.CloseTime).Lt(closeTime.Format(time.RFC3339Nano)),
				),
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.CloseTime, closeTime.Format(time.RFC3339Nano)),
					elastic.NewRangeQuery(searchattribute.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
				),
			),
			resError: nil,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			visibilityStore := NewVisibilityStore(
				s.mockESClient,
				testIndex,
				searchattribute.NewTestProvider(),
				searchattribute.NewTestMapperProvider(nil),
				s.mockProcessor,
				dynamicconfig.GetDurationPropertyFn(1*time.Minute*debug.TimeoutMultiplier),
				dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
				dynamicconfig.GetBoolPropertyFnFilteredByNamespace(tc.manualPagination),
				s.mockMetricsHandler,
			)
			params := &client.SearchParameters{
				Index:  testIndex,
				Query:  baseQuery,
				Sorter: tc.sorter,
			}
			err := visibilityStore.processPageToken(params, tc.pageToken, testNamespace)
			s.Equal(tc.resSearchAfter, params.SearchAfter)
			s.Equal(tc.resQuery, params.Query)
			s.Equal(tc.resError, err)
		})
	}
}

func (s *ESVisibilitySuite) Test_buildPaginationQuery() {
	startTime := time.Now().UTC()
	closeTime := startTime.Add(1 * time.Minute)
	datetimeNull := json.Number(fmt.Sprintf("%d", math.MaxInt64))

	testCases := []struct {
		name         string
		sorterFields []fieldSort
		searchAfter  []any
		res          []elastic.Query
		err          error
	}{
		{
			name:         "one field",
			sorterFields: []fieldSort{{searchattribute.StartTime, true, true}},
			searchAfter:  []any{json.Number(fmt.Sprintf("%d", startTime.UnixNano()))},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(searchattribute.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
				),
			},
			err: nil,
		},
		{
			name: "two fields one null",
			sorterFields: []fieldSort{
				{searchattribute.CloseTime, true, true},
				{searchattribute.StartTime, true, true},
			},
			searchAfter: []any{
				datetimeNull,
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
			},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(elastic.NewExistsQuery(searchattribute.CloseTime)),
				elastic.NewBoolQuery().
					MustNot(elastic.NewExistsQuery(searchattribute.CloseTime)).
					Filter(
						elastic.NewRangeQuery(searchattribute.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
					),
			},
			err: nil,
		},
		{
			name: "two fields no null",
			sorterFields: []fieldSort{
				{searchattribute.CloseTime, true, true},
				{searchattribute.StartTime, true, true},
			},
			searchAfter: []any{
				json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
			},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(searchattribute.CloseTime).Lt(closeTime.Format(time.RFC3339Nano)),
				),
				elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(searchattribute.CloseTime, closeTime.Format(time.RFC3339Nano)),
						elastic.NewRangeQuery(searchattribute.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
					),
			},
			err: nil,
		},
		{
			name: "three fields",
			sorterFields: []fieldSort{
				{searchattribute.CloseTime, true, true},
				{searchattribute.StartTime, true, true},
				{searchattribute.RunID, false, true},
			},
			searchAfter: []any{
				json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
				"random-run-id",
			},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(searchattribute.CloseTime).Lt(closeTime.Format(time.RFC3339Nano)),
				),
				elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(searchattribute.CloseTime, closeTime.Format(time.RFC3339Nano)),
						elastic.NewRangeQuery(searchattribute.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
					),
				elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(searchattribute.CloseTime, closeTime.Format(time.RFC3339Nano)),
						elastic.NewTermQuery(searchattribute.StartTime, startTime.Format(time.RFC3339Nano)),
						elastic.NewRangeQuery(searchattribute.RunID).Gt("random-run-id"),
					),
			},
			err: nil,
		},
		{
			name: "invalid token: wrong size",
			sorterFields: []fieldSort{
				{searchattribute.CloseTime, true, true},
				{searchattribute.StartTime, true, true},
				{searchattribute.RunID, false, true},
			},
			searchAfter: []any{
				json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
			},
			res: nil,
			err: serviceerror.NewInvalidArgument("Invalid page token for given sort fields: expected 3 fields, got 2"),
		},
		{
			name: "invalid token: last value null",
			sorterFields: []fieldSort{
				{searchattribute.CloseTime, true, true},
			},
			searchAfter: []any{datetimeNull},
			res:         nil,
			err:         serviceerror.NewInternal("Last field of sorter cannot be a nullable field: \"CloseTime\" has null values"),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := buildPaginationQuery(tc.sorterFields, tc.searchAfter, searchattribute.TestNameTypeMap)
			s.Equal(tc.err, err)
			s.Equal(tc.res, res)
		})
	}
}

func (s *ESVisibilitySuite) Test_parsePageTokenValue() {
	testCases := []struct {
		name  string
		value any
		tp    enumspb.IndexedValueType
		res   any
		err   error
	}{
		{
			name:  "IntField",
			value: 123,
			tp:    enumspb.INDEXED_VALUE_TYPE_INT,
			res:   int64(123),
			err:   nil,
		},
		{
			name:  "NullMaxIntField",
			value: math.MaxInt64,
			tp:    enumspb.INDEXED_VALUE_TYPE_INT,
			res:   nil,
			err:   nil,
		},
		{
			name:  "NullMinIntField",
			value: math.MinInt64,
			tp:    enumspb.INDEXED_VALUE_TYPE_INT,
			res:   nil,
			err:   nil,
		},
		{
			name:  "BoolFieldTrue",
			value: 1,
			tp:    enumspb.INDEXED_VALUE_TYPE_BOOL,
			res:   true,
			err:   nil,
		},
		{
			name:  "BoolFieldFalse",
			value: 0,
			tp:    enumspb.INDEXED_VALUE_TYPE_BOOL,
			res:   false,
			err:   nil,
		},
		{
			name:  "NullMaxBoolField",
			value: math.MaxInt64,
			tp:    enumspb.INDEXED_VALUE_TYPE_BOOL,
			res:   nil,
			err:   nil,
		},
		{
			name:  "NullMinBoolField",
			value: math.MinInt64,
			tp:    enumspb.INDEXED_VALUE_TYPE_BOOL,
			res:   nil,
			err:   nil,
		},
		{
			name:  "DatetimeField",
			value: 1683221689123456789,
			tp:    enumspb.INDEXED_VALUE_TYPE_DATETIME,
			res:   "2023-05-04T17:34:49.123456789Z",
			err:   nil,
		},
		{
			name:  "NullMaxDatetimeField",
			value: math.MaxInt64,
			tp:    enumspb.INDEXED_VALUE_TYPE_DATETIME,
			res:   nil,
			err:   nil,
		},
		{
			name:  "NullMinDatetimeField",
			value: math.MinInt64,
			tp:    enumspb.INDEXED_VALUE_TYPE_DATETIME,
			res:   nil,
			err:   nil,
		},
		{
			name:  "DoubleField",
			value: 3.14,
			tp:    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			res:   float64(3.14),
			err:   nil,
		},
		{
			name:  "NullMaxDoubleField",
			value: "Infinity",
			tp:    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			res:   nil,
			err:   nil,
		},
		{
			name:  "NullMinDoubleField",
			value: "-Infinity",
			tp:    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			res:   nil,
			err:   nil,
		},
		{
			name:  "KeywordField",
			value: "foo",
			tp:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			res:   "foo",
			err:   nil,
		},
		{
			name:  "NullKeywordField",
			value: nil,
			tp:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			res:   nil,
			err:   nil,
		},
		{
			name:  "IntFieldError",
			value: "123",
			tp:    enumspb.INDEXED_VALUE_TYPE_INT,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("Invalid page token: expected interger type, got \"123\""),
		},
		{
			name:  "DoubleFieldError",
			value: "foo",
			tp:    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("Invalid page token: expected float type, got \"foo\""),
		},
		{
			name:  "KeywordFieldError",
			value: 123,
			tp:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("Invalid page token: expected string type, got 123"),
		},
		{
			name:  "TextFieldError",
			value: "foo",
			tp:    enumspb.INDEXED_VALUE_TYPE_TEXT,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("Invalid field type in sorter: cannot order by \"TextFieldError\""),
		},
	}

	pageToken := &visibilityPageToken{}
	for _, tc := range testCases {
		pageToken.SearchAfter = append(pageToken.SearchAfter, tc.value)
	}
	jsonToken, _ := json.Marshal(pageToken)
	pageToken, err := s.visibilityStore.deserializePageToken(jsonToken)
	s.NoError(err)
	s.Equal(len(testCases), len(pageToken.SearchAfter))
	for i, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := parsePageTokenValue(tc.name, pageToken.SearchAfter[i], tc.tp)
			s.Equal(tc.err, err)
			s.Equal(tc.res, res)
		})
	}
}
