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

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

type (
	ESVisibilitySuite struct {
		suite.Suite
		protorequire.ProtoAssertions
		controller                         *gomock.Controller
		visibilityStore                    *VisibilityStore
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
	s.visibilityStore = &VisibilityStore{
		esClient:                       s.mockESClient,
		index:                          testIndex,
		searchAttributesProvider:       searchattribute.NewTestProvider(),
		searchAttributesMapperProvider: searchattribute.NewTestMapperProvider(nil),
		processor:                      s.mockProcessor,
		processorAckTimeout:            esProcessorAckTimeout,
		disableOrderByClause:           visibilityDisableOrderByClause,
		enableManualPagination:         visibilityEnableManualPagination,
		metricsHandler:                 s.mockMetricsHandler,
	}
}

func (s *ESVisibilitySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ESVisibilitySuite) TestGetListFieldSorter() {

	// test defaultSorter is returned when fieldSorts is empty
	fieldSorts := make([]elastic.Sorter, 0)
	sorter, err := s.visibilityStore.GetListFieldSorter(fieldSorts)
	require.NoError(s.T(), err)
	require.Equal(s.T(), defaultSorter, sorter)

	// test passing non-empty fieldSorts
	testFieldSorts := []elastic.Sorter{elastic.NewFieldSort("_test"), elastic.NewFieldSort("_second_tes")}
	sorter, err = s.visibilityStore.GetListFieldSorter(testFieldSorts[:])
	expectedSorter := make([]elastic.Sorter, len(testFieldSorts)+1)
	expectedSorter[0] = testFieldSorts[0]
	expectedSorter[1] = testFieldSorts[1]
	expectedSorter[2] = elastic.NewFieldSort(searchattribute.RunID).Desc()
	require.NoError(s.T(), err)
	require.Equal(s.T(), expectedSorter, sorter)

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
	p, err := s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), &client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request.Query = ""

	// test for open with namespace division
	request.Query = `WorkflowId="guid-2208" and TemporalNamespaceDivision="hidden-stuff"`
	// note namespace division appears in the filterQuery, not the boolQuery like the negative version
	filterQuery = elastic.NewBoolQuery().Filter(elastic.NewTermQuery(searchattribute.WorkflowID, "guid-2208"), matchNSDivision)
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery)
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), &client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request.Query = ""

	// test custom sort
	request.Query = `Order bY WorkflowId`
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery).MustNot(namespaceDivisionExists)
	s.mockMetricsHandler.EXPECT().WithTags(metrics.NamespaceTag(request.Namespace.String())).Return(s.mockMetricsHandler)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Return(metrics.NoopCounterMetricFunc)
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), &client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter: []elastic.Sorter{
			elastic.NewFieldSort(searchattribute.WorkflowID).Asc(),
			elastic.NewFieldSort(searchattribute.RunID).Desc(),
		},
	}, p)
	request.Query = ""

	// test for wrong query
	request.Query = "invalid query"
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	require.Nil(s.T(), p)
	require.Error(s.T(), err)
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
	p, err := s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), &client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request.Query = ""

	// test invalid query with ORDER BY
	request.Query = `ORDER BY WorkflowId`
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	require.Nil(s.T(), p)
	require.Error(s.T(), err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "ORDER BY clause is not supported")
	request.Query = ""
}

func (s *ESVisibilitySuite) queryToJSON(q elastic.Query) string {
	m, err := q.Source()
	require.NoError(s.T(), err)
	b, err := json.Marshal(m)
	require.NoError(s.T(), err)
	return string(b)
}

func (s *ESVisibilitySuite) sorterToJSON(sorters []elastic.Sorter) string {
	var ms []interface{}
	for _, sorter := range sorters {
		m, err := sorter.Source()
		require.NoError(s.T(), err)
		ms = append(ms, m)
	}
	b, err := json.Marshal(ms)
	require.NoError(s.T(), err)
	return string(b)
}

func (s *ESVisibilitySuite) Test_convertQuery() {
	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `WorkflowId = 'wid' or WorkflowId = 'another-wid'`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"term":{"WorkflowId":"wid"}},{"term":{"WorkflowId":"another-wid"}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `WorkflowId = 'wid' order by StartTime desc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"StartTime":{"order":"desc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `WorkflowId = 'wid' and CloseTime is null`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `WorkflowId = 'wid' or CloseTime is null`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"term":{"WorkflowId":"wid"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `CloseTime is null order by CloseTime desc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"CloseTime":{"order":"desc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `StartTime = "2018-06-07T15:04:05.123456789-08:00"`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"StartTime":{"query":"2018-06-07T15:04:05.123456789-08:00"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `WorkflowId = 'wid' and StartTime > "2018-06-07T15:04:05+00:00"`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"range":{"StartTime":{"from":"2018-06-07T15:04:05+00:00","include_lower":false,"include_upper":true,"to":null}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `ExecutionTime < 1000000`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `ExecutionTime between 1 and 2`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.000000001Z","include_lower":true,"include_upper":true,"to":"1970-01-01T00:00:00.000000002Z"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `ExecutionTime < 1000000 or ExecutionTime > 2000000`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}},{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.002Z","include_lower":false,"include_upper":true,"to":null}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by StartTime desc, CloseTime asc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"StartTime":{"order":"desc"}},{"CloseTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomTextField desc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.IsType(s.T(), &serviceerror.InvalidArgument{}, err)
	require.Equal(s.T(), err.(*serviceerror.InvalidArgument).Error(), "invalid query: unable to convert 'order by' column name: unable to sort by field of Text type, use field of type Keyword")

	query = `order by CustomIntField asc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"CustomIntField":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `ExecutionTime < "unable to parse"`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.IsType(s.T(), &serviceerror.InvalidArgument{}, err)
	require.Equal(s.T(), err.Error(), "invalid query: unable to convert filter expression: unable to convert values of comparison expression: invalid value for search attribute ExecutionTime of type Datetime: \"unable to parse\"")

	// invalid union injection
	query = `WorkflowId = 'wid' union select * from dummy`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.Nil(s.T(), queryParams)
}

func (s *ESVisibilitySuite) Test_convertQuery_Mapper() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(testNamespace).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	s.visibilityStore.searchAttributesMapperProvider = s.mockSearchAttributesMapperProvider

	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = "`AliasForCustomKeywordField` = 'pid'"
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = "`AliasWithHyphenFor-CustomKeywordField` = 'pid'"
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `CustomKeywordField = 'pid'`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `AliasForUnknownField = 'pid'`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert filter expression: unable to convert left side of \"AliasForUnknownField = 'pid'\": invalid search attribute: AliasForUnknownField")

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by AliasForCustomKeywordField asc`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"CustomKeywordField":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomKeywordField asc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `order by AliasForUnknownField asc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert 'order by' column name: invalid search attribute: AliasForUnknownField")
	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) Test_convertQuery_Mapper_Error() {
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(testNamespace).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	s.visibilityStore.searchAttributesMapperProvider = s.mockSearchAttributesMapperProvider

	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Nil(s.T(), queryParams.Sorter)

	query = `ProductId = 'pid'`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.NoError(s.T(), err)
	require.Equal(s.T(), `{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	require.Equal(s.T(), `[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomIntField asc`
	_, err = s.visibilityStore.convertQuery(testNamespace, testNamespaceID, query)
	require.Error(s.T(), err)
	require.ErrorAs(s.T(), err, &invalidArgumentErr)
	s.EqualError(err, "mapper error")

	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	// test for empty hits
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			TotalHits: &elastic.TotalHits{},
		}}
	resp, err := s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, 1)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, len(resp.NextPageToken))
	require.Equal(s.T(), 0, len(resp.Executions))

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
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, 1)
	require.NoError(s.T(), err)
	serializedToken, _ := s.visibilityStore.serializePageToken(&visibilityPageToken{SearchAfter: []interface{}{1547596872371234567, "e481009e-14b3-45ae-91af-dce6e2a88365"}})
	require.Equal(s.T(), serializedToken, resp.NextPageToken)
	require.Equal(s.T(), 1, len(resp.Executions))

	// test for last page hits
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, 2)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, len(resp.NextPageToken))
	require.Equal(s.T(), 1, len(resp.Executions))

	// test for search after
	searchResult.Hits.Hits = []*elastic.SearchHit{}
	for i := int64(0); i < searchResult.Hits.TotalHits.Value; i++ {
		searchResult.Hits.Hits = append(searchResult.Hits.Hits, searchHit)
	}
	numOfHits := len(searchResult.Hits.Hits)
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits)
	require.NoError(s.T(), err)
	require.Equal(s.T(), numOfHits, len(resp.Executions))
	nextPageToken, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	require.NoError(s.T(), err)
	resultSortValue, err := nextPageToken.SearchAfter[0].(json.Number).Int64()
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(1547596872371234567), resultSortValue)
	require.Equal(s.T(), "e481009e-14b3-45ae-91af-dce6e2a88365", nextPageToken.SearchAfter[1])
	// for last page
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits+1)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, len(resp.NextPageToken))
	require.Equal(s.T(), numOfHits, len(resp.Executions))
}

func (s *ESVisibilitySuite) TestDeserializePageToken() {
	badInput := []byte("bad input")
	result, err := s.visibilityStore.deserializePageToken(badInput)
	require.Error(s.T(), err)
	require.Nil(s.T(), result)
	err, ok := err.(*serviceerror.InvalidArgument)
	require.True(s.T(), ok)
	require.Contains(s.T(), err.Error(), "unable to deserialize page token")

	result, err = s.visibilityStore.deserializePageToken(nil)
	require.NoError(s.T(), err)
	require.Nil(s.T(), result)

	token := &visibilityPageToken{SearchAfter: []interface{}{int64(1629936710090695939), "unique"}}
	data, err := s.visibilityStore.serializePageToken(token)
	require.NoError(s.T(), err)
	result, err = s.visibilityStore.deserializePageToken(data)
	require.NoError(s.T(), err)
	resultSortValue, err := result.SearchAfter[0].(json.Number).Int64()
	require.NoError(s.T(), err)
	require.Equal(s.T(), token.SearchAfter[0].(int64), resultSortValue)
}

func (s *ESVisibilitySuite) TestSerializePageToken() {
	data, err := s.visibilityStore.serializePageToken(nil)
	require.NoError(s.T(), err)
	require.Nil(s.T(), data)
	token, err := s.visibilityStore.deserializePageToken(data)
	require.NoError(s.T(), err)
	require.Nil(s.T(), token)

	sortTime := int64(123)
	tieBreaker := "unique"
	newToken := &visibilityPageToken{SearchAfter: []interface{}{sortTime, tieBreaker}}
	data, err = s.visibilityStore.serializePageToken(newToken)
	require.NoError(s.T(), err)
	require.True(s.T(), len(data) > 0)
	token, err = s.visibilityStore.deserializePageToken(data)
	require.NoError(s.T(), err)
	resultSortValue, err := token.SearchAfter[0].(json.Number).Int64()
	require.NoError(s.T(), err)
	require.Equal(s.T(), newToken.SearchAfter[0], resultSortValue)
	require.Equal(s.T(), newToken.SearchAfter[1], token.SearchAfter[1])
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
	info, err := s.visibilityStore.ParseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), info)
	require.Equal(s.T(), "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	require.Equal(s.T(), "e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	require.Equal(s.T(), "TestWorkflowExecute", info.TypeName)
	require.Equal(s.T(), int64(10), info.StateTransitionCount)
	require.Equal(s.T(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.Status)
	expectedStartTime, err := time.Parse(time.RFC3339Nano, "2021-06-11T15:04:07.980-07:00")
	require.NoError(s.T(), err)
	require.Equal(s.T(), expectedStartTime, info.StartTime)
	require.Nil(s.T(), info.SearchAttributes)

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
	info, err = s.visibilityStore.ParseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), info)
	require.Equal(s.T(), "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	require.Equal(s.T(), "e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	require.Equal(s.T(), "TestWorkflowExecute", info.TypeName)
	require.Equal(s.T(), int64(20), info.StateTransitionCount)
	expectedStartTime, err = time.Parse(time.RFC3339Nano, "2021-06-11T15:04:07.980-07:00")
	require.NoError(s.T(), err)
	expectedCloseTime, err := time.Parse(time.RFC3339Nano, "2021-06-11T16:04:07Z")
	require.NoError(s.T(), err)
	require.Equal(s.T(), expectedStartTime, info.StartTime)
	require.Equal(s.T(), expectedCloseTime, info.CloseTime)
	require.Equal(s.T(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)
	require.Equal(s.T(), int64(29), info.HistoryLength)
	require.Nil(s.T(), info.SearchAttributes)

	// test for error case
	docSource = []byte(`corrupted data`)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchDocumentParseFailuresCount.Name()).Return(metrics.NoopCounterMetricFunc)
	info, err = s.visibilityStore.ParseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	require.Error(s.T(), err)
	require.Nil(s.T(), info)
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
	info, err := s.visibilityStore.ParseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), info)
	customSearchAttributes, err := searchattribute.Decode(info.SearchAttributes, &searchattribute.TestNameTypeMap, true)
	require.NoError(s.T(), err)

	require.Len(s.T(), customSearchAttributes, 7)

	require.Equal(s.T(), []string{"ver1", "ver2"}, customSearchAttributes["TemporalChangeVersion"])

	require.Equal(s.T(), "bfd5c907-f899-4baf-a7b2-2ab85e623ebd", customSearchAttributes["CustomKeywordField"])

	require.Equal(s.T(), "text text", customSearchAttributes["CustomTextField"])

	date1, err := time.Parse(time.RFC3339Nano, "2014-08-28T03:15:00.000-07:00")
	require.NoError(s.T(), err)
	date2, err := time.Parse(time.RFC3339Nano, "2016-04-21T05:00:00.000-07:00")
	require.NoError(s.T(), err)
	require.Equal(s.T(), []time.Time{date1, date2}, customSearchAttributes["CustomDatetimeField"])

	require.Equal(s.T(), []float64{1234.1234, 5678.5678}, customSearchAttributes["CustomDoubleField"])

	require.Equal(s.T(), true, customSearchAttributes["CustomBoolField"])

	require.Equal(s.T(), []int64{int64(111), int64(222)}, customSearchAttributes["CustomIntField"])

	_, ok := customSearchAttributes["UnknownField"]
	require.False(s.T(), ok)

	require.Equal(s.T(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)
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

	info, err := s.visibilityStore.ParseESDoc("", docSource, searchattribute.TestNameTypeMap, testNamespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), info)

	require.Len(s.T(), info.SearchAttributes.GetIndexedFields(), 7)
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "TemporalChangeVersion")
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "AliasForCustomKeywordField")
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "AliasForCustomTextField")
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "AliasForCustomDatetimeField")
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "AliasForCustomDoubleField")
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "AliasForCustomBoolField")
	require.Contains(s.T(), info.SearchAttributes.GetIndexedFields(), "AliasForCustomIntField")
	require.NotContains(s.T(), info.SearchAttributes.GetIndexedFields(), "UnknownField")
	require.Equal(s.T(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)

	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			require.Equal(s.T(), testIndex, p.Index)
			require.Equal(s.T(),
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
	require.NoError(s.T(), err)

	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).Return(nil, errTestESSearch)
	_, err = s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	require.Error(s.T(), err)
	_, ok := err.(*serviceerror.Unavailable)
	require.True(s.T(), ok)
	require.Contains(s.T(), err.Error(), "ListWorkflowExecutions failed")

	request.Query = `invalid query`
	_, err = s.visibilityStore.ListWorkflowExecutions(context.Background(), request)
	require.Error(s.T(), err)
	_, ok = err.(*serviceerror.InvalidArgument)
	require.True(s.T(), ok)
	require.True(s.T(), strings.HasPrefix(err.Error(), "invalid query"))
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
	require.Error(s.T(), err)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(s.T(), err, &invalidArgErr)
	require.Equal(s.T(), "ListWorkflowExecutions failed: elastic: Error 400 (Bad Request): error reason [type=]", invalidArgErr.Message)

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
	require.ErrorAs(s.T(), err, &unavailableErr)
	require.Equal(s.T(), "ListWorkflowExecutions failed: elastic: Error 500 (Internal Server Error): error reason [type=]", unavailableErr.Message)
}

func (s *ESVisibilitySuite) TestListOpenWorkflowExecutionsWithNamespaceDivision() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *client.SearchParameters) (*elastic.SearchResult, error) {
			source, _ := input.Query.Source()
			require.Contains(s.T(), fmt.Sprintf("%v", source), filterOpen)
			require.Contains(s.T(), fmt.Sprintf("%v", source), filterByNSDivision)
			return testSearchResult, nil
		})
	_, err := s.visibilityStore.ListWorkflowExecutions(context.Background(), createTestRequestWithNSDivision())
	require.NoError(s.T(), err)
}

func (s *ESVisibilitySuite) TestCountWorkflowExecutions() {
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			require.Equal(s.T(),
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
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(1), resp.Count)

	// test unavailable error
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			require.Equal(s.T(),
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(searchattribute.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(elastic.NewTermQuery("ExecutionStatus", "Terminated")),
				).MustNot(namespaceDivisionExists),
				query,
			)
			return int64(0), errTestESSearch
		})

	_, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	require.Error(s.T(), err)
	_, ok := err.(*serviceerror.Unavailable)
	require.True(s.T(), ok)
	require.Contains(s.T(), err.Error(), "CountWorkflowExecutions failed")

	// test bad request
	request.Query = `invalid query`
	_, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	require.Error(s.T(), err)
	_, ok = err.(*serviceerror.InvalidArgument)
	require.True(s.T(), ok)
	require.True(s.T(), strings.HasPrefix(err.Error(), "invalid query"), err.Error())
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
	require.NoError(s.T(), err)
	payload1, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	payload2, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	require.True(s.T(), temporalproto.DeepEqual(
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
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "'group by' clause supports only a single field")
	require.Nil(s.T(), resp)

	// test only allowed to group by ExecutionStatus
	request.Query = "GROUP BY WorkflowType"
	resp, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "'group by' clause is only supported for ExecutionStatus search attribute")
	require.Nil(s.T(), resp)
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
			require.NoError(s.T(), err)
			require.True(s.T(), temporalproto.DeepEqual(tc.response, resp))
		})
	}
}

func (s *ESVisibilitySuite) TestGetWorkflowExecution() {
	s.mockESClient.EXPECT().Get(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, docID string) (*elastic.GetResult, error) {
			require.Equal(s.T(), testIndex, index)
			require.Equal(s.T(), testWorkflowID+delimiter+testRunID, docID)
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
	require.NoError(s.T(), err)

	// test unavailable error
	s.mockESClient.EXPECT().Get(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, docID string) (*elastic.GetResult, error) {
			require.Equal(s.T(), testIndex, index)
			require.Equal(s.T(), testWorkflowID+delimiter+testRunID, docID)
			return nil, errTestESSearch
		})

	_, err = s.visibilityStore.GetWorkflowExecution(context.Background(), request)
	require.Error(s.T(), err)
	_, ok := err.(*serviceerror.Unavailable)
	require.True(s.T(), ok)
	require.Contains(s.T(), err.Error(), "GetWorkflowExecution failed")
}

func (s *ESVisibilitySuite) Test_detailedErrorMessage() {
	err := errors.New("test message")
	require.Equal(s.T(), "test message", detailedErrorMessage(err))

	err = &elastic.Error{
		Status: 500,
	}
	require.Equal(s.T(), "elastic: Error 500 (Internal Server Error)", detailedErrorMessage(err))

	err = &elastic.Error{
		Status: 500,
		Details: &elastic.ErrorDetails{
			Type:   "some type",
			Reason: "some reason",
		},
	}
	require.Equal(s.T(), "elastic: Error 500 (Internal Server Error): some reason [type=some type]", detailedErrorMessage(err))

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
	require.Equal(s.T(), "elastic: Error 500 (Internal Server Error): some reason [type=some type]", detailedErrorMessage(err))

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
	require.Equal(s.T(), "elastic: Error 500 (Internal Server Error): some reason [type=some type], root causes: some other reason1 [type=some other type1], some other reason2 [type=some other type2]", detailedErrorMessage(err))
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
			resError:         serviceerror.NewInvalidArgument("invalid page token for given sort fields: expected 1 fields, got 2"),
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
			visibilityStore := &VisibilityStore{
				esClient:                       s.mockESClient,
				index:                          testIndex,
				searchAttributesProvider:       searchattribute.NewTestProvider(),
				searchAttributesMapperProvider: searchattribute.NewTestMapperProvider(nil),
				processor:                      s.mockProcessor,
				processorAckTimeout:            dynamicconfig.GetDurationPropertyFn(1 * time.Minute * debug.TimeoutMultiplier),
				disableOrderByClause:           dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
				enableManualPagination:         dynamicconfig.GetBoolPropertyFnFilteredByNamespace(tc.manualPagination),
				metricsHandler:                 s.mockMetricsHandler,
			}
			params := &client.SearchParameters{
				Index:  testIndex,
				Query:  baseQuery,
				Sorter: tc.sorter,
			}
			err := visibilityStore.processPageToken(params, tc.pageToken, testNamespace)
			require.Equal(s.T(), tc.resSearchAfter, params.SearchAfter)
			require.Equal(s.T(), tc.resQuery, params.Query)
			require.Equal(s.T(), tc.resError, err)
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
			err: serviceerror.NewInvalidArgument("invalid page token for given sort fields: expected 3 fields, got 2"),
		},
		{
			name: "invalid token: last value null",
			sorterFields: []fieldSort{
				{searchattribute.CloseTime, true, true},
			},
			searchAfter: []any{datetimeNull},
			res:         nil,
			err:         serviceerror.NewInternal("last field of sorter cannot be a nullable field: \"CloseTime\" has null values"),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := buildPaginationQuery(tc.sorterFields, tc.searchAfter, searchattribute.TestNameTypeMap)
			require.Equal(s.T(), tc.err, err)
			require.Equal(s.T(), tc.res, res)
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
			err:   serviceerror.NewInvalidArgument("invalid page token: expected interger type, got \"123\""),
		},
		{
			name:  "DoubleFieldError",
			value: "foo",
			tp:    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("invalid page token: expected float type, got \"foo\""),
		},
		{
			name:  "KeywordFieldError",
			value: 123,
			tp:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("invalid page token: expected string type, got 123"),
		},
		{
			name:  "TextFieldError",
			value: "foo",
			tp:    enumspb.INDEXED_VALUE_TYPE_TEXT,
			res:   nil,
			err:   serviceerror.NewInvalidArgument("invalid field type in sorter: cannot order by \"TextFieldError\""),
		},
	}

	pageToken := &visibilityPageToken{}
	for _, tc := range testCases {
		pageToken.SearchAfter = append(pageToken.SearchAfter, tc.value)
	}
	jsonToken, _ := json.Marshal(pageToken)
	pageToken, err := s.visibilityStore.deserializePageToken(jsonToken)
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(testCases), len(pageToken.SearchAfter))
	for i, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := parsePageTokenValue(tc.name, pageToken.SearchAfter[i], tc.tp)
			require.Equal(s.T(), tc.err, err)
			require.Equal(s.T(), tc.res, res)
		})
	}
}
