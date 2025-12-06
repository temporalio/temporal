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
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

type (
	ESVisibilitySuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		controller                         *gomock.Controller
		visibilityStore                    *VisibilityStore
		mockESClient                       *client.MockClient
		mockProcessor                      *MockProcessor
		mockMetricsHandler                 *metrics.MockHandler
		mockSearchAttributesMapperProvider *searchattribute.MockMapperProvider
		chasmRegistry                      *chasm.Registry
	}

	// Test component for CHASM visibility tests
	testChasmComponent struct {
		chasm.UnimplementedComponent
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

	namespaceDivisionIsNull = elastic.NewBoolQuery().MustNot(
		elastic.NewExistsQuery(sadefs.TemporalNamespaceDivision),
	)
)

func mustEncodeValue(val interface{}, valueType enumspb.IndexedValueType) *commonpb.Payload {
	p, err := searchattribute.EncodeValue(val, valueType)
	if err != nil {
		panic(fmt.Sprintf("failed to encode value %v: %v", val, err))
	}
	return p
}

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
			sadefs.ExecutionStatus,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			sadefs.TemporalNamespaceDivision,
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
	visibilityEnableUnifiedQueryConverter := dynamicconfig.GetBoolPropertyFn(true)

	s.controller = gomock.NewController(s.T())
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ElasticsearchVisibility)).Return(s.mockMetricsHandler).AnyTimes()
	s.mockProcessor = NewMockProcessor(s.controller)
	s.mockESClient = client.NewMockClient(s.controller)
	s.mockSearchAttributesMapperProvider = searchattribute.NewMockMapperProvider(s.controller)

	// Setup CHASM registry for tests
	library := chasm.NewMockLibrary(s.controller)
	library.EXPECT().Name().Return("TestLibrary").AnyTimes()
	rc := chasm.NewRegistrableComponent[*testChasmComponent](
		"TestComponent",
		chasm.WithSearchAttributes(
			chasm.NewSearchAttributeBool("ChasmCompleted", chasm.SearchAttributeFieldBool01),
			chasm.NewSearchAttributeKeyword("ChasmStatus", chasm.SearchAttributeFieldKeyword01),
			chasm.NewSearchAttributeInt("ChasmCount", chasm.SearchAttributeFieldInt01),
			chasm.NewSearchAttributeDouble("ChasmScore", chasm.SearchAttributeFieldDouble01),
			chasm.NewSearchAttributeDateTime("ChasmStartTime", chasm.SearchAttributeFieldDateTime01),
		),
	)
	library.EXPECT().Components().Return([]*chasm.RegistrableComponent{rc}).AnyTimes()
	library.EXPECT().Tasks().Return(nil).AnyTimes()
	s.chasmRegistry = chasm.NewRegistry(log.NewNoopLogger())
	err := s.chasmRegistry.Register(library)
	s.NoError(err)

	s.visibilityStore = &VisibilityStore{
		esClient:                       s.mockESClient,
		index:                          testIndex,
		searchAttributesProvider:       searchattribute.NewTestEsProvider(),
		searchAttributesMapperProvider: s.mockSearchAttributesMapperProvider,
		chasmRegistry:                  s.chasmRegistry,
		processor:                      s.mockProcessor,
		processorAckTimeout:            esProcessorAckTimeout,
		disableOrderByClause:           visibilityDisableOrderByClause,
		enableManualPagination:         visibilityEnableManualPagination,
		enableUnifiedQueryConverter:    visibilityEnableUnifiedQueryConverter,
		metricsHandler:                 s.mockMetricsHandler,
	}

	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(testNamespace).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()
}

func (s *ESVisibilitySuite) TearDownTest() {
	s.controller.Finish()
}

func (tc *testChasmComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (s *ESVisibilitySuite) TestGetListFieldSorter() {

	// test defaultSorter is returned when fieldSorts is empty
	fieldSorts := make([]elastic.Sorter, 0)
	sorter, err := s.visibilityStore.GetListFieldSorter(fieldSorts)
	s.NoError(err)
	s.Equal(defaultSorter, sorter)

	// test passing non-empty fieldSorts
	testFieldSorts := []elastic.Sorter{elastic.NewFieldSort("_test"), elastic.NewFieldSort("_second_tes")}
	sorter, err = s.visibilityStore.GetListFieldSorter(testFieldSorts[:])
	expectedSorter := make([]elastic.Sorter, len(testFieldSorts)+1)
	expectedSorter[0] = testFieldSorts[0]
	expectedSorter[1] = testFieldSorts[1]
	expectedSorter[2] = elastic.NewFieldSort(sadefs.RunID).Desc()
	s.NoError(err)
	s.Equal(expectedSorter, sorter)

}

func (s *ESVisibilitySuite) TestBuildSearchParametersV2() {
	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    testPageSize,
	}

	matchNamespaceQuery := elastic.NewTermQuery(sadefs.NamespaceID, request.NamespaceID.String())
	matchNSDivision := elastic.NewTermQuery(sadefs.TemporalNamespaceDivision, "hidden-stuff")

	var filterQuery elastic.Query

	// test for open
	request.Query = `WorkflowId="guid-2208"`
	filterQuery = elastic.NewTermQuery(sadefs.WorkflowID, "guid-2208")
	boolQuery := elastic.NewBoolQuery().Filter(
		matchNamespaceQuery,
		elastic.NewBoolQuery().Filter(namespaceDivisionIsNull, filterQuery),
	)
	p, err := s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
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
	filterQuery = elastic.NewBoolQuery().Filter(elastic.NewTermQuery(sadefs.WorkflowID, "guid-2208"), matchNSDivision)
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery, filterQuery)
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)
	request.Query = ""

	// test custom sort
	request.Query = `Order bY WorkflowId`
	boolQuery = elastic.NewBoolQuery().Filter(matchNamespaceQuery, namespaceDivisionIsNull)
	s.mockMetricsHandler.EXPECT().WithTags(metrics.NamespaceTag(request.Namespace.String())).Return(s.mockMetricsHandler)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Return(metrics.NoopCounterMetricFunc)
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter: []elastic.Sorter{
			elastic.NewFieldSort(sadefs.WorkflowID).Asc(),
			elastic.NewFieldSort(sadefs.RunID).Desc(),
		},
	}, p)
	request.Query = ""

	// test for wrong query
	request.Query = "invalid query"
	p, err = s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
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

	matchNamespaceQuery := elastic.NewTermQuery(sadefs.NamespaceID, request.NamespaceID.String())

	// disable ORDER BY clause
	s.visibilityStore.disableOrderByClause = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	// test valid query
	request.Query = `WorkflowId="guid-2208"`
	filterQuery := elastic.NewTermQuery(sadefs.WorkflowID, "guid-2208")
	boolQuery := elastic.NewBoolQuery().Filter(
		matchNamespaceQuery,
		elastic.NewBoolQuery().Filter(namespaceDivisionIsNull, filterQuery),
	)
	p, err := s.visibilityStore.BuildSearchParametersV2(request, s.visibilityStore.GetListFieldSorter)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
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

func (s *ESVisibilitySuite) Test_convertQueryLegacy() {
	s.visibilityStore.searchAttributesMapperProvider = searchattribute.NewTestMapperProvider(nil)

	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' or WorkflowId = 'another-wid'`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"term":{"WorkflowId":"wid"}},{"term":{"WorkflowId":"another-wid"}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' order by StartTime desc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"StartTime":{"order":"desc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `WorkflowId = 'wid' and CloseTime is null`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' or CloseTime is null`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"term":{"WorkflowId":"wid"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `CloseTime is null order by CloseTime desc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"CloseTime":{"order":"desc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `StartTime = "2018-06-07T15:04:05.123456789-08:00"`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"StartTime":{"query":"2018-06-07T15:04:05.123456789-08:00"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `WorkflowId = 'wid' and StartTime > "2018-06-07T15:04:05+00:00"`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"range":{"StartTime":{"from":"2018-06-07T15:04:05+00:00","include_lower":false,"include_upper":true,"to":null}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ExecutionTime < 1000000`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ExecutionTime between 1 and 2`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.000000001Z","include_lower":true,"include_upper":true,"to":"1970-01-01T00:00:00.000000002Z"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ExecutionTime < 1000000 or ExecutionTime > 2000000`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"should":[{"range":{"ExecutionTime":{"from":null,"include_lower":true,"include_upper":false,"to":"1970-01-01T00:00:00.001Z"}}},{"range":{"ExecutionTime":{"from":"1970-01-01T00:00:00.002Z","include_lower":false,"include_upper":true,"to":null}}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by StartTime desc, CloseTime asc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"StartTime":{"order":"desc"}},{"CloseTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomTextField desc`
	_, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal(err.(*serviceerror.InvalidArgument).Error(), "invalid query: unable to convert 'order by' column name: unable to sort by field of Text type, use field of type Keyword")

	query = `order by CustomIntField asc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"CustomIntField":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `ExecutionTime < "unable to parse"`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal(err.Error(), "invalid query: unable to convert filter expression: unable to convert values of comparison expression: invalid value for search attribute ExecutionTime of type Datetime: \"unable to parse\"")
	s.Nil(queryParams)

	// invalid union injection
	query = `WorkflowId = 'wid' union select * from dummy`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	s.Nil(queryParams)
}

func (s *ESVisibilitySuite) Test_convertQueryLegacy_Mapper() {
	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = "`AliasForCustomKeywordField` = 'pid'"
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = "`AliasWithHyphenFor-CustomKeywordField` = 'pid'"
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `CustomKeywordField = 'pid'`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"CustomKeywordField":"pid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `AliasForUnknownField = 'pid'`
	_, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert filter expression: unable to convert left side of \"AliasForUnknownField = 'pid'\": invalid search attribute: AliasForUnknownField")

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by AliasForCustomKeywordField asc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"CustomKeywordField":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	query = `order by CustomKeywordField asc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.NotNil(queryParams.Sorter)

	query = `order by AliasForUnknownField asc`
	_, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert 'order by' column name: invalid search attribute: AliasForUnknownField")
	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) Test_convertQueryLegacy_Mapper_Error() {
	query := `WorkflowId = 'wid'`
	queryParams, err := s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"WorkflowId":"wid"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	query = `ProductId = 'pid'`
	_, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.EqualError(err, "invalid query: unable to convert filter expression: unable to convert left side of \"ProductId = 'pid'\": invalid search attribute: ProductId")

	query = `order by ExecutionTime`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, query, nil, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.Equal(`{"bool":{"filter":{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Equal(`[{"ExecutionTime":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	s.visibilityStore.searchAttributesMapperProvider = nil
}

func (s *ESVisibilitySuite) Test_convertQuery() {
	namespaceIDQuery := elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String())

	testCases := []struct {
		name  string
		query string
		want  *esQueryParams
		err   string
	}{
		{
			name:  "empty",
			query: "",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					namespaceDivisionIsNull,
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{},
			},
		},
		{
			name:  "one comparison",
			query: "WorkflowId = 'wid'",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery(sadefs.WorkflowID, "wid"),
					),
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{},
			},
		},
		{
			name:  "custom order by",
			query: "WorkflowId = 'wid' ORDER BY WorkflowId",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery(sadefs.WorkflowID, "wid"),
					),
				),
				Sorter:  []elastic.Sorter{elastic.NewFieldSort(sadefs.WorkflowID)},
				GroupBy: []string{},
			},
		},
		{
			name:  "group by",
			query: "WorkflowId = 'wid' GROUP BY ExecutionStatus",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery(sadefs.WorkflowID, "wid"),
					),
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{sadefs.ExecutionStatus},
			},
		},
		{
			name:  "custom search attributes",
			query: "WorkflowId = 'wid' AND AliasForCustomKeywordField = 'foo' OR AliasForCustomIntField = 123 ORDER BY AliasForCustomKeywordField",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewBoolQuery().
							Should(
								elastic.NewBoolQuery().Filter(
									elastic.NewTermQuery(sadefs.WorkflowID, "wid"),
									elastic.NewTermQuery("CustomKeywordField", "foo"),
								),
								elastic.NewTermQuery("CustomIntField", int64(123)),
							).
							MinimumNumberShouldMatch(1),
					),
				),
				Sorter:  []elastic.Sorter{elastic.NewFieldSort("CustomKeywordField")},
				GroupBy: []string{},
			},
		},
		{
			name:  "invalid query",
			query: "WorkflowId LIKE 'wid'",
			err:   query.NotSupportedErrMessage,
		},
		{
			name:  "invalid custom search attributes",
			query: "WorkflowId = 'wid' AND InvalidField = 'foo'",
			err:   query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			got, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, tc.query, nil, 0)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var invalidArgumentErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidArgumentErr)
			} else {
				s.NoError(err)
				s.Equal(tc.want, got)
			}
		})
	}
}

func (s *ESVisibilitySuite) TestGetListWorkflowExecutionsResponse() {
	// test for empty hits
	searchResult := &elastic.SearchResult{
		Hits: &elastic.SearchHits{
			TotalHits: &elastic.TotalHits{},
		}}
	resp, err := s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, 1, nil)
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
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, 1, nil)
	s.NoError(err)
	serializedToken, _ := s.visibilityStore.serializePageToken(&visibilityPageToken{SearchAfter: []interface{}{1547596872371234567, "e481009e-14b3-45ae-91af-dce6e2a88365"}})
	s.Equal(serializedToken, resp.NextPageToken)
	s.Equal(1, len(resp.Executions))

	// test for last page hits
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, 2, nil)
	s.NoError(err)
	s.Equal(0, len(resp.NextPageToken))
	s.Equal(1, len(resp.Executions))

	// test for search after
	searchResult.Hits.Hits = []*elastic.SearchHit{}
	for i := int64(0); i < searchResult.Hits.TotalHits.Value; i++ {
		searchResult.Hits.Hits = append(searchResult.Hits.Hits, searchHit)
	}
	numOfHits := len(searchResult.Hits.Hits)
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits, nil)
	s.NoError(err)
	s.Equal(numOfHits, len(resp.Executions))
	nextPageToken, err := s.visibilityStore.deserializePageToken(resp.NextPageToken)
	s.NoError(err)
	resultSortValue, err := nextPageToken.SearchAfter[0].(json.Number).Int64()
	s.NoError(err)
	s.Equal(int64(1547596872371234567), resultSortValue)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", nextPageToken.SearchAfter[1])
	// for last page
	resp, err = s.visibilityStore.GetListWorkflowExecutionsResponse(searchResult, testNamespace, numOfHits+1, nil)
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
	saTypeMap := searchattribute.TestEsNameTypeMap()
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
	info, err := s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, nil)
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
	info, err = s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, nil)
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
	info, err = s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, nil)
	s.Error(err)
	s.Nil(info)
}

func (s *ESVisibilitySuite) TestParseESDoc_SearchAttributes() {
	s.visibilityStore.searchAttributesMapperProvider = searchattribute.NewTestMapperProvider(nil)

	saTypeMap := searchattribute.TestEsNameTypeMap()
	docSource := []byte(`{"ExecutionStatus": "Completed",
          "TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomTextField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`)
	info, err := s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, nil)
	s.NoError(err)
	s.NotNil(info)
	customSearchAttributes, err := searchattribute.Decode(info.SearchAttributes, &saTypeMap, true)
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
	saTypeMap := searchattribute.TestEsNameTypeMap()
	docSource := []byte(`{"ExecutionStatus": "Completed",
          "TemporalChangeVersion": ["ver1", "ver2"],
          "CustomKeywordField": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
          "CustomTextField": "text text",
          "CustomDatetimeField": ["2014-08-28T03:15:00.000-07:00", "2016-04-21T05:00:00.000-07:00"],
          "CustomDoubleField": [1234.1234,5678.5678],
          "CustomIntField": [111,222],
          "CustomBoolField": true,
          "UnknownField": "random"}`)

	info, err := s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, nil)
	s.NoError(err)
	s.NotNil(info)

	// Visibility store returns DB column names (not aliased). Aliasing is done at visibility manager layer.
	s.Len(info.SearchAttributes.GetIndexedFields(), 7)
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalChangeVersion")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "CustomKeywordField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "CustomTextField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "CustomDatetimeField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "CustomDoubleField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "CustomBoolField")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "CustomIntField")
	s.NotContains(info.SearchAttributes.GetIndexedFields(), "UnknownField")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)
}

func (s *ESVisibilitySuite) TestListWorkflowExecutions() {
	s.mockESClient.EXPECT().Search(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, p *client.SearchParameters) (*elastic.SearchResult, error) {
			s.Equal(testIndex, p.Index)
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("ExecutionStatus", "Terminated"),
					),
				),
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

func (s *ESVisibilitySuite) TestCountWorkflowExecutions() {
	s.mockESClient.EXPECT().Count(gomock.Any(), testIndex, gomock.Any()).DoAndReturn(
		func(ctx context.Context, index string, query elastic.Query) (int64, error) {
			s.Equal(
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("ExecutionStatus", "Terminated"),
					),
				),
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
					elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("ExecutionStatus", "Terminated"),
					),
				),
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
				Filter(
					elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
					namespaceDivisionIsNull,
				),
			sadefs.ExecutionStatus,
			elastic.NewTermsAggregation().Field(sadefs.ExecutionStatus),
		).
		Return(
			&elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					sadefs.ExecutionStatus: json.RawMessage(
						`{"buckets":[{"key":"Completed","doc_count":100},{"key":"Running","doc_count":10}]}`,
					),
				},
			},
			nil,
		)
	resp, err := s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.NoError(err)
	expectedResp := &store.InternalCountExecutionsResponse{
		Count: 110,
		Groups: []store.InternalAggregationGroup{
			{
				GroupValues: []*commonpb.Payload{
					mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
				Count: 100,
			},
			{
				GroupValues: []*commonpb.Payload{
					mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
				Count: 10,
			},
		},
	}
	s.True(temporalproto.DeepEqual(expectedResp, resp))

	// test only allowed to group by a single field
	request.Query = "GROUP BY ExecutionStatus, WorkflowType"
	resp, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.ErrorContains(err, "'GROUP BY' clause supports only a single field")
	s.Nil(resp)

	// test only allowed to group by ExecutionStatus and LowCardinalityKeyword fields
	request.Query = "GROUP BY WorkflowType"
	resp, err = s.visibilityStore.CountWorkflowExecutions(context.Background(), request)
	s.ErrorContains(err, "'GROUP BY' clause is only supported for ExecutionStatus")
	s.Nil(resp)
}

func (s *ESVisibilitySuite) TestCountGroupByWorkflowExecutions() {
	testCases := []struct {
		name         string
		groupBy      []string
		aggName      string
		agg          elastic.Aggregation
		mockResponse *elastic.SearchResult
		response     *store.InternalCountExecutionsResponse
	}{
		{
			name:    "group by one field",
			groupBy: []string{sadefs.ExecutionStatus},
			aggName: sadefs.ExecutionStatus,
			agg:     elastic.NewTermsAggregation().Field(sadefs.ExecutionStatus),
			mockResponse: &elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					sadefs.ExecutionStatus: json.RawMessage(
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
			response: &store.InternalCountExecutionsResponse{
				Count: 110,
				Groups: []store.InternalAggregationGroup{
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 100,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 10,
					},
				},
			},
		},

		{
			name:    "group by two fields",
			groupBy: []string{sadefs.ExecutionStatus, sadefs.WorkflowType},
			aggName: sadefs.ExecutionStatus,
			agg: elastic.NewTermsAggregation().Field(sadefs.ExecutionStatus).SubAggregation(
				sadefs.WorkflowType,
				elastic.NewTermsAggregation().Field(sadefs.WorkflowType),
			),
			mockResponse: &elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					sadefs.ExecutionStatus: json.RawMessage(
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
			response: &store.InternalCountExecutionsResponse{
				Count: 110,
				Groups: []store.InternalAggregationGroup{
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 75,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 25,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 7,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 3,
					},
				},
			},
		},

		{
			name: "group by three fields",
			groupBy: []string{
				sadefs.ExecutionStatus,
				sadefs.WorkflowType,
				sadefs.WorkflowID,
			},
			aggName: sadefs.ExecutionStatus,
			agg: elastic.NewTermsAggregation().Field(sadefs.ExecutionStatus).SubAggregation(
				sadefs.WorkflowType,
				elastic.NewTermsAggregation().Field(sadefs.WorkflowType).SubAggregation(
					sadefs.WorkflowID,
					elastic.NewTermsAggregation().Field(sadefs.WorkflowID),
				),
			),
			mockResponse: &elastic.SearchResult{
				Aggregations: map[string]json.RawMessage{
					sadefs.ExecutionStatus: json.RawMessage(
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
			response: &store.InternalCountExecutionsResponse{
				Count: 110,
				Groups: []store.InternalAggregationGroup{
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-id-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 75,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-id-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 20,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-id-3", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 5,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-1", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-id-4", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 7,
					},
					{
						GroupValues: []*commonpb.Payload{
							mustEncodeValue(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-type-2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
							mustEncodeValue("wf-id-5", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
						},
						Count: 3,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			searchParams := &esQueryParams{
				Query: elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
						namespaceDivisionIsNull,
					),
				GroupBy: tc.groupBy,
			}
			s.mockESClient.EXPECT().
				CountGroupBy(
					gomock.Any(),
					testIndex,
					elastic.NewBoolQuery().
						Filter(
							elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String()),
							namespaceDivisionIsNull,
						),
					tc.aggName,
					tc.agg,
				).
				Return(tc.mockResponse, nil)
			resp, err := s.visibilityStore.countGroupByExecutions(context.Background(), searchParams, nil)
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
		Filter(elastic.NewTermQuery(sadefs.NamespaceID, testNamespace.String()))

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
					elastic.NewRangeQuery(sadefs.CloseTime).Lt(closeTime.Format(time.RFC3339Nano)),
				),
				elastic.NewBoolQuery().Filter(
					elastic.NewTermQuery(sadefs.CloseTime, closeTime.Format(time.RFC3339Nano)),
					elastic.NewRangeQuery(sadefs.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
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
	saTypeMap := searchattribute.TestEsNameTypeMap()

	testCases := []struct {
		name         string
		sorterFields []fieldSort
		searchAfter  []any
		res          []elastic.Query
		err          error
	}{
		{
			name:         "one field",
			sorterFields: []fieldSort{{sadefs.StartTime, true, true}},
			searchAfter:  []any{json.Number(fmt.Sprintf("%d", startTime.UnixNano()))},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(sadefs.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
				),
			},
			err: nil,
		},
		{
			name: "two fields one null",
			sorterFields: []fieldSort{
				{sadefs.CloseTime, true, true},
				{sadefs.StartTime, true, true},
			},
			searchAfter: []any{
				datetimeNull,
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
			},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(elastic.NewExistsQuery(sadefs.CloseTime)),
				elastic.NewBoolQuery().
					MustNot(elastic.NewExistsQuery(sadefs.CloseTime)).
					Filter(
						elastic.NewRangeQuery(sadefs.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
					),
			},
			err: nil,
		},
		{
			name: "two fields no null",
			sorterFields: []fieldSort{
				{sadefs.CloseTime, true, true},
				{sadefs.StartTime, true, true},
			},
			searchAfter: []any{
				json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
			},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(sadefs.CloseTime).Lt(closeTime.Format(time.RFC3339Nano)),
				),
				elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(sadefs.CloseTime, closeTime.Format(time.RFC3339Nano)),
						elastic.NewRangeQuery(sadefs.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
					),
			},
			err: nil,
		},
		{
			name: "three fields",
			sorterFields: []fieldSort{
				{sadefs.CloseTime, true, true},
				{sadefs.StartTime, true, true},
				{sadefs.RunID, false, true},
			},
			searchAfter: []any{
				json.Number(fmt.Sprintf("%d", closeTime.UnixNano())),
				json.Number(fmt.Sprintf("%d", startTime.UnixNano())),
				"random-run-id",
			},
			res: []elastic.Query{
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery(sadefs.CloseTime).Lt(closeTime.Format(time.RFC3339Nano)),
				),
				elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(sadefs.CloseTime, closeTime.Format(time.RFC3339Nano)),
						elastic.NewRangeQuery(sadefs.StartTime).Lt(startTime.Format(time.RFC3339Nano)),
					),
				elastic.NewBoolQuery().
					Filter(
						elastic.NewTermQuery(sadefs.CloseTime, closeTime.Format(time.RFC3339Nano)),
						elastic.NewTermQuery(sadefs.StartTime, startTime.Format(time.RFC3339Nano)),
						elastic.NewRangeQuery(sadefs.RunID).Gt("random-run-id"),
					),
			},
			err: nil,
		},
		{
			name: "invalid token: wrong size",
			sorterFields: []fieldSort{
				{sadefs.CloseTime, true, true},
				{sadefs.StartTime, true, true},
				{sadefs.RunID, false, true},
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
				{sadefs.CloseTime, true, true},
			},
			searchAfter: []any{datetimeNull},
			res:         nil,
			err:         serviceerror.NewInternal("last field of sorter cannot be a nullable field: \"CloseTime\" has null values"),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := buildPaginationQuery(tc.sorterFields, tc.searchAfter, saTypeMap)
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

func (s *ESVisibilitySuite) Test_convertQueryLegacy_ChasmMapper() {
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":     "ChasmCompleted",
			"TemporalKeyword01":  "ChasmStatus",
			"TemporalInt01":      "ChasmCount",
			"TemporalDouble01":   "ChasmScore",
			"TemporalDatetime01": "ChasmStartTime",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":     enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"TemporalInt01":      enumspb.INDEXED_VALUE_TYPE_INT,
			"TemporalDouble01":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"TemporalDatetime01": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	)

	queryStr := `ChasmCompleted = true`
	queryParams, err := s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	// Legacy converter uses "match" for non-KEYWORD types, "term" for KEYWORD types
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"TemporalBool01":{"query":true}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	queryStr = `ChasmStatus = 'active'`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"term":{"TemporalKeyword01":"active"}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	queryStr = `ChasmCount = 42`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"TemporalInt01":{"query":42}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	queryStr = `ChasmScore = 3.14`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"TemporalDouble01":{"query":3.14}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	queryStr = `ChasmStartTime = "2018-06-07T15:04:05.123456789-08:00"`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"TemporalDatetime01":{"query":"2018-06-07T15:04:05.123456789-08:00"}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	queryStr = `ChasmCompleted = true order by ChasmStatus asc`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":{"match":{"TemporalBool01":{"query":true}}}}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.JSONEq(`[{"TemporalKeyword01":{"order":"asc"}}]`, s.sorterToJSON(queryParams.Sorter))

	queryStr = `ChasmStatus = 'active' and WorkflowId = 'wid'`
	queryParams, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.NoError(err)
	s.JSONEq(`{"bool":{"filter":[{"term":{"NamespaceId":"bfd5c907-f899-4baf-a7b2-2ab85e623ebd"}},{"bool":{"filter":[{"term":{"TemporalKeyword01":"active"}},{"term":{"WorkflowId":"wid"}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`, s.queryToJSON(queryParams.Query))
	s.Nil(queryParams.Sorter)

	queryStr = `UnknownChasmField = 'value'`
	_, err = s.visibilityStore.convertQueryLegacy(testNamespace, testNamespaceID, queryStr, chasmMapper, chasm.UnspecifiedArchetypeID)
	s.Error(err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.Contains(err.Error(), "invalid search attribute: UnknownChasmField")
}

func (s *ESVisibilitySuite) Test_convertQuery_ChasmMapper() {
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":    "ChasmCompleted",
			"TemporalKeyword01": "ChasmStatus",
			"TemporalInt01":     "ChasmCount",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"TemporalInt01":     enumspb.INDEXED_VALUE_TYPE_INT,
		},
	)

	namespaceIDQuery := elastic.NewTermQuery(sadefs.NamespaceID, testNamespaceID.String())

	testCases := []struct {
		name  string
		query string
		want  *esQueryParams
		err   string
	}{
		{
			name:  "chasm bool attribute",
			query: "ChasmCompleted = true",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("TemporalBool01", true),
					),
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{},
			},
		},
		{
			name:  "chasm keyword attribute",
			query: "ChasmStatus = 'active'",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("TemporalKeyword01", "active"),
					),
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{},
			},
		},
		{
			name:  "chasm int attribute",
			query: "ChasmCount = 42",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("TemporalInt01", int64(42)),
					),
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{},
			},
		},
		{
			name:  "chasm attribute with order by",
			query: "ChasmCompleted = true ORDER BY ChasmStatus",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewTermQuery("TemporalBool01", true),
					),
				),
				Sorter:  []elastic.Sorter{elastic.NewFieldSort("TemporalKeyword01")},
				GroupBy: []string{},
			},
		},
		{
			name:  "chasm and regular attribute",
			query: "ChasmStatus = 'active' AND WorkflowId = 'wid'",
			want: &esQueryParams{
				Query: elastic.NewBoolQuery().Filter(
					namespaceIDQuery,
					elastic.NewBoolQuery().Filter(
						namespaceDivisionIsNull,
						elastic.NewBoolQuery().Filter(
							elastic.NewTermQuery("TemporalKeyword01", "active"),
							elastic.NewTermQuery(sadefs.WorkflowID, "wid"),
						),
					),
				),
				Sorter:  []elastic.Sorter{},
				GroupBy: []string{},
			},
		},
		{
			name:  "invalid chasm attribute",
			query: "UnknownChasmField = 'value'",
			err:   query.InvalidExpressionErrMessage,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			got, err := s.visibilityStore.convertQuery(testNamespace, testNamespaceID, tc.query, chasmMapper, chasm.UnspecifiedArchetypeID)
			if tc.err != "" {
				s.Error(err)
				s.ErrorContains(err, tc.err)
				var invalidArgumentErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidArgumentErr)
			} else {
				s.NoError(err)
				s.Equal(tc.want, got)
			}
		})
	}
}

func (s *ESVisibilitySuite) TestBuildSearchParametersV2_ChasmMapper() {
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":    "ChasmCompleted",
			"TemporalKeyword01": "ChasmStatus",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	request := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		PageSize:    testPageSize,
	}

	matchNamespaceQuery := elastic.NewTermQuery(sadefs.NamespaceID, request.NamespaceID.String())

	request.Query = `ChasmCompleted = true`
	filterQuery := elastic.NewTermQuery("TemporalBool01", true)
	boolQuery := elastic.NewBoolQuery().Filter(
		matchNamespaceQuery,
		elastic.NewBoolQuery().Filter(namespaceDivisionIsNull, filterQuery),
	)
	p, err := s.visibilityStore.BuildChasmSearchParameters(
		&manager.ListChasmExecutionsRequest{
			NamespaceID: testNamespaceID,
			Namespace:   testNamespace,
			PageSize:    testPageSize,
			Query:       request.Query,
		},
		s.visibilityStore.GetListFieldSorter,
		chasmMapper,
	)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter:      defaultSorter,
	}, p)

	request.Query = `ChasmStatus = 'active' ORDER BY ChasmStatus`
	filterQuery = elastic.NewTermQuery("TemporalKeyword01", "active")
	boolQuery = elastic.NewBoolQuery().Filter(
		matchNamespaceQuery,
		elastic.NewBoolQuery().Filter(namespaceDivisionIsNull, filterQuery),
	)
	s.mockMetricsHandler.EXPECT().WithTags(metrics.NamespaceTag(request.Namespace.String())).Return(s.mockMetricsHandler)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ElasticsearchCustomOrderByClauseCount.Name()).Return(metrics.NoopCounterMetricFunc)
	p, err = s.visibilityStore.BuildChasmSearchParameters(
		&manager.ListChasmExecutionsRequest{
			NamespaceID: testNamespaceID,
			Namespace:   testNamespace,
			PageSize:    testPageSize,
			Query:       request.Query,
		},
		s.visibilityStore.GetListFieldSorter,
		chasmMapper,
	)
	s.NoError(err)
	s.Equal(&client.SearchParameters{
		Index:       testIndex,
		Query:       boolQuery,
		SearchAfter: nil,
		PageSize:    testPageSize,
		Sorter: []elastic.Sorter{
			elastic.NewFieldSort("TemporalKeyword01").Asc(),
			elastic.NewFieldSort(sadefs.RunID).Desc(),
		},
	}, p)
}

func (s *ESVisibilitySuite) TestParseESDoc_ChasmSearchAttributes() {
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":     "ChasmCompleted",
			"TemporalKeyword01":  "ChasmStatus",
			"TemporalInt01":      "ChasmCount",
			"TemporalDouble01":   "ChasmScore",
			"TemporalDatetime01": "ChasmStartTime",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":     enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"TemporalInt01":      enumspb.INDEXED_VALUE_TYPE_INT,
			"TemporalDouble01":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"TemporalDatetime01": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	)

	saTypeMap := searchattribute.TestEsNameTypeMap()
	docSource := []byte(`{
		"ExecutionStatus": "Completed",
		"NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
		"RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
		"StartTime": "2021-06-11T15:04:07.980-07:00",
		"WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
		"WorkflowType": "TestWorkflowExecute",
		"TemporalBool01": true,
		"TemporalKeyword01": "active",
		"TemporalInt01": 42,
		"TemporalDouble01": 3.14,
		"TemporalDatetime01": "2018-06-07T15:04:05.123456789-08:00"
	}`)

	info, err := s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, chasmMapper)
	s.NoError(err)
	s.NotNil(info)
	s.Equal("6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256", info.WorkflowID)
	s.Equal("e481009e-14b3-45ae-91af-dce6e2a88365", info.RunID)
	s.Equal("TestWorkflowExecute", info.TypeName)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.Status)

	// Search attributes are now combined (CHASM + custom) in encoded form
	s.NotNil(info.SearchAttributes)
	s.NotNil(info.SearchAttributes.GetIndexedFields())
	// Verify CHASM search attributes are present in encoded form
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalBool01")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalKeyword01")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalInt01")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalDouble01")
	s.Contains(info.SearchAttributes.GetIndexedFields(), "TemporalDatetime01")
}

func (s *ESVisibilitySuite) TestParseESDoc_ChasmSearchAttributes_NoMapper() {
	saTypeMap := searchattribute.TestEsNameTypeMap()
	docSource := []byte(`{
		"ExecutionStatus": "Completed",
		"NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
		"RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
		"StartTime": "2021-06-11T15:04:07.980-07:00",
		"WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
		"WorkflowType": "TestWorkflowExecute",
		"TemporalBool01": true,
		"TemporalKeyword01": "active"
	}`)

	info, err := s.visibilityStore.ParseESDoc("", docSource, saTypeMap, testNamespace, nil)
	s.NoError(err)
	s.NotNil(info)
	// Without a chasmMapper, CHASM attributes (TemporalBool01, TemporalKeyword01) are not registered
	// in the type map, so they are ignored as unknown fields. Only system fields are parsed.
	// Search attributes should be nil or empty since there are no custom search attributes in this doc.
	if info.SearchAttributes != nil {
		s.Empty(info.SearchAttributes.GetIndexedFields())
	}
}

func (s *ESVisibilitySuite) TestNameInterceptor_ChasmMapper() {
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":    "ChasmCompleted",
			"TemporalKeyword01": "ChasmStatus",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	ni := NewNameInterceptor(
		testNamespace,
		searchattribute.TestEsNameTypeMap(),
		s.mockSearchAttributesMapperProvider,
		chasmMapper,
	)

	fieldName, err := ni.Name("ChasmCompleted", query.FieldNameFilter)
	s.NoError(err)
	s.Equal("TemporalBool01", fieldName)

	fieldName, err = ni.Name("ChasmStatus", query.FieldNameFilter)
	s.NoError(err)
	s.Equal("TemporalKeyword01", fieldName)

	fieldName, err = ni.Name("ChasmStatus", query.FieldNameSorter)
	s.NoError(err)
	s.Equal("TemporalKeyword01", fieldName)

	_, err = ni.Name("UnknownChasmField", query.FieldNameFilter)
	s.Error(err)
	var converterErr *query.ConverterError
	s.ErrorAs(err, &converterErr)
}

func (s *ESVisibilitySuite) TestValuesInterceptor_ChasmMapper() {
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":     "ChasmCompleted",
			"TemporalKeyword01":  "ChasmStatus",
			"TemporalInt01":      "ChasmCount",
			"TemporalDouble01":   "ChasmScore",
			"TemporalDatetime01": "ChasmStartTime",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":     enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"TemporalInt01":      enumspb.INDEXED_VALUE_TYPE_INT,
			"TemporalDouble01":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"TemporalDatetime01": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
	)

	vi := NewValuesInterceptor(
		testNamespace,
		searchattribute.TestEsNameTypeMap(),
		chasmMapper,
	)

	values, err := vi.Values("ChasmCompleted", "TemporalBool01", true)
	s.NoError(err)
	s.Len(values, 1)
	s.Equal(true, values[0])

	values, err = vi.Values("ChasmStatus", "TemporalKeyword01", "active")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal("active", values[0])

	values, err = vi.Values("ChasmCount", "TemporalInt01", int64(42))
	s.NoError(err)
	s.Len(values, 1)
	s.Equal(int64(42), values[0])

	values, err = vi.Values("ChasmScore", "TemporalDouble01", 3.14)
	s.NoError(err)
	s.Len(values, 1)
	s.InDelta(3.14, values[0], 0.0001)

	testTime := time.Unix(0, 1528358645123456789).UTC()
	values, err = vi.Values("ChasmStartTime", "TemporalDatetime01", testTime.UnixNano())
	s.NoError(err)
	s.Len(values, 1)
	s.Equal("2018-06-07T08:04:05.123456789Z", values[0])

	_, err = vi.Values("ChasmCompleted", "TemporalBool01", "not-a-bool")
	s.Error(err)
	var converterErr *query.ConverterError
	s.ErrorAs(err, &converterErr)
}
