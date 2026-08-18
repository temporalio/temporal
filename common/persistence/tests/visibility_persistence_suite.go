package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
)

type (
	// VisibilityPersistenceSuite tests visibility persistence
	VisibilityPersistenceSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		controller *gomock.Controller

		// TaskID is monotonic increasing every time a request is created using createOpenWorkflowRecord
		// and createClosedWorkflowRecord. It tries to simulate the real task ID.
		taskID int64

		*persistencetests.TestBase
		NamespaceRegistry              namespace.Registry
		VisibilityMgr                  manager.VisibilityManager
		SearchAttributesProvider       searchattribute.Provider
		SearchAttributesMapperProvider searchattribute.MapperProvider
		CustomVisibilityStoreFactory   visibility.VisibilityStoreFactory

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation
func (s *VisibilityPersistenceSuite) SetupSuite() {
	s.DefaultTestCluster.SetupTestDatabase()
	cfg := s.DefaultTestCluster.Config()

	var err error
	s.controller = gomock.NewController(s.T())
	s.SearchAttributesProvider = searchattribute.NewTestProvider()
	s.SearchAttributesMapperProvider = searchattribute.NewTestMapperProvider(nil)
	s.NamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.VisibilityMgr, err = visibility.NewManager(
		cfg,
		resolver.NewNoopResolver(),
		s.CustomVisibilityStoreFactory,
		&elasticsearch.ProcessorConfig{
			IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(10),
			ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(2),
			ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
			ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(1 * 1024 * 1024), // 1MB
			ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(time.Second),
			ESProcessorAckTimeout:    dynamicconfig.GetDurationPropertyFn(30 * time.Second),
		},
		s.SearchAttributesProvider,
		s.SearchAttributesMapperProvider,
		s.NamespaceRegistry,
		chasm.NewRegistry(nil),
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetFloatPropertyFn(0.2),
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetStringPropertyFn(visibility.SecondaryVisibilityWritingModeOff),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFn(true),
		metrics.NoopMetricsHandler,
		s.Logger,
		serialization.NewSerializer(),
	)

	if err != nil {
		// s.NoError doesn't work here.
		s.Logger.Fatal("Unable to create visibility manager", tag.Error(err))
	}
}

// SetupTest implementation
func (s *VisibilityPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
}

func (s *VisibilityPersistenceSuite) TearDownTest() {
	s.cancel()
}

// TearDownSuite implementation
func (s *VisibilityPersistenceSuite) TearDownSuite() {
	s.VisibilityMgr.Close()
	s.DefaultTestCluster.TearDownTestDatabase()
}

// TestBasicVisibility test
func (s *VisibilityPersistenceSuite) TestBasicVisibility() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	startTime := time.Now().UTC().Add(time.Second * -5)
	startReq := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-workflow-test",
		"visibility-workflow",
		startTime,
		startTime,
		"test-queue",
	)

	// ListOpenWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, startReq, resp.Executions[0])
		},
	)

	closeReq := s.createClosedWorkflowRecord(
		startReq,
		time.Now(),
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	// ListOpenWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Empty(t, resp.Executions)
		},
	)

	// ListClosedWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
				sadefs.CloseTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.CloseTime,
				time.Now().Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closeReq, resp.Executions[0])
		},
	)
}

// TestBasicVisibilityTimeSkew test
func (s *VisibilityPersistenceSuite) TestBasicVisibilityTimeSkew() {
	testNamespaceUUID := namespace.ID(uuid.NewString())

	startTime := time.Now()
	openRecord := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-workflow-test-time-skew",
		"visibility-workflow",
		startTime,
		startTime,
		"test-queue",
	)

	// ListOpenWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord, resp.Executions[0])
		},
	)

	closedRecord := s.createClosedWorkflowRecord(
		openRecord,
		startTime.Add(-10*time.Millisecond),
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	// ListOpenWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Empty(t, resp.Executions)
		},
	)

	// ListClosedWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
				sadefs.CloseTime,
				startTime.Add(-10*time.Millisecond).Format(time.RFC3339Nano),
				sadefs.CloseTime,
				startTime.Add(-10*time.Millisecond).Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closedRecord, resp.Executions[0])
		},
	)
}

func (s *VisibilityPersistenceSuite) TestBasicVisibilityShortWorkflow() {
	testNamespaceUUID := namespace.ID(uuid.NewString())

	startTime := time.Now().UTC()
	openRecord := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-workflow-test-short-workflow",
		"visibility-workflow",
		startTime,
		startTime,
		"test-queue",
	)
	closedRecord := s.createClosedWorkflowRecord(
		openRecord,
		startTime.Add(10*time.Millisecond),
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	// ListOpenWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Empty(t, resp.Executions)
		},
	)

	// ListClosedWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
				sadefs.CloseTime,
				startTime.Add(10*time.Millisecond).Format(time.RFC3339Nano),
				sadefs.CloseTime,
				startTime.Add(10*time.Millisecond).Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closedRecord, resp.Executions[0])
		},
	)
}

// TestVisibilityPagination test
func (s *VisibilityPersistenceSuite) TestVisibilityPagination() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	var nextPageToken []byte

	// Create 2 executions
	startTime1 := time.Now().UTC()
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-pagination-test1",
		"visibility-workflow",
		startTime1,
		startTime1,
		"test-queue",
	)

	startTime2 := startTime1.Add(time.Second)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-pagination-test2",
		"visibility-workflow",
		startTime2,
		startTime2,
		"test-queue",
	)

	// Get the first one
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime1.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime2.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord2, resp.Executions[0])
			nextPageToken = resp.NextPageToken
		},
	)

	// Use token to get the second one
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    1,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime1.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime2.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
			NextPageToken: nextPageToken,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord1, resp.Executions[0])
			nextPageToken = resp.NextPageToken
		},
	)

	// It is possible to not return non empty token which is going to return empty result
	if len(nextPageToken) != 0 {
		// Now should get empty result by using token
		s.assertListWorkflowExecutions(
			&manager.ListWorkflowExecutionsRequestV2{
				NamespaceID: testNamespaceUUID,
				PageSize:    1,
				Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
					sadefs.StartTime,
					startTime1.Format(time.RFC3339Nano),
					sadefs.StartTime,
					startTime2.Format(time.RFC3339Nano),
					sadefs.ExecutionStatus,
					enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				),
				NextPageToken: nextPageToken,
			},
			func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
				require.NoError(t, err)
				require.Empty(t, resp.Executions)
			},
		)
	}
}

// TestPaginationEdgeCase verifies that the datatime filters works as expected
// in the pagination filter.
// Particularly, when a datetime is missing a component (eg: nanoseconds),
// Elasticsearch fills it in unexpected ways.
// Eg: if a workflow has CloseTime at 2023-04-05T06:07:08.100000000Z, a filter
// like `CloseTime = '2023-04-05T06:07:08Z'` will match the workflow.
// This can leads to unexpected behaviors during pagination.
func (s *VisibilityPersistenceSuite) TestPaginationEdgeCase() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	startTime := time.Date(2023, 4, 5, 6, 7, 8, 0, time.UTC)
	closeTime := startTime.Add(2 * time.Second)

	// Generating workflows with oposite orders for CloseTime and StartTime.
	// Eg: WF1 (CT1, ST1), WF2 (CT2, ST2), etc.
	// We want ST1 < ST2 < ST3 < ... and CT1 > CT2 > CT3 > ...
	// Furthermore, we want all of them to be within the same second and
	// CTn must be an exact second, ie., no nanoseconds part.
	// Since the step in time is 100ms, n must be less than 10.
	n := 3
	var startReqs []*manager.RecordWorkflowExecutionStartedRequest
	for i := range n {
		r := s.createOpenWorkflowRecord(
			testNamespaceUUID,
			fmt.Sprintf("visibility-datetime-filter-%d", i),
			"visibility-datetime-filter",
			startTime.Add(time.Duration(i)*100*time.Millisecond),
			startTime.Add(time.Duration(i)*100*time.Millisecond),
			"test-queue",
		)
		startReqs = append(startReqs, r)
	}
	for i := range startReqs {
		r := s.createClosedWorkflowRecord(
			startReqs[i],
			closeTime.Add(time.Duration(n-i-1)*100*time.Millisecond),
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		)
		fmt.Printf("FOO %d (%d, %d)\n", i, r.CloseTime.UTC().UnixNano(), r.StartTime.UTC().UnixNano())
	}

	s.assertCountWorkflowExecutions(
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "ExecutionStatus = 'Completed'",
		},
		func(t require.TestingT, resp *manager.CountWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Equal(t, int64(n), resp.Count)
		},
	)

	pageSize := n
	var nextPageToken []byte
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    pageSize,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, pageSize)
			require.NotEmpty(t, resp.NextPageToken)
			nextPageToken = resp.NextPageToken
		},
	)

	fmt.Printf("FOO page token: %s\n", nextPageToken)

	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   testNamespaceUUID,
			PageSize:      pageSize,
			NextPageToken: nextPageToken,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Empty(t, resp.Executions)
		},
	)
}

// TestFilteringByStartTime test
func (s *VisibilityPersistenceSuite) TestFilteringByStartTime() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	startTime := time.Now()

	// Create 2 open workflows, one started 2hrs ago, the other started just now.
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow-1",
		startTime.Add(-2*time.Hour),
		startTime.Add(-2*time.Hour),
		"test-queue",
	)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow-2",
		startTime,
		startTime,
		"test-queue",
	)

	// List open workflows with start time filter
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.StartTime,
				time.Now().Add(-time.Hour).Format(time.RFC3339Nano),
				sadefs.StartTime,
				time.Now().Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord2, resp.Executions[0])
		},
	)

	// List with WorkflowType filter in query string
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf(
				`StartTime BETWEEN "%v" AND "%v"`,
				time.Now().Add(-time.Hour).Format(time.RFC3339Nano),
				time.Now().Format(time.RFC3339Nano),
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord2, resp.Executions[0])
		},
	)

	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf(
				`StartTime BETWEEN "%v" AND "%v"`,
				time.Now().Add(-3*time.Hour).Format(time.RFC3339Nano),
				time.Now().Format(time.RFC3339Nano),
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 2)
		},
	)

	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf(
				`StartTime BETWEEN "%v" AND "%v" AND WorkflowType = "visibility-workflow-1"`,
				time.Now().Add(-3*time.Hour).Format(time.RFC3339Nano),
				time.Now().Format(time.RFC3339Nano),
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(s.T(), openRecord1, resp.Executions[0])
		},
	)
}

// TestFilteringByType test
func (s *VisibilityPersistenceSuite) TestFilteringByType() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	startTime := time.Now()

	// Create 2 executions
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow-1",
		startTime,
		startTime,
		"test-queue",
	)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow-2",
		startTime,
		startTime,
		"test-queue",
	)

	// List open with filtering: ListOpenWorkflowExecutionsByType
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				sadefs.WorkflowType,
				"visibility-workflow-1",
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord1, resp.Executions[0])
		},
	)

	// List with WorkflowType filter in query string
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query:       `WorkflowType = "visibility-workflow-1"`,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord1, resp.Executions[0])
		},
	)

	// Close both executions
	s.createClosedWorkflowRecord(openRecord1, time.Now(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	closedRecord2 := s.createClosedWorkflowRecord(
		openRecord2,
		time.Now(),
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	// List closed with filtering: ListClosedWorkflowExecutionsByType
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s' AND %s = '%s'",
				sadefs.CloseTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.CloseTime,
				time.Now().Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				sadefs.WorkflowType,
				"visibility-workflow-2",
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closedRecord2, resp.Executions[0])
		},
	)

	// List with WorkflowType filter in query string
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query:       `WorkflowType = "visibility-workflow-2"`,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closedRecord2, resp.Executions[0])
		},
	)
}

// TestFilteringByWorkflowID test
func (s *VisibilityPersistenceSuite) TestFilteringByWorkflowID() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	startTime := time.Now()

	// Create 2 executions
	openRecord1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow",
		startTime,
		startTime,
		"test-queue",
	)
	openRecord2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow",
		startTime,
		startTime,
		"test-queue",
	)

	// List open with filtering: ListOpenWorkflowExecutionsByWorkflowID
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s' AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				sadefs.WorkflowID,
				"visibility-filtering-test1",
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord1, resp.Executions[0])
		},
	)

	// List workflow with workflowID filter in query string
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query:       `WorkflowId = "visibility-filtering-test1"`,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertOpenExecutionEquals(t, openRecord1, resp.Executions[0])
		},
	)

	// Close both executions
	s.createClosedWorkflowRecord(openRecord1, time.Now(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	closedRecord2 := s.createClosedWorkflowRecord(
		openRecord2,
		time.Now(),
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	// List closed with filtering: ListClosedWorkflowExecutionsByWorkflowID
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s' AND %s = '%s'",
				sadefs.CloseTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.CloseTime,
				time.Now().Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				sadefs.WorkflowID,
				"visibility-filtering-test2",
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closedRecord2, resp.Executions[0])
		},
	)

	// List workflow with workflowID filter in query string
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query:       `WorkflowId = "visibility-filtering-test2"`,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closedRecord2, resp.Executions[0])
		},
	)
}

// TestFilteringByStatus test
func (s *VisibilityPersistenceSuite) TestFilteringByStatus() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	startTime := time.Now()
	executionTime := startTime

	// Create 2 executions
	startReq1 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test1",
		"visibility-workflow",
		startTime,
		executionTime,
		"test-queue",
	)

	startReq2 := s.createOpenWorkflowRecord(
		testNamespaceUUID,
		"visibility-filtering-test2",
		"visibility-workflow",
		startTime,
		executionTime,
		"test-queue",
	)

	// Close both executions with different status
	closeTime := time.Now().UTC()
	s.createClosedWorkflowRecord(startReq1, closeTime, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	closeRecord2 := s.createClosedWorkflowRecord(
		startReq2,
		closeTime,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	)

	// List closed with filtering: ListClosedWorkflowExecutionsByStatus
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    2,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s = '%s'",
				sadefs.CloseTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.CloseTime,
				time.Now().Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closeRecord2, resp.Executions[0])
		},
	)

	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    5,
			Query:       `ExecutionStatus = "Failed"`,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, 1)
			s.assertClosedExecutionEquals(t, closeRecord2, resp.Executions[0])
		},
	)
}

// TestDelete test
func (s *VisibilityPersistenceSuite) TestDeleteWorkflow() {
	openRows := 10
	closedRows := 5
	testNamespaceUUID := namespace.ID(uuid.NewString())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)
	executionTime := closeTime.Add(-4 * time.Second)
	var startRequests []*manager.RecordWorkflowExecutionStartedRequest
	for range openRows {
		startReq := s.createOpenWorkflowRecord(
			testNamespaceUUID,
			uuid.NewString(),
			"visibility-workflow",
			startTime,
			executionTime,
			"test-queue",
		)
		startRequests = append(startRequests, startReq)
	}

	for i := range closedRows {
		s.createClosedWorkflowRecord(
			startRequests[i],
			closeTime,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		)
	}

	// ListClosedWorkflowExecutions
	var executions []*workflowpb.WorkflowExecutionInfo
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    10,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
				sadefs.CloseTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.CloseTime,
				closeTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, closedRows)
			executions = resp.Executions
		},
	)

	// Delete closed workflow
	for _, row := range executions {
		s.deleteWorkflowRecord(
			testNamespaceUUID,
			row.GetExecution().GetWorkflowId(),
			row.GetExecution().GetRunId(),
		)
	}

	// ListClosedWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			PageSize:    10,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s' AND %s != '%s'",
				sadefs.CloseTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.CloseTime,
				closeTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Empty(t, resp.Executions)
		},
	)

	// ListOpenWorkflowExecutions
	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s'AND %s = '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				closeTime.Format(time.RFC3339Nano),
				sadefs.ExecutionStatus,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			),
			PageSize: 10,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Len(t, resp.Executions, openRows-closedRows)
			executions = resp.Executions
		},
	)

	// Delete open workflow
	for _, row := range executions {
		s.deleteWorkflowRecord(
			testNamespaceUUID,
			row.GetExecution().GetWorkflowId(),
			row.GetExecution().GetRunId(),
		)
	}

	s.assertListWorkflowExecutions(
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: testNamespaceUUID,
			Query: fmt.Sprintf("%s >= '%s' AND %s <= '%s'",
				sadefs.StartTime,
				startTime.Format(time.RFC3339Nano),
				sadefs.StartTime,
				closeTime.Format(time.RFC3339Nano),
			),
			PageSize: 10,
		},
		func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Empty(t, resp.Executions)
		},
	)
}

// TestUpsertWorkflowExecution test
func (s *VisibilityPersistenceSuite) TestUpsertWorkflowExecution() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	temporalChangeVersionPayload, _ := payload.Encode([]string{"dummy"})
	now := time.Now()
	tests := []struct {
		request  *manager.UpsertWorkflowExecutionRequest
		expected error
	}{
		{
			request: &manager.UpsertWorkflowExecutionRequest{
				VisibilityRequestBase: &manager.VisibilityRequestBase{
					NamespaceID: testNamespaceUUID,
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: "upsert-workflow-1",
						RunId:      uuid.NewString(),
					},
					WorkflowTypeName: "upsert-workflow",
					StartTime:        now,
					ExecutionTime:    now,
					TaskID:           1,
					Memo:             nil,
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							sadefs.TemporalChangeVersion: temporalChangeVersionPayload,
						},
					},
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			expected: nil,
		},
		{
			request: &manager.UpsertWorkflowExecutionRequest{
				VisibilityRequestBase: &manager.VisibilityRequestBase{
					NamespaceID: testNamespaceUUID,
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: "upsert-workflow-2",
						RunId:      uuid.NewString(),
					},
					WorkflowTypeName: "upsert-workflow",
					StartTime:        now,
					ExecutionTime:    now,
					TaskID:           2,
					Memo:             nil,
					SearchAttributes: nil,
					Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			// To avoid blocking the task queue processors on non-ElasticSearch visibility stores
			// we simply treat any attempts to perform Upserts as "no-ops"
			// Attempts to Scan, Count or List will still fail for non-ES stores.
			expected: nil,
		},
	}

	for _, test := range tests {
		s.Equal(test.expected, s.VisibilityMgr.UpsertWorkflowExecution(s.ctx, test.request))
	}
}

// TestGetWorkflowExecution test
func (s *VisibilityPersistenceSuite) TestGetWorkflowExecution() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)

	var startRequests []*manager.RecordWorkflowExecutionStartedRequest
	for range 5 {
		startRequests = append(
			startRequests,
			s.createOpenWorkflowRecord(
				testNamespaceUUID,
				"visibility-workflow-test",
				"visibility-workflow",
				startTime,
				startTime,
				"test-queue",
			),
		)
	}
	for _, req := range startRequests {
		s.assertGetWorkflowExecution(
			&manager.GetWorkflowExecutionRequest{
				NamespaceID: testNamespaceUUID,
				WorkflowID:  req.Execution.WorkflowId,
				RunID:       req.Execution.RunId,
			},
			func(t require.TestingT, resp *manager.GetWorkflowExecutionResponse, err error) {
				require.NoErrorf(t, err, "execution %s not found", req.Execution.RunId)
				s.assertOpenExecutionEquals(t, req, resp.Execution)
			},
		)
	}

	var closeRequests []*manager.RecordWorkflowExecutionClosedRequest
	for _, startReq := range startRequests {
		closeRequests = append(
			closeRequests,
			s.createClosedWorkflowRecord(
				startReq,
				closeTime,
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			),
		)
	}
	for _, req := range closeRequests {
		s.assertGetWorkflowExecution(
			&manager.GetWorkflowExecutionRequest{
				NamespaceID: testNamespaceUUID,
				WorkflowID:  req.Execution.WorkflowId,
				RunID:       req.Execution.RunId,
			},
			func(t require.TestingT, resp *manager.GetWorkflowExecutionResponse, err error) {
				require.NoError(t, err)
				s.assertClosedExecutionEquals(t, req, resp.Execution)
			},
		)
	}
}

// TestAdvancedVisibilityPagination test
func (s *VisibilityPersistenceSuite) TestAdvancedVisibilityPagination() {
	testNamespaceUUID := namespace.ID(uuid.NewString())

	// Generate 5 workflow records, keep 2 open and 3 closed.
	var startReqs []*manager.RecordWorkflowExecutionStartedRequest
	var closeReqs []*manager.RecordWorkflowExecutionClosedRequest
	for i := range 5 {
		startTime := time.Now()
		startReq := s.createOpenWorkflowRecord(
			testNamespaceUUID,
			fmt.Sprintf("advanced-visibility-%v", i),
			"visibility-workflow",
			startTime,
			startTime,
			"test-queue",
		)
		if i <= 1 {
			startReqs = append([]*manager.RecordWorkflowExecutionStartedRequest{startReq}, startReqs...)
		} else {
			closeReq := s.createClosedWorkflowRecord(
				startReq,
				time.Now(),
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			)
			closeReqs = append([]*manager.RecordWorkflowExecutionClosedRequest{closeReq}, closeReqs...)
		}
	}

	// Wait until all workflows are persisted.
	s.assertCountWorkflowExecutions(
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "GROUP BY ExecutionStatus",
		},
		func(t require.TestingT, resp *manager.CountWorkflowExecutionsResponse, err error) {
			require.Equal(t, int64(5), resp.Count)
			require.Len(t, resp.Groups, 2)
			require.Subset(t, []int64{2, 3}, []int64{resp.Groups[0].Count, resp.Groups[1].Count})
			var openGroup, closedGroup *workflowservice.CountWorkflowExecutionsResponse_AggregationGroup
			if resp.Groups[0].Count == 2 {
				openGroup = resp.Groups[0]
				closedGroup = resp.Groups[1]
			} else {
				openGroup = resp.Groups[1]
				closedGroup = resp.Groups[0]
			}
			require.Equal(t,
				sadefs.MustEncodeValue(
					enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
					enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				),
				openGroup.GroupValues[0],
			)
			require.Equal(t,
				sadefs.MustEncodeValue(
					enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED.String(),
					enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				),
				closedGroup.GroupValues[0],
			)
		},
	)

	for pageSize := 1; pageSize <= 5; pageSize++ {
		executions := make(map[string]*workflowpb.WorkflowExecutionInfo)
		for _, e := range s.listWithPagination(testNamespaceUUID, 5) {
			executions[e.GetExecution().GetWorkflowId()] = e
		}

		// there is no order guarantee from the list method, so we have to find the right execution
		for _, r := range startReqs {
			id := r.Execution.GetWorkflowId()
			e, ok := executions[id]
			s.True(ok)
			s.assertOpenExecutionEquals(s.T(), r, e)
			delete(executions, id)
		}
		for _, r := range closeReqs {
			id := r.Execution.GetWorkflowId()
			e, ok := executions[id]
			s.True(ok)
			s.assertClosedExecutionEquals(s.T(), r, e)
			delete(executions, id)
		}
		s.Empty(executions, "Unexpected executions returned from list method")
	}
}

func (s *VisibilityPersistenceSuite) TestCountWorkflowExecutions() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)

	for range 5 {
		s.createOpenWorkflowRecord(
			testNamespaceUUID,
			"visibility-workflow-test",
			"visibility-workflow",
			startTime,
			startTime,
			"test-queue",
		)
	}

	s.assertCountWorkflowExecutions(
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "",
		},
		func(t require.TestingT, resp *manager.CountWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Equal(t, int64(5), resp.Count)
			require.Nil(t, resp.Groups)
		},
	)
}

func (s *VisibilityPersistenceSuite) TestCountGroupByWorkflowExecutions() {
	testNamespaceUUID := namespace.ID(uuid.NewString())
	closeTime := time.Now().UTC()
	startTime := closeTime.Add(-5 * time.Second)

	var startRequests []*manager.RecordWorkflowExecutionStartedRequest
	for range 5 {
		startRequests = append(
			startRequests,
			s.createOpenWorkflowRecord(
				testNamespaceUUID,
				"visibility-workflow-test",
				"visibility-workflow",
				startTime,
				startTime,
				"test-queue",
			),
		)
	}

	s.assertCountWorkflowExecutions(
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "GROUP BY ExecutionStatus",
		},
		func(t require.TestingT, resp *manager.CountWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Equal(t, int64(5), resp.Count)
			require.Equal(
				t,
				[]*workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
					{
						GroupValues: []*commonpb.Payload{
							sadefs.MustEncodeValue(
								enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
								enumspb.INDEXED_VALUE_TYPE_KEYWORD,
							),
						},
						Count: int64(5),
					},
				},
				resp.Groups,
			)
		},
	)

	for i := range 2 {
		s.createClosedWorkflowRecord(
			startRequests[i],
			closeTime,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		)
	}

	s.assertCountWorkflowExecutions(
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: testNamespaceUUID,
			Query:       "GROUP BY ExecutionStatus",
		},
		func(t require.TestingT, resp *manager.CountWorkflowExecutionsResponse, err error) {
			require.NoError(t, err)
			require.Equal(t, int64(5), resp.Count)
		},
	)
}

func (s *VisibilityPersistenceSuite) listWithPagination(
	namespaceID namespace.ID,
	pageSize int,
) []*workflowpb.WorkflowExecutionInfo {
	var executions []*workflowpb.WorkflowExecutionInfo
	resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespaceID,
		PageSize:    pageSize,
		Query:       "",
	})
	s.NoError(err)
	executions = append(executions, resp.Executions...)

	for len(resp.NextPageToken) > 0 {
		resp, err = s.VisibilityMgr.ListWorkflowExecutions(s.ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceID,
			PageSize:      pageSize,
			Query:         "",
			NextPageToken: resp.NextPageToken,
		})
		s.NoError(err)
		executions = append(executions, resp.Executions...)
	}

	return executions
}

func (s *VisibilityPersistenceSuite) createClosedWorkflowRecord(
	startReq *manager.RecordWorkflowExecutionStartedRequest,
	closeTime time.Time,
	status enumspb.WorkflowExecutionStatus,
) *manager.RecordWorkflowExecutionClosedRequest {
	s.taskID++
	closeReq := &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      startReq.NamespaceID,
			Execution:        startReq.Execution,
			WorkflowTypeName: startReq.WorkflowTypeName,
			StartTime:        startReq.StartTime,
			ExecutionTime:    startReq.ExecutionTime,
			Status:           status,
			TaskID:           s.taskID,
		},
		CloseTime:         closeTime,
		ExecutionDuration: closeTime.Sub(startReq.ExecutionTime),
		HistoryLength:     5,
	}
	err := s.VisibilityMgr.RecordWorkflowExecutionClosed(s.ctx, closeReq)
	s.NoError(err)
	return closeReq
}

func (s *VisibilityPersistenceSuite) createOpenWorkflowRecord(
	namespaceID namespace.ID,
	workflowID string,
	workflowType string,
	startTime time.Time,
	executionTime time.Time,
	taskQueue string,
) *manager.RecordWorkflowExecutionStartedRequest {
	s.taskID++
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.NewString(),
	}
	startReq := &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID:      namespaceID,
			Execution:        &workflowExecution,
			WorkflowTypeName: workflowType,
			StartTime:        startTime,
			ExecutionTime:    executionTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        taskQueue,
			TaskID:           s.taskID,
		},
	}
	err := s.VisibilityMgr.RecordWorkflowExecutionStarted(s.ctx, startReq)
	s.NoError(err)
	return startReq
}

func (s *VisibilityPersistenceSuite) deleteWorkflowRecord(
	namespaceID namespace.ID,
	workflowID string,
	runID string,
) {
	s.taskID++
	deleteReq := &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TaskID:      s.taskID,
	}
	err := s.VisibilityMgr.DeleteWorkflowExecution(s.ctx, deleteReq)
	s.NoError(err)
}

func (s *VisibilityPersistenceSuite) assertClosedExecutionEquals(
	t require.TestingT,
	req *manager.RecordWorkflowExecutionClosedRequest,
	resp *workflowpb.WorkflowExecutionInfo,
) {
	require.Equal(t, req.Execution.RunId, resp.Execution.RunId)
	require.Equal(t, req.Execution.WorkflowId, resp.Execution.WorkflowId)
	require.Equal(t, req.WorkflowTypeName, resp.GetType().GetName())
	require.Equal(t, persistence.UnixMilliseconds(req.StartTime), persistence.UnixMilliseconds(timestamp.TimeValue(resp.GetStartTime())))
	require.Equal(t, persistence.UnixMilliseconds(req.CloseTime), persistence.UnixMilliseconds(timestamp.TimeValue(resp.GetCloseTime())))
	require.Equal(t, req.Status, resp.GetStatus())
	require.Equal(t, req.HistoryLength, resp.HistoryLength)
	require.Equal(t, req.ExecutionDuration, resp.GetExecutionDuration().AsDuration())
}

func (s *VisibilityPersistenceSuite) assertOpenExecutionEquals(
	t require.TestingT,
	req *manager.RecordWorkflowExecutionStartedRequest,
	resp *workflowpb.WorkflowExecutionInfo,
) {
	require.Equal(t, req.Execution.GetRunId(), resp.Execution.GetRunId())
	require.Equal(t, req.Execution.WorkflowId, resp.Execution.WorkflowId)
	require.Equal(t, req.WorkflowTypeName, resp.GetType().GetName())
	require.Equal(t, persistence.UnixMilliseconds(req.StartTime), persistence.UnixMilliseconds(timestamp.TimeValue(resp.GetStartTime())))
	require.Nil(t, resp.CloseTime)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, resp.Status)
	require.Zero(t, resp.HistoryLength)
}

func (s *VisibilityPersistenceSuite) assertListWorkflowExecutions(
	request *manager.ListWorkflowExecutionsRequestV2,
	assertFn func(t require.TestingT, resp *manager.ListWorkflowExecutionsResponse, err error),
) {
	await.Require(
		s.ctx,
		s.T(),
		func(t *await.T) {
			resp, err := s.VisibilityMgr.ListWorkflowExecutions(s.ctx, request)
			assertFn(t, resp, err)
		},
		4*time.Second,
		200*time.Millisecond,
	)
}

func (s *VisibilityPersistenceSuite) assertCountWorkflowExecutions(
	request *manager.CountWorkflowExecutionsRequest,
	assertFn func(t require.TestingT, resp *manager.CountWorkflowExecutionsResponse, err error),
) {
	await.Require(
		s.ctx,
		s.T(),
		func(t *await.T) {
			resp, err := s.VisibilityMgr.CountWorkflowExecutions(s.ctx, request)
			assertFn(t, resp, err)
		},
		4*time.Second,
		200*time.Millisecond,
	)
}

func (s *VisibilityPersistenceSuite) assertGetWorkflowExecution(
	request *manager.GetWorkflowExecutionRequest,
	assertFn func(t require.TestingT, resp *manager.GetWorkflowExecutionResponse, err error),
) {
	await.Require(
		s.ctx,
		s.T(),
		func(t *await.T) {
			resp, err := s.VisibilityMgr.GetWorkflowExecution(s.ctx, request)
			assertFn(t, resp, err)
		},
		4*time.Second,
		200*time.Millisecond,
	)
}
