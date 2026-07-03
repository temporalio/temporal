package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	"go.temporal.io/api/operatorservice/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/scanner/build_ids"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	esPollInterval = 300 * time.Millisecond

	testSearchAttributeKey = "CustomTextField"
	testSearchAttributeVal = "test value"
)

type AdvancedVisibilitySuite struct {
	parallelsuite.Suite[*AdvancedVisibilitySuite]
}

func TestAdvancedVisibilitySuite(t *testing.T) {
	parallelsuite.Run(t, &AdvancedVisibilitySuite{}, true)
}

func TestAdvancedVisibilitySuiteLegacy(t *testing.T) {
	parallelsuite.Run(t, &AdvancedVisibilitySuite{}, false)
}

// newTestEnv creates a TestEnv with the dynamic config this suite needs.
// Additional per-test options may be passed in opts.
func (s *AdvancedVisibilitySuite) newTestEnv(enableUnifiedQueryConverter bool, opts ...testcore.TestOption) *testcore.TestEnv {
	// This cluster use customized threshold for history config
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.VisibilityDisableOrderByClause, false),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningDataAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.ReachabilityTaskQueueScanLimit, 2),
		testcore.WithDynamicConfig(dynamicconfig.ReachabilityQueryBuildIdLimit, 1),
		testcore.WithDynamicConfig(dynamicconfig.BuildIdScavengerEnabled, true),
		// Allow the scavenger to remove any build ID regardless of when it was last default for a set.
		testcore.WithDynamicConfig(dynamicconfig.RemovableBuildIdDurationSinceDefault, time.Microsecond),
		// Enable the unified query converter
		testcore.WithDynamicConfig(dynamicconfig.VisibilityEnableUnifiedQueryConverter, enableUnifiedQueryConverter),
		// Enable external payload tracking for TestListWorkflow_ExternalPayloadSearchAttributes
		testcore.WithDynamicConfig(dynamicconfig.ExternalPayloadsEnabled, true),
	}
	env := testcore.NewEnv(s.T(), append(baseOpts, opts...)...)

	if !testcore.UseSQLVisibility() {
		// To ensure that Elasticsearch won't return more than defaultPageSize documents,
		// but returns error if page size on request is greater than defaultPageSize.
		// Probably can be removed and replaced with assert on items count in response.
		s.updateMaxResultWindow(env)
	}

	return env
}

func (s *AdvancedVisibilitySuite) TestListOpenWorkflow(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-start-workflow-test"
	wt := "es-functional-start-workflow-test-type"
	tl := "es-functional-start-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	attrPayload := sadefs.MustEncodeValue(testSearchAttributeVal, enumspb.INDEXED_VALUE_TYPE_TEXT)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			testSearchAttributeKey: attrPayload,
		},
	}
	request.SearchAttributes = searchAttr

	startTime := time.Now().UTC()
	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = timestamppb.New(startTime)
	var openExecution *workflowpb.WorkflowExecutionInfo
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			startFilter.LatestTime = timestamppb.New(time.Now().UTC())
			resp, err := env.FrontendClient().ListOpenWorkflowExecutions(
				s.Context(), &workflowservice.ListOpenWorkflowExecutionsRequest{
					Namespace:       env.Namespace().String(),
					MaximumPageSize: testcore.DefaultPageSize,
					StartTimeFilter: startFilter,
					Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{
						ExecutionFilter: &filterpb.WorkflowExecutionFilter{
							WorkflowId: id,
						},
					},
				},
			)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			openExecution = resp.GetExecutions()[0]
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.NotNil(openExecution)
	s.Equal(we.GetRunId(), openExecution.GetExecution().GetRunId())

	s.Len(openExecution.GetSearchAttributes().GetIndexedFields(), 1)
	attrPayloadFromResponse, attrExist := openExecution.GetSearchAttributes().GetIndexedFields()[testSearchAttributeKey]
	s.True(attrExist)
	s.Equal(attrPayload.GetData(), attrPayloadFromResponse.GetData())
	attrType, typeSet := attrPayloadFromResponse.GetMetadata()[sadefs.MetadataType]
	s.True(typeSet)
	s.NotEmpty(attrType)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-test"
	wt := "es-functional-list-workflow-test-type"
	tl := "es-functional-list-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(env, we.GetRunId(), query)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_ExecutionTime(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-execution-time-test"
	wt := "es-functional-list-workflow-execution-time-test-type"
	tl := "es-functional-list-workflow-execution-time-test-taskqueue"

	now := time.Now().UTC()
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	// Start workflow with ExecutionTime equal to StartTime
	weNonCron, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	cronID := id + "-cron"
	request.CronSchedule = "@every 1m"
	request.WorkflowId = cronID

	// Start workflow with ExecutionTime equal to StartTime + 1 minute (cron delay)
	weCron, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	//       <<1s    <<1s                  1m
	// ----+-----+------------+----------------------------+--------------
	//    now  nonCronStart  cronStart                  cronExecutionTime
	//         =nonCronExecutionTime

	expectedNonCronMaxExecutionTime := now.Add(1 * time.Second)                   // 1 second for time skew
	expectedCronMaxExecutionTime := now.Add(1 * time.Minute).Add(1 * time.Second) // 1 second for time skew

	// WorkflowId filter is to filter workflows from other tests.
	nonCronQueryNanos := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < %d`, id, cronID, expectedNonCronMaxExecutionTime.UnixNano())
	s.testHelperForReadOnce(env, weNonCron.GetRunId(), nonCronQueryNanos)

	cronQueryNanos := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < %d AND ExecutionTime > %d`, id, cronID, expectedCronMaxExecutionTime.UnixNano(), expectedNonCronMaxExecutionTime.UnixNano())
	s.testHelperForReadOnce(env, weCron.GetRunId(), cronQueryNanos)

	nonCronQuery := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < "%s"`, id, cronID, expectedNonCronMaxExecutionTime.Format(time.RFC3339Nano))
	s.testHelperForReadOnce(env, weNonCron.GetRunId(), nonCronQuery)

	cronQuery := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < "%s" AND ExecutionTime > "%s"`, id, cronID, expectedCronMaxExecutionTime.Format(time.RFC3339Nano), expectedNonCronMaxExecutionTime.Format(time.RFC3339Nano))
	s.testHelperForReadOnce(env, weCron.GetRunId(), cronQuery)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_SearchAttribute(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-by-search-attr-test"
	wt := "es-functional-list-workflow-by-search-attr-test-type"
	tl := "es-functional-list-workflow-by-search-attr-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	attrValBytes := sadefs.MustEncodeValue(testSearchAttributeVal, enumspb.INDEXED_VALUE_TYPE_TEXT)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	query := fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, testSearchAttributeKey, testSearchAttributeVal)
	openExecution := s.testHelperForReadOnce(env, we.GetRunId(), query)
	searchValBytes, ok := openExecution.GetSearchAttributes().GetIndexedFields()[testSearchAttributeKey]
	s.True(ok)
	var searchVal string
	_ = payload.Decode(searchValBytes, &searchVal)
	s.Equal(testSearchAttributeVal, searchVal)

	searchAttributes := s.createSearchAttributes()
	// test upsert
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
				SearchAttributes: searchAttributes,
			}}}

		return []*commandpb.Command{upsertCommand}, nil
	}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            "worker1",
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}
	res, err := poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running' and BinaryChecksums = 'binary-v1'`, wt),
	}
	// verify upsert data is on ES
	s.testListResultForUpsertSearchAttributes(env, listRequest)

	// verify DescribeWorkflowExecution
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), descRequest)
	s.NoError(err)
	// Add one for BuildIds={unversioned}
	s.Len(descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields(), len(searchAttributes.GetIndexedFields())+1)
	for attrName, expectedPayload := range searchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[sadefs.MetadataType]
		s.True(typeSet)
		s.NotEmpty(attrType)
	}
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_PageToken(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-token-test"
	wt := "es-functional-list-workflow-token-test-type"
	tl := "es-functional-list-workflow-token-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	numOfWorkflows := testcore.DefaultPageSize - 1 // == 4
	pageSize := 3

	s.testListWorkflowHelper(env, numOfWorkflows, pageSize, request, id, wt)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_SearchAfter(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-searchAfter-test"
	wt := "es-functional-list-workflow-searchAfter-test-type"
	tl := "es-functional-list-workflow-searchAfter-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	numOfWorkflows := testcore.DefaultPageSize + 1 // == 6
	pageSize := 4

	s.testListWorkflowHelper(env, numOfWorkflows, pageSize, request, id, wt)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_OrQuery(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-or-query-test"
	wt := "es-functional-list-workflow-or-query-test-type"
	tl := "es-functional-list-workflow-or-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	// start 3 workflows
	key := "CustomIntField"
	attrValBytes := sadefs.MustEncodeValue(1, enumspb.INDEXED_VALUE_TYPE_INT)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			key: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr
	we1, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	request.RequestId = uuid.NewString()
	request.WorkflowId = id + "-2"
	attrValBytes = sadefs.MustEncodeValue(2, enumspb.INDEXED_VALUE_TYPE_INT)
	searchAttr.IndexedFields[key] = attrValBytes
	we2, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	request.RequestId = uuid.NewString()
	request.WorkflowId = id + "-3"
	attrValBytes = sadefs.MustEncodeValue(3, enumspb.INDEXED_VALUE_TYPE_INT)
	searchAttr.IndexedFields[key] = attrValBytes
	we3, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	// query 1 workflow with search attr
	query1 := fmt.Sprintf(`CustomIntField = %d`, 1)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     query1,
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			openExecution = resp.GetExecutions()[0]
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.False(openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[key]
	var searchVal int
	_ = payload.Decode(searchValBytes, &searchVal)
	s.Equal(1, searchVal)

	// query with or clause
	query2 := fmt.Sprintf(`CustomIntField = %d or CustomIntField = %d`, 1, 2)
	listRequest.Query = query2
	var openExecutions []*workflowpb.WorkflowExecutionInfo
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 2)
			openExecutions = resp.GetExecutions()
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.Len(openExecutions, 2)
	e1 := openExecutions[0]
	e2 := openExecutions[1]
	if e1.GetExecution().GetRunId() != we1.GetRunId() {
		// results are sorted by [CloseTime,RunID] desc, so find the correct mapping first
		e1, e2 = e2, e1
	}
	s.Equal(we1.GetRunId(), e1.GetExecution().GetRunId())
	s.Equal(we2.GetRunId(), e2.GetExecution().GetRunId())
	searchValBytes = e2.SearchAttributes.GetIndexedFields()[key]
	_ = payload.Decode(searchValBytes, &searchVal)
	s.Equal(2, searchVal)

	// query for open
	query3 := fmt.Sprintf(`(CustomIntField = %d or CustomIntField = %d) and ExecutionStatus = 'Running'`, 2, 3)
	listRequest.Query = query3
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 2)
			openExecutions = resp.GetExecutions()
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.Len(openExecutions, 2)
	e1 = openExecutions[0]
	e2 = openExecutions[1]
	s.Equal(we3.GetRunId(), e1.GetExecution().GetRunId())
	s.Equal(we2.GetRunId(), e2.GetExecution().GetRunId())
	searchValBytes = e1.SearchAttributes.GetIndexedFields()[key]
	err = payload.Decode(searchValBytes, &searchVal)
	s.NoError(err)
	s.Equal(3, searchVal)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_KeywordQuery(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-keyword-query-test"
	wt := "es-functional-list-workflow-keyword-query-test-type"
	tl := "es-functional-list-workflow-keyword-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	searchAttr := map[string]any{
		"CustomKeywordField": "justice for all",
	}
	encodedSearchAttr, err := searchattribute.Encode(searchAttr, nil)
	s.NoError(err)

	request.SearchAttributes = encodedSearchAttr
	we1, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	// Exact match Keyword (supported)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomKeywordField = "justice for all"`,
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			openExecution = resp.GetExecutions()[0]
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.False(openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	saPayload := openExecution.SearchAttributes.GetIndexedFields()["CustomKeywordField"]
	var saValue string
	err = payload.Decode(saPayload, &saValue)
	s.NoError(err)
	s.Equal("justice for all", saValue)

	// Partial match on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomKeywordField = "justice"`,
	}
	resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Empty(resp.GetExecutions())

	// Inordered match on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomKeywordField = "all for justice"`,
	}
	resp, err = env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Empty(resp.GetExecutions())

	// Prefix search
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomKeywordField STARTS_WITH "justice"`,
	}
	resp, err = env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)
	s.Equal(id, resp.Executions[0].GetExecution().GetWorkflowId())
	s.Equal(wt, resp.Executions[0].GetType().GetName())

	decodedSearchAttr, err := searchattribute.Decode(resp.Executions[0].GetSearchAttributes(), nil, false)
	s.NoError(err)
	s.Equal(searchAttr, decodedSearchAttr)

	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     fmt.Sprintf(`WorkflowId = %q AND CustomKeywordField NOT STARTS_WITH "justice"`, id),
	}
	resp, err = env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Empty(resp.GetExecutions())
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_StringQuery(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-string-query-test"
	wt := "es-functional-list-workflow-string-query-test-type"
	tl := "es-functional-list-workflow-string-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomTextField": sadefs.MustEncodeValue("nothing else matters", enumspb.INDEXED_VALUE_TYPE_TEXT),
		},
	}
	request.SearchAttributes = searchAttr
	we1, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	// Exact match String (supported)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomTextField = "nothing else matters"`,
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			openExecution = resp.GetExecutions()[0]
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.False(openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	saPayload := openExecution.SearchAttributes.GetIndexedFields()["CustomTextField"]
	var saValue string
	err = payload.Decode(saPayload, &saValue)
	s.NoError(err)
	s.Equal("nothing else matters", saValue)

	// Partial match on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomTextField = "nothing"`,
	}
	resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// Inordered match on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     `CustomTextField = "else nothing matters"`,
	}
	resp, err = env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)
}

// To test last page search trigger max window size error
func (s *AdvancedVisibilitySuite) TestListWorkflow_MaxWindowSize(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-list-workflow-max-window-size-test"
	wt := "es-functional-list-workflow-max-window-size-test-type"
	tl := "es-functional-list-workflow-max-window-size-test-taskqueue"
	startRequest := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	for i := range testcore.DefaultPageSize {
		startRequest.RequestId = uuid.NewString()
		startRequest.WorkflowId = id + strconv.Itoa(i)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), startRequest)
		s.NoError(err)
	}

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	var listResp *workflowservice.ListWorkflowExecutionsResponse
	var nextPageToken []byte

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     env.Namespace().String(),
		PageSize:      int32(testcore.DefaultPageSize),
		NextPageToken: nextPageToken,
		Query:         fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = "Running"`, wt),
	}
	// get first page
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), testcore.DefaultPageSize)
			listResp = resp
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.NotNil(listResp)
	s.NotEmpty(listResp.GetNextPageToken())

	// the last request
	listRequest.NextPageToken = listResp.GetNextPageToken()
	resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Empty(resp.GetExecutions())
	s.Nil(resp.GetNextPageToken())
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_OrderBy(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	if testcore.UseSQLVisibility() {
		s.T().Skip("This test is only for Elasticsearch")
	}

	id := "es-functional-list-workflow-order-by-test"
	wt := "es-functional-list-workflow-order-by-test-type"
	tl := "es-functional-list-workflow-order-by-test-taskqueue"

	initialTime := time.Now().UTC()
	for i := range testcore.DefaultPageSize + 1 { // start 6
		startRequest := s.createStartWorkflowExecutionRequest(env, id, wt, tl)
		startRequest.RequestId = uuid.NewString()
		startRequest.WorkflowId = id + strconv.Itoa(i)

		if i < testcore.DefaultPageSize-1 { // 4 workflows have search attributes.
			intVal := sadefs.MustEncodeValue(int64(i), enumspb.INDEXED_VALUE_TYPE_INT)
			doubleVal := sadefs.MustEncodeValue(float64(i), enumspb.INDEXED_VALUE_TYPE_DOUBLE)
			strVal := sadefs.MustEncodeValue(strconv.Itoa(i), enumspb.INDEXED_VALUE_TYPE_KEYWORD)
			timeVal := sadefs.MustEncodeValue(initialTime.Add(time.Duration(i)), enumspb.INDEXED_VALUE_TYPE_DATETIME)
			startRequest.SearchAttributes = &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomIntField":      intVal,
					"CustomDoubleField":   doubleVal,
					"CustomKeywordField":  strVal,
					"CustomDatetimeField": timeVal,
				},
			}
		} else {
			// To sort on CustomDatetimeField in single shard index on ES 7.10, there must be no null values in that field.
			// Otherwise, ES returns internal server error.
			timeVal := sadefs.MustEncodeValue(initialTime.Add(time.Duration(i)), enumspb.INDEXED_VALUE_TYPE_DATETIME)
			startRequest.SearchAttributes = &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomDatetimeField": timeVal,
				},
			}
		}

		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), startRequest)
		s.NoError(err)
	}

	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().CountWorkflowExecutions(
				s.Context(),
				&workflowservice.CountWorkflowExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     fmt.Sprintf(`WorkflowType = "%s"`, wt),
				},
			)
			s.NoError(err)
			s.EqualValues(6, resp.GetCount())
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	desc := "desc"
	asc := "asc"
	queryTemplate := `WorkflowType = "%s" order by %s %s`
	pageSize := int32(testcore.DefaultPageSize)

	// order by CloseTime asc
	query1 := fmt.Sprintf(queryTemplate, wt, sadefs.CloseTime, asc)
	var openExecutions []*workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  pageSize,
		Query:     query1,
	}
	resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), int(pageSize))
	openExecutions = resp.GetExecutions()
	for i := int32(1); i < pageSize; i++ {
		e1 := openExecutions[i-1]
		e2 := openExecutions[i]
		if e1.GetCloseTime() != nil && e2.GetCloseTime() != nil {
			s.NotEqual(time.Time{}, *e1.GetCloseTime())
			s.GreaterOrEqual(e2.GetCloseTime(), e1.GetCloseTime())
		}
	}

	// greatest effort to reduce duplicate code
	testHelper := func(query, searchAttrKey string, prevVal, currVal any) {
		listRequest.Query = query
		listRequest.NextPageToken = []byte{}
		resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
		s.NoError(err)
		openExecutions = resp.GetExecutions()
		dec := json.NewDecoder(bytes.NewReader(openExecutions[0].GetSearchAttributes().GetIndexedFields()[searchAttrKey].GetData()))
		dec.UseNumber()
		err = dec.Decode(&prevVal)
		s.NoError(err)
		for i := int32(1); i < pageSize; i++ {
			indexedFields := openExecutions[i].GetSearchAttributes().GetIndexedFields()
			searchAttrBytes, ok := indexedFields[searchAttrKey]
			if !ok { // last one doesn't have search attr
				s.Equal(pageSize-1, i)
				break
			}
			dec := json.NewDecoder(bytes.NewReader(searchAttrBytes.GetData()))
			dec.UseNumber()
			err = dec.Decode(&currVal)
			s.NoError(err)
			switch searchAttrKey {
			case "CustomIntField":
				val1, _ := prevVal.(json.Number).Int64()
				val2, _ := currVal.(json.Number).Int64()
				s.Greater(val1, val2)
			case "CustomDoubleField":
				val1, _ := prevVal.(json.Number).Float64()
				val2, _ := currVal.(json.Number).Float64()
				s.Greater(val1, val2)
			case "CustomKeywordField":
				s.Greater(prevVal.(string), currVal.(string))
			case "CustomDatetimeField":
				val1, _ := time.Parse(time.RFC3339Nano, prevVal.(string))
				val2, _ := time.Parse(time.RFC3339Nano, currVal.(string))
				s.Greater(val1, val2)
			}
			prevVal = currVal
		}
		listRequest.NextPageToken = resp.GetNextPageToken()
		resp, err = env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest) // last page
		s.NoError(err)
		s.Len(resp.GetExecutions(), 1)
	}

	// order by CustomIntField desc
	field := "CustomIntField"
	query := fmt.Sprintf(queryTemplate, wt, field, desc)
	var int1, int2 int
	testHelper(query, field, int1, int2)

	// order by CustomDoubleField desc
	field = "CustomDoubleField"
	query = fmt.Sprintf(queryTemplate, wt, field, desc)
	var double1, double2 float64
	testHelper(query, field, double1, double2)

	// order by CustomKeywordField desc
	field = "CustomKeywordField"
	query = fmt.Sprintf(queryTemplate, wt, field, desc)
	var s1, s2 string
	testHelper(query, field, s1, s2)

	// order by CustomDatetimeField desc
	field = "CustomDatetimeField"
	query = fmt.Sprintf(queryTemplate, wt, field, desc)
	var t1, t2 time.Time
	testHelper(query, field, t1, t2)
}

func (s *AdvancedVisibilitySuite) testListWorkflowHelper(
	env *testcore.TestEnv,
	numOfWorkflows, pageSize int,
	startRequest *workflowservice.StartWorkflowExecutionRequest,
	wid, wType string,
) {
	// this function assumes numOfWorkflows > pageSize
	s.Greater(numOfWorkflows, pageSize, "numOfWorkflows must be greater than pageSize")

	// start enough number of workflows
	for i := range numOfWorkflows {
		startRequest.RequestId = uuid.NewString()
		startRequest.WorkflowId = wid + strconv.Itoa(i)
		_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), startRequest)
		s.NoError(err)
	}

	s.Await(
		func(s *AdvancedVisibilitySuite) {
			countRequest := &workflowservice.CountWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wType),
			}
			countResponse, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), countRequest)
			s.NoError(err)
			s.Equal(int64(numOfWorkflows), countResponse.GetCount())
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	var nextPageToken []byte
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     env.Namespace().String(),
		PageSize:      int32(pageSize),
		NextPageToken: nextPageToken,
		Query:         fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wType),
	}

	// test first page
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			listResponse, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(listResponse.GetExecutions(), pageSize)
			nextPageToken = listResponse.GetNextPageToken()
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.NotEmpty(nextPageToken)

	// test last page
	listRequest.NextPageToken = nextPageToken
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			listResponse, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(listResponse.GetExecutions(), numOfWorkflows-pageSize)
			nextPageToken = listResponse.GetNextPageToken()
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	// nextPageToken might be not be empty, depends on store implementation
	if len(nextPageToken) > 0 {
		// test page after last page is empty (true last page)
		listRequest.NextPageToken = nextPageToken
		s.Await(
			func(s *AdvancedVisibilitySuite) {
				listResponse, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
				s.NoError(err)
				s.Empty(listResponse.GetExecutions())
				nextPageToken = listResponse.GetNextPageToken()
			},
			testcore.WaitForESToSettle,
			esPollInterval,
		)
		s.Empty(nextPageToken)
	}
}

func (s *AdvancedVisibilitySuite) testHelperForReadOnce(env *testcore.TestEnv, expectedRunID string, query string) *workflowpb.WorkflowExecutionInfo {
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  testcore.DefaultPageSize,
		Query:     query,
	}

	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			openExecution = resp.GetExecutions()[0]
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.NotNil(openExecution)
	s.Equal(expectedRunID, openExecution.GetExecution().GetRunId())
	s.False(openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	return openExecution
}

func (s *AdvancedVisibilitySuite) TestCountWorkflow(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-count-workflow-test"
	wt := "es-functional-count-workflow-test-type"
	tl := "es-functional-count-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	attrValBytes := sadefs.MustEncodeValue(testSearchAttributeVal, enumspb.INDEXED_VALUE_TYPE_TEXT)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, testSearchAttributeKey, testSearchAttributeVal)
	countRequest := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		Query:     query,
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), countRequest)
			s.NoError(err)
			s.Equal(int64(1), resp.GetCount())
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	query = fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, testSearchAttributeKey, "noMatch")
	countRequest.Query = query
	resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), countRequest)
	s.NoError(err)
	s.Equal(int64(0), resp.GetCount())
}

func (s *AdvancedVisibilitySuite) TestCountGroupByWorkflow(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-count-groupby-workflow-test"
	wt := "es-functional-count-groupby-workflow-test-type"
	tl := "es-functional-count-groupby-workflow-test-taskqueue"

	numWorkflows := 10
	numClosedWorkflows := 4
	for i := range numWorkflows {
		wfid := id + strconv.Itoa(i)
		request := s.createStartWorkflowExecutionRequest(env, wfid, wt, tl)
		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
		s.NoError(err)
		if i < numClosedWorkflows {
			_, err := env.FrontendClient().TerminateWorkflowExecution(
				s.Context(),
				&workflowservice.TerminateWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: wfid,
						RunId:      we.RunId,
					},
				},
			)
			s.NoError(err)
		}
	}

	query := fmt.Sprintf(`WorkflowType = %q GROUP BY ExecutionStatus`, wt)
	countRequest := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		Query:     query,
	}
	var countResp *workflowservice.CountWorkflowExecutionsResponse
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), countRequest)
			s.NoError(err)
			s.Equal(int64(numWorkflows), resp.GetCount())
			countResp = resp
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
	s.Len(countResp.Groups, 2)

	runningStatusPayload, _ := sadefs.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	terminatedStatusPayload, _ := sadefs.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED.String(),
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	s.ProtoEqual(
		&workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
			GroupValues: []*commonpb.Payload{runningStatusPayload},
			Count:       int64(numWorkflows - numClosedWorkflows),
		},
		countResp.Groups[0],
	)
	s.ProtoEqual(
		&workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
			GroupValues: []*commonpb.Payload{terminatedStatusPayload},
			Count:       int64(numClosedWorkflows),
		},
		countResp.Groups[1],
	)

	query = `GROUP BY WorkflowType`
	countRequest.Query = query
	_, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), countRequest)
	s.Error(err)
	s.Contains(strings.ToLower(err.Error()), "'group by' clause is only supported for")

	query = `GROUP BY ExecutionStatus, WorkflowType`
	countRequest.Query = query
	_, err = env.FrontendClient().CountWorkflowExecutions(s.Context(), countRequest)
	s.Error(err)
	s.Contains(strings.ToLower(err.Error()), "'group by' clause supports only a single field")
}

func (s *AdvancedVisibilitySuite) createStartWorkflowExecutionRequest(env *testcore.TestEnv, id, wt, tl string) *workflowservice.StartWorkflowExecutionRequest {
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	return request
}

func (s *AdvancedVisibilitySuite) TestUpsertWorkflowExecutionSearchAttributes(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-upsert-workflow-search-attributes-test"
	wt := "es-functional-upsert-workflow-search-attributes-test-type"
	tl := "es-functional-upsert-workflow-search-attributes-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	commandCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{
				UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{},
			},
		}

		// handle first upsert
		if commandCount == 0 {
			commandCount++
			attrValPayload := sadefs.MustEncodeValue(testSearchAttributeVal, enumspb.INDEXED_VALUE_TYPE_TEXT)
			upsertSearchAttr := &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					testSearchAttributeKey: attrValPayload,
				},
			}
			upsertCommand.GetUpsertWorkflowSearchAttributesCommandAttributes().SearchAttributes = upsertSearchAttr
			return []*commandpb.Command{upsertCommand}, nil
		}

		// handle second upsert, which update existing field and add new field
		if commandCount == 1 {
			commandCount++
			upsertCommand.GetUpsertWorkflowSearchAttributesCommandAttributes().SearchAttributes = s.createSearchAttributes()
			return []*commandpb.Command{upsertCommand}, nil
		}

		// handle third upsert, which update existing field to nil and empty list
		if commandCount == 2 {
			commandCount++
			nilPayload, _ := payload.Encode(nil)
			emptySlicePayload, _ := payload.Encode([]int{})
			upsertSearchAttr := &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomTextField": nilPayload,
					"CustomIntField":  emptySlicePayload,
				},
			}
			upsertCommand.GetUpsertWorkflowSearchAttributesCommandAttributes().SearchAttributes = upsertSearchAttr
			return []*commandpb.Command{upsertCommand}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// process 1st workflow task and assert workflow task is handled correctly.
	res, err := poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Len(newTask.WorkflowTask.History.Events, 4)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	// verify upsert data is on ES
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			execution := resp.GetExecutions()[0]
			s.NotEmpty(execution.GetSearchAttributes().GetIndexedFields())
			searchValBytes := execution.GetSearchAttributes().GetIndexedFields()[testSearchAttributeKey]
			var searchVal string
			err = payload.Decode(searchValBytes, &searchVal)
			s.NoError(err)
			s.Equal(testSearchAttributeVal, searchVal)
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	// process 2nd workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Len(newTask.WorkflowTask.History.Events, 4)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	// verify upsert data is on ES
	s.testListResultForUpsertSearchAttributes(env, listRequest)

	// process 3rd workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Len(newTask.WorkflowTask.History.Events, 4)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	// verify search attributes are unset
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			execution := resp.GetExecutions()[0]
			s.NotEmpty(execution.GetSearchAttributes().GetIndexedFields())
			s.NotContains(execution.GetSearchAttributes().GetIndexedFields(), "CustomTextField")
			s.NotContains(execution.GetSearchAttributes().GetIndexedFields(), "CustomIntField")
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	// verify query by unset search attribute
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running' and CustomTextField is null and CustomIntField is null`, wt),
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	// verify search attributes from DescribeWorkflowExecution
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), descRequest)
	s.NoError(err)
	expectedSearchAttributes, _ := searchattribute.Encode(
		map[string]any{
			"CustomDoubleField":    22.0878,
			sadefs.BinaryChecksums: []string{"binary-v1", "binary-v2"},
			sadefs.BuildIds:        []string{worker_versioning.UnversionedSearchAttribute},
		},
		nil,
	)
	s.Len(
		descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields(), len(expectedSearchAttributes.GetIndexedFields()),
	)
	for attrName, expectedPayload := range expectedSearchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[searchattribute.MetadataType]
		s.True(typeSet)
		s.NotEmpty(attrType)
	}

	// process close workflow task and assert search attributes is correct after workflow is closed
	res, err = poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.Nil(newTask.WorkflowTask)

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	// verify search attributes from DescribeWorkflowExecution
	descRequest = &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), descRequest)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
	s.Len(
		descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields(), len(expectedSearchAttributes.GetIndexedFields()),
	)
	for attrName, expectedPayload := range expectedSearchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[searchattribute.MetadataType]
		s.True(typeSet)
		s.NotEmpty(attrType)
	}
}

func (s *AdvancedVisibilitySuite) TestModifyWorkflowExecutionProperties(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-modify-workflow-properties-test"
	wt := "es-functional-modify-workflow-properties-test-type"
	tl := "es-functional-modify-workflow-properties-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	commandCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		modifyCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES,
			Attributes: &commandpb.Command_ModifyWorkflowPropertiesCommandAttributes{
				ModifyWorkflowPropertiesCommandAttributes: &commandpb.ModifyWorkflowPropertiesCommandAttributes{},
			},
		}

		// handle first upsert
		if commandCount == 0 {
			commandCount++
			attrValPayload1, _ := payload.Encode("test memo val 1")
			attrValPayload2, _ := payload.Encode("test memo val 2")
			memo := &commonpb.Memo{
				Fields: map[string]*commonpb.Payload{
					"test_memo_key_1": attrValPayload1,
					"test_memo_key_2": attrValPayload2,
				},
			}
			modifyCommand.GetModifyWorkflowPropertiesCommandAttributes().UpsertedMemo = memo
			return []*commandpb.Command{modifyCommand}, nil
		}

		// handle second upsert, which update existing fields and add new field
		if commandCount == 1 {
			commandCount++
			attrValPayload1, _ := payload.Encode("test memo val 1 new")
			attrValPayload2, _ := payload.Encode(nil)
			attrValPayload3, _ := payload.Encode("test memo val 3")
			memo := &commonpb.Memo{
				Fields: map[string]*commonpb.Payload{
					"test_memo_key_1": attrValPayload1,
					"test_memo_key_2": attrValPayload2,
					"test_memo_key_3": attrValPayload3,
				},
			}
			modifyCommand.GetModifyWorkflowPropertiesCommandAttributes().UpsertedMemo = memo
			return []*commandpb.Command{modifyCommand}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// process 1st workflow task and assert workflow task is handled correctly.
	res, err := poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Len(newTask.WorkflowTask.History.Events, 4)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	attrValPayload1, _ := payload.Encode("test memo val 1")
	attrValPayload2, _ := payload.Encode("test memo val 2")
	expectedMemo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"test_memo_key_1": attrValPayload1,
			"test_memo_key_2": attrValPayload2,
		},
	}

	// verify memo data is on ES
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			s.True(proto.Equal(expectedMemo, resp.Executions[0].Memo))
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	// process 2nd workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Len(newTask.WorkflowTask.History.Events, 4)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	attrValPayload1, _ = payload.Encode("test memo val 1 new")
	attrValPayload3, _ := payload.Encode("test memo val 3")
	expectedMemo = &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"test_memo_key_1": attrValPayload1,
			"test_memo_key_3": attrValPayload3,
		},
	}

	// verify memo data is on ES
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			s.True(proto.Equal(expectedMemo, resp.Executions[0].Memo))
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)

	// process close workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		testcore.WithPollSticky,
		testcore.WithRespondSticky,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.Nil(newTask.WorkflowTask)

	time.Sleep(testcore.WaitForESToSettle) //nolint:forbidigo

	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), descRequest)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
	s.ProtoEqual(expectedMemo, descResp.WorkflowExecutionInfo.Memo)
}

func (s *AdvancedVisibilitySuite) testListResultForUpsertSearchAttributes(env *testcore.TestEnv, listRequest *workflowservice.ListWorkflowExecutionsRequest) {
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), listRequest)
			s.NoError(err)
			s.Len(resp.GetExecutions(), 1)
			execution := resp.GetExecutions()[0]
			s.NotEmpty(execution.GetSearchAttributes().GetIndexedFields())
			fields := execution.GetSearchAttributes().GetIndexedFields()

			searchValBytes := fields[testSearchAttributeKey]
			var searchVal string
			err = payload.Decode(searchValBytes, &searchVal)
			s.NoError(err)
			s.Equal("another string", searchVal)

			searchValBytes2 := fields["CustomIntField"]
			var searchVal2 int
			err = payload.Decode(searchValBytes2, &searchVal2)
			s.NoError(err)
			s.Equal(123, searchVal2)

			doublePayload := fields["CustomDoubleField"]
			var doubleVal float64
			err = payload.Decode(doublePayload, &doubleVal)
			s.NoError(err)
			s.InDelta(22.0878, doubleVal, 1e-6)

			binaryChecksumsBytes := fields[sadefs.BinaryChecksums]
			var binaryChecksums []string
			err = payload.Decode(binaryChecksumsBytes, &binaryChecksums)
			s.NoError(err)
			s.Equal([]string{"binary-v1", "binary-v2"}, binaryChecksums)

			buildIdsBytes := fields[sadefs.BuildIds]
			var buildIds []string
			err = payload.Decode(buildIdsBytes, &buildIds)
			s.NoError(err)
			s.Equal([]string{worker_versioning.UnversionedSearchAttribute}, buildIds)
		},
		testcore.WaitForESToSettle,
		esPollInterval,
	)
}

func (s *AdvancedVisibilitySuite) createSearchAttributes() *commonpb.SearchAttributes {
	searchAttributes, err := searchattribute.Encode(map[string]any{
		"CustomTextField":      "another string",
		"CustomIntField":       123,
		"CustomDoubleField":    22.0878,
		sadefs.BinaryChecksums: []string{"binary-v1", "binary-v2"},
	}, nil)
	s.NoError(err)
	return searchAttributes
}

func (s *AdvancedVisibilitySuite) TestUpsertWorkflowExecution_InvalidKey(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-upsert-workflow-failed-test"
	wt := "es-functional-upsert-workflow-failed-test-type"
	tl := "es-functional-upsert-workflow-failed-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"INVALIDKEY": payload.EncodeBytes([]byte("1")),
					},
				},
			}}}
		return []*commandpb.Command{upsertCommand}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	historyEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	s.ErrorContains(err, fmt.Sprintf("BadSearchAttributes: Namespace %s has no mapping defined for search attribute INVALIDKEY", env.Namespace().String()))
	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed {"Cause":23,"Failure":{"Message":"BadSearchAttributes: Namespace %s has no mapping defined for search attribute INVALIDKEY"}}
  5 WorkflowTaskScheduled`, env.Namespace().String()), historyEvents)
}

func (s *AdvancedVisibilitySuite) TestChildWorkflow_ParentWorkflow(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	var (
		wfID        = testcore.RandomizeStr(s.T().Name())
		childWfID   = testcore.RandomizeStr(s.T().Name())
		childWfType = "child-wf-type-" + wfID
		wfType      = "wf-type-" + wfID
	)

	childWf := func(ctx workflow.Context) error {
		return nil
	}
	wf := func(ctx workflow.Context) error {
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID: childWfID,
		}
		return workflow.
			ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), childWfType).
			Get(ctx, nil)
	}

	env.SdkWorker().RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: wfType})
	env.SdkWorker().RegisterWorkflowWithOptions(childWf, workflow.RegisterOptions{Name: childWfType})

	startOptions := sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: env.WorkerTaskQueue(),
	}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), startOptions, wfType)
	s.NoError(err)
	s.NoError(run.Get(s.Context(), nil))

	// check main workflow doesn't have parent workflow and root is itself
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(
				s.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     fmt.Sprintf("WorkflowType = %q", wfType),
					PageSize:  testcore.DefaultPageSize,
				},
			)
			s.NoError(err)
			s.Len(resp.Executions, 1)
			wfInfo := resp.Executions[0]
			s.Nil(wfInfo.GetParentExecution())
			s.NotNil(wfInfo.GetRootExecution())
			s.Equal(wfID, wfInfo.RootExecution.GetWorkflowId())
			s.Equal(run.GetRunID(), wfInfo.RootExecution.GetRunId())
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	// check child workflow has parent workflow and root is the parent
	var childWfInfo *workflowpb.WorkflowExecutionInfo
	s.Await(
		func(s *AdvancedVisibilitySuite) {
			resp, err := env.FrontendClient().ListWorkflowExecutions(
				s.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     fmt.Sprintf("WorkflowType = %q", childWfType),
					PageSize:  testcore.DefaultPageSize,
				},
			)
			s.NoError(err)
			s.Len(resp.Executions, 1)
			childWfInfo = resp.Executions[0]
			s.NotNil(childWfInfo.GetParentExecution())
			s.Equal(wfID, childWfInfo.ParentExecution.GetWorkflowId())
			s.Equal(run.GetRunID(), childWfInfo.ParentExecution.GetRunId())
			s.NotNil(childWfInfo.GetRootExecution())
			s.Equal(wfID, childWfInfo.RootExecution.GetWorkflowId())
			s.Equal(run.GetRunID(), childWfInfo.RootExecution.GetRunId())
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *AdvancedVisibilitySuite) Test_LongWorkflowID(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	if env.GetTestClusterConfig().Persistence.StoreType == config.StoreTypeSQL {
		// TODO: remove this when workflow_id field size is increased from varchar(255) in SQL schema.
		return
	}

	id := strings.Repeat("a", 1000)
	wt := "es-functional-long-workflow-id-test-type"
	tl := "es-functional-long-workflow-id-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(env, id, wt, tl)

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(env, we.GetRunId(), query)
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnCompletion_UnversionedWorker(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := testcore.RandomizeStr(s.T().Name())
	workflowType := "functional-build-id"
	taskQueue := testcore.RandomizeStr(s.T().Name())

	request := s.createStartWorkflowExecutionRequest(env, id, workflowType, taskQueue)
	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	pollRequest := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: env.Namespace().String(), TaskQueue: request.TaskQueue, Identity: id}
	task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), pollRequest)
	s.NoError(err)
	s.NotEmpty(task.TaskToken)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          env.Namespace().String(),
		Identity:           id,
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: "1.0"},
		TaskToken:          task.TaskToken,
	})
	s.NoError(err)

	buildIDs := s.getBuildIds(env, task.WorkflowExecution)
	s.Equal([]string{
		worker_versioning.UnversionedSearchAttribute,
		worker_versioning.UnversionedBuildIdSearchAttribute("1.0"),
	}, buildIDs)

	_, err = env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{Namespace: env.Namespace().String(), WorkflowExecution: task.WorkflowExecution, SignalName: "continue"})
	s.NoError(err)

	task, err = env.FrontendClient().PollWorkflowTaskQueue(s.Context(), pollRequest)
	s.NoError(err)
	s.NotEmpty(task.TaskToken)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          env.Namespace().String(),
		Identity:           id,
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: "1.1"},
		TaskToken:          task.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType: task.WorkflowType,
					TaskQueue:    request.TaskQueue,
				},
			},
		}},
	})
	s.NoError(err)

	buildIDs = s.getBuildIds(env, task.WorkflowExecution)
	s.Equal([]string{
		worker_versioning.UnversionedSearchAttribute,
		worker_versioning.UnversionedBuildIdSearchAttribute("1.0"),
		worker_versioning.UnversionedBuildIdSearchAttribute("1.1"),
	}, buildIDs)

	task, err = env.FrontendClient().PollWorkflowTaskQueue(s.Context(), pollRequest)
	s.NoError(err)
	s.NotEmpty(task.TaskToken)

	buildIDs = s.getBuildIds(env, task.WorkflowExecution)
	s.Equal([]string{}, buildIDs)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          env.Namespace().String(),
		Identity:           id,
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: "1.2"},
		TaskToken:          task.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)

	buildIDs = s.getBuildIds(env, task.WorkflowExecution)
	s.Equal([]string{worker_versioning.UnversionedSearchAttribute, worker_versioning.UnversionedBuildIdSearchAttribute("1.2")}, buildIDs)

	for minor := 1; minor <= 2; minor++ {
		s.Await(func(s *AdvancedVisibilitySuite) {
			response, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("BuildIds = '%s'", worker_versioning.UnversionedBuildIdSearchAttribute(fmt.Sprintf("1.%d", minor))),
				PageSize:  testcore.DefaultPageSize,
			})
			s.NoError(err)
			s.NotEmpty(response.Executions)
			s.Equal(id, response.Executions[0].Execution.WorkflowId)
		}, 10*time.Second, 100*time.Millisecond)
	}
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnCompletion_VersionedWorker(enableUnifiedQueryConverter bool) {
	// Use only one partition to avoid having to wait for user data propagation later
	env := s.newTestEnv(enableUnifiedQueryConverter,
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	id := testcore.RandomizeStr(s.T().Name())
	childId1 := "child1-" + id
	childId2 := "child2-" + id
	workflowType := "functional-build-id"
	taskQueue := testcore.RandomizeStr(s.T().Name())
	buildIdv1 := s.T().Name() + "-v1"
	buildIdv11 := s.T().Name() + "-v11"

	startedCh := make(chan string, 1)

	wf := func(ctx workflow.Context) error {
		info := workflow.GetInfo(ctx)
		if info.ContinuedExecutionRunID == "" {
			if !workflow.IsReplaying(ctx) {
				startedCh <- info.WorkflowExecution.RunID
			}
			workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)

			// Start compatible child
			c1Ctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: childId1, TaskQueue: taskQueue, VersioningIntent: temporal.VersioningIntentCompatible})
			if err := workflow.ExecuteChildWorkflow(c1Ctx, "doesnt-exist").GetChildWorkflowExecution().Get(ctx, nil); err != nil {
				return err
			}
			// Start default child
			c2Ctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: childId2, TaskQueue: taskQueue, VersioningIntent: temporal.VersioningIntentDefault})
			if err := workflow.ExecuteChildWorkflow(c2Ctx, "doesnt-exist").GetChildWorkflowExecution().Get(ctx, nil); err != nil {
				return err
			}
			// First continue-as-new to compatible set
			return workflow.NewContinueAsNewError(ctx, workflowType, 1)
		}
		if !workflow.IsReplaying(ctx) {
			startedCh <- info.WorkflowExecution.RunID
		}
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		useDefault := workflow.WithWorkflowVersioningIntent(ctx, temporal.VersioningIntentDefault)
		// Finally continue-as-new to latest
		return workflow.NewContinueAsNewError(useDefault, "doesnt-exist")
	}

	// Declare v1
	_, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildIdv1,
		},
	})
	s.NoError(err)

	// Start first worker
	w1 := worker.New(env.SdkClient(), taskQueue, worker.Options{
		BuildID:                      buildIdv1,
		UseBuildIDForVersioning:      true,
		StickyScheduleToStartTimeout: time.Second,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w1.Start())

	// Start the workflow and wait for first WFT
	startOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: taskQueue,
	}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), startOptions, workflowType)
	s.NoError(err)

	<-startedCh
	w1.Stop()

	// Verify first WFT was processed by our v1 worker
	s.Await(func(s *AdvancedVisibilitySuite) {
		buildIDs := s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: id})
		s.NotEmpty(buildIDs)
		s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(buildIdv1)}, buildIDs)
	}, time.Second*15, time.Millisecond*100)

	// Update sets with v1.1
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				ExistingCompatibleBuildId: buildIdv1,
				NewBuildId:                buildIdv11,
			},
		},
	})
	s.NoError(err)

	// Start v1.1 worker
	w11 := worker.New(env.SdkClient(), taskQueue, worker.Options{
		BuildID:                 buildIdv11,
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w11.Start())

	defer w11.Stop()

	// Resume workflow execution and wait for first task after CAN
	err = env.SdkClient().SignalWorkflow(s.Context(), id, "", "continue", nil)
	s.NoError(err)

	err = run.GetWithOptions(s.Context(), nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var canError *workflow.ContinueAsNewError
	s.ErrorAs(err, &canError)

	secondRunId := <-startedCh

	// Verify both workers appear in the search attribute for first run in chain
	buildIDs := s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: id, RunId: run.GetRunID()})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(buildIdv1), worker_versioning.VersionedBuildIdSearchAttribute(buildIdv11)}, buildIDs)

	// Check search attribute is propagated after first continue as new
	buildIDs = s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: id})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(buildIdv11)}, buildIDs)

	// Resume and wait for the workflow CAN for the last time
	err = env.SdkClient().SignalWorkflow(s.Context(), id, "", "continue", nil)
	s.NoError(err)

	run = env.SdkClient().GetWorkflow(s.Context(), id, secondRunId)
	err = run.GetWithOptions(s.Context(), nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	s.ErrorAs(err, &canError)

	// Check search attribute is not propagated after second continue as new
	buildIDs = s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: id})
	s.Equal([]string{}, buildIDs)

	// Check search attribute is propagated to first child
	buildIDs = s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: childId1})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(buildIdv11)}, buildIDs)

	// Check search attribute is not propagated to second child
	buildIDs = s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: childId2})
	s.Equal([]string{}, buildIDs)

	// We should have 3 runs with the v1.1 search attribute: First and second run in chain, and single child
	s.Await(func(s *AdvancedVisibilitySuite) {
		response, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("BuildIds = %q", worker_versioning.VersionedBuildIdSearchAttribute(buildIdv11)),
			PageSize:  testcore.DefaultPageSize,
		})
		s.NoError(err)
		s.Len(response.Executions, 3)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnReset(enableUnifiedQueryConverter bool) {
	// Use only one partition to avoid having to wait for user data propagation later
	env := s.newTestEnv(enableUnifiedQueryConverter,
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	id := testcore.RandomizeStr(s.T().Name())
	workflowType := "functional-build-id"
	taskQueue := testcore.RandomizeStr(s.T().Name())
	buildIdv1 := s.T().Name() + "-v1"

	startedCh := make(chan struct{})
	wf := func(ctx workflow.Context) error {
		// Continue-as-new once
		if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
			return workflow.NewContinueAsNewError(ctx, workflowType)
		}
		if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
			return err
		}
		startedCh <- struct{}{}
		return nil
	}

	// Declare v1
	_, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildIdv1,
		},
	})
	s.NoError(err)

	// Start a worker
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{
		BuildID:                      buildIdv1,
		UseBuildIDForVersioning:      true,
		StickyScheduleToStartTimeout: time.Second,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// Start the workflow and wait for CAN
	startOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: taskQueue,
	}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), startOptions, workflowType)
	s.NoError(err)

	err = run.GetWithOptions(s.Context(), nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var canError *workflow.ContinueAsNewError
	s.ErrorAs(err, &canError)

	// Confirm first WFT is complete before resetting
	<-startedCh

	resetResult, err := env.SdkClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id},
		WorkflowTaskFinishEventId: 3,
	})
	s.NoError(err)
	buildIDs := s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: id, RunId: resetResult.RunId})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(buildIdv1)}, buildIDs)

	s.Await(func(s *AdvancedVisibilitySuite) {
		response, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("BuildIds = %q AND RunId = %q", worker_versioning.VersionedBuildIdSearchAttribute(buildIdv1), resetResult.RunId),
			PageSize:  testcore.DefaultPageSize,
		})
		s.NoError(err)
		s.Len(response.Executions, 1)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnRetry(enableUnifiedQueryConverter bool) {
	// Use only one partition to avoid having to wait for user data propagation later
	env := s.newTestEnv(enableUnifiedQueryConverter,
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	id := testcore.RandomizeStr(s.T().Name())
	workflowType := "functional-build-id"
	taskQueue := testcore.RandomizeStr(s.T().Name())
	buildIdv1 := s.T().Name() + "-v1"

	wf := func(ctx workflow.Context) error {
		return fmt.Errorf("fail")
	}

	// Declare v1
	_, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildIdv1,
		},
	})
	s.NoError(err)

	// Start a worker
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{
		BuildID:                      buildIdv1,
		UseBuildIDForVersioning:      true,
		StickyScheduleToStartTimeout: time.Second,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// Start the workflow and wait for CAN
	startOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: taskQueue,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Millisecond,
			MaximumAttempts: 2,
		},
	}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), startOptions, workflowType)
	s.NoError(err)
	s.Error(run.Get(s.Context(), nil))

	buildIDs := s.getBuildIds(env, &commonpb.WorkflowExecution{WorkflowId: id})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(buildIdv1)}, buildIDs)

	s.Await(func(s *AdvancedVisibilitySuite) {
		response, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("BuildIds = %q", worker_versioning.VersionedBuildIdSearchAttribute(buildIdv1)),
			PageSize:  testcore.DefaultPageSize,
		})
		s.NoError(err)
		// Both runs should be associated with this build ID
		s.Len(response.Executions, 2)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_ByBuildId(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	tq1 := s.T().Name()
	tq2 := s.T().Name() + "-2"
	tq3 := s.T().Name() + "-3"
	v0 := s.T().Name() + "-v0"
	v01 := s.T().Name() + "-v0.1"
	buildIdv1 := s.T().Name() + "-v1"
	var err error

	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq1,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.NoError(err)
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq1,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				ExistingCompatibleBuildId: v0,
				NewBuildId:                v01,
			},
		},
	})
	s.NoError(err)
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq2,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.NoError(err)

	// Map v0 to a third queue to test limit enforcement
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq3,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.NoError(err)

	var reachabilityResponse *workflowservice.GetWorkerTaskReachabilityResponse

	reachabilityResponse, err = env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace:    env.Namespace().String(),
		BuildIds:     []string{v0},
		Reachability: enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS,
	})
	s.NoError(err)
	s.Equal([]*taskqueuepb.BuildIdReachability{{
		BuildId: v0,
		TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{
			{TaskQueue: tq1, Reachability: []enumspb.TaskReachability(nil)},
			{TaskQueue: tq2, Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_NEW_WORKFLOWS}},
			{TaskQueue: tq3, Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_UNSPECIFIED}},
		},
	}}, reachabilityResponse.BuildIdReachability)

	// Start a workflow on tq1 and verify it affects the reachability of v0.1
	_, err = env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   testcore.RandomizeStr(s.T().Name()),
		WorkflowType: &commonpb.WorkflowType{Name: "dont-care"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: tq1, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)

	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS)

	// Complete the workflow and verify it affects reachability of v0.1
	task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:                 env.Namespace().String(),
		TaskQueue:                 &taskqueuepb.TaskQueue{Name: tq1, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{BuildId: v01, UseVersioning: true},
	})
	s.NoError(err)
	s.NotEmpty(task.GetTaskToken())
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          env.Namespace().String(),
		TaskToken:          task.TaskToken,
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: v01, UseVersioning: true},
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}},
	})
	s.NoError(err)

	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	// Make v1 default for queue 1
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq1,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildIdv1,
		},
	})
	s.NoError(err)

	// Verify new workflows are considered reachable by v01 which is no longer queue default within the configured
	// duration
	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	env.OverrideDynamicConfig(dynamicconfig.ReachabilityQuerySetDurationSinceDefault, time.Microsecond)
	// Verify new workflows aren't reachable
	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq1, v01, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_ByBuildId_NotInNamespace(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	buildId := s.T().Name() + "v0"

	reachabilityResponse, err := env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace:    env.Namespace().String(),
		BuildIds:     []string{buildId},
		Reachability: enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS,
	})
	s.NoError(err)
	protorequire.ProtoSliceEqual(s.T(), []*taskqueuepb.BuildIdReachability{{
		BuildId:               buildId,
		TaskQueueReachability: []*taskqueuepb.TaskQueueReachability(nil),
	}}, reachabilityResponse.BuildIdReachability)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_ByBuildId_NotInTaskQueue(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	tq := s.T().Name()
	v0 := s.T().Name() + "v0"
	v01 := s.T().Name() + "v0.1"

	checkReachability := func() {
		reachabilityResponse, err := env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
			Namespace:  env.Namespace().String(),
			BuildIds:   []string{v01},
			TaskQueues: []string{tq},
		})
		s.NoError(err)
		protorequire.ProtoSliceEqual(s.T(), []*taskqueuepb.BuildIdReachability{{
			BuildId:               v01,
			TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{{TaskQueue: tq, Reachability: []enumspb.TaskReachability(nil)}},
		}}, reachabilityResponse.BuildIdReachability)
	}

	// Check once with an unversioned task queue
	checkReachability()

	// Same but with a versioned task queue
	_, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.NoError(err)
	checkReachability()
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_EmptyBuildIds(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)

	_, err := env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: env.Namespace().String(),
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_TooManyBuildIds(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)

	_, err := env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: env.Namespace().String(),
		BuildIds:  []string{"", "v1"},
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_Unversioned_InNamespace(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)

	_, err := env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: env.Namespace().String(),
		BuildIds:  []string{""},
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_Unversioned_InTaskQueue(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	tq := s.T().Name()

	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   testcore.RandomizeStr(s.T().Name()),
		WorkflowType: &commonpb.WorkflowType{Name: "dont-care"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)

	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS)

	task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotEmpty(task.GetTaskToken())
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}},
	})
	s.NoError(err)

	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	// Make the task queue versioned and rerun our assertion
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: s.T().Name() + "-v0",
		},
	})
	s.NoError(err)

	// Verify new workflows are considered reachable by the unversioned worker immediately after making the queue versioned
	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	env.OverrideDynamicConfig(dynamicconfig.ReachabilityQuerySetDurationSinceDefault, time.Microsecond)

	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(env, tq, "", enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)
}

func (s *AdvancedVisibilitySuite) TestBuildIdScavenger_DeletesUnusedBuildId(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter, testcore.WithWorkerService("build id scavenger workflow"))
	tq := s.T().Name()
	buildIdv0 := s.T().Name() + "-v0"
	buildIdv1 := s.T().Name() + "-v1"
	var err error

	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildIdv0,
		},
	})
	s.NoError(err)
	_, err = env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildIdv1,
		},
	})
	s.NoError(err)

	// client for the system namespace
	sysSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  env.FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	defer sysSDKClient.Close()

	run, err := sysSDKClient.ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        s.T().Name() + "-scavenger",
		TaskQueue: build_ids.BuildIdScavengerTaskQueueName,
	}, build_ids.BuildIdScavangerWorkflowName, build_ids.BuildIdScavangerInput{
		IgnoreRetentionTime: true,
	})
	s.NoError(err)
	err = run.Get(s.Context(), nil)
	s.NoError(err)

	//nolint:staticcheck // SA1019 legacy test.
	compatibility, err := env.SdkClient().GetWorkerBuildIdCompatibility(s.Context(), &sdkclient.GetWorkerBuildIdCompatibilityOptions{
		TaskQueue: tq,
	})
	s.NoError(err)
	s.Len(compatibility.Sets, 1)
	s.Equal([]string{buildIdv1}, compatibility.Sets[0].BuildIDs)
	// Make sure the build ID was removed from the build ID->task queue mapping
	res, err := env.SdkClient().WorkflowService().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: env.Namespace().String(),
		BuildIds:  []string{buildIdv0},
	})
	s.NoError(err)
	s.Empty(res.BuildIdReachability[0].TaskQueueReachability)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_ExternalPayloadSearchAttributes(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)
	id := "es-functional-external-payload-test"
	wt := "es-functional-external-payload-test-type"
	tl := "es-functional-external-payload-test-taskqueue"

	// Create workflow input with external payload
	externalPayloadSize := int64(1024)
	workflowInput := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{
			Data: []byte("workflow input"),
			ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
				{SizeBytes: externalPayloadSize},
			},
		}},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               workflowInput,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "test-identity",
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	// Complete the workflow using the new taskpoller API
	tv := testvars.New(s.T()).WithTaskQueue(tl)
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("Done"),
						},
					},
				}},
			}, nil
		},
	)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s" AND %s = 1`, id, sadefs.TemporalExternalPayloadCount)
	s.testHelperForReadOnce(env, we.GetRunId(), query)

	query = fmt.Sprintf(`WorkflowId = "%s" AND %s = %d`, id, sadefs.TemporalExternalPayloadSizeBytes, externalPayloadSize)
	s.testHelperForReadOnce(env, we.GetRunId(), query)
}

func (s *AdvancedVisibilitySuite) TestScheduleListingWithSearchAttributes(enableUnifiedQueryConverter bool) {
	env := s.newTestEnv(enableUnifiedQueryConverter)

	// Test 1: List schedule with "scheduleId" query
	scheduleID := "test-schedule-" + uuid.NewString()
	workflowType := "test-workflow-type"
	workflowID := "test-schedule-" + uuid.NewString()

	schedule := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		RequestId:  uuid.NewString(),
		ScheduleId: scheduleID,
		Schedule: &schedulepb.Schedule{
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   workflowID,
						WorkflowType: &commonpb.WorkflowType{Name: workflowType},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: "default"},
					},
				},
			},
			Policies: &schedulepb.SchedulePolicies{},
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(time.Hour)},
				},
			},
		},
	}

	_, err := env.FrontendClient().CreateSchedule(s.Context(), schedule)
	s.NoError(err)

	listRequest := &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 1,
		Query:           fmt.Sprintf(`%s = "%s"`, sadefs.ScheduleID, scheduleID),
	}

	s.Await(func(s *AdvancedVisibilitySuite) {
		listResponse, err := env.FrontendClient().ListSchedules(s.Context(), listRequest)
		s.NoError(err)
		s.Len(listResponse.Schedules, 1)
		s.Equal(scheduleID, listResponse.Schedules[0].ScheduleId)
	}, 30*time.Second, 1*time.Second)

	listRequest.Query = fmt.Sprintf(`%s IN ("%s", "foo", "bar")`, sadefs.ScheduleID, scheduleID)
	listResponse, err := env.FrontendClient().ListSchedules(s.Context(), listRequest)
	s.NoError(err)
	s.Len(listResponse.Schedules, 1)
	s.Equal(listResponse.Schedules[0].ScheduleId, scheduleID)

	// Test 2: List schedule with custom "scheduleId" search attribute
	s.addCustomKeywordSearchAttribute(env, sadefs.ScheduleID)

	// Create the schedule with the new search attribute and verify it can be listed
	customScheduleID := "test-schedule-" + uuid.NewString()
	customSearchAttrValue := "testScheduleId"

	schedule.RequestId = uuid.NewString()
	schedule.ScheduleId = customScheduleID
	schedule.SearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			sadefs.ScheduleID: sadefs.MustEncodeValue(customSearchAttrValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		},
	}

	_, err = env.FrontendClient().CreateSchedule(s.Context(), schedule)
	s.NoError(err)

	listRequest.Query = fmt.Sprintf(`%s = "%s"`, sadefs.ScheduleID, customSearchAttrValue)
	s.Await(func(s *AdvancedVisibilitySuite) {
		listResponse, err := env.FrontendClient().ListSchedules(s.Context(), listRequest)
		s.NoError(err)
		s.Len(listResponse.Schedules, 1)
		s.Equal(customScheduleID, listResponse.Schedules[0].ScheduleId)
	}, 30*time.Second, 1*time.Second)

	listRequest.Query = fmt.Sprintf(`%s IN ("%s", "foo", "bar")`, sadefs.ScheduleID, customSearchAttrValue)
	listResponse, err = env.FrontendClient().ListSchedules(s.Context(), listRequest)
	s.NoError(err)
	s.Len(listResponse.Schedules, 1)
	s.Equal(listResponse.Schedules[0].ScheduleId, customScheduleID)
}

func (s *AdvancedVisibilitySuite) checkReachability(env *testcore.TestEnv, taskQueue, buildID string, expectedReachability ...enumspb.TaskReachability) {
	s.Await(func(s *AdvancedVisibilitySuite) {
		reachabilityResponse, err := env.FrontendClient().GetWorkerTaskReachability(s.Context(), &workflowservice.GetWorkerTaskReachabilityRequest{
			Namespace:    env.Namespace().String(),
			BuildIds:     []string{buildID},
			TaskQueues:   []string{taskQueue},
			Reachability: expectedReachability[len(expectedReachability)-1],
		})
		s.NoError(err)
		s.Len(reachabilityResponse.BuildIdReachability[0].TaskQueueReachability[0].Reachability, len(expectedReachability))
		actualReachability := reachabilityResponse.BuildIdReachability[0].TaskQueueReachability[0].Reachability
		for i, expected := range expectedReachability {
			s.Equal(expected, actualReachability[i])
		}
		s.Equal(
			[]*taskqueuepb.BuildIdReachability{{
				BuildId: buildID,
				TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{
					{TaskQueue: taskQueue, Reachability: expectedReachability},
				},
			}}, reachabilityResponse.BuildIdReachability)
	}, 15*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) getBuildIds(env *testcore.TestEnv, execution *commonpb.WorkflowExecution) []string {
	description, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
	})
	s.NoError(err)
	attr, found := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[sadefs.BuildIds]
	if !found {
		return []string{}
	}
	var buildIDs []string
	err = payload.Decode(attr, &buildIDs)
	s.NoError(err)
	return buildIDs
}

func (s *AdvancedVisibilitySuite) updateMaxResultWindow(env *testcore.TestEnv) {
	esConfig := env.GetTestClusterConfig().ESConfig

	esClient, err := esclient.NewFunctionalTestsClient(esConfig, env.Logger)
	s.NoError(err)

	acknowledged, err := esClient.IndexPutSettings(
		s.Context(),
		esConfig.GetVisibilityIndex(),
		fmt.Sprintf(`{"max_result_window" : %d}`, testcore.DefaultPageSize))
	s.NoError(err)
	s.True(acknowledged)

	s.Awaitf(
		func(s *AdvancedVisibilitySuite) {
			settings, err := esClient.IndexGetSettings(s.Context(), esConfig.GetVisibilityIndex())
			s.NoError(err)
			indexSettings, ok := settings[esConfig.GetVisibilityIndex()].Settings["index"].(map[string]any)
			s.True(ok)
			maxResultWindow, ok := indexSettings["max_result_window"].(string)
			s.True(ok)
			s.Equal(strconv.Itoa(testcore.DefaultPageSize), maxResultWindow)
		},
		testcore.WaitForESToSettle,
		esPollInterval,
		"ES max result window size hasn't reach target size within %v",
		testcore.WaitForESToSettle,
	)
}

func (s *AdvancedVisibilitySuite) addCustomKeywordSearchAttribute(env *testcore.TestEnv, attrName string) {
	// Add new search attribute
	_, err := env.OperatorClient().AddSearchAttributes(s.Context(), &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			attrName: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)

	// Wait for search attribute to be available
	s.Await(func(s *AdvancedVisibilitySuite) {
		descResp, err := env.OperatorClient().ListSearchAttributes(s.Context(), &operatorservice.ListSearchAttributesRequest{
			Namespace: env.Namespace().String(),
		})
		s.NoError(err)

		_, exists := descResp.CustomAttributes[attrName]
		s.True(exists)
	}, 30*time.Second, 1*time.Second)
}
