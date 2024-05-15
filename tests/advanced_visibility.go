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

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/historyrequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/scanner/build_ids"
)

const (
	numOfRetry        = 50
	waitTimeInMs      = 400
	waitForESToSettle = 4 * time.Second // wait es shards for some time ensure data consistent
)

type AdvancedVisibilitySuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire
	FunctionalTestBase
	isElasticsearchEnabled bool

	testSearchAttributeKey string
	testSearchAttributeVal string
	sdkClient              sdkclient.Client
	// client for the system namespace
	sysSDKClient sdkclient.Client
}

// This cluster use customized threshold for history config
func (s *AdvancedVisibilitySuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.VisibilityDisableOrderByClause.Key():             false,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true,
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():     true,
		dynamicconfig.ReachabilityTaskQueueScanLimit.Key():             2,
		dynamicconfig.ReachabilityQueryBuildIdLimit.Key():              1,
		dynamicconfig.BuildIdScavengerEnabled.Key():                    true,
		// Allow the scavenger to remove any build ID regardless of when it was last default for a set.
		dynamicconfig.RemovableBuildIdDurationSinceDefault.Key(): time.Microsecond,
	}

	if UsingSQLAdvancedVisibility() {
		s.setupSuite("testdata/cluster.yaml")
		s.Logger.Info(fmt.Sprintf("Running advanced visibility test with %s/%s persistence", TestFlags.PersistenceType, TestFlags.PersistenceDriver))
		s.isElasticsearchEnabled = false
	} else {
		s.setupSuite("testdata/es_cluster.yaml")
		s.Logger.Info("Running advanced visibility test with Elasticsearch persistence")
		s.isElasticsearchEnabled = true
		// To ensure that Elasticsearch won't return more than defaultPageSize documents,
		// but returns error if page size on request is greater than defaultPageSize.
		// Probably can be removed and replaced with assert on items count in response.
		s.updateMaxResultWindow()
	}

	clientAddr := "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		clientAddr = TestFlags.FrontendAddr
	}
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  clientAddr,
		Namespace: s.namespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
	sysSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  clientAddr,
		Namespace: primitives.SystemLocalNamespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sysSDKClient = sysSDKClient
}

func (s *AdvancedVisibilitySuite) TearDownSuite() {
	s.sdkClient.Close()
	s.tearDownSuite()
}

func (s *AdvancedVisibilitySuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())
	s.testSearchAttributeKey = "CustomTextField"
	s.testSearchAttributeVal = "test value"
}

func (s *AdvancedVisibilitySuite) TestListOpenWorkflow() {
	id := "es-functional-start-workflow-test"
	wt := "es-functional-start-workflow-test-type"
	tl := "es-functional-start-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrPayload, _ := payload.Encode(s.testSearchAttributeVal)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: attrPayload,
		},
	}
	request.SearchAttributes = searchAttr

	startTime := time.Now().UTC()
	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = timestamppb.New(startTime)
	var openExecution *workflowpb.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		startFilter.LatestTime = timestamppb.New(time.Now().UTC())
		resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: defaultPageSize,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			s.Nil(resp.NextPageToken)
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(we.GetRunId(), openExecution.GetExecution().GetRunId())

	s.Equal(1, len(openExecution.GetSearchAttributes().GetIndexedFields()))
	attrPayloadFromResponse, attrExist := openExecution.GetSearchAttributes().GetIndexedFields()[s.testSearchAttributeKey]
	s.True(attrExist)
	s.Equal(attrPayload.GetData(), attrPayloadFromResponse.GetData())
	attrType, typeSet := attrPayloadFromResponse.GetMetadata()[searchattribute.MetadataType]
	s.True(typeSet)
	s.True(len(attrType) > 0)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow() {
	id := "es-functional-list-workflow-test"
	wt := "es-functional-list-workflow-test-type"
	tl := "es-functional-list-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_ExecutionTime() {
	id := "es-functional-list-workflow-execution-time-test"
	wt := "es-functional-list-workflow-execution-time-test-type"
	tl := "es-functional-list-workflow-execution-time-test-taskqueue"

	now := time.Now().UTC()
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	// Start workflow with ExecutionTime equal to StartTime
	weNonCron, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	cronID := id + "-cron"
	request.CronSchedule = "@every 1m"
	request.WorkflowId = cronID

	// Start workflow with ExecutionTime equal to StartTime + 1 minute (cron delay)
	weCron, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	//       <<1s    <<1s                  1m
	// ----+-----+------------+----------------------------+--------------
	//    now  nonCronStart  cronStart                  cronExecutionTime
	//         =nonCronExecutionTime

	expectedNonCronMaxExecutionTime := now.Add(1 * time.Second)                   // 1 second for time skew
	expectedCronMaxExecutionTime := now.Add(1 * time.Minute).Add(1 * time.Second) // 1 second for time skew

	// WorkflowId filter is to filter workflows from other tests.
	nonCronQueryNanos := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < %d`, id, cronID, expectedNonCronMaxExecutionTime.UnixNano())
	s.testHelperForReadOnce(weNonCron.GetRunId(), nonCronQueryNanos, false)

	cronQueryNanos := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < %d AND ExecutionTime > %d`, id, cronID, expectedCronMaxExecutionTime.UnixNano(), expectedNonCronMaxExecutionTime.UnixNano())
	s.testHelperForReadOnce(weCron.GetRunId(), cronQueryNanos, false)

	nonCronQuery := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < "%s"`, id, cronID, expectedNonCronMaxExecutionTime.Format(time.RFC3339Nano))
	s.testHelperForReadOnce(weNonCron.GetRunId(), nonCronQuery, false)

	cronQuery := fmt.Sprintf(`(WorkflowId = "%s" or WorkflowId = "%s") AND ExecutionTime < "%s" AND ExecutionTime > "%s"`, id, cronID, expectedCronMaxExecutionTime.Format(time.RFC3339Nano), expectedNonCronMaxExecutionTime.Format(time.RFC3339Nano))
	s.testHelperForReadOnce(weCron.GetRunId(), cronQuery, false)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_SearchAttribute() {
	id := "es-functional-list-workflow-by-search-attr-test"
	wt := "es-functional-list-workflow-by-search-attr-test-type"
	tl := "es-functional-list-workflow-by-search-attr-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := payload.Encode(s.testSearchAttributeVal)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	query := fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	s.testHelperForReadOnce(we.GetRunId(), query, false)

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
	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            "worker1",
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}
	res, err := poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)

	time.Sleep(waitForESToSettle)

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running' and BinaryChecksums = 'binary-v1'`, wt),
	}
	// verify upsert data is on ES
	s.testListResultForUpsertSearchAttributes(listRequest)

	// verify DescribeWorkflowExecution
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	// Add one for BuildIds={unversioned}
	s.Equal(len(searchAttributes.GetIndexedFields())+1, len(descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()))
	for attrName, expectedPayload := range searchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[searchattribute.MetadataType]
		s.True(typeSet)
		s.True(len(attrType) > 0)
	}
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_PageToken() {
	id := "es-functional-list-workflow-token-test"
	wt := "es-functional-list-workflow-token-test-type"
	tl := "es-functional-list-workflow-token-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	numOfWorkflows := defaultPageSize - 1 // == 4
	pageSize := 3

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, false)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_SearchAfter() {
	id := "es-functional-list-workflow-searchAfter-test"
	wt := "es-functional-list-workflow-searchAfter-test-type"
	tl := "es-functional-list-workflow-searchAfter-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	numOfWorkflows := defaultPageSize + 1 // == 6
	pageSize := 4

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, false)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_OrQuery() {
	id := "es-functional-list-workflow-or-query-test"
	wt := "es-functional-list-workflow-or-query-test-type"
	tl := "es-functional-list-workflow-or-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	// start 3 workflows
	key := "CustomIntField"
	attrValBytes, _ := payload.Encode(1)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			key: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr
	we1, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	request.RequestId = uuid.New()
	request.WorkflowId = id + "-2"
	attrValBytes, _ = payload.Encode(2)
	searchAttr.IndexedFields[key] = attrValBytes
	we2, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	request.RequestId = uuid.New()
	request.WorkflowId = id + "-3"
	attrValBytes, _ = payload.Encode(3)
	searchAttr.IndexedFields[key] = attrValBytes
	we3, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	time.Sleep(waitForESToSettle)

	// query 1 workflow with search attr
	query1 := fmt.Sprintf(`CustomIntField = %d`, 1)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     query1,
	}
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.True(!openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[key]
	var searchVal int
	payload.Decode(searchValBytes, &searchVal)
	s.Equal(1, searchVal)

	// query with or clause
	query2 := fmt.Sprintf(`CustomIntField = %d or CustomIntField = %d`, 1, 2)
	listRequest.Query = query2
	var openExecutions []*workflowpb.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 2 {
			openExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.Equal(2, len(openExecutions))
	e1 := openExecutions[0]
	e2 := openExecutions[1]
	if e1.GetExecution().GetRunId() != we1.GetRunId() {
		// results are sorted by [CloseTime,RunID] desc, so find the correct mapping first
		e1, e2 = e2, e1
	}
	s.Equal(we1.GetRunId(), e1.GetExecution().GetRunId())
	s.Equal(we2.GetRunId(), e2.GetExecution().GetRunId())
	searchValBytes = e2.SearchAttributes.GetIndexedFields()[key]
	payload.Decode(searchValBytes, &searchVal)
	s.Equal(2, searchVal)

	// query for open
	query3 := fmt.Sprintf(`(CustomIntField = %d or CustomIntField = %d) and ExecutionStatus = 'Running'`, 2, 3)
	listRequest.Query = query3
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 2 {
			openExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.Equal(2, len(openExecutions))
	e1 = openExecutions[0]
	e2 = openExecutions[1]
	s.Equal(we3.GetRunId(), e1.GetExecution().GetRunId())
	s.Equal(we2.GetRunId(), e2.GetExecution().GetRunId())
	searchValBytes = e1.SearchAttributes.GetIndexedFields()[key]
	payload.Decode(searchValBytes, &searchVal)
	s.Equal(3, searchVal)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_KeywordQuery() {
	id := "es-functional-list-workflow-keyword-query-test"
	wt := "es-functional-list-workflow-keyword-query-test-type"
	tl := "es-functional-list-workflow-keyword-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	searchAttr, err := searchattribute.Encode(
		map[string]any{
			"CustomKeywordField": "justice for all",
		},
		&searchattribute.TestNameTypeMap,
	)
	s.NoError(err)

	request.SearchAttributes = searchAttr
	we1, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	time.Sleep(waitForESToSettle)

	// Exact match Keyword (supported)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomKeywordField = "justice for all"`,
	}
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.True(!openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	saPayload := openExecution.SearchAttributes.GetIndexedFields()["CustomKeywordField"]
	var saValue string
	err = payload.Decode(saPayload, &saValue)
	s.NoError(err)
	s.Equal("justice for all", saValue)

	// Partial match on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomKeywordField = "justice"`,
	}
	resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// Inordered match on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomKeywordField = "all for justice"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// Prefix search
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomKeywordField STARTS_WITH "justice"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)
	s.Equal(id, resp.Executions[0].GetExecution().GetWorkflowId())
	s.Equal(wt, resp.Executions[0].GetType().GetName())
	s.ProtoEqual(searchAttr, resp.Executions[0].GetSearchAttributes())

	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     fmt.Sprintf(`WorkflowId = %q AND CustomKeywordField NOT STARTS_WITH "justice"`, id),
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_StringQuery() {
	id := "es-functional-list-workflow-string-query-test"
	wt := "es-functional-list-workflow-string-query-test-type"
	tl := "es-functional-list-workflow-string-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomTextField": payload.EncodeString("nothing else matters"),
		},
	}
	request.SearchAttributes = searchAttr
	we1, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	time.Sleep(waitForESToSettle)

	// Exact match String (supported)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomTextField = "nothing else matters"`,
	}
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.True(!openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	saPayload := openExecution.SearchAttributes.GetIndexedFields()["CustomTextField"]
	var saValue string
	err = payload.Decode(saPayload, &saValue)
	s.NoError(err)
	s.Equal("nothing else matters", saValue)

	// Partial match on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomTextField = "nothing"`,
	}
	resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// Inordered match on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     `CustomTextField = "else nothing matters"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)
}

// To test last page search trigger max window size error
func (s *AdvancedVisibilitySuite) TestListWorkflow_MaxWindowSize() {
	id := "es-functional-list-workflow-max-window-size-test"
	wt := "es-functional-list-workflow-max-window-size-test-type"
	tl := "es-functional-list-workflow-max-window-size-test-taskqueue"
	startRequest := s.createStartWorkflowExecutionRequest(id, wt, tl)

	for i := 0; i < defaultPageSize; i++ {
		startRequest.RequestId = uuid.New()
		startRequest.WorkflowId = id + strconv.Itoa(i)
		_, err := s.engine.StartWorkflowExecution(NewContext(), startRequest)
		s.NoError(err)
	}

	time.Sleep(waitForESToSettle)

	var listResp *workflowservice.ListWorkflowExecutionsResponse
	var nextPageToken []byte

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     s.namespace,
		PageSize:      int32(defaultPageSize),
		NextPageToken: nextPageToken,
		Query:         fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = "Running"`, wt),
	}
	// get first page
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == defaultPageSize {
			listResp = resp
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(listResp)
	s.True(len(listResp.GetNextPageToken()) != 0)

	// the last request
	listRequest.NextPageToken = listResp.GetNextPageToken()
	resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.True(len(resp.GetExecutions()) == 0)
	s.Nil(resp.GetNextPageToken())
}

func (s *AdvancedVisibilitySuite) TestListWorkflow_OrderBy() {
	if !s.isElasticsearchEnabled {
		s.T().Skip("This test is only for Elasticsearch")
	}

	id := "es-functional-list-workflow-order-by-test"
	wt := "es-functional-list-workflow-order-by-test-type"
	tl := "es-functional-list-workflow-order-by-test-taskqueue"

	initialTime := time.Now().UTC()
	for i := 0; i < defaultPageSize+1; i++ { // start 6
		startRequest := s.createStartWorkflowExecutionRequest(id, wt, tl)
		startRequest.RequestId = uuid.New()
		startRequest.WorkflowId = id + strconv.Itoa(i)

		if i < defaultPageSize-1 { // 4 workflows have search attributes.
			intVal, _ := payload.Encode(i)
			doubleVal, _ := payload.Encode(float64(i))
			strVal, _ := payload.Encode(strconv.Itoa(i))
			timeVal, _ := payload.Encode(initialTime.Add(time.Duration(i)))
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
			timeVal, _ := payload.Encode(initialTime.Add(time.Duration(i)))
			startRequest.SearchAttributes = &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomDatetimeField": timeVal,
				},
			}
		}

		_, err := s.engine.StartWorkflowExecution(NewContext(), startRequest)
		s.NoError(err)
	}

	time.Sleep(waitForESToSettle)

	desc := "desc"
	asc := "asc"
	queryTemplate := `WorkflowType = "%s" order by %s %s`
	pageSize := int32(defaultPageSize)

	// order by CloseTime asc
	query1 := fmt.Sprintf(queryTemplate, wt, searchattribute.CloseTime, asc)
	var openExecutions []*workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  pageSize,
		Query:     query1,
	}
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if int32(len(resp.GetExecutions())) == listRequest.GetPageSize() {
			openExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecutions)
	for i := int32(1); i < pageSize; i++ {
		e1 := openExecutions[i-1]
		e2 := openExecutions[i]
		if e2.GetCloseTime() != nil {
			s.NotEqual(time.Time{}, *e1.GetCloseTime())
			s.GreaterOrEqual(e2.GetCloseTime(), e1.GetCloseTime())
		}
	}

	// greatest effort to reduce duplicate code
	testHelper := func(query, searchAttrKey string, prevVal, currVal interface{}) {
		listRequest.Query = query
		listRequest.NextPageToken = []byte{}
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
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
		resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest) // last page
		s.NoError(err)
		s.Equal(1, len(resp.GetExecutions()))
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

//nolint:revive // isScan is a control flag
func (s *AdvancedVisibilitySuite) testListWorkflowHelper(numOfWorkflows, pageSize int,
	startRequest *workflowservice.StartWorkflowExecutionRequest, wid, wType string, isScan bool) {

	// start enough number of workflows
	for i := 0; i < numOfWorkflows; i++ {
		startRequest.RequestId = uuid.New()
		startRequest.WorkflowId = wid + strconv.Itoa(i)
		_, err := s.engine.StartWorkflowExecution(NewContext(), startRequest)
		s.NoError(err)
	}

	time.Sleep(waitForESToSettle)

	var openExecutions []*workflowpb.WorkflowExecutionInfo
	var nextPageToken []byte

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     s.namespace,
		PageSize:      int32(pageSize),
		NextPageToken: nextPageToken,
		Query:         fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wType),
	}

	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace:     s.namespace,
		PageSize:      int32(pageSize),
		NextPageToken: nextPageToken,
		Query:         fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wType),
	}

	// test first page
	for i := 0; i < numOfRetry; i++ {
		if isScan {
			scanResponse, err := s.engine.ScanWorkflowExecutions(NewContext(), scanRequest)
			s.NoError(err)
			if len(scanResponse.GetExecutions()) == pageSize {
				openExecutions = scanResponse.GetExecutions()
				nextPageToken = scanResponse.GetNextPageToken()
				break
			}
		} else {
			listResponse, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
			s.NoError(err)
			if len(listResponse.GetExecutions()) == pageSize {
				openExecutions = listResponse.GetExecutions()
				nextPageToken = listResponse.GetNextPageToken()
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecutions)
	s.NotNil(nextPageToken)
	s.True(len(nextPageToken) > 0)

	// test last page
	listRequest.NextPageToken = nextPageToken
	scanRequest.NextPageToken = nextPageToken
	inIf := false
	for i := 0; i < numOfRetry; i++ {
		if isScan {
			scanResponse, err := s.engine.ScanWorkflowExecutions(NewContext(), scanRequest)
			s.NoError(err)
			if len(scanResponse.GetExecutions()) == numOfWorkflows-pageSize {
				inIf = true
				openExecutions = scanResponse.GetExecutions()
				nextPageToken = scanResponse.GetNextPageToken()
				break
			}
		} else {
			listResponse, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
			s.NoError(err)
			if len(listResponse.GetExecutions()) == numOfWorkflows-pageSize {
				inIf = true
				openExecutions = listResponse.GetExecutions()
				nextPageToken = listResponse.GetNextPageToken()
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(inIf)
	s.NotNil(openExecutions)
	s.Nil(nextPageToken)
}

//nolint:revive // isScan is a control flag
func (s *AdvancedVisibilitySuite) testHelperForReadOnce(expectedRunID string, query string, isScan bool) {
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     query,
	}
	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultPageSize,
		Query:     query,
	}

	for i := 0; i < numOfRetry; i++ {
		if isScan {
			scanResponse, err := s.engine.ScanWorkflowExecutions(NewContext(), scanRequest)
			s.NoError(err)
			if len(scanResponse.GetExecutions()) == 1 {
				openExecution = scanResponse.GetExecutions()[0]
				s.Nil(scanResponse.NextPageToken)
				break
			}
		} else {
			listResponse, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
			s.NoError(err)
			if len(listResponse.GetExecutions()) == 1 {
				openExecution = listResponse.GetExecutions()[0]
				s.Nil(listResponse.NextPageToken)
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(expectedRunID, openExecution.GetExecution().GetRunId())
	s.True(!openExecution.GetExecutionTime().AsTime().Before(openExecution.GetStartTime().AsTime()))
	if openExecution.SearchAttributes != nil && len(openExecution.SearchAttributes.GetIndexedFields()) > 0 {
		searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey]
		var searchVal string
		payload.Decode(searchValBytes, &searchVal)
		s.Equal(s.testSearchAttributeVal, searchVal)
	}
}

func (s *AdvancedVisibilitySuite) TestScanWorkflow() {
	if !s.isElasticsearchEnabled {
		s.T().Skip("This test is only for Elasticsearch")
	}

	id := "es-functional-scan-workflow-test"
	wt := "es-functional-scan-workflow-test-type"
	tl := "es-functional-scan-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, true)
}

func (s *AdvancedVisibilitySuite) TestScanWorkflow_SearchAttribute() {
	if !s.isElasticsearchEnabled {
		s.T().Skip("This test is only for Elasticsearch")
	}

	id := "es-functional-scan-workflow-search-attr-test"
	wt := "es-functional-scan-workflow-search-attr-test-type"
	tl := "es-functional-scan-workflow-search-attr-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := payload.Encode(s.testSearchAttributeVal)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	query := fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	s.testHelperForReadOnce(we.GetRunId(), query, true)
}

func (s *AdvancedVisibilitySuite) TestScanWorkflow_PageToken() {
	if !s.isElasticsearchEnabled {
		s.T().Skip("This test is only for Elasticsearch")
	}

	id := "es-functional-scan-workflow-token-test"
	wt := "es-functional-scan-workflow-token-test-type"
	tl := "es-functional-scan-workflow-token-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           s.namespace,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	numOfWorkflows := 4
	pageSize := 3

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, true)
}

func (s *AdvancedVisibilitySuite) TestCountWorkflow() {
	id := "es-functional-count-workflow-test"
	wt := "es-functional-count-workflow-test-type"
	tl := "es-functional-count-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := payload.Encode(s.testSearchAttributeVal)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	_, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	countRequest := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.namespace,
		Query:     query,
	}
	var resp *workflowservice.CountWorkflowExecutionsResponse
	for i := 0; i < numOfRetry; i++ {
		resp, err = s.engine.CountWorkflowExecutions(NewContext(), countRequest)
		s.NoError(err)
		if resp.GetCount() == int64(1) {
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.Equal(int64(1), resp.GetCount())

	query = fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, "noMatch")
	countRequest.Query = query
	resp, err = s.engine.CountWorkflowExecutions(NewContext(), countRequest)
	s.NoError(err)
	s.Equal(int64(0), resp.GetCount())
}

func (s *AdvancedVisibilitySuite) TestCountGroupByWorkflow() {
	id := "es-functional-count-groupby-workflow-test"
	wt := "es-functional-count-groupby-workflow-test-type"
	tl := "es-functional-count-groupby-workflow-test-taskqueue"

	numWorkflows := 10
	numClosedWorkflows := 4
	for i := 0; i < numWorkflows; i++ {
		wfid := id + strconv.Itoa(i)
		request := s.createStartWorkflowExecutionRequest(wfid, wt, tl)
		we, err := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err)
		if i < numClosedWorkflows {
			_, err := s.engine.TerminateWorkflowExecution(
				NewContext(),
				&workflowservice.TerminateWorkflowExecutionRequest{
					Namespace: s.namespace,
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
		Namespace: s.namespace,
		Query:     query,
	}
	var resp *workflowservice.CountWorkflowExecutionsResponse
	var err error
	for i := 0; i < numOfRetry; i++ {
		resp, err = s.engine.CountWorkflowExecutions(NewContext(), countRequest)
		s.NoError(err)
		if resp.GetCount() == int64(numWorkflows) {
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.Equal(int64(numWorkflows), resp.GetCount())
	s.Equal(2, len(resp.Groups))

	runningStatusPayload, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	terminatedStatusPayload, _ := searchattribute.EncodeValue(
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED.String(),
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	s.ProtoEqual(
		&workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
			GroupValues: []*commonpb.Payload{runningStatusPayload},
			Count:       int64(numWorkflows - numClosedWorkflows),
		},
		resp.Groups[0],
	)
	s.ProtoEqual(
		&workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{
			GroupValues: []*commonpb.Payload{terminatedStatusPayload},
			Count:       int64(numClosedWorkflows),
		},
		resp.Groups[1],
	)

	query = `GROUP BY WorkflowType`
	countRequest.Query = query
	_, err = s.engine.CountWorkflowExecutions(NewContext(), countRequest)
	s.Error(err)
	s.Contains(err.Error(), "'group by' clause is only supported for ExecutionStatus search attribute")

	query = `GROUP BY ExecutionStatus, WorkflowType`
	countRequest.Query = query
	_, err = s.engine.CountWorkflowExecutions(NewContext(), countRequest)
	s.Error(err)
	s.Contains(err.Error(), "'group by' clause supports only a single field")
}

func (s *AdvancedVisibilitySuite) createStartWorkflowExecutionRequest(id, wt, tl string) *workflowservice.StartWorkflowExecutionRequest {
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
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

func (s *AdvancedVisibilitySuite) TestUpsertWorkflowExecutionSearchAttributes() {
	id := "es-functional-upsert-workflow-search-attributes-test"
	wt := "es-functional-upsert-workflow-search-attributes-test-type"
	tl := "es-functional-upsert-workflow-search-attributes-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
			attrValPayload, _ := payload.Encode(s.testSearchAttributeVal)
			upsertSearchAttr := &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					s.testSearchAttributeKey: attrValPayload,
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// process 1st workflow task and assert workflow task is handled correctly.
	res, err := poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

	// verify upsert data is on ES
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	verified := false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			execution := resp.GetExecutions()[0]
			retrievedSearchAttr := execution.SearchAttributes
			if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) > 0 {
				searchValBytes := retrievedSearchAttr.GetIndexedFields()[s.testSearchAttributeKey]
				var searchVal string
				err = payload.Decode(searchValBytes, &searchVal)
				s.NoError(err)
				s.Equal(s.testSearchAttributeVal, searchVal)
				verified = true
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)

	// process 2nd workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

	// verify upsert data is on ES
	s.testListResultForUpsertSearchAttributes(listRequest)

	// process 3rd workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

	// verify search attributes are unset
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	verified = false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			execution := resp.GetExecutions()[0]
			retrievedSearchAttr := execution.SearchAttributes
			if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) > 0 {
				s.NotContains(retrievedSearchAttr.GetIndexedFields(), "CustomTextField")
				s.NotContains(retrievedSearchAttr.GetIndexedFields(), "CustomIntField")
				verified = true
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)

	// verify query by unset search attribute
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running' and CustomTextField is null and CustomIntField is null`, wt),
	}
	verified = false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			verified = true
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)

	// verify search attributes from DescribeWorkflowExecution
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	expectedSearchAttributes, _ := searchattribute.Encode(
		map[string]interface{}{
			"CustomDoubleField":             22.0878,
			searchattribute.BinaryChecksums: []string{"binary-v1", "binary-v2"},
			searchattribute.BuildIds:        []string{worker_versioning.UnversionedSearchAttribute},
		},
		nil,
	)
	s.Equal(
		len(expectedSearchAttributes.GetIndexedFields()),
		len(descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()),
	)
	for attrName, expectedPayload := range expectedSearchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[searchattribute.MetadataType]
		s.True(typeSet)
		s.True(len(attrType) > 0)
	}

	// process close workflow task and assert search attributes is correct after workflow is closed
	res, err = poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.Nil(newTask.WorkflowTask)

	time.Sleep(waitForESToSettle)

	// verify search attributes from DescribeWorkflowExecution
	descRequest = &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err = s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
	s.Equal(
		len(expectedSearchAttributes.GetIndexedFields()),
		len(descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()),
	)
	for attrName, expectedPayload := range expectedSearchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[searchattribute.MetadataType]
		s.True(typeSet)
		s.True(len(attrType) > 0)
	}
}

func (s *AdvancedVisibilitySuite) TestModifyWorkflowExecutionProperties() {
	id := "es-functional-modify-workflow-properties-test"
	wt := "es-functional-modify-workflow-properties-test-type"
	tl := "es-functional-modify-workflow-properties-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// process 1st workflow task and assert workflow task is handled correctly.
	res, err := poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

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
		Namespace: s.namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	verified := false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			s.True(proto.Equal(expectedMemo, resp.Executions[0].Memo))
			verified = true
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)

	// process 2nd workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

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
		Namespace: s.namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	verified = false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			s.True(proto.Equal(expectedMemo, resp.Executions[0].Memo))
			verified = true
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)

	// process close workflow task and assert workflow task is handled correctly.
	res, err = poller.PollAndProcessWorkflowTask(
		WithPollSticky,
		WithRespondSticky,
		WithExpectedAttemptCount(0),
		WithRetries(1),
		WithForceNewWorkflowTask)
	s.NoError(err)
	newTask = res.NewTask
	s.NotNil(newTask)
	s.Nil(newTask.WorkflowTask)

	time.Sleep(waitForESToSettle)

	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
	s.ProtoEqual(expectedMemo, descResp.WorkflowExecutionInfo.Memo)
}

func (s *AdvancedVisibilitySuite) testListResultForUpsertSearchAttributes(listRequest *workflowservice.ListWorkflowExecutionsRequest) {
	verified := false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			s.Nil(resp.NextPageToken)
			execution := resp.GetExecutions()[0]
			retrievedSearchAttr := execution.SearchAttributes
			if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) == 5 {
				fields := retrievedSearchAttr.GetIndexedFields()
				searchValBytes := fields[s.testSearchAttributeKey]
				var searchVal string
				err := payload.Decode(searchValBytes, &searchVal)
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
				s.Equal(22.0878, doubleVal)

				binaryChecksumsBytes := fields[searchattribute.BinaryChecksums]
				var binaryChecksums []string
				err = payload.Decode(binaryChecksumsBytes, &binaryChecksums)
				s.NoError(err)
				s.Equal([]string{"binary-v1", "binary-v2"}, binaryChecksums)

				buildIdsBytes := fields[searchattribute.BuildIds]
				var buildIds []string
				err = payload.Decode(buildIdsBytes, &buildIds)
				s.NoError(err)
				s.Equal([]string{worker_versioning.UnversionedSearchAttribute}, buildIds)

				verified = true
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)
}

func (s *AdvancedVisibilitySuite) createSearchAttributes() *commonpb.SearchAttributes {
	searchAttributes, err := searchattribute.Encode(map[string]interface{}{
		"CustomTextField":               "another string",
		"CustomIntField":                123,
		"CustomDoubleField":             22.0878,
		searchattribute.BinaryChecksums: []string{"binary-v1", "binary-v2"},
	}, nil)
	s.NoError(err)
	return searchAttributes
}

func (s *AdvancedVisibilitySuite) TestUpsertWorkflowExecution_InvalidKey() {
	id := "es-functional-upsert-workflow-failed-test"
	wt := "es-functional-upsert-workflow-failed-test-type"
	tl := "es-functional-upsert-workflow-failed-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	historyEvents := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	if s.isElasticsearchEnabled {
		s.ErrorContains(err, "BadSearchAttributes: search attribute INVALIDKEY is not defined")
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed {"Cause":23,"Failure":{"Message":"BadSearchAttributes: search attribute INVALIDKEY is not defined"}}`, historyEvents)
	} else {
		s.ErrorContains(err, fmt.Sprintf("BadSearchAttributes: Namespace %s has no mapping defined for search attribute INVALIDKEY", s.namespace))
		s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed {"Cause":23,"Failure":{"Message":"BadSearchAttributes: Namespace %s has no mapping defined for search attribute INVALIDKEY"}}`, s.namespace), historyEvents)
	}
}

func (s *AdvancedVisibilitySuite) TestChildWorkflow_ParentWorkflow() {
	var (
		ctx         = NewContext()
		wfID        = s.randomizeStr(s.T().Name())
		childWfID   = s.randomizeStr(s.T().Name())
		childWfType = "child-wf-type-" + wfID
		wfType      = "wf-type-" + wfID
		taskQueue   = "task-queue-" + wfID
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

	w := worker.New(s.sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: wfType})
	w.RegisterWorkflowWithOptions(childWf, workflow.RegisterOptions{Name: childWfType})
	s.Require().NoError(w.Start())

	startOptions := sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: taskQueue,
	}
	run, err := s.sdkClient.ExecuteWorkflow(ctx, startOptions, wfType)
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
	w.Stop()

	// check main workflow doesn't have parent workflow and root is itself
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := s.engine.ListWorkflowExecutions(
				ctx,
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.namespace,
					Query:     fmt.Sprintf("WorkflowType = %q", wfType),
					PageSize:  defaultPageSize,
				},
			)
			assert.NoError(c, err)
			if assert.Len(c, resp.Executions, 1) {
				wfInfo := resp.Executions[0]
				assert.Nil(c, wfInfo.GetParentExecution())
				assert.NotNil(c, wfInfo.GetRootExecution())
				assert.Equal(c, wfID, wfInfo.RootExecution.GetWorkflowId())
				assert.Equal(c, run.GetRunID(), wfInfo.RootExecution.GetRunId())
			}
		},
		waitForESToSettle,
		100*time.Millisecond,
	)

	// check child workflow has parent workflow and root is the parent
	var childWfInfo *workflowpb.WorkflowExecutionInfo
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := s.engine.ListWorkflowExecutions(
				ctx,
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.namespace,
					Query:     fmt.Sprintf("WorkflowType = %q", childWfType),
					PageSize:  defaultPageSize,
				},
			)
			assert.NoError(c, err)
			if assert.Len(c, resp.Executions, 1) {
				childWfInfo = resp.Executions[0]
				assert.NotNil(c, childWfInfo.GetParentExecution())
				assert.Equal(c, wfID, childWfInfo.ParentExecution.GetWorkflowId())
				assert.Equal(c, run.GetRunID(), childWfInfo.ParentExecution.GetRunId())
				assert.NotNil(c, childWfInfo.GetRootExecution())
				assert.Equal(c, wfID, childWfInfo.RootExecution.GetWorkflowId())
				assert.Equal(c, run.GetRunID(), childWfInfo.RootExecution.GetRunId())
			}
		},
		waitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *AdvancedVisibilitySuite) Test_LongWorkflowID() {
	if s.testClusterConfig.Persistence.StoreType == config.StoreTypeSQL {
		// TODO: remove this when workflow_id field size is increased from varchar(255) in SQL schema.
		return
	}

	id := strings.Repeat("a", 1000)
	wt := "es-functional-long-workflow-id-test-type"
	tl := "es-functional-long-workflow-id-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnCompletion_UnversionedWorker() {
	ctx := NewContext()
	id := s.randomizeStr(s.T().Name())
	workflowType := "functional-build-id"
	taskQueue := s.randomizeStr(s.T().Name())

	request := s.createStartWorkflowExecutionRequest(id, workflowType, taskQueue)
	_, err := s.engine.StartWorkflowExecution(ctx, request)
	s.NoError(err)

	pollRequest := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: s.namespace, TaskQueue: request.TaskQueue, Identity: id}
	task, err := s.engine.PollWorkflowTaskQueue(ctx, pollRequest)
	s.NoError(err)
	s.Greater(len(task.TaskToken), 0)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          s.namespace,
		Identity:           id,
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: "1.0"},
		TaskToken:          task.TaskToken,
	})
	s.NoError(err)

	buildIDs := s.getBuildIds(ctx, task.WorkflowExecution)
	s.Equal([]string{
		worker_versioning.UnversionedSearchAttribute,
		worker_versioning.UnversionedBuildIdSearchAttribute("1.0"),
	}, buildIDs)

	_, err = s.engine.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{Namespace: s.namespace, WorkflowExecution: task.WorkflowExecution, SignalName: "continue"})
	s.NoError(err)

	task, err = s.engine.PollWorkflowTaskQueue(ctx, pollRequest)
	s.NoError(err)
	s.Greater(len(task.TaskToken), 0)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          s.namespace,
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

	buildIDs = s.getBuildIds(ctx, task.WorkflowExecution)
	s.Equal([]string{
		worker_versioning.UnversionedSearchAttribute,
		worker_versioning.UnversionedBuildIdSearchAttribute("1.0"),
		worker_versioning.UnversionedBuildIdSearchAttribute("1.1"),
	}, buildIDs)

	task, err = s.engine.PollWorkflowTaskQueue(ctx, pollRequest)
	s.NoError(err)
	s.Greater(len(task.TaskToken), 0)

	buildIDs = s.getBuildIds(ctx, task.WorkflowExecution)
	s.Equal([]string{}, buildIDs)

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          s.namespace,
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

	buildIDs = s.getBuildIds(ctx, task.WorkflowExecution)
	s.Equal([]string{worker_versioning.UnversionedSearchAttribute, worker_versioning.UnversionedBuildIdSearchAttribute("1.2")}, buildIDs)

	for minor := 1; minor <= 2; minor++ {
		s.Eventually(func() bool {
			response, err := s.engine.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.namespace,
				Query:     fmt.Sprintf("BuildIds = '%s'", worker_versioning.UnversionedBuildIdSearchAttribute(fmt.Sprintf("1.%d", minor))),
				PageSize:  defaultPageSize,
			})
			if err != nil {
				return false
			}
			if len(response.Executions) == 0 {
				return false
			}
			s.Equal(id, response.Executions[0].Execution.WorkflowId)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	}
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnCompletion_VersionedWorker() {
	// Use only one partition to avoid having to wait for user data propagation later
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	ctx := NewContext()
	id := s.randomizeStr(s.T().Name())
	childId1 := "child1-" + id
	childId2 := "child2-" + id
	workflowType := "functional-build-id"
	taskQueue := s.randomizeStr(s.T().Name())
	v1 := s.T().Name() + "-v1"
	v11 := s.T().Name() + "-v11"

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
	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v1,
		},
	})
	s.Require().NoError(err)

	// Start first worker
	w1 := worker.New(s.sdkClient, taskQueue, worker.Options{
		BuildID:                      v1,
		UseBuildIDForVersioning:      true,
		StickyScheduleToStartTimeout: time.Second,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.Require().NoError(w1.Start())

	// Start the workflow and wait for first WFT
	startOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: taskQueue,
	}
	run, err := s.sdkClient.ExecuteWorkflow(ctx, startOptions, workflowType)
	s.NoError(err)

	<-startedCh
	w1.Stop()

	// Verify first WFT was processed by our v1 worker
	s.Eventually(func() bool {
		buildIDs := s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: id})
		if len(buildIDs) == 0 {
			return false
		}
		s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(v1)}, buildIDs)
		return true
	}, time.Second*15, time.Millisecond*100)

	// Update sets with v1.1
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				ExistingCompatibleBuildId: v1,
				NewBuildId:                v11,
			},
		},
	})
	s.Require().NoError(err)

	// Start v1.1 worker
	w11 := worker.New(s.sdkClient, taskQueue, worker.Options{
		BuildID:                 v11,
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.Require().NoError(w11.Start())

	defer w11.Stop()

	// Resume workflow execution and wait for first task after CAN
	err = s.sdkClient.SignalWorkflow(ctx, id, "", "continue", nil)
	s.Require().NoError(err)

	err = run.GetWithOptions(ctx, nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var canError *workflow.ContinueAsNewError
	s.Require().ErrorAs(err, &canError)

	secondRunId := <-startedCh

	// Verify both workers appear in the search attribute for first run in chain
	buildIDs := s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: id, RunId: run.GetRunID()})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(v1), worker_versioning.VersionedBuildIdSearchAttribute(v11)}, buildIDs)

	// Check search attribute is propagated after first continue as new
	buildIDs = s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: id})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(v11)}, buildIDs)

	// Resume and wait for the workflow CAN for the last time
	err = s.sdkClient.SignalWorkflow(ctx, id, "", "continue", nil)
	s.Require().NoError(err)

	run = s.sdkClient.GetWorkflow(ctx, id, secondRunId)
	err = run.GetWithOptions(ctx, nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	s.Require().ErrorAs(err, &canError)

	// Check search attribute is not propagated after second continue as new
	buildIDs = s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: id})
	s.Equal([]string{}, buildIDs)

	// Check search attribute is propagated to first child
	buildIDs = s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: childId1})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(v11)}, buildIDs)

	// Check search attribute is not propagated to second child
	buildIDs = s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: childId2})
	s.Equal([]string{}, buildIDs)

	// We should have 3 runs with the v1.1 search attribute: First and second run in chain, and single child
	s.Eventually(func() bool {
		response, err := s.engine.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.namespace,
			Query:     fmt.Sprintf("BuildIds = %q", worker_versioning.VersionedBuildIdSearchAttribute(v11)),
			PageSize:  defaultPageSize,
		})
		if err != nil {
			return false
		}
		if len(response.Executions) != 3 {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnReset() {
	// Use only one partition to avoid having to wait for user data propagation later
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	ctx := NewContext()
	id := s.randomizeStr(s.T().Name())
	workflowType := "functional-build-id"
	taskQueue := s.randomizeStr(s.T().Name())
	v1 := s.T().Name() + "-v1"

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
	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v1,
		},
	})
	s.Require().NoError(err)

	// Start a worker
	w := worker.New(s.sdkClient, taskQueue, worker.Options{
		BuildID:                      v1,
		UseBuildIDForVersioning:      true,
		StickyScheduleToStartTimeout: time.Second,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.Require().NoError(w.Start())
	defer w.Stop()

	// Start the workflow and wait for CAN
	startOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: taskQueue,
	}
	run, err := s.sdkClient.ExecuteWorkflow(ctx, startOptions, workflowType)
	s.Require().NoError(err)

	err = run.GetWithOptions(ctx, nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var canError *workflow.ContinueAsNewError
	s.Require().ErrorAs(err, &canError)

	// Confirm first WFT is complete before resetting
	<-startedCh

	resetResult, err := s.sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.namespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id},
		WorkflowTaskFinishEventId: 3,
	})
	s.Require().NoError(err)
	buildIDs := s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: id, RunId: resetResult.RunId})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(v1)}, buildIDs)

	s.Eventually(func() bool {
		response, err := s.engine.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.namespace,
			Query:     fmt.Sprintf("BuildIds = %q AND RunId = %q", worker_versioning.VersionedBuildIdSearchAttribute(v1), resetResult.RunId),
			PageSize:  defaultPageSize,
		})
		if err != nil {
			return false
		}
		if len(response.Executions) != 1 {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) Test_BuildIdIndexedOnRetry() {
	// Use only one partition to avoid having to wait for user data propagation later
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	ctx := NewContext()
	id := s.randomizeStr(s.T().Name())
	workflowType := "functional-build-id"
	taskQueue := s.randomizeStr(s.T().Name())
	v1 := s.T().Name() + "-v1"

	wf := func(ctx workflow.Context) error {
		return fmt.Errorf("fail")
	}

	// Declare v1
	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v1,
		},
	})
	s.Require().NoError(err)

	// Start a worker
	w := worker.New(s.sdkClient, taskQueue, worker.Options{
		BuildID:                      v1,
		UseBuildIDForVersioning:      true,
		StickyScheduleToStartTimeout: time.Second,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.Require().NoError(w.Start())
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
	run, err := s.sdkClient.ExecuteWorkflow(ctx, startOptions, workflowType)
	s.Require().NoError(err)
	s.Require().Error(run.Get(ctx, nil))

	buildIDs := s.getBuildIds(ctx, &commonpb.WorkflowExecution{WorkflowId: id})
	s.Equal([]string{worker_versioning.VersionedBuildIdSearchAttribute(v1)}, buildIDs)

	s.Eventually(func() bool {
		response, err := s.engine.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.namespace,
			Query:     fmt.Sprintf("BuildIds = %q", worker_versioning.VersionedBuildIdSearchAttribute(v1)),
			PageSize:  defaultPageSize,
		})
		if err != nil {
			return false
		}
		// Both runs should be associated with this build ID
		if len(response.Executions) != 2 {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_ByBuildId() {
	ctx := NewContext()
	tq1 := s.T().Name()
	tq2 := s.T().Name() + "-2"
	tq3 := s.T().Name() + "-3"
	v0 := s.T().Name() + "-v0"
	v01 := s.T().Name() + "-v0.1"
	v1 := s.T().Name() + "-v1"
	var err error

	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq1,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.Require().NoError(err)
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq1,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				ExistingCompatibleBuildId: v0,
				NewBuildId:                v01,
			},
		},
	})
	s.Require().NoError(err)
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq2,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.Require().NoError(err)

	// Map v0 to a third queue to test limit enforcement
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq3,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.Require().NoError(err)

	var reachabilityResponse *workflowservice.GetWorkerTaskReachabilityResponse

	reachabilityResponse, err = s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace:    s.namespace,
		BuildIds:     []string{v0},
		Reachability: enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS,
	})
	s.Require().NoError(err)
	s.Require().Equal([]*taskqueuepb.BuildIdReachability{{
		BuildId: v0,
		TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{
			{TaskQueue: tq1, Reachability: []enumspb.TaskReachability(nil)},
			{TaskQueue: tq2, Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_NEW_WORKFLOWS}},
			{TaskQueue: tq3, Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_UNSPECIFIED}},
		},
	}}, reachabilityResponse.BuildIdReachability)

	// Start a workflow on tq1 and verify it affects the reachability of v0.1
	_, err = s.engine.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   s.randomizeStr(s.T().Name()),
		WorkflowType: &commonpb.WorkflowType{Name: "dont-care"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: tq1, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.Require().NoError(err)

	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS)

	// Complete the workflow and verify it affects reachability of v0.1
	task, err := s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:                 s.namespace,
		TaskQueue:                 &taskqueuepb.TaskQueue{Name: tq1, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{BuildId: v01, UseVersioning: true},
	})
	s.Require().NoError(err)
	s.Require().NotEmpty(task.GetTaskToken())
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          s.namespace,
		TaskToken:          task.TaskToken,
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{BuildId: v01, UseVersioning: true},
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}},
	})
	s.Require().NoError(err)

	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	// Make v1 default for queue 1
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq1,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v1,
		},
	})
	s.Require().NoError(err)

	dc := s.testCluster.host.dcClient
	// Verify new workflows are considered reachable by v01 which is no longer queue default within the configured
	// duration
	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	dc.OverrideValue(s.T(), dynamicconfig.ReachabilityQuerySetDurationSinceDefault, time.Microsecond)
	// Verify new workflows aren't reachable
	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq1, v01, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_ByBuildId_NotInNamespace() {
	ctx := NewContext()
	buildId := s.T().Name() + "v0"

	reachabilityResponse, err := s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace:    s.namespace,
		BuildIds:     []string{buildId},
		Reachability: enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS,
	})
	s.Require().NoError(err)
	protorequire.ProtoSliceEqual(s.T(), []*taskqueuepb.BuildIdReachability{{
		BuildId:               buildId,
		TaskQueueReachability: []*taskqueuepb.TaskQueueReachability(nil),
	}}, reachabilityResponse.BuildIdReachability)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_ByBuildId_NotInTaskQueue() {
	ctx := NewContext()
	tq := s.T().Name()
	v0 := s.T().Name() + "v0"
	v01 := s.T().Name() + "v0.1"

	checkReachability := func() {
		reachabilityResponse, err := s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
			Namespace:  s.namespace,
			BuildIds:   []string{v01},
			TaskQueues: []string{tq},
		})
		s.Require().NoError(err)
		protorequire.ProtoSliceEqual(s.T(), []*taskqueuepb.BuildIdReachability{{
			BuildId:               v01,
			TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{{TaskQueue: tq, Reachability: []enumspb.TaskReachability(nil)}},
		}}, reachabilityResponse.BuildIdReachability)
	}

	// Check once with an unversioned task queue
	checkReachability()

	// Same but with a versioned task queue
	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.Require().NoError(err)
	checkReachability()
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_EmptyBuildIds() {
	ctx := NewContext()

	_, err := s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: s.namespace,
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.Require().ErrorAs(err, &invalidArgument)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_TooManyBuildIds() {
	ctx := NewContext()

	_, err := s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: s.namespace,
		BuildIds:  []string{"", "v1"},
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.Require().ErrorAs(err, &invalidArgument)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_Unversioned_InNamespace() {
	ctx := NewContext()

	_, err := s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: s.namespace,
		BuildIds:  []string{""},
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.Require().ErrorAs(err, &invalidArgument)
}

func (s *AdvancedVisibilitySuite) TestWorkerTaskReachability_Unversioned_InTaskQueue() {
	ctx := NewContext()
	tq := s.T().Name()

	_, err := s.engine.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   s.randomizeStr(s.T().Name()),
		WorkflowType: &commonpb.WorkflowType{Name: "dont-care"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.Require().NoError(err)

	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS)

	task, err := s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.Require().NoError(err)
	s.Require().NotEmpty(task.GetTaskToken())
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}},
	})
	s.Require().NoError(err)

	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	// Make the task queue versioned and rerun our assertion
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: s.T().Name() + "-v0",
		},
	})
	s.Require().NoError(err)

	dc := s.testCluster.host.dcClient
	// Verify new workflows are considered reachable by the unversioned worker immediately after making the queue versioned
	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_NEW_WORKFLOWS, enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)

	dc.OverrideValue(s.T(), dynamicconfig.ReachabilityQuerySetDurationSinceDefault, time.Microsecond)

	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_EXISTING_WORKFLOWS)
	s.checkReachability(ctx, tq, "", enumspb.TASK_REACHABILITY_CLOSED_WORKFLOWS)
}

func (s *AdvancedVisibilitySuite) TestBuildIdScavenger_DeletesUnusedBuildId() {
	ctx := NewContext()
	tq := s.T().Name()
	v0 := s.T().Name() + "-v0"
	v1 := s.T().Name() + "-v1"
	var err error

	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v0,
		},
	})
	s.Require().NoError(err)
	_, err = s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v1,
		},
	})
	s.Require().NoError(err)

	run, err := s.sysSDKClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        s.T().Name() + "-scavenger",
		TaskQueue: build_ids.BuildIdScavengerTaskQueueName,
	}, build_ids.BuildIdScavangerWorkflowName, build_ids.BuildIdScavangerInput{
		IgnoreRetentionTime: true,
	})
	s.Require().NoError(err)
	err = run.Get(ctx, nil)
	s.Require().NoError(err)

	compatibility, err := s.sdkClient.GetWorkerBuildIdCompatibility(ctx, &sdkclient.GetWorkerBuildIdCompatibilityOptions{
		TaskQueue: tq,
	})
	s.Require().NoError(err)
	s.Require().Equal(1, len(compatibility.Sets))
	s.Require().Equal([]string{v1}, compatibility.Sets[0].BuildIDs)
	// Make sure the build ID was removed from the build ID->task queue mapping
	res, err := s.sdkClient.WorkflowService().GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
		Namespace: s.namespace,
		BuildIds:  []string{v0},
	})
	s.Require().NoError(err)
	s.Require().Equal(0, len(res.BuildIdReachability[0].TaskQueueReachability))
}

func (s *AdvancedVisibilitySuite) checkReachability(ctx context.Context, taskQueue, buildId string, expectedReachability ...enumspb.TaskReachability) {
	s.Require().Eventually(func() bool {
		reachabilityResponse, err := s.engine.GetWorkerTaskReachability(ctx, &workflowservice.GetWorkerTaskReachabilityRequest{
			Namespace:    s.namespace,
			BuildIds:     []string{buildId},
			TaskQueues:   []string{taskQueue},
			Reachability: expectedReachability[len(expectedReachability)-1],
		})
		s.Require().NoError(err)
		if len(reachabilityResponse.BuildIdReachability[0].TaskQueueReachability[0].Reachability) != len(expectedReachability) {
			return false
		}
		actualReachability := reachabilityResponse.BuildIdReachability[0].TaskQueueReachability[0].Reachability
		for i, expected := range expectedReachability {
			actual := actualReachability[i]
			if expected != actual {
				return false
			}
		}
		s.Require().Equal(
			[]*taskqueuepb.BuildIdReachability{{
				BuildId: buildId,
				TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{
					{TaskQueue: taskQueue, Reachability: expectedReachability},
				},
			}}, reachabilityResponse.BuildIdReachability)
		return true
	}, 15*time.Second, 100*time.Millisecond)
}

func (s *AdvancedVisibilitySuite) getBuildIds(ctx context.Context, execution *commonpb.WorkflowExecution) []string {
	description, err := s.engine.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: execution,
	})
	s.NoError(err)
	attr, found := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.BuildIds]
	if !found {
		return []string{}
	}
	var buildIDs []string
	err = payload.Decode(attr, &buildIDs)
	s.NoError(err)
	return buildIDs
}

func (s *AdvancedVisibilitySuite) updateMaxResultWindow() {
	esConfig := s.testClusterConfig.ESConfig

	esClient, err := esclient.NewFunctionalTestsClient(esConfig, s.Logger)
	s.Require().NoError(err)

	acknowledged, err := esClient.IndexPutSettings(
		context.Background(),
		esConfig.GetVisibilityIndex(),
		fmt.Sprintf(`{"max_result_window" : %d}`, defaultPageSize))
	s.Require().NoError(err)
	s.Require().True(acknowledged)

	for i := 0; i < numOfRetry; i++ {
		settings, err := esClient.IndexGetSettings(context.Background(), esConfig.GetVisibilityIndex())
		s.Require().NoError(err)
		if settings[esConfig.GetVisibilityIndex()].Settings["index"].(map[string]interface{})["max_result_window"].(string) == strconv.Itoa(defaultPageSize) {
			return
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.FailNow(fmt.Sprintf("ES max result window size hasn't reach target size within %v", (numOfRetry*waitTimeInMs)*time.Millisecond))
}
