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

//go:build esintegration
// +build esintegration

// to run locally, make sure Elasticsearch is running,
// then run cmd `go test -v ./host -run TestElasticsearchIntegrationSuite -tags esintegration`
package host

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

const (
	numOfRetry        = 50
	waitTimeInMs      = 400
	waitForESToSettle = 4 * time.Second // wait es shards for some time ensure data consistent
)

type elasticsearchIntegrationSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	IntegrationBase
	esClient esclient.IntegrationTestsClient

	testSearchAttributeKey string
	testSearchAttributeVal string
}

// This cluster use customized threshold for history config
func (s *elasticsearchIntegrationSuite) SetupSuite() {
	s.setupSuite("testdata/integration_elasticsearch_cluster.yaml")
	s.esClient = CreateESClient(s.Suite, s.testClusterConfig.ESConfig, s.Logger)
	PutIndexTemplate(s.Suite, s.esClient, fmt.Sprintf("testdata/es_%s_index_template.json", s.testClusterConfig.ESConfig.Version), "test-visibility-template")
	indexName := s.testClusterConfig.ESConfig.GetVisibilityIndex()
	CreateIndex(s.Suite, s.esClient, indexName)
	s.putIndexSettings(indexName, defaultTestValueOfESIndexMaxResultWindow)
}

func (s *elasticsearchIntegrationSuite) TearDownSuite() {
	s.tearDownSuite()
	DeleteIndex(s.Suite, s.esClient, s.testClusterConfig.ESConfig.GetVisibilityIndex())
}

func (s *elasticsearchIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.testSearchAttributeKey = "CustomTextField"
	s.testSearchAttributeVal = "test value"
}

func TestElasticsearchIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(elasticsearchIntegrationSuite))
}

func (s *elasticsearchIntegrationSuite) TestListOpenWorkflow() {
	id := "es-integration-start-workflow-test"
	wt := "es-integration-start-workflow-test-type"
	tl := "es-integration-start-workflow-test-taskqueue"
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
	startFilter.EarliestTime = &startTime
	var openExecution *workflowpb.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
		resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: defaultTestValueOfESIndexMaxResultWindow,
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

func (s *elasticsearchIntegrationSuite) TestListWorkflow() {
	id := "es-integration-list-workflow-test"
	wt := "es-integration-list-workflow-test-type"
	tl := "es-integration-list-workflow-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_ExecutionTime() {
	id := "es-integration-list-workflow-execution-time-test"
	wt := "es-integration-list-workflow-execution-time-test-type"
	tl := "es-integration-list-workflow-execution-time-test-taskqueue"

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

func (s *elasticsearchIntegrationSuite) TestListWorkflow_SearchAttribute() {
	id := "es-integration-list-workflow-by-search-attr-test"
	wt := "es-integration-list-workflow-by-search-attr-test-type"
	tl := "es-integration-list-workflow-by-search-attr-test-taskqueue"
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
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
				SearchAttributes: searchAttributes,
			}}}

		return []*commandpb.Command{upsertCommand}, nil
	}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
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
	_, newTask, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		true,
		true,
		0,
		1,
		true,
		nil)
	s.NoError(err)
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
	s.Equal(len(searchAttributes.GetIndexedFields()), len(descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()))
	for attrName, expectedPayload := range searchAttributes.GetIndexedFields() {
		respAttr, ok := descResp.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[attrName]
		s.True(ok)
		s.Equal(expectedPayload.GetData(), respAttr.GetData())
		attrType, typeSet := respAttr.GetMetadata()[searchattribute.MetadataType]
		s.True(typeSet)
		s.True(len(attrType) > 0)
	}
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_PageToken() {
	id := "es-integration-list-workflow-token-test"
	wt := "es-integration-list-workflow-token-test-type"
	tl := "es-integration-list-workflow-token-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	numOfWorkflows := defaultTestValueOfESIndexMaxResultWindow - 1 // == 4
	pageSize := 3

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_SearchAfter() {
	id := "es-integration-list-workflow-searchAfter-test"
	wt := "es-integration-list-workflow-searchAfter-test-type"
	tl := "es-integration-list-workflow-searchAfter-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	numOfWorkflows := defaultTestValueOfESIndexMaxResultWindow + 1 // == 6
	pageSize := 4

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_OrQuery() {
	id := "es-integration-list-workflow-or-query-test"
	wt := "es-integration-list-workflow-or-query-test-type"
	tl := "es-integration-list-workflow-or-query-test-taskqueue"
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
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
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
	s.True(!openExecution.GetExecutionTime().Before(*openExecution.GetStartTime()))
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

func (s *elasticsearchIntegrationSuite) TestListWorkflow_KeywordQuery() {
	id := "es-integration-list-workflow-keyword-query-test"
	wt := "es-integration-list-workflow-keyword-query-test-type"
	tl := "es-integration-list-workflow-keyword-query-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": payload.EncodeString("justice for all"),
		},
	}
	request.SearchAttributes = searchAttr
	we1, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	time.Sleep(waitForESToSettle)

	// Exact match Keyword (supported)
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
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
	s.True(!openExecution.GetExecutionTime().Before(*openExecution.GetStartTime()))
	saPayload := openExecution.SearchAttributes.GetIndexedFields()["CustomKeywordField"]
	var saValue string
	err = payload.Decode(saPayload, &saValue)
	s.NoError(err)
	s.Equal("justice for all", saValue)

	// Partial match on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomKeywordField = "justice"`,
	}
	resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// Inordered match on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomKeywordField = "all for justice"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// LIKE exact match on Keyword (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomKeywordField LIKE "%justice for all%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// LIKE %word% on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomKeywordField LIKE "%justice%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// LIKE %chars% on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomKeywordField LIKE "%ice%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// LIKE NOT %chars% on Keyword (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomKeywordField NOT LIKE "%ice%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	executionCount := 0
	for _, execution := range resp.GetExecutions() {
		saPayload := execution.SearchAttributes.GetIndexedFields()["CustomKeywordField"]
		var saValue string
		err = payload.Decode(saPayload, &saValue)
		s.NoError(err)
		if strings.Contains(saValue, "ice") {
			executionCount++ // execution will be found because NOT LIKE is not supported.
		}
	}
	s.Equal(executionCount, 1)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_StringQuery() {
	id := "es-integration-list-workflow-string-query-test"
	wt := "es-integration-list-workflow-string-query-test-type"
	tl := "es-integration-list-workflow-string-query-test-taskqueue"
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
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
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
	s.True(!openExecution.GetExecutionTime().Before(*openExecution.GetStartTime()))
	saPayload := openExecution.SearchAttributes.GetIndexedFields()["CustomTextField"]
	var saValue string
	err = payload.Decode(saPayload, &saValue)
	s.NoError(err)
	s.Equal("nothing else matters", saValue)

	// Partial match on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomTextField = "nothing"`,
	}
	resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// Inordered match on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomTextField = "else nothing matters"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// LIKE %word% on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomTextField LIKE "%else%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// LIKE word on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomTextField LIKE "else"`, // Same as previous because % just removed for LIKE queries.
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 1)

	// LIKE %chars% on String (not supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomTextField LIKE "%ls%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	s.Len(resp.GetExecutions(), 0)

	// LIKE NOT %word% on String (supported)
	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     `CustomTextField NOT LIKE "%else%"`,
	}
	resp, err = s.engine.ListWorkflowExecutions(NewContext(), listRequest)
	s.NoError(err)
	executionCount := 0
	for _, execution := range resp.GetExecutions() {
		saPayload := execution.SearchAttributes.GetIndexedFields()["CustomTextField"]
		var saValue string
		err = payload.Decode(saPayload, &saValue)
		s.NoError(err)
		if strings.Contains(saValue, "else") {
			executionCount++
		}
	}
	s.Equal(executionCount, 0)
}

// To test last page search trigger max window size error
func (s *elasticsearchIntegrationSuite) TestListWorkflow_MaxWindowSize() {
	id := "es-integration-list-workflow-max-window-size-test"
	wt := "es-integration-list-workflow-max-window-size-test-type"
	tl := "es-integration-list-workflow-max-window-size-test-taskqueue"
	startRequest := s.createStartWorkflowExecutionRequest(id, wt, tl)

	for i := 0; i < defaultTestValueOfESIndexMaxResultWindow; i++ {
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
		PageSize:      int32(defaultTestValueOfESIndexMaxResultWindow),
		NextPageToken: nextPageToken,
		Query:         fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = "Running"`, wt),
	}
	// get first page
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == defaultTestValueOfESIndexMaxResultWindow {
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

func (s *elasticsearchIntegrationSuite) TestListWorkflow_OrderBy() {
	id := "es-integration-list-workflow-order-by-test"
	wt := "es-integration-list-workflow-order-by-test-type"
	tl := "es-integration-list-workflow-order-by-test-taskqueue"
	startRequest := s.createStartWorkflowExecutionRequest(id, wt, tl)

	for i := 0; i < defaultTestValueOfESIndexMaxResultWindow+1; i++ { // start 6
		startRequest.RequestId = uuid.New()
		startRequest.WorkflowId = id + strconv.Itoa(i)

		if i < defaultTestValueOfESIndexMaxResultWindow-1 { // 4 workflow has search attr
			intVal, _ := payload.Encode(i)
			doubleVal, _ := payload.Encode(float64(i))
			strVal, _ := payload.Encode(strconv.Itoa(i))
			timeVal, _ := payload.Encode(time.Now().UTC())
			searchAttr := &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomIntField":      intVal,
					"CustomDoubleField":   doubleVal,
					"CustomKeywordField":  strVal,
					"CustomDatetimeField": timeVal,
				},
			}
			startRequest.SearchAttributes = searchAttr
		} else {
			startRequest.SearchAttributes = &commonpb.SearchAttributes{}
		}

		_, err := s.engine.StartWorkflowExecution(NewContext(), startRequest)
		s.NoError(err)
	}

	time.Sleep(waitForESToSettle)

	desc := "desc"
	asc := "asc"
	queryTemplate := `WorkflowType = "%s" order by %s %s`
	pageSize := int32(defaultTestValueOfESIndexMaxResultWindow)

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
			var v1, v2 interface{}
			switch searchAttrKey {
			case "CustomIntField":
				v1, _ = prevVal.(json.Number).Int64()
				v2, _ = currVal.(json.Number).Int64()
				s.True(v1.(int64) >= v2.(int64))
			case "CustomDoubleField":
				v1, _ = prevVal.(json.Number).Float64()
				v2, _ = currVal.(json.Number).Float64()
				s.True(v1.(float64) >= v2.(float64))
			case "CustomKeywordField":
				s.True(prevVal.(string) >= currVal.(string))
			case "CustomDatetimeField":
				v1, _ = time.Parse(time.RFC3339Nano, prevVal.(string))
				v2, _ = time.Parse(time.RFC3339Nano, currVal.(string))
				s.True(v1.(time.Time).After(v2.(time.Time)))
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

func (s *elasticsearchIntegrationSuite) testListWorkflowHelper(numOfWorkflows, pageSize int,
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

func (s *elasticsearchIntegrationSuite) testHelperForReadOnce(expectedRunID string, query string, isScan bool) {
	var openExecution *workflowpb.WorkflowExecutionInfo
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
		Query:     query,
	}
	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  defaultTestValueOfESIndexMaxResultWindow,
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
	s.True(!openExecution.GetExecutionTime().Before(*openExecution.GetStartTime()))
	if openExecution.SearchAttributes != nil && len(openExecution.SearchAttributes.GetIndexedFields()) > 0 {
		searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey]
		var searchVal string
		payload.Decode(searchValBytes, &searchVal)
		s.Equal(s.testSearchAttributeVal, searchVal)
	}
}

func (s *elasticsearchIntegrationSuite) TestScanWorkflow() {
	id := "es-integration-scan-workflow-test"
	wt := "es-integration-scan-workflow-test-type"
	tl := "es-integration-scan-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, true)
}

func (s *elasticsearchIntegrationSuite) TestScanWorkflow_SearchAttribute() {
	id := "es-integration-scan-workflow-search-attr-test"
	wt := "es-integration-scan-workflow-search-attr-test-type"
	tl := "es-integration-scan-workflow-search-attr-test-taskqueue"
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

func (s *elasticsearchIntegrationSuite) TestScanWorkflow_PageToken() {
	id := "es-integration-scan-workflow-token-test"
	wt := "es-integration-scan-workflow-token-test-type"
	tl := "es-integration-scan-workflow-token-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           s.namespace,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	numOfWorkflows := 4
	pageSize := 3

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, true)
}

func (s *elasticsearchIntegrationSuite) TestCountWorkflow() {
	id := "es-integration-count-workflow-test"
	wt := "es-integration-count-workflow-test-type"
	tl := "es-integration-count-workflow-test-taskqueue"
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

func (s *elasticsearchIntegrationSuite) createStartWorkflowExecutionRequest(id, wt, tl string) *workflowservice.StartWorkflowExecutionRequest {
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}
	return request
}

func (s *elasticsearchIntegrationSuite) TestUpsertWorkflowExecution() {
	id := "es-integration-upsert-workflow-test"
	wt := "es-integration-upsert-workflow-test-type"
	tl := "es-integration-upsert-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	commandCount := 0
	wtHandler := func(
		execution *commonpb.WorkflowExecution,
		wt *commonpb.WorkflowType,
		previousStartedEventID,
		startedEventID int64,
		history *historypb.History,
	) ([]*commandpb.Command, error) {

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
	_, newTask, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		true,
		true,
		0,
		1,
		true,
		nil)
	s.NoError(err)
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
	_, newTask, err = poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		true,
		true,
		0,
		1,
		true,
		nil)
	s.NoError(err)
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
	_, newTask, err = poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		true,
		true,
		0,
		1,
		true,
		nil)
	s.NoError(err)
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
}

func (s *elasticsearchIntegrationSuite) testListResultForUpsertSearchAttributes(listRequest *workflowservice.ListWorkflowExecutionsRequest) {
	verified := false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(NewContext(), listRequest)
		s.NoError(err)
		if len(resp.GetExecutions()) == 1 {
			s.Nil(resp.NextPageToken)
			execution := resp.GetExecutions()[0]
			retrievedSearchAttr := execution.SearchAttributes
			if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) == 4 {
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

				verified = true
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)
}

func (s *elasticsearchIntegrationSuite) createSearchAttributes() *commonpb.SearchAttributes {
	searchAttributes, err := searchattribute.Encode(map[string]interface{}{
		"CustomTextField":               "another string",
		"CustomIntField":                123,
		"CustomDoubleField":             22.0878,
		searchattribute.BinaryChecksums: []string{"binary-v1", "binary-v2"},
	}, nil)
	s.NoError(err)
	return searchAttributes
}

func (s *elasticsearchIntegrationSuite) TestUpsertWorkflowExecution_InvalidKey() {
	id := "es-integration-upsert-workflow-failed-test"
	wt := "es-integration-upsert-workflow-failed-test-type"
	tl := "es-integration-upsert-workflow-failed-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadSearchAttributes: search attribute INVALIDKEY is not defined", err.Error())

	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	history := historyResponse.History
	workflowTaskFailedEvent := history.GetEvents()[3]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, workflowTaskFailedEvent.GetEventType())
	failedEventAttr := workflowTaskFailedEvent.GetWorkflowTaskFailedEventAttributes()
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, failedEventAttr.GetCause())
	s.NotNil(failedEventAttr.GetFailure())
}

func (s *elasticsearchIntegrationSuite) Test_LongWorkflowID() {
	if s.testClusterConfig.Persistence.StoreType == config.StoreTypeSQL {
		// TODO: remove this when workflow_id field size is increased from varchar(255) in SQL schema.
		return
	}

	id := strings.Repeat("a", 1000)
	wt := "es-integration-long-workflow-id-test-type"
	tl := "es-integration-long-workflow-id-test-taskqueue"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	query := fmt.Sprintf(`WorkflowId = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *elasticsearchIntegrationSuite) putIndexSettings(indexName string, maxResultWindowSize int) {
	acknowledged, err := s.esClient.IndexPutSettings(
		context.Background(),
		indexName,
		fmt.Sprintf(`{"max_result_window" : %d}`, defaultTestValueOfESIndexMaxResultWindow))
	s.Require().NoError(err)
	s.Require().True(acknowledged)
	s.verifyMaxResultWindowSize(indexName, defaultTestValueOfESIndexMaxResultWindow)
}

func (s *elasticsearchIntegrationSuite) verifyMaxResultWindowSize(indexName string, targetSize int) {
	for i := 0; i < numOfRetry; i++ {
		settings, err := s.esClient.IndexGetSettings(context.Background(), indexName)
		s.Require().NoError(err)
		if settings[indexName].Settings["index"].(map[string]interface{})["max_result_window"].(string) == strconv.Itoa(targetSize) {
			return
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.FailNow(fmt.Sprintf("ES max result window size hasn't reach target size within %v", (numOfRetry*waitTimeInMs)*time.Millisecond))
}
