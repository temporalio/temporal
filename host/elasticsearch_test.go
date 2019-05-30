// Copyright (c) 2016 Uber Technologies, Inc.
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
//+build esintegration

// to run locally, make sure kafka and es is running,
// then run cmd `go test -v ./host -run TestElasticsearchIntegrationSuite -tags esintegration`
package host

import (
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
)

const (
	numOfRetry   = 50
	waitTimeInMs = 400
)

type elasticsearchIntegrationSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	IntegrationBase
	esClient *elastic.Client

	testSearchAttributeKey string
	testSearchAttributeVal string
}

// This cluster use customized threshold for history config
func (s *elasticsearchIntegrationSuite) SetupSuite() {
	s.setupSuite("testdata/integration_elasticsearch_cluster.yaml")
	s.esClient = CreateESClient(s.Suite, s.testClusterConfig.ESConfig.URL.String())
	PutIndexTemplate(s.Suite, s.esClient, "testdata/es_index_template.json", "test-visibility-template")
	CreateIndex(s.Suite, s.esClient, s.testClusterConfig.ESConfig.Indices[common.VisibilityAppName])
}

func (s *elasticsearchIntegrationSuite) TearDownSuite() {
	s.tearDownSuite()
	DeleteIndex(s.Suite, s.esClient, s.testClusterConfig.ESConfig.Indices[common.VisibilityAppName])
}

func (s *elasticsearchIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.testSearchAttributeKey = definition.CustomStringField
	s.testSearchAttributeVal = "test value"
}

func TestElasticsearchIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(elasticsearchIntegrationSuite))
}

func (s *elasticsearchIntegrationSuite) TestListOpenWorkflow() {
	id := "es-integration-start-workflow-test"
	wt := "es-integration-start-workflow-test-type"
	tl := "es-integration-start-workflow-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	startTime := time.Now().UnixNano()
	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	startFilter := &workflow.StartTimeFilter{}
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	var openExecution *workflow.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())
		resp, err := s.engine.ListOpenWorkflowExecutions(createContext(), &workflow.ListOpenWorkflowExecutionsRequest{
			Domain:          common.StringPtr(s.domainName),
			MaximumPageSize: common.Int32Ptr(100),
			StartTimeFilter: startFilter,
			ExecutionFilter: &workflow.WorkflowExecutionFilter{
				WorkflowId: common.StringPtr(id),
			},
		})
		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(we.GetRunId(), openExecution.GetExecution().GetRunId())
	s.Equal(attrValBytes, openExecution.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey])
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow() {
	id := "es-integration-list-workflow-test"
	wt := "es-integration-list-workflow-test-type"
	tl := "es-integration-list-workflow-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	query := fmt.Sprintf(`WorkflowID = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_ExecutionTime() {
	id := "es-integration-list-workflow-execution-time-test"
	wt := "es-integration-list-workflow-execution-time-test-type"
	tl := "es-integration-list-workflow-execution-time-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	cronID := id + "-cron"
	request.CronSchedule = common.StringPtr("@every 1m")
	request.WorkflowId = common.StringPtr(cronID)

	weCron, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	query := fmt.Sprintf(`(WorkflowID = "%s" or WorkflowID = "%s") and ExecutionTime < %v`, id, cronID, time.Now().UnixNano()+int64(time.Minute))
	s.testHelperForReadOnce(weCron.GetRunId(), query, false)

	query = fmt.Sprintf(`WorkflowID = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_SearchAttribute() {
	id := "es-integration-list-workflow-by-search-attr-test"
	wt := "es-integration-list-workflow-by-search-attr-test-type"
	tl := "es-integration-list-workflow-by-search-attr-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)
	query := fmt.Sprintf(`WorkflowID = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	s.testHelperForReadOnce(we.GetRunId(), query, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_PageToken() {
	id := "es-integration-list-workflow-token-test"
	wt := "es-integration-list-workflow-token-test-type"
	tl := "es-integration-list-workflow-token-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	numOfWorkflows := defaultTestValueOfESIndexMaxResultWindow - 1 // == 4
	pageSize := 3

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_SearchAfter() {
	id := "es-integration-list-workflow-searchAfter-test"
	wt := "es-integration-list-workflow-searchAfter-test-type"
	tl := "es-integration-list-workflow-searchAfter-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	numOfWorkflows := defaultTestValueOfESIndexMaxResultWindow + 1 // == 6
	pageSize := 4

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, false)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_OrQuery() {
	id := "es-integration-list-workflow-or-query-test"
	wt := "es-integration-list-workflow-or-query-test-type"
	tl := "es-integration-list-workflow-or-query-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	// start 3 workflows
	key := definition.CustomIntField
	attrValBytes, _ := json.Marshal(1)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			key: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr
	we1, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	request.RequestId = common.StringPtr(uuid.New())
	request.WorkflowId = common.StringPtr(id + "-2")
	attrValBytes, _ = json.Marshal(2)
	searchAttr.IndexedFields[key] = attrValBytes
	we2, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	request.RequestId = common.StringPtr(uuid.New())
	request.WorkflowId = common.StringPtr(id + "-3")
	attrValBytes, _ = json.Marshal(3)
	searchAttr.IndexedFields[key] = attrValBytes
	we3, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	// query 1 workflow with search attr
	query1 := fmt.Sprintf(`CustomIntField = %d`, 1)
	var openExecution *workflow.WorkflowExecutionInfo
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.domainName),
		PageSize: common.Int32Ptr(100),
		Query:    common.StringPtr(query1),
	}
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(we1.GetRunId(), openExecution.GetExecution().GetRunId())
	s.True(openExecution.GetExecutionTime() >= openExecution.GetStartTime())
	searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[key]
	var searchVal int
	json.Unmarshal(searchValBytes, &searchVal)
	s.Equal(1, searchVal)

	// query with or clause
	query2 := fmt.Sprintf(`CustomIntField = %d or CustomIntField = %d`, 1, 2)
	listRequest.Query = common.StringPtr(query2)
	var openExecutions []*workflow.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
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
	json.Unmarshal(searchValBytes, &searchVal)
	s.Equal(2, searchVal)

	// query for open
	query3 := fmt.Sprintf(`(CustomIntField = %d or CustomIntField = %d) and CloseTime = missing`, 2, 3)
	listRequest.Query = common.StringPtr(query3)
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
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
	json.Unmarshal(searchValBytes, &searchVal)
	s.Equal(3, searchVal)
}

func (s *elasticsearchIntegrationSuite) testListWorkflowHelper(numOfWorkflows, pageSize int,
	startRequest *workflow.StartWorkflowExecutionRequest, wid, wType string, isScan bool) {

	// start enough number of workflows
	for i := 0; i < numOfWorkflows; i++ {
		startRequest.RequestId = common.StringPtr(uuid.New())
		startRequest.WorkflowId = common.StringPtr(wid + strconv.Itoa(i))
		_, err := s.engine.StartWorkflowExecution(createContext(), startRequest)
		s.Nil(err)
	}

	var openExecutions []*workflow.WorkflowExecutionInfo
	var nextPageToken []byte

	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:        common.StringPtr(s.domainName),
		PageSize:      common.Int32Ptr(int32(pageSize)),
		NextPageToken: nextPageToken,
		Query:         common.StringPtr(fmt.Sprintf(`WorkflowType = '%s' and CloseTime = missing`, wType)),
	}
	// test first page
	for i := 0; i < numOfRetry; i++ {
		var resp *workflow.ListWorkflowExecutionsResponse
		var err error

		if isScan {
			resp, err = s.engine.ScanWorkflowExecutions(createContext(), listRequest)
		} else {
			resp, err = s.engine.ListWorkflowExecutions(createContext(), listRequest)
		}
		s.Nil(err)
		if len(resp.GetExecutions()) == pageSize {
			openExecutions = resp.GetExecutions()
			nextPageToken = resp.NextPageToken
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecutions)
	s.NotNil(nextPageToken)
	s.True(len(nextPageToken) > 0)

	// test last page
	listRequest.NextPageToken = nextPageToken
	inIf := false
	for i := 0; i < numOfRetry; i++ {
		var resp *workflow.ListWorkflowExecutionsResponse
		var err error

		if isScan {
			resp, err = s.engine.ScanWorkflowExecutions(createContext(), listRequest)
		} else {
			resp, err = s.engine.ListWorkflowExecutions(createContext(), listRequest)
		}
		s.Nil(err)
		if len(resp.GetExecutions()) == numOfWorkflows-pageSize {
			inIf = true
			openExecutions = resp.GetExecutions()
			nextPageToken = resp.NextPageToken
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(inIf)
	s.NotNil(openExecutions)
	s.Nil(nextPageToken)
}

func (s *elasticsearchIntegrationSuite) testHelperForReadOnce(runID, query string, isScan bool) {
	var openExecution *workflow.WorkflowExecutionInfo
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.domainName),
		PageSize: common.Int32Ptr(100),
		Query:    common.StringPtr(query),
	}
	for i := 0; i < numOfRetry; i++ {
		var resp *workflow.ListWorkflowExecutionsResponse
		var err error

		if isScan {
			resp, err = s.engine.ScanWorkflowExecutions(createContext(), listRequest)
		} else {
			resp, err = s.engine.ListWorkflowExecutions(createContext(), listRequest)
		}

		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution)
	s.Equal(runID, openExecution.GetExecution().GetRunId())
	s.True(openExecution.GetExecutionTime() >= openExecution.GetStartTime())
	if openExecution.SearchAttributes != nil && len(openExecution.SearchAttributes.GetIndexedFields()) > 0 {
		searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey]
		var searchVal string
		json.Unmarshal(searchValBytes, &searchVal)
		s.Equal(s.testSearchAttributeVal, searchVal)
	}
}

func (s *elasticsearchIntegrationSuite) TestScanWorkflow() {
	id := "es-integration-scan-workflow-test"
	wt := "es-integration-scan-workflow-test-type"
	tl := "es-integration-scan-workflow-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)
	query := fmt.Sprintf(`WorkflowID = "%s"`, id)
	s.testHelperForReadOnce(we.GetRunId(), query, true)
}

func (s *elasticsearchIntegrationSuite) TestScanWorkflow_SearchAttribute() {
	id := "es-integration-scan-workflow-search-attr-test"
	wt := "es-integration-scan-workflow-search-attr-test-type"
	tl := "es-integration-scan-workflow-search-attr-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)
	query := fmt.Sprintf(`WorkflowID = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	s.testHelperForReadOnce(we.GetRunId(), query, true)
}

func (s *elasticsearchIntegrationSuite) TestScanWorkflow_PageToken() {
	id := "es-integration-scan-workflow-token-test"
	wt := "es-integration-scan-workflow-token-test-type"
	tl := "es-integration-scan-workflow-token-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		Domain:                              common.StringPtr(s.domainName),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	numOfWorkflows := 4
	pageSize := 3

	s.testListWorkflowHelper(numOfWorkflows, pageSize, request, id, wt, true)
}

func (s *elasticsearchIntegrationSuite) TestCountWorkflow() {
	id := "es-integration-count-workflow-test"
	wt := "es-integration-count-workflow-test-type"
	tl := "es-integration-count-workflow-test-tasklist"
	request := s.createStartWorkflowExecutionRequest(id, wt, tl)

	attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	request.SearchAttributes = searchAttr

	_, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)

	query := fmt.Sprintf(`WorkflowID = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	countRequest := &workflow.CountWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.domainName),
		Query:  common.StringPtr(query),
	}
	var resp *workflow.CountWorkflowExecutionsResponse
	for i := 0; i < numOfRetry; i++ {
		resp, err = s.engine.CountWorkflowExecutions(createContext(), countRequest)
		s.Nil(err)
		if resp.GetCount() == int64(1) {
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.Equal(int64(1), resp.GetCount())

	query = fmt.Sprintf(`WorkflowID = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, "noMatch")
	countRequest.Query = common.StringPtr(query)
	resp, err = s.engine.CountWorkflowExecutions(createContext(), countRequest)
	s.Nil(err)
	s.Equal(int64(0), resp.GetCount())
}

func (s *elasticsearchIntegrationSuite) createStartWorkflowExecutionRequest(id, wt, tl string) *workflow.StartWorkflowExecutionRequest {
	identity := "worker1"
	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	return request
}
