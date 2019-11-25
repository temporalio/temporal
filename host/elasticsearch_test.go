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
	"bytes"
	"context"
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
	"github.com/uber/cadence/common/log/tag"
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
	esClient *elastic.Client

	testSearchAttributeKey string
	testSearchAttributeVal string
}

// This cluster use customized threshold for history config
func (s *elasticsearchIntegrationSuite) SetupSuite() {
	s.setupSuite("testdata/integration_elasticsearch_cluster.yaml")
	s.esClient = CreateESClient(s.Suite, s.testClusterConfig.ESConfig.URL.String())
	PutIndexTemplate(s.Suite, s.esClient, "testdata/es_index_template.json", "test-visibility-template")
	indexName := s.testClusterConfig.ESConfig.Indices[common.VisibilityAppName]
	CreateIndex(s.Suite, s.esClient, indexName)
	s.putIndexSettings(indexName, defaultTestValueOfESIndexMaxResultWindow)
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
			MaximumPageSize: common.Int32Ptr(defaultTestValueOfESIndexMaxResultWindow),
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

	// test upsert
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		upsertDecision := &workflow.Decision{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeUpsertWorkflowSearchAttributes),
			UpsertWorkflowSearchAttributesDecisionAttributes: &workflow.UpsertWorkflowSearchAttributesDecisionAttributes{
				SearchAttributes: getUpsertSearchAttributes(),
			}}

		return nil, []*workflow.Decision{upsertDecision}, nil
	}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		StickyTaskList:  taskList,
		Identity:        "worker1",
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}
	_, newTask, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		false,
		false,
		true,
		true,
		int64(0),
		1,
		true,
		nil)
	s.Nil(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)

	time.Sleep(waitForESToSettle)

	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.domainName),
		PageSize: common.Int32Ptr(int32(2)),
		Query:    common.StringPtr(fmt.Sprintf(`WorkflowType = '%s' and CloseTime = missing and BinaryChecksums = 'binary-v1'`, wt)),
	}
	// verify upsert data is on ES
	s.testListResultForUpsertSearchAttributes(listRequest)

	// verify DescribeWorkflowExecution
	descRequest := &workflow.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
	}
	descResp, err := s.engine.DescribeWorkflowExecution(createContext(), descRequest)
	s.Nil(err)
	expectedSearchAttributes := getUpsertSearchAttributes()
	s.Equal(expectedSearchAttributes, descResp.WorkflowExecutionInfo.GetSearchAttributes())
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

	time.Sleep(waitForESToSettle)

	// query 1 workflow with search attr
	query1 := fmt.Sprintf(`CustomIntField = %d`, 1)
	var openExecution *workflow.WorkflowExecutionInfo
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.domainName),
		PageSize: common.Int32Ptr(defaultTestValueOfESIndexMaxResultWindow),
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

// To test last page search trigger max window size error
func (s *elasticsearchIntegrationSuite) TestListWorkflow_MaxWindowSize() {
	id := "es-integration-list-workflow-max-window-size-test"
	wt := "es-integration-list-workflow-max-window-size-test-type"
	tl := "es-integration-list-workflow-max-window-size-test-tasklist"
	startRequest := s.createStartWorkflowExecutionRequest(id, wt, tl)

	for i := 0; i < defaultTestValueOfESIndexMaxResultWindow; i++ {
		startRequest.RequestId = common.StringPtr(uuid.New())
		startRequest.WorkflowId = common.StringPtr(id + strconv.Itoa(i))
		_, err := s.engine.StartWorkflowExecution(createContext(), startRequest)
		s.Nil(err)
	}

	time.Sleep(waitForESToSettle)

	var listResp *workflow.ListWorkflowExecutionsResponse
	var nextPageToken []byte

	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:        common.StringPtr(s.domainName),
		PageSize:      common.Int32Ptr(int32(defaultTestValueOfESIndexMaxResultWindow)),
		NextPageToken: nextPageToken,
		Query:         common.StringPtr(fmt.Sprintf(`WorkflowType = '%s' and CloseTime = missing`, wt)),
	}
	// get first page
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
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
	resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
	s.Nil(err)
	s.True(len(resp.GetExecutions()) == 0)
	s.True(len(resp.GetNextPageToken()) == 0)
}

func (s *elasticsearchIntegrationSuite) TestListWorkflow_OrderBy() {
	id := "es-integration-list-workflow-order-by-test"
	wt := "es-integration-list-workflow-order-by-test-type"
	tl := "es-integration-list-workflow-order-by-test-tasklist"
	startRequest := s.createStartWorkflowExecutionRequest(id, wt, tl)

	for i := 0; i < defaultTestValueOfESIndexMaxResultWindow+1; i++ { // start 6
		startRequest.RequestId = common.StringPtr(uuid.New())
		startRequest.WorkflowId = common.StringPtr(id + strconv.Itoa(i))

		if i < defaultTestValueOfESIndexMaxResultWindow-1 { // 4 workflow has search attr
			intVal, _ := json.Marshal(i)
			doubleVal, _ := json.Marshal(float64(i))
			strVal, _ := json.Marshal(strconv.Itoa(i))
			timeVal, _ := json.Marshal(time.Now())
			searchAttr := &workflow.SearchAttributes{
				IndexedFields: map[string][]byte{
					definition.CustomIntField:      intVal,
					definition.CustomDoubleField:   doubleVal,
					definition.CustomKeywordField:  strVal,
					definition.CustomDatetimeField: timeVal,
				},
			}
			startRequest.SearchAttributes = searchAttr
		} else {
			startRequest.SearchAttributes = &workflow.SearchAttributes{}
		}

		_, err := s.engine.StartWorkflowExecution(createContext(), startRequest)
		s.Nil(err)
	}

	time.Sleep(waitForESToSettle)

	desc := "desc"
	asc := "asc"
	queryTemplate := `WorkflowType = "%s" order by %s %s`
	pageSize := int32(defaultTestValueOfESIndexMaxResultWindow)

	// order by CloseTime asc
	query1 := fmt.Sprintf(queryTemplate, wt, definition.CloseTime, asc)
	var openExecutions []*workflow.WorkflowExecutionInfo
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.domainName),
		PageSize: common.Int32Ptr(pageSize),
		Query:    common.StringPtr(query1),
	}
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		if int32(len(resp.GetExecutions())) == listRequest.GetPageSize() {
			openExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecutions)
	for i := int32(1); i < pageSize; i++ {
		s.True(openExecutions[i-1].GetCloseTime() <= openExecutions[i].GetCloseTime())
	}

	// greatest effort to reduce duplicate code
	testHelper := func(query, searchAttrKey string, prevVal, currVal interface{}) {
		listRequest.Query = common.StringPtr(query)
		listRequest.NextPageToken = []byte{}
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		openExecutions = resp.GetExecutions()
		dec := json.NewDecoder(bytes.NewReader(openExecutions[0].GetSearchAttributes().GetIndexedFields()[searchAttrKey]))
		dec.UseNumber()
		err = dec.Decode(&prevVal)
		s.Nil(err)
		for i := int32(1); i < pageSize; i++ {
			indexedFields := openExecutions[i].GetSearchAttributes().GetIndexedFields()
			searchAttrBytes, ok := indexedFields[searchAttrKey]
			if !ok { // last one doesn't have search attr
				s.Equal(pageSize-1, i)
				break
			}
			dec := json.NewDecoder(bytes.NewReader(searchAttrBytes))
			dec.UseNumber()
			err = dec.Decode(&currVal)
			s.Nil(err)
			var v1, v2 interface{}
			switch searchAttrKey {
			case definition.CustomIntField:
				v1, _ = prevVal.(json.Number).Int64()
				v2, _ = currVal.(json.Number).Int64()
				s.True(v1.(int64) >= v2.(int64))
			case definition.CustomDoubleField:
				v1, _ = prevVal.(json.Number).Float64()
				v2, _ = currVal.(json.Number).Float64()
				s.True(v1.(float64) >= v2.(float64))
			case definition.CustomKeywordField:
				s.True(prevVal.(string) >= currVal.(string))
			case definition.CustomDatetimeField:
				v1, _ = time.Parse(time.RFC3339, prevVal.(string))
				v2, _ = time.Parse(time.RFC3339, currVal.(string))
				s.True(v1.(time.Time).After(v2.(time.Time)))
			}
			prevVal = currVal
		}
		listRequest.NextPageToken = resp.GetNextPageToken()
		resp, err = s.engine.ListWorkflowExecutions(createContext(), listRequest) // last page
		s.Nil(err)
		s.Equal(1, len(resp.GetExecutions()))
	}

	// order by CustomIntField desc
	field := definition.CustomIntField
	query := fmt.Sprintf(queryTemplate, wt, field, desc)
	var int1, int2 int
	testHelper(query, field, int1, int2)

	// order by CustomDoubleField desc
	field = definition.CustomDoubleField
	query = fmt.Sprintf(queryTemplate, wt, field, desc)
	var double1, double2 float64
	testHelper(query, field, double1, double2)

	// order by CustomKeywordField desc
	field = definition.CustomKeywordField
	query = fmt.Sprintf(queryTemplate, wt, field, desc)
	var s1, s2 string
	testHelper(query, field, s1, s2)

	// order by CustomDatetimeField desc
	field = definition.CustomDatetimeField
	query = fmt.Sprintf(queryTemplate, wt, field, desc)
	var t1, t2 time.Time
	testHelper(query, field, t1, t2)
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

	time.Sleep(waitForESToSettle)

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
			nextPageToken = resp.GetNextPageToken()
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
			nextPageToken = resp.GetNextPageToken()
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
		PageSize: common.Int32Ptr(defaultTestValueOfESIndexMaxResultWindow),
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

func (s *elasticsearchIntegrationSuite) TestUpsertWorkflowExecution() {
	id := "es-integration-upsert-workflow-test"
	wt := "es-integration-upsert-workflow-test-type"
	tl := "es-integration-upsert-workflow-test-tasklist"
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

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	decisionCount := 0
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		upsertDecision := &workflow.Decision{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeUpsertWorkflowSearchAttributes),
			UpsertWorkflowSearchAttributesDecisionAttributes: &workflow.UpsertWorkflowSearchAttributesDecisionAttributes{}}

		// handle first upsert
		if decisionCount == 0 {
			decisionCount++

			attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
			upsertSearchAttr := &workflow.SearchAttributes{
				IndexedFields: map[string][]byte{
					s.testSearchAttributeKey: attrValBytes,
				},
			}
			upsertDecision.UpsertWorkflowSearchAttributesDecisionAttributes.SearchAttributes = upsertSearchAttr
			return nil, []*workflow.Decision{upsertDecision}, nil
		}
		// handle second upsert, which update existing field and add new field
		if decisionCount == 1 {
			decisionCount++
			upsertDecision.UpsertWorkflowSearchAttributesDecisionAttributes.SearchAttributes = getUpsertSearchAttributes()
			return nil, []*workflow.Decision{upsertDecision}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		StickyTaskList:  taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// process 1st decision and assert decision is handled correctly.
	_, newTask, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		false,
		false,
		true,
		true,
		int64(0),
		1,
		true,
		nil)
	s.Nil(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)
	s.Equal(int64(3), newTask.DecisionTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.DecisionTask.GetStartedEventId())
	s.Equal(4, len(newTask.DecisionTask.History.Events))
	s.Equal(workflow.EventTypeDecisionTaskCompleted, newTask.DecisionTask.History.Events[0].GetEventType())
	s.Equal(workflow.EventTypeUpsertWorkflowSearchAttributes, newTask.DecisionTask.History.Events[1].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, newTask.DecisionTask.History.Events[2].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskStarted, newTask.DecisionTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

	// verify upsert data is on ES
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.domainName),
		PageSize: common.Int32Ptr(int32(2)),
		Query:    common.StringPtr(fmt.Sprintf(`WorkflowType = '%s' and CloseTime = missing`, wt)),
	}
	verified := false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			execution := resp.GetExecutions()[0]
			retrievedSearchAttr := execution.SearchAttributes
			if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) > 0 {
				searchValBytes := retrievedSearchAttr.GetIndexedFields()[s.testSearchAttributeKey]
				var searchVal string
				json.Unmarshal(searchValBytes, &searchVal)
				s.Equal(s.testSearchAttributeVal, searchVal)
				verified = true
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)

	// process 2nd decision and assert decision is handled correctly.
	_, newTask, err = poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		false,
		false,
		true,
		true,
		int64(0),
		1,
		true,
		nil)
	s.Nil(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)
	s.Equal(4, len(newTask.DecisionTask.History.Events))
	s.Equal(workflow.EventTypeDecisionTaskCompleted, newTask.DecisionTask.History.Events[0].GetEventType())
	s.Equal(workflow.EventTypeUpsertWorkflowSearchAttributes, newTask.DecisionTask.History.Events[1].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, newTask.DecisionTask.History.Events[2].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskStarted, newTask.DecisionTask.History.Events[3].GetEventType())

	time.Sleep(waitForESToSettle)

	// verify upsert data is on ES
	s.testListResultForUpsertSearchAttributes(listRequest)
}

func (s *elasticsearchIntegrationSuite) testListResultForUpsertSearchAttributes(listRequest *workflow.ListWorkflowExecutionsRequest) {
	verified := false
	for i := 0; i < numOfRetry; i++ {
		resp, err := s.engine.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			execution := resp.GetExecutions()[0]
			retrievedSearchAttr := execution.SearchAttributes
			if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) == 3 {
				fields := retrievedSearchAttr.GetIndexedFields()
				searchValBytes := fields[s.testSearchAttributeKey]
				var searchVal string
				err := json.Unmarshal(searchValBytes, &searchVal)
				s.Nil(err)
				s.Equal("another string", searchVal)

				searchValBytes2 := fields[definition.CustomIntField]
				var searchVal2 int
				err = json.Unmarshal(searchValBytes2, &searchVal2)
				s.Nil(err)
				s.Equal(123, searchVal2)

				binaryChecksumsBytes := fields[definition.BinaryChecksums]
				var binaryChecksums []string
				err = json.Unmarshal(binaryChecksumsBytes, &binaryChecksums)
				s.Nil(err)
				s.Equal([]string{"binary-v1", "binary-v2"}, binaryChecksums)

				verified = true
				break
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.True(verified)
}

func getUpsertSearchAttributes() *workflow.SearchAttributes {
	attrValBytes1, _ := json.Marshal("another string")
	attrValBytes2, _ := json.Marshal(123)
	binaryChecksums, _ := json.Marshal([]string{"binary-v1", "binary-v2"})
	upsertSearchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			definition.CustomStringField: attrValBytes1,
			definition.CustomIntField:    attrValBytes2,
			definition.BinaryChecksums:   binaryChecksums,
		},
	}
	return upsertSearchAttr
}

func (s *elasticsearchIntegrationSuite) TestUpsertWorkflowExecution_InvalidKey() {
	id := "es-integration-upsert-workflow-failed-test"
	wt := "es-integration-upsert-workflow-failed-test-type"
	tl := "es-integration-upsert-workflow-failed-test-tasklist"
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

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		upsertDecision := &workflow.Decision{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeUpsertWorkflowSearchAttributes),
			UpsertWorkflowSearchAttributesDecisionAttributes: &workflow.UpsertWorkflowSearchAttributesDecisionAttributes{
				SearchAttributes: &workflow.SearchAttributes{
					IndexedFields: map[string][]byte{
						"INVALIDKEY": []byte(`1`),
					},
				},
			}}
		return nil, []*workflow.Decision{upsertDecision}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		StickyTaskList:  taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Nil(err)

	historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(s.domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
	})
	s.Nil(err)
	history := historyResponse.History
	decisionFailedEvent := history.GetEvents()[3]
	s.Equal(workflow.EventTypeDecisionTaskFailed, decisionFailedEvent.GetEventType())
	failedDecisionAttr := decisionFailedEvent.DecisionTaskFailedEventAttributes
	s.Equal(workflow.DecisionTaskFailedCauseBadSearchAttributes, failedDecisionAttr.GetCause())
	s.True(len(failedDecisionAttr.GetDetails()) > 0)
}

func (s *elasticsearchIntegrationSuite) putIndexSettings(indexName string, maxResultWindowSize int) {
	_, err := s.esClient.IndexPutSettings(indexName).
		BodyString(fmt.Sprintf(`{"max_result_window" : %d}`, defaultTestValueOfESIndexMaxResultWindow)).
		Do(context.Background())
	s.Require().NoError(err)
	s.verifyMaxResultWindowSize(indexName, defaultTestValueOfESIndexMaxResultWindow)
}

func (s *elasticsearchIntegrationSuite) verifyMaxResultWindowSize(indexName string, targetSize int) {
	for i := 0; i < numOfRetry; i++ {
		settings, err := s.esClient.IndexGetSettings(indexName).Do(context.Background())
		s.Require().NoError(err)
		if settings[indexName].Settings["index"].(map[string]interface{})["max_result_window"].(string) == strconv.Itoa(targetSize) {
			return
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.FailNow(fmt.Sprintf("ES max result window size hasn't reach target size within %v.", (numOfRetry*waitTimeInMs)*time.Millisecond))
}
