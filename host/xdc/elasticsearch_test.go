// Copyright (c) 2018 Uber Technologies, Inc.
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

// +build !race
// +build esintegration

// to run locally, make sure kafka and es is running,
// then run cmd `go test -v ./host/xdc -run TestESCrossDCTestSuite -tags esintegration`
package xdc

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/host"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

const (
	numOfRetry   = 50
	waitTimeInMs = 400
)

type esCrossDCTestSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	suite.Suite
	cluster1       *host.TestCluster
	cluster2       *host.TestCluster
	logger         log.Logger
	enableEventsV2 bool
	clusterConfigs []*host.TestClusterConfig
	esClient       *elastic.Client

	testSearchAttributeKey string
	testSearchAttributeVal string
}

func TestESCrossDCTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(esCrossDCTestSuite))
}

var (
	clusterNameES              = []string{"active-es", "standby-es"}
	clusterReplicationConfigES = []*workflow.ClusterReplicationConfiguration{
		{
			ClusterName: common.StringPtr(clusterNameES[0]),
		},
		{
			ClusterName: common.StringPtr(clusterNameES[1]),
		},
	}
)

func (s *esCrossDCTestSuite) SetupSuite() {
	zapLogger, err := zap.NewDevelopment()
	// cannot use s.Nil since it is not initialized
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)

	fileName := "../testdata/xdc_integration_es_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := ioutil.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	s.clusterConfigs = clusterConfigs

	c, err := host.NewCluster(clusterConfigs[0], s.logger.WithTags(tag.ClusterName(clusterNameES[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = host.NewCluster(clusterConfigs[1], s.logger.WithTags(tag.ClusterName(clusterNameES[1])))
	s.Require().NoError(err)
	s.cluster2 = c

	s.esClient = host.CreateESClient(s.Suite, s.clusterConfigs[0].ESConfig.URL.String())
	host.PutIndexTemplate(s.Suite, s.esClient, "../testdata/es_index_template.json", "test-visibility-template")
	host.CreateIndex(s.Suite, s.esClient, s.clusterConfigs[0].ESConfig.Indices[common.VisibilityAppName])
	host.CreateIndex(s.Suite, s.esClient, s.clusterConfigs[1].ESConfig.Indices[common.VisibilityAppName])

	s.testSearchAttributeKey = definition.CustomStringField
	s.testSearchAttributeVal = "test value"
}

func (s *esCrossDCTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *esCrossDCTestSuite) TearDownSuite() {
	s.cluster1.TearDownCluster()
	s.cluster2.TearDownCluster()
	host.DeleteIndex(s.Suite, s.esClient, s.clusterConfigs[0].ESConfig.Indices[common.VisibilityAppName])
	host.DeleteIndex(s.Suite, s.esClient, s.clusterConfigs[1].ESConfig.Indices[common.VisibilityAppName])
}

func (s *esCrossDCTestSuite) TestSearchAttributes() {
	domainName := "test-xdc-search-attr-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Clusters:                               clusterReplicationConfigES,
		ActiveClusterName:                      common.StringPtr(clusterNameES[0]),
		IsGlobalDomain:                         common.BoolPtr(true),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(1),
	}
	err := client1.RegisterDomain(createContext(), regReq)
	s.NoError(err)

	descReq := &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	}
	resp, err := client1.DescribeDomain(createContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for domain cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby
	resp2, err := client2.DescribeDomain(createContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "xdc-search-attr-test-" + uuid.New()
	wt := "xdc-search-attr-test-type"
	tl := "xdc-search-attr-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
		SearchAttributes:                    searchAttr,
	}
	startTime := time.Now().UnixNano()
	we, err := client1.StartWorkflowExecution(createContext(), startReq)
	s.Nil(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	// List workflow in active
	engine1 := s.cluster1.GetFrontendClient()
	startFilter := &workflow.StartTimeFilter{}
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	query := fmt.Sprintf(`WorkflowID = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(domainName),
		PageSize: common.Int32Ptr(100),
		Query:    common.StringPtr(query),
	}
	var openExecution1 *workflow.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

		resp, err := engine1.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution1 = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution1)
	s.Equal(we.GetRunId(), openExecution1.GetExecution().GetRunId())
	searchValBytes1 := openExecution1.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey]
	var searchVal1 string
	json.Unmarshal(searchValBytes1, &searchVal1)
	s.Equal(s.testSearchAttributeVal, searchVal1)

	// List workflow in standby
	engine2 := s.cluster2.GetFrontendClient()
	var openExecution2 *workflow.WorkflowExecutionInfo
	for i := 0; i < numOfRetry; i++ {
		startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

		resp, err := engine2.ListWorkflowExecutions(createContext(), listRequest)
		s.Nil(err)
		if len(resp.GetExecutions()) == 1 {
			openExecution2 = resp.GetExecutions()[0]
			break
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NotNil(openExecution2)
	s.Equal(we.GetRunId(), openExecution2.GetExecution().GetRunId())
	searchValBytes2 := openExecution1.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey]
	var searchVal2 string
	json.Unmarshal(searchValBytes2, &searchVal2)
	s.Equal(s.testSearchAttributeVal, searchVal2)
}
