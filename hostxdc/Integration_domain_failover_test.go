// Copyright (c) 2017 Uber Technologies, Inc.
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
// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package hostxdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	wsc "github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/host"
	"go.uber.org/zap"
)

type (
	integrationClustersTestSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
		cluster1 *testCluster
		cluster2 *testCluster
		logger   bark.Logger
	}

	testCluster struct {
		persistence.TestBase
		host   host.Cadence
		engine wsc.Interface
		logger bark.Logger
	}
)

const (
	testNumberOfHistoryShards = 4
	testNumberOfHistoryHosts  = 1
)

var (
	integration  = flag.Bool("integration2", true, "run integration tests")
	domainName   = "integration-cross-dc-test-domain"
	clusterName  = []string{"active", "standby"}
	topicName    = []string{"active", "standby"}
	clustersInfo = []*config.ClustersInfo{
		{
			EnableGlobalDomain:             true,
			FailoverVersionIncrement:       10,
			MasterClusterName:              clusterName[0],
			CurrentClusterName:             clusterName[0],
			ClusterInitialFailoverVersions: map[string]int64{clusterName[0]: 0, clusterName[1]: 1},
		},
		{
			EnableGlobalDomain:             true,
			FailoverVersionIncrement:       10,
			MasterClusterName:              clusterName[0],
			CurrentClusterName:             clusterName[1],
			ClusterInitialFailoverVersions: map[string]int64{clusterName[0]: 0, clusterName[1]: 1},
		},
	}
	clusterReplicationConfig = []*workflow.ClusterReplicationConfiguration{
		{
			ClusterName: common.StringPtr(clusterName[0]),
		},
		{
			ClusterName: common.StringPtr(clusterName[1]),
		},
	}
)

func (s *integrationClustersTestSuite) newTestCluster(no int) *testCluster {
	c := &testCluster{logger: s.logger.WithField("Cluster", clusterName[no])}
	c.setupCluster(no)
	return c
}

func (s *testCluster) setupCluster(no int) {
	options := persistence.TestBaseOptions{}
	options.ClusterHost = "127.0.0.1"
	options.KeySpace = "integration_" + clusterName[no]
	options.DropKeySpace = true
	options.SchemaDir = ".."
	clusterInfo := clustersInfo[no]
	metadata := cluster.NewMetadata(
		dynamicconfig.GetBoolPropertyFn(clusterInfo.EnableGlobalDomain),
		clusterInfo.FailoverVersionIncrement,
		clusterInfo.MasterClusterName,
		clusterInfo.CurrentClusterName,
		clusterInfo.ClusterInitialFailoverVersions,
	)
	s.SetupWorkflowStoreWithOptions(options, metadata)
	s.setupShards()
	messagingClient := s.createMessagingClient()
	testNumberOfHistoryShards := 1 // use 1 shard so we can be sure when failover completed in standby cluster
	s.host = host.NewCadence(s.ClusterMetadata, messagingClient, s.MetadataProxy, s.MetadataManagerV2, s.ShardMgr, s.HistoryMgr, s.ExecutionMgrFactory, s.TaskMgr,
		s.VisibilityMgr, testNumberOfHistoryShards, testNumberOfHistoryHosts, s.logger, no, true)
	s.host.Start()
}

func (s *testCluster) tearDownCluster() {
	s.host.Stop()
	s.host = nil
	s.TearDownWorkflowStore()
}

func (s *testCluster) createMessagingClient() messaging.Client {
	clusters := make(map[string]messaging.ClusterConfig)
	clusters["test"] = messaging.ClusterConfig{
		Brokers: []string{"127.0.0.1:9092"},
	}
	topics := make(map[string]messaging.TopicConfig)
	topics[topicName[0]] = messaging.TopicConfig{
		Cluster: "test",
	}
	topics[topicName[1]] = messaging.TopicConfig{
		Cluster: "test",
	}
	topics[topicName[0]+"-dlq"] = messaging.TopicConfig{
		Cluster: "test",
	}
	topics[topicName[1]+"-dlq"] = messaging.TopicConfig{
		Cluster: "test",
	}
	topics[topicName[0]+"-retry"] = messaging.TopicConfig{
		Cluster: "test",
	}
	topics[topicName[1]+"-retry"] = messaging.TopicConfig{
		Cluster: "test",
	}
	clusterToTopic := make(map[string]messaging.TopicList)
	clusterToTopic[clusterName[0]] = getTopicList(topicName[0])
	clusterToTopic[clusterName[1]] = getTopicList(topicName[1])
	kafkaConfig := messaging.KafkaConfig{
		Clusters:       clusters,
		Topics:         topics,
		ClusterToTopic: clusterToTopic,
	}
	return messaging.NewKafkaClient(&kafkaConfig, zap.NewNop(), s.logger, tally.NoopScope)
}

func getTopicList(topicName string) messaging.TopicList {
	return messaging.TopicList{
		Topic:      topicName,
		RetryTopic: topicName + "-retry",
		DLQTopic:   topicName + "-dlq",
	}
}

func (s *testCluster) setupShards() {
	// shard 0 is always created, we create additional shards if needed
	for shardID := 1; shardID < testNumberOfHistoryShards; shardID++ {
		err := s.CreateShard(shardID, "", 0)
		if err != nil {
			s.logger.WithField("error", err).Fatal("Failed to create shard")
		}
	}
}

func TestIntegrationClustersTestSuite(t *testing.T) {
	flag.Parse()
	if *integration {
		s := new(integrationClustersTestSuite)
		suite.Run(t, s)
	} else {
		t.Skip()
	}
}

func (s *integrationClustersTestSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	logger := log.New()
	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	logger.Formatter = formatter
	//logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)

	s.cluster1 = s.newTestCluster(0)
	s.cluster2 = s.newTestCluster(1)
}

func (s *integrationClustersTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *integrationClustersTestSuite) TearDownSuite() {
	s.cluster1.tearDownCluster()
	s.cluster2.tearDownCluster()
}

func (s *integrationClustersTestSuite) TestDomainFailover() {
	domainName := "test-domain-for-fail-over-" + common.GenerateRandomString(5)
	client1 := s.cluster1.host.GetFrontendClient() // active
	regReq := &workflow.RegisterDomainRequest{
		Name:              common.StringPtr(domainName),
		Clusters:          clusterReplicationConfig,
		ActiveClusterName: common.StringPtr(clusterName[0]),
	}
	err := client1.RegisterDomain(createContext(), regReq)
	s.NoError(err)

	descReq := &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	}
	resp, err := client1.DescribeDomain(createContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.host.GetFrontendClient() // standby
	var resp2 *workflow.DescribeDomainResponse
	for i := 0; i < 30; i++ { // retry to wait domain been replicated to cluster2
		if resp2, err = client2.DescribeDomain(createContext(), descReq); err != nil {
			s.Equal(&workflow.EntityNotExistsError{Message: "Domain " + domainName + " does not exist."}, err)
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// update domain to fail over
	updateReq := &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(clusterName[1]),
		},
	}
	updateResp, err := client1.UpdateDomain(createContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	updated := false
	var resp3 *workflow.DescribeDomainResponse
	for i := 0; i < 30; i++ {
		resp3, err = client2.DescribeDomain(createContext(), descReq)
		s.NoError(err)
		if resp3.ReplicationConfiguration.GetActiveClusterName() == clusterName[1] {
			updated = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.True(updated)
	s.NotNil(resp3)
	s.Equal(int64(1), resp3.GetFailoverVersion())

	// start workflow in new cluster
	id := "integration-domain-failover-test"
	wt := "integration-domain-failover-test-type"
	tl := "integration-domain-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	var we *workflow.StartWorkflowExecutionResponse
	for i := 0; i < 30; i++ {
		we, err = client2.StartWorkflowExecution(createContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())
}

func (s *integrationClustersTestSuite) TestSimpleWorkflowFailover() {
	domainName := "test-simple-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.host.GetFrontendClient() // active
	regReq := &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Clusters:                               clusterReplicationConfig,
		ActiveClusterName:                      common.StringPtr(clusterName[0]),
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

	client2 := s.cluster2.host.GetFrontendClient() // standby
	var resp2 *workflow.DescribeDomainResponse
	for i := 0; i < 30; i++ { // retry to wait domain been replicated to cluster2
		if resp2, err = client2.DescribeDomain(createContext(), descReq); err != nil {
			s.Equal(&workflow.EntityNotExistsError{Message: "Domain " + domainName + " does not exist."}, err)
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "integration-simple-workflow-failover-test"
	wt := "integration-simple-workflow-failover-test-type"
	tl := "integration-simple-workflow-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	var we *workflow.StartWorkflowExecutionResponse
	for i := 0; i < 30; i++ {
		we, err = client1.StartWorkflowExecution(createContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())
	rid := we.GetRunId()

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityName := "activity_type1"
	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := host.TaskPoller{
		Engine:          client1,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// make some progress in cluster 1
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// update domain to fail over
	updateReq := &workflow.UpdateDomainRequest{
		Name: common.StringPtr(domainName),
		ReplicationConfiguration: &workflow.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(clusterName[1]),
		},
	}
	updateResp, err := client1.UpdateDomain(createContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfiguration.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// wait till failover completed
	failoverDone := false
	for i := 0; i < 15; i++ {
		we, err = client2.StartWorkflowExecution(createContext(), startReq)
		if _, failoverDone = err.(*workflow.WorkflowExecutionAlreadyStartedError); failoverDone {
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.True(failoverDone)

	// check history matched
	getHistoryReq := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(rid),
		},
	}
	var historyResponse *workflow.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
	for i := 0; i < 15; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 5 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.Nil(err)
	s.True(eventsReplicated)

	// make process in cluster 2
	err = poller2.PollAndProcessActivityTask(false)
	s.logger.Infof("PollAndProcessActivityTask 2: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	_, err = poller2.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask 2: %v", err)
	s.Nil(err)
	s.True(workflowComplete)

	// check history replicated in cluster 1
	eventsReplicated = false
	for i := 0; i < 15; i++ {
		historyResponse, err = client1.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 11 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.Nil(err)
}

func createContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 90*time.Second)
	return ctx
}
