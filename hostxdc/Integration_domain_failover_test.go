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
	"errors"
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
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/persistence/persistence-tests"
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
		cluster1       *testCluster
		cluster2       *testCluster
		logger         bark.Logger
		enableEventsV2 bool
	}

	testCluster struct {
		persistencetests.TestBase
		host   host.Cadence
		engine wsc.Interface
		logger bark.Logger
	}
)

const (
	testNumberOfHistoryShards = 4
	testNumberOfHistoryHosts  = 1

	cacheRefreshInterval = cache.DomainCacheRefreshInterval + time.Second
)

var (
	integration     = flag.Bool("integration2", true, "run integration tests")
	testEventsV2Xdc = flag.Bool("eventsV2xdc", false, "run integration tests with eventsV2 for XDC suite")

	domainName     = "integration-cross-dc-test-domain"
	clusterName    = []string{"active", "standby"}
	topicName      = []string{"active", "standby"}
	clusterAddress = []string{cluster.TestCurrentClusterFrontendAddress, cluster.TestAlternativeClusterFrontendAddress}
	clustersInfo   = []*config.ClustersInfo{
		{
			EnableGlobalDomain:             true,
			FailoverVersionIncrement:       10,
			MasterClusterName:              clusterName[0],
			CurrentClusterName:             clusterName[0],
			ClusterInitialFailoverVersions: map[string]int64{clusterName[0]: 0, clusterName[1]: 1},
			ClusterAddress:                 map[string]string{clusterName[0]: clusterAddress[0], clusterName[1]: clusterAddress[1]},
		},
		{
			EnableGlobalDomain:             true,
			FailoverVersionIncrement:       10,
			MasterClusterName:              clusterName[0],
			CurrentClusterName:             clusterName[1],
			ClusterInitialFailoverVersions: map[string]int64{clusterName[0]: 0, clusterName[1]: 1},
			ClusterAddress:                 map[string]string{clusterName[0]: clusterAddress[0], clusterName[1]: clusterAddress[1]},
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
	c.setupCluster(no, s.enableEventsV2)
	return c
}

func (s *testCluster) setupCluster(no int, enableEventsV2 bool) {
	options := persistencetests.TestBaseOptions{}
	options.DBName = "integration_" + clusterName[no]
	clusterInfo := clustersInfo[no]
	options.ClusterMetadata = cluster.NewMetadata(
		dynamicconfig.GetBoolPropertyFn(clusterInfo.EnableGlobalDomain),
		clusterInfo.FailoverVersionIncrement,
		clusterInfo.MasterClusterName,
		clusterInfo.CurrentClusterName,
		clusterInfo.ClusterInitialFailoverVersions,
		clusterInfo.ClusterAddress,
	)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&options)
	s.TestBase.Setup()
	s.setupShards()
	messagingClient := s.createMessagingClient()
	testNumberOfHistoryShards := 1 // use 1 shard so we can be sure when failover completed in standby cluster
	s.host = host.NewCadence(s.ClusterMetadata, client.NewIPYarpcDispatcherProvider(), messagingClient, s.MetadataProxy, s.MetadataManagerV2, s.ShardMgr, s.HistoryMgr, s.HistoryV2Mgr, s.ExecutionMgrFactory, s.TaskMgr,
		s.VisibilityMgr, testNumberOfHistoryShards, testNumberOfHistoryHosts, s.logger, no, true, enableEventsV2)
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
	return messaging.NewKafkaClient(&kafkaConfig, zap.NewNop(), s.logger, tally.NoopScope, true)
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

func createContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 90*time.Second)
	return ctx
}

func TestIntegrationClustersTestSuite(t *testing.T) {
	if *integration && !*testEventsV2Xdc {
		s := new(integrationClustersTestSuite)
		suite.Run(t, s)
	} else {
		t.Skip()
	}
}

func TestIntegrationClustersTestSuiteEventsV2(t *testing.T) {
	flag.Parse()
	if *integration && *testEventsV2Xdc {
		s := new(integrationClustersTestSuite)
		s.enableEventsV2 = true
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
	// Wait for domain cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby
	resp2, err := client2.DescribeDomain(createContext(), descReq)
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
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
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
	// Wait for domain cache to pick the chenge
	time.Sleep(cache.DomainCacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby
	resp2, err := client2.DescribeDomain(createContext(), descReq)
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
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err := client1.StartWorkflowExecution(createContext(), startReq)
	s.Nil(err)
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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(30),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(20),
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
	time.Sleep(cacheRefreshInterval)

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

func (s *integrationClustersTestSuite) TestTerminateFailover() {
	domainName := "test-terminate-workflow-failover-" + common.GenerateRandomString(5)
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
	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby

	// start a workflow
	id := "integration-terminate-workflow-failover-test"
	wt := "integration-terminate-workflow-failover-test-type"
	tl := "integration-terminate-workflow-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
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
	}
	we, err := client1.StartWorkflowExecution(createContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

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

	poller := &host.TaskPoller{
		Engine:          client1,
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
	time.Sleep(cacheRefreshInterval)

	// terminate workflow at cluster 2
	terminateReason := "terminate reason."
	terminateDetails := []byte("terminate details.")
	err = client2.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		Reason:   common.StringPtr(terminateReason),
		Details:  terminateDetails,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	// check terminate done
	executionTerminated := false
	getHistoryReq := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
	}
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := client2.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		s.Nil(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionTerminated {
			s.logger.Warnf("Execution not terminated yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.WorkflowExecutionTerminatedEventAttributes
		s.Equal(terminateReason, *terminateEventAttributes.Reason)
		s.Equal(terminateDetails, terminateEventAttributes.Details)
		s.Equal(identity, *terminateEventAttributes.Identity)
		executionTerminated = true
		break GetHistoryLoop
	}
	s.True(executionTerminated)

	// check history replicated to the other cluster
	var historyResponse *workflow.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
GetHistoryLoop2:
	for i := 0; i < 15; i++ {
		historyResponse, err = client1.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if err == nil {
			history := historyResponse.History
			lastEvent := history.Events[len(history.Events)-1]
			if *lastEvent.EventType == workflow.EventTypeWorkflowExecutionTerminated {
				terminateEventAttributes := lastEvent.WorkflowExecutionTerminatedEventAttributes
				s.Equal(terminateReason, *terminateEventAttributes.Reason)
				s.Equal(terminateDetails, terminateEventAttributes.Details)
				s.Equal(identity, *terminateEventAttributes.Identity)
				eventsReplicated = true
				break GetHistoryLoop2
			}
		}
		time.Sleep(1 * time.Second)
	}
	s.Nil(err)
	s.True(eventsReplicated)
}

func (s *integrationClustersTestSuite) TestContinueAsNewFailover() {
	domainName := "test-continueAsNew-workflow-failover-" + common.GenerateRandomString(5)
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
	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby

	// start a workflow
	id := "integration-continueAsNew-workflow-failover-test"
	wt := "integration-continueAsNew-workflow-failover-test-type"
	tl := "integration-continueAsNew-workflow-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
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
	}
	we, err := client1.StartWorkflowExecution(createContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	workflowComplete := false
	continueAsNewCount := int32(5)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = execution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeContinueAsNewWorkflowExecution),
				ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:                        workflowType,
					TaskList:                            &workflow.TaskList{Name: &tl},
					Input:                               buf.Bytes(),
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
				},
			}}, nil
		}

		lastRunStartedEvent = history.Events[0]
		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &host.TaskPoller{
		Engine:          client1,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// make some progress in cluster 1 and did some continueAsNew
	for i := 0; i < 3; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(err, strconv.Itoa(i))
	}

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
	time.Sleep(cacheRefreshInterval)

	// finish the rest in cluster 2
	for i := 0; i < 2; i++ {
		_, err := poller2.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err = poller2.PollAndProcessDecisionTask(false, false)
	s.Nil(err)
	s.True(workflowComplete)
	s.Equal(previousRunID, lastRunStartedEvent.WorkflowExecutionStartedEventAttributes.GetContinuedExecutionRunId())
}

func (s *integrationClustersTestSuite) TestSignalFailover() {
	domainName := "test-signal-workflow-failover-" + common.GenerateRandomString(5)
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
	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-signal-workflow-failover-test"
	wt := "integration-signal-workflow-failover-test-type"
	tl := "integration-signal-workflow-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(300),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err := client1.StartWorkflowExecution(createContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	eventSignaled := false
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !eventSignaled {
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					eventSignaled = true
					return nil, []*workflow.Decision{}, nil
				}
			}
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &host.TaskPoller{
		Engine:          client1,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Send a signal in cluster 1
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	err = client1.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(we.GetRunId()),
		},
		SignalName: common.StringPtr(signalName),
		Input:      signalInput,
		Identity:   common.StringPtr(identity),
	})
	s.Nil(err)

	// Process signal in cluster 1
	s.False(eventSignaled)
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(eventSignaled)

	// Update domain to fail over
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

	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	// check history matched
	getHistoryReq := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
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

	// Send another signal in cluster 2
	signalName2 := "my signal 2"
	signalInput2 := []byte("my signal input 2.")
	err = client2.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		SignalName: common.StringPtr(signalName2),
		Input:      signalInput2,
		Identity:   common.StringPtr(identity),
	})
	s.Nil(err)

	// Process signal in cluster 2
	eventSignaled = false
	_, err = poller2.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask 2: %v", err)
	s.Nil(err)
	s.True(eventSignaled)

	// check history matched
	eventsReplicated = false
	for i := 0; i < 15; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 9 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.Nil(err)
	s.True(eventsReplicated)
}

func (s *integrationClustersTestSuite) TestActivityHeartbeatFailover() {
	domainName := "test-activity-hearbeat-workflow-failover-" + common.GenerateRandomString(5)
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
	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-activity-hearbeat-workflow-failover-test"
	wt := "integration-activity-hearbeat-workflow-failover-test-type"
	tl := "integration-activity-hearbeat-workflow-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(300),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}
	var we *workflow.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(createContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	activitySent := false
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !activitySent {
			activitySent = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(1)),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("some random activity type")},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         []byte("some random input"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1000),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(1000),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(3),
					RetryPolicy: &workflow.RetryPolicy{
						InitialIntervalInSeconds:    common.Int32Ptr(1),
						MaximumAttempts:             common.Int32Ptr(3),
						MaximumIntervalInSeconds:    common.Int32Ptr(1),
						NonRetriableErrorReasons:    []string{"bad-bug"},
						BackoffCoefficient:          common.Float64Ptr(1),
						ExpirationIntervalInSeconds: common.Int32Ptr(100),
					},
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	activity1Called := false
	atHandler1 := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		activity1Called = true
		time.Sleep(5 * time.Second)
		return []byte("Activity Result."), false, nil
	}

	// activity handler
	activity2Called := false
	atHandler2 := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		activity2Called = true
		return []byte("Activity Result."), false, nil
	}

	poller1 := &host.TaskPoller{
		Engine:          client1,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler1,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler2,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err = poller1.PollAndProcessDecisionTask(false, false)
	s.Nil(err)
	err = poller1.PollAndProcessActivityTask(false)
	s.IsType(&workflow.EntityNotExistsError{}, err)

	// Update domain to fail over
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

	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)
	for i := 0; i < 10; i++ {
		poller2.PollAndProcessActivityTask(false)
		if activity2Called {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	s.True(activity1Called)
	s.True(activity2Called)

	// TODO when EnableSyncActivityHeartbeat is enabled by default, uncomment the code below
	// historyResponse, err := client2.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
	// 	Domain: common.StringPtr(domainName),
	// 	Execution: &workflow.WorkflowExecution{
	// 		WorkflowId: common.StringPtr(id),
	// 	},
	// })
	// s.Nil(err)
	// history := historyResponse.History
	// common.PrettyPrintHistory(history, s.logger)

	// activityRetryFound := false
	// for _, event := range history.Events {
	// 	if event.GetEventType() == workflow.EventTypeActivityTaskStarted {
	// 		attribute := event.ActivityTaskStartedEventAttributes
	// 		s.True(attribute.GetAttempt() > 0)
	// 		activityRetryFound = true
	// 	}
	// }
	// s.True(activityRetryFound)
}

func (s *integrationClustersTestSuite) TestTransientDecisionFailover() {
	domainName := "test-transient-decision-workflow-failover-" + common.GenerateRandomString(5)
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
	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.host.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-transient-decision-workflow-failover-test"
	wt := "integration-transient-decision-workflow-failover-test-type"
	tl := "integration-transient-decision-workflow-failover-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(300),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(8),
		Identity:                            common.StringPtr(identity),
	}
	var we *workflow.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(createContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	decisionFailed := false
	workflowFinished := false
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !decisionFailed {
			decisionFailed = true
			return nil, nil, errors.New("random fail decision reason")
		}

		workflowFinished = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:          client1,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// this will fail the decision
	_, err = poller1.PollAndProcessDecisionTask(false, false)
	s.Nil(err)

	// Update domain to fail over
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

	// Wait for domain cache to pick the chenge
	time.Sleep(cacheRefreshInterval)

	// for failover transient decision, it is guaranteed that the transient decision
	// after the failover has attempt 0
	// for details see ReplicateTransientDecisionTaskScheduled
	_, err = poller2.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, 0)
	s.Nil(err)
	s.True(workflowFinished)
}
