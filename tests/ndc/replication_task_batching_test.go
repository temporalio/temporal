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

package ndc

import (
	"context"
	"flag"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tests"
)

type (
	NDCReplicationTaskBatchingTestSuite struct {
		*require.Assertions
		protorequire.ProtoAssertions
		suite.Suite

		testClusterFactory          tests.TestClusterFactory
		standByReplicationTasksChan chan *replicationspb.ReplicationTask
		mockAdminClient             map[string]adminservice.AdminServiceClient
		namespace                   namespace.Name
		namespaceID                 namespace.ID
		standByTaskID               int64

		controller      *gomock.Controller
		passtiveCluster *tests.TestCluster
		generator       test.Generator
		serializer      serialization.Serializer
		logger          log.Logger
	}
)

func TestNDCReplicationTaskBatching(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NDCReplicationTaskBatchingTestSuite))
}

func (s *NDCReplicationTaskBatchingTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.serializer = serialization.NewSerializer()
	s.testClusterFactory = tests.NewTestClusterFactory()

	fileName := "../testdata/ndc_clusters.yaml"
	if tests.TestFlags.TestClusterConfigFile != "" {
		fileName = tests.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()
	s.standByTaskID = 0

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*tests.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))

	passiveClusterConfig := clusterConfigs[1]
	passiveClusterConfig.WorkerConfig = &tests.WorkerConfig{}
	passiveClusterConfig.DynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.EnableReplicationStream:       true,
		dynamicconfig.EnableEagerNamespaceRefresher: true,
		dynamicconfig.EnableReplicationTaskBatching: true,
	}
	s.controller = gomock.NewController(s.T())
	mockActiveStreamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)

	mockActiveStreamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockActiveStreamClient.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesResponse, error) {
		println("Passive is receiving!!!!")
		return s.GetReplicationMessagesMock()
	}).AnyTimes()
	s.standByReplicationTasksChan = make(chan *replicationspb.ReplicationTask, 100)

	mockActiveClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockActiveClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockActiveStreamClient, nil).AnyTimes()
	s.mockAdminClient = map[string]adminservice.AdminServiceClient{
		"cluster-a": mockActiveClient,
	}
	passiveClusterConfig.MockAdminClient = s.mockAdminClient
	passiveClusterConfig.ClusterMetadata.MasterClusterName = "cluster-b"
	cluster, err := s.testClusterFactory.NewCluster(s.T(), passiveClusterConfig, log.With(s.logger, tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.passtiveCluster = cluster

	s.registerNamespace()
}

func (s *NDCReplicationTaskBatchingTestSuite) TearDownSuite() {
	if s.generator != nil {
		s.generator.Reset()
	}
	s.controller.Finish()
	s.NoError(s.passtiveCluster.TearDownCluster())
}

func (s *NDCReplicationTaskBatchingTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

func (s *NDCReplicationTaskBatchingTestSuite) TestAbc() {

	workflowID := "replication-message-dlq-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	var historyBatch []*historypb.History
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, 1)

	events := s.generator.GetNextVertices()
	historyEvents := &historypb.History{}
	for _, event := range events {
		historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
	}
	historyBatch = append(historyBatch, historyEvents)

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)

	s.NotNil(historyBatch)
	historyBatch[0].Events[1].Version = 1

	s.applyEventsThroughFetcher(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory,
		historyBatch,
	)
	time.Sleep(10 * time.Second)
}

func (s *NDCReplicationTaskBatchingTestSuite) registerNamespace() {
	s.namespace = namespace.Name("test-simple-workflow-ndc-" + common.GenerateRandomString(5))
	passiveFrontend := s.passtiveCluster.GetFrontendClient() //
	_, err := passiveFrontend.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        s.namespace.String(),
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[1],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	})
	s.Require().NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(2 * tests.NamespaceCacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: s.namespace.String(),
	}
	resp, err := passiveFrontend.DescribeNamespace(context.Background(), descReq)
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.namespaceID = namespace.ID(resp.GetNamespaceInfo().GetId())

	s.logger.Info("Registered namespace", tag.WorkflowNamespace(s.namespace.String()), tag.WorkflowNamespaceID(s.namespaceID.String()))
}

func (s *NDCReplicationTaskBatchingTestSuite) GetReplicationMessagesMock() (*adminservice.StreamWorkflowReplicationMessagesResponse, error) {
	task := <-s.standByReplicationTasksChan
	taskID := atomic.AddInt64(&s.standByTaskID, 1)
	task.SourceTaskId = taskID
	tasks := []*replicationspb.ReplicationTask{task}
	for len(s.standByReplicationTasksChan) > 0 {
		task = <-s.standByReplicationTasksChan
		taskID := atomic.AddInt64(&s.standByTaskID, 1)
		task.SourceTaskId = taskID
		tasks = append(tasks, task)
	}

	replicationMessage := &repicationpb.WorkflowReplicationMessages{
		ReplicationTasks:           tasks,
		ExclusiveHighWatermark:     100,
		ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, 100)),
	}

	return &adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: replicationMessage,
		},
	}, nil
}

func (s *NDCReplicationTaskBatchingTestSuite) applyEventsThroughFetcher(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
) {
	for _, batch := range eventBatches {
		eventBlob, newRunEventBlob := s.generateEventBlobs(workflowID, runID, workflowType, taskqueue, batch)

		taskType := enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK
		replicationTask := &replicationspb.ReplicationTask{
			TaskType:     taskType,
			SourceTaskId: 1,
			Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
					NamespaceId:         s.namespaceID.String(),
					WorkflowId:          workflowID,
					RunId:               runID,
					VersionHistoryItems: versionHistory.GetItems(),
					Events:              eventBlob,
					NewRunEvents:        newRunEventBlob,
				}},
		}

		s.standByReplicationTasksChan <- replicationTask
		// this is to test whether dedup works
		s.standByReplicationTasksChan <- replicationTask
	}
}

func (s *NDCReplicationTaskBatchingTestSuite) generateEventBlobs(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	batch *historypb.History,
) (*commonpb.DataBlob, *commonpb.DataBlob) {
	// TODO temporary code to generate next run first event
	//  we should generate these as part of modeled based testing
	lastEvent := batch.Events[len(batch.Events)-1]
	newRunEventBlob := s.generateNewRunHistory(
		lastEvent, s.namespace, s.namespaceID, workflowID, runID, lastEvent.GetVersion(), workflowType, taskqueue,
	)
	// must serialize events batch after attempt on continue as new as generateNewRunHistory will
	// modify the NewExecutionRunId attr
	eventBlob, err := s.serializer.SerializeEvents(batch.Events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	return eventBlob, newRunEventBlob
}

func (s *NDCReplicationTaskBatchingTestSuite) generateNewRunHistory(
	event *historypb.HistoryEvent,
	nsName namespace.Name,
	nsID namespace.ID,
	workflowID string,
	runID string,
	version int64,
	workflowType string,
	taskQueue string,
) *commonpb.DataBlob {

	// TODO temporary code to generate first event & version history
	//  we should generate these as part of modeled based testing

	if event.GetWorkflowExecutionContinuedAsNewEventAttributes() == nil {
		return nil
	}

	event.GetWorkflowExecutionContinuedAsNewEventAttributes().NewExecutionRunId = uuid.New()

	newRunFirstEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Version:   version,
		TaskId:    1,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:              &commonpb.WorkflowType{Name: workflowType},
			ParentWorkflowNamespace:   nsName.String(),
			ParentWorkflowNamespaceId: nsID.String(),
			ParentWorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: uuid.New(),
				RunId:      uuid.New(),
			},
			ParentInitiatedEventId:          event.GetEventId(),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			ContinuedExecutionRunId:         runID,
			Initiator:                       enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
			OriginalExecutionRunId:          runID,
			Identity:                        "NDC-test",
			FirstExecutionRunId:             runID,
			Attempt:                         1,
			WorkflowExecutionExpirationTime: timestamppb.New(time.Now().UTC().Add(time.Minute)),
		}},
	}

	eventBlob, err := s.serializer.SerializeEvents([]*historypb.HistoryEvent{newRunFirstEvent}, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)

	return eventBlob
}

func (s *NDCReplicationTaskBatchingTestSuite) eventBatchesToVersionHistory(
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
) *historyspb.VersionHistory {

	// TODO temporary code to generate version history
	//  we should generate version as part of modeled based testing
	if versionHistory == nil {
		versionHistory = versionhistory.NewVersionHistory(nil, nil)
	}
	for _, batch := range eventBatches {
		for _, event := range batch.Events {
			err := versionhistory.AddOrUpdateVersionHistoryItem(versionHistory,
				versionhistory.NewVersionHistoryItem(
					event.GetEventId(),
					event.GetVersion(),
				))
			s.NoError(err)
		}
	}

	return versionHistory
}
