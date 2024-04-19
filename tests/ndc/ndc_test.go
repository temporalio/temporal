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
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/update/v1"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/ndc"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/versionhistory"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tests"
)

type (
	NDCFunctionalTestSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		suite.Suite

		testClusterFactory tests.TestClusterFactory

		controller *gomock.Controller
		cluster    *tests.TestCluster
		generator  test.Generator
		serializer serialization.Serializer
		logger     log.Logger

		namespace                   namespace.Name
		namespaceID                 namespace.ID
		version                     int64
		versionIncrement            int64
		mockAdminClient             map[string]adminservice.AdminServiceClient
		standByReplicationTasksChan chan *replicationspb.ReplicationTask
		standByTaskID               int64
	}
)

func TestNDCFuncTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NDCFunctionalTestSuite))
}

func (s *NDCFunctionalTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.serializer = serialization.NewSerializer()
	s.testClusterFactory = tests.NewTestClusterFactory()

	fileName := "../testdata/ndc_clusters.yaml"
	if tests.TestFlags.TestClusterConfigFile != "" {
		fileName = tests.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*tests.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	clusterConfigs[0].WorkerConfig = &tests.WorkerConfig{}
	clusterConfigs[1].WorkerConfig = &tests.WorkerConfig{}

	s.controller = gomock.NewController(s.T())
	mockStreamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)
	mockStreamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockStreamClient.EXPECT().Recv().Return(&adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &repicationpb.WorkflowReplicationMessages{
				ReplicationTasks:           []*repicationpb.ReplicationTask{},
				ExclusiveHighWatermark:     100,
				ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, 100)),
			},
		},
	}, nil).AnyTimes()
	mockStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()

	s.standByReplicationTasksChan = make(chan *replicationspb.ReplicationTask, 100)

	s.standByTaskID = 0
	mockStandbyClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockStandbyClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).DoAndReturn(s.GetReplicationMessagesMock).AnyTimes()
	mockStandbyClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockStreamClient, nil).AnyTimes()
	mockOtherClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockOtherClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetReplicationMessagesResponse{
			ShardMessages: make(map[int32]*replicationspb.ReplicationMessages),
		}, nil).AnyTimes()
	mockOtherClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockStreamClient, nil).AnyTimes()
	s.mockAdminClient = map[string]adminservice.AdminServiceClient{
		"cluster-b": mockStandbyClient,
		"cluster-c": mockOtherClient,
	}
	clusterConfigs[0].MockAdminClient = s.mockAdminClient

	cluster, err := s.testClusterFactory.NewCluster(s.T(), clusterConfigs[0], log.With(s.logger, tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.cluster = cluster

	s.registerNamespace()

	s.version = clusterConfigs[1].ClusterMetadata.ClusterInformation[clusterConfigs[1].ClusterMetadata.CurrentClusterName].InitialFailoverVersion
	s.versionIncrement = clusterConfigs[0].ClusterMetadata.FailoverVersionIncrement
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, s.version)
}

func (s *NDCFunctionalTestSuite) GetReplicationMessagesMock(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetReplicationMessagesResponse, error) {
	select {
	case task := <-s.standByReplicationTasksChan:
		taskID := atomic.AddInt64(&s.standByTaskID, 1)
		task.SourceTaskId = taskID
		tasks := []*replicationspb.ReplicationTask{task}
		for len(s.standByReplicationTasksChan) > 0 {
			task = <-s.standByReplicationTasksChan
			taskID := atomic.AddInt64(&s.standByTaskID, 1)
			task.SourceTaskId = taskID
			tasks = append(tasks, task)
		}

		replicationMessage := &replicationspb.ReplicationMessages{
			ReplicationTasks:       tasks,
			LastRetrievedMessageId: tasks[len(tasks)-1].SourceTaskId,
			HasMore:                true,
		}

		return &adminservice.GetReplicationMessagesResponse{
			ShardMessages: map[int32]*replicationspb.ReplicationMessages{1: replicationMessage},
		}, nil
	default:
		return &adminservice.GetReplicationMessagesResponse{
			ShardMessages: make(map[int32]*replicationspb.ReplicationMessages),
		}, nil
	}
}

func (s *NDCFunctionalTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, s.version)
}

func (s *NDCFunctionalTestSuite) TearDownSuite() {
	if s.generator != nil {
		s.generator.Reset()
	}
	s.controller.Finish()
	s.NoError(s.cluster.TearDownCluster())
}

func (s *NDCFunctionalTestSuite) TestSingleBranch() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-single-branch-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	versions := []int64{3, 13, 2, 202, 302, 402, 602, 502, 802, 1002, 902, 702, 1102}
	for _, version := range versions {
		runID := uuid.New()
		historySize := int64(0)

		var historyBatch []*historypb.History
		s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)

		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			historyBatch = append(historyBatch, historyEvents)
		}

		versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)
		s.applyEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			versionHistory,
			historyBatch,
			historyClient,
		)
		s.verifyEventHistorySize(workflowID, runID, historySize)
		s.verifyEventHistory(workflowID, runID, historyBatch)
	}
}

func (s *NDCFunctionalTestSuite) TestMultipleBranches() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-multiple-branches-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	versions := []int64{102, 2, 202}
	versionIncs := [][]int64{{1, 10}, {11, 10}}
	versionInc := versionIncs[rand.Intn(len(versionIncs))]
	for _, version := range versions {
		runID := uuid.New()
		historySize := int64(0)

		var baseBranch []*historypb.History
		baseGenerator := test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)
		baseGenerator.SetVersion(version)

		for i := 0; i < 10 && baseGenerator.HasNextVertex(); i++ {
			events := baseGenerator.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			baseBranch = append(baseBranch, historyEvents)
		}
		baseVersionHistory := s.eventBatchesToVersionHistory(nil, baseBranch)

		var branch1 []*historypb.History
		branchVersionHistory1 := versionhistory.CopyVersionHistory(baseVersionHistory)
		branchGenerator1 := baseGenerator.DeepCopy()
		for i := 0; i < 10 && branchGenerator1.HasNextVertex(); i++ {
			events := branchGenerator1.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			branch1 = append(branch1, historyEvents)
		}
		branchVersionHistory1 = s.eventBatchesToVersionHistory(branchVersionHistory1, branch1)

		var branch2 []*historypb.History
		branchVersionHistory2 := versionhistory.CopyVersionHistory(baseVersionHistory)
		branchGenerator2 := baseGenerator.DeepCopy()
		branchGenerator2.SetVersion(branchGenerator2.GetVersion() + versionInc[0])
		for i := 0; i < 10 && branchGenerator2.HasNextVertex(); i++ {
			events := branchGenerator2.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			branch2 = append(branch2, historyEvents)
		}
		branchVersionHistory2 = s.eventBatchesToVersionHistory(branchVersionHistory2, branch2)

		var branch3 []*historypb.History
		branchVersionHistory3 := versionhistory.CopyVersionHistory(baseVersionHistory)
		branchGenerator3 := baseGenerator.DeepCopy()
		branchGenerator3.SetVersion(branchGenerator3.GetVersion() + versionInc[1])
		for i := 0; i < 10 && branchGenerator3.HasNextVertex(); i++ {
			events := branchGenerator3.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			branch3 = append(branch3, historyEvents)
		}
		branchVersionHistory3 = s.eventBatchesToVersionHistory(branchVersionHistory3, branch3)

		s.applyEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			baseVersionHistory,
			baseBranch,
			historyClient,
		)
		s.applyEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			branchVersionHistory1,
			branch1,
			historyClient,
		)
		s.applyEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			branchVersionHistory2,
			branch2,
			historyClient,
		)
		s.applyEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			branchVersionHistory3,
			branch3,
			historyClient,
		)
		s.verifyEventHistorySize(workflowID, runID, historySize)
		s.verifyVersionHistory(
			workflowID,
			runID,
			branchVersionHistory1,
		)
		s.verifyVersionHistory(
			workflowID,
			runID,
			branchVersionHistory2,
		)
		s.verifyVersionHistory(
			workflowID,
			runID,
			branchVersionHistory3,
		)
	}
}

func (s *NDCFunctionalTestSuite) TestEmptyVersionAndNonEmptyVersion() {
	workflowID := "ndc-migration-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	runID := uuid.New()

	version := common.EmptyVersion
	var baseBranch []*historypb.History
	baseGenerator := test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)
	baseGenerator.SetVersion(version)

	for i := 0; i < 10 && baseGenerator.HasNextVertex(); i++ {
		events := baseGenerator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
		baseBranch = append(baseBranch, historyEvents)
	}
	baseVersionHistory := s.eventBatchesToVersionHistory(nil, baseBranch)

	var branch1 []*historypb.History
	branchVersionHistory1 := versionhistory.CopyVersionHistory(baseVersionHistory)
	branchGenerator1 := baseGenerator.DeepCopy()
	branchGenerator1.SetVersion(2)
	for i := 0; i < 10 && branchGenerator1.HasNextVertex(); i++ {
		events := branchGenerator1.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
		branch1 = append(branch1, historyEvents)
	}
	branchVersionHistory1 = s.eventBatchesToVersionHistory(branchVersionHistory1, branch1)

	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		baseVersionHistory,
		baseBranch,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		branchVersionHistory1,
		branch1,
		historyClient,
	)
}

func (s *NDCFunctionalTestSuite) TestReplicateWorkflowState_PartialReplicated() {

	s.setupRemoteFrontendClients()
	workflowID := "replicate-workflow-state-partially-replicated" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()
	var historyBatch []*historypb.History
	// standby initial failover version 2
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, 12)

	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
		historyBatch = append(historyBatch, historyEvents)
	}

	partialHistoryBatch := historyBatch[:1]
	partialVersionHistory := s.eventBatchesToVersionHistory(nil, partialHistoryBatch)
	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)
	workflowState := &persistence.WorkflowMutableState{
		ExecutionState: &persistence.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			RunId:  runID,
		},
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			NamespaceId: s.namespaceID.String(),
			WorkflowId:  workflowID,
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories:                  []*historyspb.VersionHistory{versionHistory},
			},
		},
	}
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		partialVersionHistory,
		partialHistoryBatch,
		historyClient,
	)
	_, err := historyClient.ReplicateWorkflowState(context.Background(), &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: workflowState,
		RemoteCluster: "cluster-b",
		NamespaceId:   s.namespaceID.String(),
	})
	s.Error(err)

	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory,
		historyBatch,
		historyClient,
	)
	_, err = historyClient.ReplicateWorkflowState(context.Background(), &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: workflowState,
		RemoteCluster: "cluster-b",
		NamespaceId:   s.namespaceID.String(),
	})
	s.NoError(err)
}

func (s *NDCFunctionalTestSuite) TestHandcraftedMultipleBranches() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-handcrafted-multiple-branches-test" + uuid.New()
	runID := uuid.New()
	historySize := int64(0)

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"
	identity := "worker-identity"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	eventsBatch1 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   1,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                    nil,
					WorkflowRunTimeout:       durationpb.New(1000 * time.Second),
					WorkflowTaskTimeout:      durationpb.New(1000 * time.Second),
					FirstWorkflowTaskBackoff: durationpb.New(100 * time.Second),
					Initiator:                enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
			{
				EventId:   2,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   3,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 2,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   4,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         identity,
				}},
			},
			{
				EventId:   5,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
				Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
					MarkerName: "some marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					},
					WorkflowTaskCompletedEventId: 4,
				}},
			},
			{
				EventId:   6,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId: 4,
					ActivityId:                   "0",
					ActivityType:                 &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                        nil,
					ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
					ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
					StartToCloseTimeout:          durationpb.New(20 * time.Second),
					HeartbeatTimeout:             durationpb.New(20 * time.Second),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   7,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: 6,
					Identity:         identity,
					RequestId:        uuid.New(),
					Attempt:          1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   8,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 1",
					Input:      payloads.EncodeString("some signal details 1"),
					Identity:   identity,
				}},
			},
			{
				EventId:   9,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   10,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 9,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   11,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   12,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 2",
					Input:      payloads.EncodeString("some signal details 2"),
					Identity:   identity,
				}},
			},
			{
				EventId:   13,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
			{
				EventId:   14,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 13,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
	}

	eventsBatch2 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   32,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
					RetryState: enumspb.RETRY_STATE_TIMEOUT,
				}},
			},
		}},
	}

	eventsBatch3 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
					ScheduledEventId: 13,
					StartedEventId:   14,
					TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				}},
			},
			{
				EventId:   16,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					ScheduledEventId: 6,
					StartedEventId:   7,
					Failure: &failurepb.Failure{
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
						}},
					},
				}},
			},
			{
				EventId:   17,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   18,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 17,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   19,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
			{
				EventId:   20,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					WorkflowTaskCompletedEventId: 19,
					Failure:                      failure.NewServerFailure("some random reason", false),
				}},
			},
		}},
	}
	for _, eventBatch := range eventsBatch1 {
		historySize += s.sizeOfHistoryEvents(eventBatch.Events)
	}
	for _, eventBatch := range eventsBatch2 {
		historySize += s.sizeOfHistoryEvents(eventBatch.Events)
	}
	for _, eventBatch := range eventsBatch3 {
		historySize += s.sizeOfHistoryEvents(eventBatch.Events)
	}

	versionHistory1 := s.eventBatchesToVersionHistory(nil, eventsBatch1)

	versionHistory2, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory1,
		versionhistory.NewVersionHistoryItem(14, 22),
	)
	s.NoError(err)
	versionHistory2 = s.eventBatchesToVersionHistory(versionHistory2, eventsBatch2)

	versionHistory3, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory1,
		versionhistory.NewVersionHistoryItem(14, 22),
	)
	s.NoError(err)
	versionHistory3 = s.eventBatchesToVersionHistory(versionHistory3, eventsBatch3)

	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory1,
		eventsBatch1,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory3,
		eventsBatch3,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory2,
		eventsBatch2,
		historyClient,
	)
	s.verifyEventHistorySize(workflowID, runID, historySize)
}

func (s *NDCFunctionalTestSuite) TestHandcraftedMultipleBranchesWithZombieContinueAsNew() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-handcrafted-multiple-branches-with-continue-as-new-test" + uuid.New()
	runID := uuid.New()
	historySize := int64(0)

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"
	identity := "worker-identity"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	eventsBatch1 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   1,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                    nil,
					WorkflowRunTimeout:       durationpb.New(1000 * time.Second),
					WorkflowTaskTimeout:      durationpb.New(1000 * time.Second),
					FirstWorkflowTaskBackoff: durationpb.New(100 * time.Second),
					Initiator:                enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
			{
				EventId:   2,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   3,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 2,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   4,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         identity,
				}},
			},
			{
				EventId:   5,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
				Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
					MarkerName: "some marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					},
					WorkflowTaskCompletedEventId: 4,
				}},
			},
			{
				EventId:   6,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId: 4,
					ActivityId:                   "0",
					ActivityType:                 &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                        nil,
					ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
					ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
					StartToCloseTimeout:          durationpb.New(20 * time.Second),
					HeartbeatTimeout:             durationpb.New(20 * time.Second),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   7,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: 6,
					Identity:         identity,
					RequestId:        uuid.New(),
					Attempt:          1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   8,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 1",
					Input:      payloads.EncodeString("some signal details 1"),
					Identity:   identity,
				}},
			},
			{
				EventId:   9,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   10,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 9,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   11,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   12,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 2",
					Input:      payloads.EncodeString("some signal details 2"),
					Identity:   identity,
				}},
			},
			{
				EventId:   13,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
			{
				EventId:   14,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 13,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
	}

	eventsBatch2 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   33,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
		}},
		// need to keep the workflow open for testing
	}

	eventsBatch3 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
			{
				EventId:   16,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
					NewExecutionRunId:            uuid.New(),
					WorkflowType:                 &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                        nil,
					WorkflowRunTimeout:           durationpb.New(1000 * time.Second),
					WorkflowTaskTimeout:          durationpb.New(1000 * time.Second),
					WorkflowTaskCompletedEventId: 19,
					Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
		}},
	}
	for _, eventBatch := range eventsBatch1 {
		historySize += s.sizeOfHistoryEvents(eventBatch.Events)
	}
	for _, eventBatch := range eventsBatch2 {
		historySize += s.sizeOfHistoryEvents(eventBatch.Events)
	}
	for _, eventBatch := range eventsBatch3 {
		historySize += s.sizeOfHistoryEvents(eventBatch.Events)
	}

	versionHistory1 := s.eventBatchesToVersionHistory(nil, eventsBatch1)

	versionHistory2, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory1,
		versionhistory.NewVersionHistoryItem(14, 22),
	)
	s.NoError(err)
	versionHistory2 = s.eventBatchesToVersionHistory(versionHistory2, eventsBatch2)

	versionHistory3, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory1,
		versionhistory.NewVersionHistoryItem(14, 22),
	)
	s.NoError(err)
	versionHistory3 = s.eventBatchesToVersionHistory(versionHistory3, eventsBatch3)

	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory1,
		eventsBatch1,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory2,
		eventsBatch2,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory3,
		eventsBatch3,
		historyClient,
	)
	s.verifyEventHistorySize(workflowID, runID, historySize)
}

func (s *NDCFunctionalTestSuite) TestImportSingleBranch() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-import-single-branch-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	versions := []int64{3, 13, 2, 202, 301, 401, 602, 502, 803, 1002, 902, 701, 1103}
	for _, version := range versions {
		runID := uuid.New()
		historySize := int64(0)

		var historyBatch []*historypb.History
		s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)
		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			historyBatch = append(historyBatch, historyEvents)
		}

		versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)
		s.importEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			versionHistory,
			historyBatch,
			historyClient,
			true,
		)
		s.verifyEventHistorySize(workflowID, runID, historySize)
		s.verifyEventHistory(workflowID, runID, historyBatch)
	}
}

func (s *NDCFunctionalTestSuite) TestImportMultipleBranches() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-import-multiple-branches-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	versions := []int64{102, 2, 202}
	versionIncs := [][]int64{
		{1, 10},
		{11, 10},
		{9, 10},
		{19, 10},
	}
	versionInc := versionIncs[rand.Intn(len(versionIncs))]
	for _, version := range versions {
		runID := uuid.New()
		historySize := int64(0)

		var baseBranch []*historypb.History
		baseGenerator := test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)
		baseGenerator.SetVersion(version)

		for i := 0; i < 10 && baseGenerator.HasNextVertex(); i++ {
			events := baseGenerator.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			baseBranch = append(baseBranch, historyEvents)
		}
		baseVersionHistory := s.eventBatchesToVersionHistory(nil, baseBranch)

		var branch1 []*historypb.History
		branchVersionHistory1 := versionhistory.CopyVersionHistory(baseVersionHistory)
		branchGenerator1 := baseGenerator.DeepCopy()
		for i := 0; i < 10 && branchGenerator1.HasNextVertex(); i++ {
			events := branchGenerator1.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			branch1 = append(branch1, historyEvents)
		}
		branchVersionHistory1 = s.eventBatchesToVersionHistory(branchVersionHistory1, branch1)

		var branch2 []*historypb.History
		branchVersionHistory2 := versionhistory.CopyVersionHistory(baseVersionHistory)
		branchGenerator2 := baseGenerator.DeepCopy()
		branchGenerator2.SetVersion(branchGenerator2.GetVersion() + versionInc[0])
		for i := 0; i < 10 && branchGenerator2.HasNextVertex(); i++ {
			events := branchGenerator2.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			branch2 = append(branch2, historyEvents)
		}
		branchVersionHistory2 = s.eventBatchesToVersionHistory(branchVersionHistory2, branch2)

		var branch3 []*historypb.History
		branchVersionHistory3 := versionhistory.CopyVersionHistory(baseVersionHistory)
		branchGenerator3 := baseGenerator.DeepCopy()
		branchGenerator3.SetVersion(branchGenerator3.GetVersion() + versionInc[1])
		for i := 0; i < 10 && branchGenerator3.HasNextVertex(); i++ {
			events := branchGenerator3.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historySize += s.sizeOfHistoryEvents(historyEvents.Events)
			branch3 = append(branch3, historyEvents)
		}
		branchVersionHistory3 = s.eventBatchesToVersionHistory(branchVersionHistory3, branch3)

		s.importEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			baseVersionHistory,
			baseBranch,
			historyClient,
			true,
		)
		s.importEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			branchVersionHistory1,
			branch1,
			historyClient,
			false,
		)
		s.importEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			branchVersionHistory2,
			branch2,
			historyClient,
			false,
		)
		s.importEvents(
			workflowID,
			runID,
			workflowType,
			taskqueue,
			branchVersionHistory3,
			branch3,
			historyClient,
			false,
		)
		s.verifyEventHistorySize(workflowID, runID, historySize)

		s.verifyVersionHistory(
			workflowID,
			runID,
			branchVersionHistory1,
		)
		s.verifyVersionHistory(
			workflowID,
			runID,
			branchVersionHistory2,
		)
		s.verifyVersionHistory(
			workflowID,
			runID,
			branchVersionHistory3,
		)
	}
}

func (s *NDCFunctionalTestSuite) TestEventsReapply_ZombieWorkflow() {

	workflowID := "ndc-events-reapply-zombie-workflow-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// cluster has initial version 1
	historyClient := s.cluster.GetHistoryClient()

	version := int64(102)
	runID := uuid.New()
	historySize := int64(0)
	historyBatch := []*historypb.History{}
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)

	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
		historySize += s.sizeOfHistoryEvents(historyEvents.Events)
		historyBatch = append(historyBatch, historyEvents)
	}

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory,
		historyBatch,
		historyClient,
	)
	s.verifyEventHistorySize(workflowID, runID, historySize)

	version = int64(2)
	runID = uuid.New()
	historySize = int64(0)
	historyBatch = []*historypb.History{}
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)

	// verify two batches of zombie workflow call reapply API
	reapplyCount := 0
	for i := 0; i < 2 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		reapply := false
		for _, event := range events {
			historyEvent := event.GetData().(*historypb.HistoryEvent)
			if historyEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				reapply = true
			}
			historyEvents.Events = append(historyEvents.Events, historyEvent)
		}
		if reapply {
			reapplyCount += 1
		}
		historySize += s.sizeOfHistoryEvents(historyEvents.Events)
		historyBatch = append(historyBatch, historyEvents)
	}
	s.mockAdminClient["cluster-b"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&adminservice.ReapplyEventsResponse{},
		nil,
	).Times(reapplyCount * 2)

	versionHistory = s.eventBatchesToVersionHistory(nil, historyBatch)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory,
		historyBatch,
		historyClient,
	)
	s.verifyEventHistorySize(workflowID, runID, historySize)
}

func (s *NDCFunctionalTestSuite) TestEventsReapply_NonCurrentBranch_Signal() {
	s.testEventsReapplyNonCurrentBranch(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
}

func (s *NDCFunctionalTestSuite) TestEventsReapply_NonCurrentBranch_UpdateAdmitted() {
	s.testEventsReapplyNonCurrentBranch(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED)
}

func (s *NDCFunctionalTestSuite) TestEventsReapply_NonCurrentBranch_UpdateAccepted() {
	s.testEventsReapplyNonCurrentBranch(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
}

func (s *NDCFunctionalTestSuite) testEventsReapplyNonCurrentBranch(staleEventType enumspb.EventType) {
	workflowID := "ndc-events-reapply-non-current-test" + uuid.New()
	runID := uuid.New()
	historySize := int64(0)
	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"
	version := int64(102)
	isWorkflowFinished := false

	historyClient := s.cluster.GetHistoryClient()

	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)
	baseBranch := []*historypb.History{}
	var taskID int64
	for i := 0; i < 4 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvent := event.GetData().(*historypb.HistoryEvent)
			taskID = historyEvent.GetTaskId()
			historyEvents.Events = append(historyEvents.Events, historyEvent)
			switch historyEvent.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				isWorkflowFinished = true
			}
		}
		historySize += s.sizeOfHistoryEvents(historyEvents.Events)
		baseBranch = append(baseBranch, historyEvents)
	}
	if isWorkflowFinished {
		// cannot proceed since the test below requires workflow not finished
		// this is ok since build kite will run this test several times
		s.logger.Info("Encounter finish workflow history event during randomization test, skip")
		return
	}

	versionHistory := s.eventBatchesToVersionHistory(nil, baseBranch)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory,
		baseBranch,
		historyClient,
	)
	s.verifyEventHistorySize(workflowID, runID, historySize)

	newGenerator := s.generator.DeepCopy()
	var newBranch []*historypb.History
	newVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	newGenerator.SetVersion(newGenerator.GetVersion() + 1) // simulate events from other cluster
	for i := 0; i < 4 && newGenerator.HasNextVertex(); i++ {
		events := newGenerator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			history := event.GetData().(*historypb.HistoryEvent)
			taskID = history.GetTaskId()
			historyEvents.Events = append(historyEvents.Events, history)
		}
		historySize += s.sizeOfHistoryEvents(historyEvents.Events)
		newBranch = append(newBranch, historyEvents)
	}
	newVersionHistory = s.eventBatchesToVersionHistory(newVersionHistory, newBranch)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		newVersionHistory,
		newBranch,
		historyClient,
	)
	s.verifyEventHistorySize(workflowID, runID, historySize)

	s.mockAdminClient["cluster-b"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil)
	// Handcraft a stale signal event
	baseBranchLastEventBatch := baseBranch[len(baseBranch)-1].GetEvents()
	baseBranchLastEvent := baseBranchLastEventBatch[len(baseBranchLastEventBatch)-1]
	staleBranch := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   baseBranchLastEvent.GetEventId() + 1,
					EventType: staleEventType,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   baseBranchLastEvent.GetVersion(), // dummy event from other cluster
					TaskId:    taskID,
				},
			},
		},
	}
	if staleEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
		staleBranch[0].Events[0].Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Identity:   "ndc_functional_test",
		}}
	} else if staleEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED {
		staleBranch[0].Events[0].Attributes = &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
			Request: &update.Request{Input: &update.Input{Args: payloads.EncodeString("update-request-payload")}},
			Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
		}}
	} else if staleEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
		staleBranch[0].Events[0].Attributes = &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
			AcceptedRequest: &update.Request{Input: &update.Input{Args: payloads.EncodeString("update-request-payload")}},
		}}
	}
	staleVersionHistory := s.eventBatchesToVersionHistory(versionhistory.CopyVersionHistory(versionHistory), staleBranch)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		staleVersionHistory,
		staleBranch,
		historyClient,
	)
}

func (s *NDCFunctionalTestSuite) TestResend() {

	workflowID := "ndc-re-send-test" + uuid.New()
	runID := uuid.New()
	workflowType := "ndc-re-send-workflow-type"
	taskqueue := "event-generator-taskQueue"
	identity := "ndc-re-send-test"

	historyClient := s.cluster.GetHistoryClient()
	adminClient := s.cluster.GetAdminClient()
	getHistory := func(
		nsName namespace.Name,
		nsID namespace.ID,
		workflowID string,
		runID string,
		startEventID int64,
		startEventVersion int64,
		endEventID int64,
		endEventVersion int64,
		pageSize int,
		token []byte,
	) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

		execution := &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		}
		return adminClient.GetWorkflowExecutionRawHistoryV2(s.newContext(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId:       nsID.String(),
			Execution:         execution,
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        endEventID,
			EndEventVersion:   endEventVersion,
			MaximumPageSize:   int32(pageSize),
			NextPageToken:     token,
		})
	}

	eventsBatch1 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   1,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                    nil,
					WorkflowRunTimeout:       durationpb.New(1000 * time.Second),
					WorkflowTaskTimeout:      durationpb.New(1000 * time.Second),
					FirstWorkflowTaskBackoff: durationpb.New(100 * time.Second),
					Initiator:                enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
			{
				EventId:   2,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   3,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 2,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   4,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         identity,
				}},
			},
			{
				EventId:   5,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
				Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
					MarkerName: "some marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					},
					WorkflowTaskCompletedEventId: 4,
				}},
			},
			{
				EventId:   6,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId: 4,
					ActivityId:                   "0",
					ActivityType:                 &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                        nil,
					ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
					ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
					StartToCloseTimeout:          durationpb.New(20 * time.Second),
					HeartbeatTimeout:             durationpb.New(20 * time.Second),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   7,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: 6,
					Identity:         identity,
					RequestId:        uuid.New(),
					Attempt:          1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   8,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 1",
					Input:      payloads.EncodeString("some signal details 1"),
					Identity:   identity,
				}},
			},
			{
				EventId:   9,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   10,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 9,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   11,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   12,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 2",
					Input:      payloads.EncodeString("some signal details 2"),
					Identity:   identity,
				}},
			},
			{
				EventId:   13,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
			{
				EventId:   14,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   22,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 13,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
	}

	eventsBatch2 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   32,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   16,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   32,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId: 4,
					ActivityId:                   "0",
					ActivityType:                 &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                        nil,
					ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
					ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
					StartToCloseTimeout:          durationpb.New(20 * time.Second),
					HeartbeatTimeout:             durationpb.New(20 * time.Second),
				}},
			},
		}},
	}

	eventsBatch3 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
					ScheduledEventId: 13,
					StartedEventId:   14,
					TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				}},
			},
			{
				EventId:   16,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					ScheduledEventId: 6,
					StartedEventId:   7,
					Failure: &failurepb.Failure{
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
						}},
					},
				}},
			},
			{
				EventId:   17,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					StartToCloseTimeout: durationpb.New(1000 * time.Second),
					Attempt:             1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   18,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 17,
					Identity:         identity,
					RequestId:        uuid.New(),
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   19,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
			{
				EventId:   20,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					WorkflowTaskCompletedEventId: 19,
					Failure:                      failure.NewServerFailure("some random reason", false),
				}},
			},
		}},
	}

	eventsBatch4 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   17,
				EventTime: timestamppb.New(time.Now().UTC()),
				Version:   33,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
					RetryState: enumspb.RETRY_STATE_TIMEOUT,
				}},
			},
		}},
	}

	versionHistory1 := s.eventBatchesToVersionHistory(nil, eventsBatch1)

	versionHistory2, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory1,
		versionhistory.NewVersionHistoryItem(14, 22),
	)
	s.NoError(err)
	versionHistory2 = s.eventBatchesToVersionHistory(versionHistory2, eventsBatch2)

	versionHistory3, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory1,
		versionhistory.NewVersionHistoryItem(14, 22),
	)
	s.NoError(err)
	versionHistory3 = s.eventBatchesToVersionHistory(versionHistory3, eventsBatch3)

	versionHistory4, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory2,
		versionhistory.NewVersionHistoryItem(16, 32),
	)
	s.NoError(err)
	versionHistory4 = s.eventBatchesToVersionHistory(versionHistory4, eventsBatch4)

	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory1,
		eventsBatch1,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory3,
		eventsBatch3,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory2,
		eventsBatch2,
		historyClient,
	)
	s.applyEvents(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory4,
		eventsBatch4,
		historyClient,
	)

	// GetWorkflowExecutionRawHistoryV2 start and end
	var token []byte
	batchCount := 0
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err := getHistory(
			s.namespace,
			s.namespaceID,
			workflowID,
			runID,
			14,
			22,
			20,
			31,
			1,
			token,
		)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= 1)
		batchCount++
		token = resp.NextPageToken
	}
	s.Equal(batchCount, 4)

	// GetWorkflowExecutionRawHistoryV2 start and end not on the same branch
	token = nil
	batchCount = 0
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err := getHistory(
			s.namespace,
			s.namespaceID,
			workflowID,
			runID,
			17,
			31,
			17,
			33,
			1,
			token,
		)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= 1)
		batchCount++
		token = resp.NextPageToken
	}
	s.Equal(batchCount, 2)

	// GetWorkflowExecutionRawHistoryV2 start boundary
	token = nil
	batchCount = 0
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err := getHistory(
			s.namespace,
			s.namespaceID,
			workflowID,
			runID,
			14,
			22,
			common.EmptyEventID,
			common.EmptyVersion,
			1,
			token,
		)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= 1)
		batchCount++
		token = resp.NextPageToken
	}
	s.Equal(batchCount, 3)

	// GetWorkflowExecutionRawHistoryV2 end boundary
	token = nil
	batchCount = 0
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err := getHistory(
			s.namespace,
			s.namespaceID,
			workflowID,
			runID,
			common.EmptyEventID,
			common.EmptyVersion,
			17,
			33,
			1,
			token,
		)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= 1)
		batchCount++
		token = resp.NextPageToken
	}
	s.Equal(batchCount, 10)
}

func (s *NDCFunctionalTestSuite) registerNamespace() {
	s.namespace = namespace.Name("test-simple-workflow-ndc-" + common.GenerateRandomString(5))
	client1 := s.cluster.GetFrontendClient() // cluster
	_, err := client1.RegisterNamespace(s.newContext(), &workflowservice.RegisterNamespaceRequest{
		Namespace:         s.namespace.String(),
		IsGlobalNamespace: true,
		Clusters:          clusterReplicationConfig,
		// make the cluster `cluster-a` `passive` and replicate from `active` cluster `cluster-b`
		ActiveClusterName:                clusterName[1],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	})
	s.Require().NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(2 * tests.NamespaceCacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: s.namespace.String(),
	}
	resp, err := client1.DescribeNamespace(s.newContext(), descReq)
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.namespaceID = namespace.ID(resp.GetNamespaceInfo().GetId())

	s.logger.Info("Registered namespace", tag.WorkflowNamespace(s.namespace.String()), tag.WorkflowNamespaceID(s.namespaceID.String()))
}

func (s *NDCFunctionalTestSuite) generateNewRunHistory(
	event *historypb.HistoryEvent,
	nsName namespace.Name,
	nsID namespace.ID,
	workflowID string,
	runID string,
	version int64,
	workflowType string,
	taskQueue string,
) (*commonpb.DataBlob, string) {

	// TODO temporary code to generate first event & version history
	//  we should generate these as part of modeled based testing

	if event.GetWorkflowExecutionContinuedAsNewEventAttributes() == nil {
		return nil, ""
	}

	newRunID := uuid.New()
	event.GetWorkflowExecutionContinuedAsNewEventAttributes().NewExecutionRunId = newRunID

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
			WorkflowId:                      workflowID,
		}},
	}

	eventBlob, err := s.serializer.SerializeEvents([]*historypb.HistoryEvent{newRunFirstEvent}, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)

	return eventBlob, newRunID
}

func (s *NDCFunctionalTestSuite) generateEventBlobs(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	batch *historypb.History,
) (*commonpb.DataBlob, *commonpb.DataBlob, string) {
	// TODO temporary code to generate next run first event
	//  we should generate these as part of modeled based testing
	lastEvent := batch.Events[len(batch.Events)-1]
	newRunEventBlob, newRunID := s.generateNewRunHistory(
		lastEvent, s.namespace, s.namespaceID, workflowID, runID, lastEvent.GetVersion(), workflowType, taskqueue,
	)
	// must serialize events batch after attempt on continue as new as generateNewRunHistory will
	// modify the NewExecutionRunId attr
	eventBlob, err := s.serializer.SerializeEvents(batch.Events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	return eventBlob, newRunEventBlob, newRunID
}

func (s *NDCFunctionalTestSuite) applyEvents(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
	historyClient tests.HistoryClient,
) {
	historyClient = history.NewRetryableClient(
		historyClient,
		common.CreateHistoryClientRetryPolicy(),
		common.IsServiceClientTransientError,
	)
	for _, batch := range eventBatches {
		eventBlob, newRunEventBlob, newRunID := s.generateEventBlobs(workflowID, runID, workflowType, taskqueue, batch)
		req := &historyservice.ReplicateEventsV2Request{
			NamespaceId: s.namespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistoryItems: versionHistory.GetItems(),
			Events:              eventBlob,
			NewRunEvents:        newRunEventBlob,
			NewRunId:            newRunID,
		}

		resp, err := historyClient.ReplicateEventsV2(s.newContext(), req)
		s.NoError(err, "Failed to replicate history event")
		s.ProtoEqual(&historyservice.ReplicateEventsV2Response{}, resp)
		resp, err = historyClient.ReplicateEventsV2(s.newContext(), req)
		s.NoError(err, "Failed to dedup replicate history event")
		s.ProtoEqual(&historyservice.ReplicateEventsV2Response{}, resp)
	}
}

func (s *NDCFunctionalTestSuite) importEvents(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
	historyClient tests.HistoryClient,
	verifyWorkflowNotExists bool,
) {
	if len(eventBatches) == 0 {
		return
	}

	historyClient = history.NewRetryableClient(
		historyClient,
		common.CreateHistoryClientRetryPolicy(),
		common.IsResourceExhausted,
	)
	var token []byte
	for _, batch := range eventBatches {
		eventBlob, _, _ := s.generateEventBlobs(workflowID, runID, workflowType, taskqueue, batch)
		req := &historyservice.ImportWorkflowExecutionRequest{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistory: versionHistory,
			HistoryBatches: []*commonpb.DataBlob{eventBlob},
			Token:          token,
		}
		resp, err := historyClient.ImportWorkflowExecution(s.newContext(), req)
		s.NoError(err, "Failed to import history event")
		token = resp.Token
	}

	if verifyWorkflowNotExists {
		_, err := historyClient.GetMutableState(s.newContext(), &historyservice.GetMutableStateRequest{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
		s.IsType(&serviceerror.NotFound{}, err)
	}

	req := &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistory: versionHistory,
		HistoryBatches: []*commonpb.DataBlob{},
		Token:          token,
	}
	resp, err := historyClient.ImportWorkflowExecution(s.newContext(), req)
	s.NoError(err, "Failed to import history event")
	s.Nil(resp.Token)
}

func (s *NDCFunctionalTestSuite) applyEventsThroughFetcher(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
) {
	for _, batch := range eventBatches {
		eventBlob, newRunEventBlob, newRunID := s.generateEventBlobs(workflowID, runID, workflowType, taskqueue, batch)

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
					NewRunId:            newRunID,
				}},
		}

		s.standByReplicationTasksChan <- replicationTask
		// this is to test whether dedup works
		s.standByReplicationTasksChan <- replicationTask
	}
}

func (s *NDCFunctionalTestSuite) eventBatchesToVersionHistory(
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

func (s *NDCFunctionalTestSuite) verifyEventHistorySize(
	workflowID string,
	runID string,
	historySize int64,
) {
	// get replicated history events from passive side
	describeWorkflow, err := s.cluster.GetFrontendClient().DescribeWorkflowExecution(
		s.newContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	)
	s.NoError(err)
	// NOTE: non current branch can contain force termination event
	//  so calculation should be updated, for now only assert below
	s.True(historySize <= describeWorkflow.WorkflowExecutionInfo.HistorySizeBytes)
}

func (s *NDCFunctionalTestSuite) verifyVersionHistory(
	workflowID string,
	runID string,
	expectedVersionHistory *historyspb.VersionHistory,
) {
	// get replicated history events from passive side
	resp, err := s.cluster.GetHistoryClient().GetMutableState(
		s.newContext(),
		&historyservice.GetMutableStateRequest{
			NamespaceId: string(s.namespaceID),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	)
	s.NoError(err)
	if s.IsForceTerminated(workflowID, runID) {
		index := resp.VersionHistories.CurrentVersionHistoryIndex
		currentVersionHistory := resp.VersionHistories.Histories[index]
		currentVersionHistory.Items[len(currentVersionHistory.Items)-1].EventId -= 1
		// force termination event is generated by service itself, need to exclude
	}
	for _, actualVersionHistory := range resp.VersionHistories.Histories {
		actualVersionHistory.BranchToken = nil
	}
	for _, actualVersionHistory := range resp.VersionHistories.Histories {
		actualVersionHistory.BranchToken = nil
		lcaItem, err := versionhistory.FindLCAVersionHistoryItem(
			expectedVersionHistory,
			actualVersionHistory,
		)
		s.NoError(err)
		lastItem, err := versionhistory.GetLastVersionHistoryItem(expectedVersionHistory)
		s.NoError(err)
		if versionhistory.IsEqualVersionHistoryItem(lastItem, lcaItem) {
			return
		}
	}
	s.Fail(fmt.Sprintf(
		"unable to find version history in mutable state %v vs %v",
		expectedVersionHistory,
		resp.VersionHistories.Histories,
	))
}

func (s *NDCFunctionalTestSuite) verifyEventHistory(
	workflowID string,
	runID string,
	historyBatch []*historypb.History,
) {
	// get replicated history events from passive side
	replicatedHistory, err := s.cluster.GetFrontendClient().GetWorkflowExecutionHistory(
		s.newContext(),
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			MaximumPageSize:        1000,
			NextPageToken:          nil,
			WaitNewEvent:           false,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		},
	)
	s.NoError(err)

	// compare origin events with replicated events
	batchIndex := 0
	batch := historyBatch[batchIndex].Events
	eventIndex := 0
	for _, event := range replicatedHistory.GetHistory().GetEvents() {
		if eventIndex >= len(batch) {
			batchIndex++
			batch = historyBatch[batchIndex].Events
			eventIndex = 0
		}
		originEvent := batch[eventIndex]
		eventIndex++
		s.Equal(originEvent.GetEventType(), event.GetEventType())
	}
}

func (s *NDCFunctionalTestSuite) setupRemoteFrontendClients() {
	s.mockAdminClient["cluster-b"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).AnyTimes()
	s.mockAdminClient["cluster-c"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).AnyTimes()
}

func (s *NDCFunctionalTestSuite) sizeOfHistoryEvents(
	events []*historypb.HistoryEvent,
) int64 {
	blob, err := serialization.NewSerializer().SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	return int64(len(blob.Data))
}

func (s *NDCFunctionalTestSuite) newContext() context.Context {
	ctx := tests.NewContext()
	return headers.SetCallerInfo(
		ctx,
		headers.NewCallerInfo(s.namespace.String(), headers.CallerTypeAPI, ""),
	)
}

func (s *NDCFunctionalTestSuite) IsForceTerminated(
	workflowID string,
	runID string,
) bool {
	var token []byte
	var lastEvent *historypb.HistoryEvent
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		historyResp, err := s.cluster.GetFrontendClient().GetWorkflowExecutionHistory(
			s.newContext(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: s.namespace.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				MaximumPageSize:        100,
				NextPageToken:          token,
				WaitNewEvent:           false,
				HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
			},
		)
		s.NoError(err)
		token = historyResp.GetNextPageToken()
		events := historyResp.GetHistory().GetEvents()
		if len(events) != 0 {
			lastEvent = events[len(events)-1]
		}
	}
	if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
		return false
	}
	terminationEventAttr := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
	return terminationEventAttr.Reason == ndc.WorkflowTerminationReason &&
		terminationEventAttr.Identity == ndc.WorkflowTerminationIdentity
}
