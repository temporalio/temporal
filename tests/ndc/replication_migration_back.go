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
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
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
	ReplicationMigrationBackTestSuite struct {
		*require.Assertions
		protorequire.ProtoAssertions
		suite.Suite

		testClusterFactory          tests.TestClusterFactory
		standByReplicationTasksChan chan *repicationpb.ReplicationTask
		mockAdminClient             map[string]adminservice.AdminServiceClient
		namespace                   namespace.Name
		namespaceID                 namespace.ID
		standByTaskID               int64
		autoIncrementTaskID         int64
		passiveClusterName          string

		controller      *gomock.Controller
		passtiveCluster *tests.TestCluster
		generator       test.Generator
		serializer      serialization.Serializer
		logger          log.Logger
	}
)

func (s *ReplicationMigrationBackTestSuite) SetupSuite() {
	s.logger = log.NewNoopLogger()
	s.serializer = serialization.NewSerializer()
	s.testClusterFactory = tests.NewTestClusterFactory()
	s.passiveClusterName = "cluster-b"

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
		dynamicconfig.EnableReplicationStream:             true,
		dynamicconfig.EnableEagerNamespaceRefresher:       true,
		dynamicconfig.EnableReplicateLocalGeneratedEvents: true,
	}
	s.controller = gomock.NewController(s.T())
	mockActiveStreamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)

	// below is to mock stream client, so we can directly put replication tasks into passive cluster without involving active cluster
	mockActiveStreamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockActiveStreamClient.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesResponse, error) {
		return s.GetReplicationMessagesMock()
	}).AnyTimes()
	mockActiveStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	s.standByReplicationTasksChan = make(chan *repicationpb.ReplicationTask, 100)

	mockActiveClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockActiveClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockActiveStreamClient, nil).AnyTimes()
	s.mockAdminClient = map[string]adminservice.AdminServiceClient{
		"cluster-a": mockActiveClient,
	}
	passiveClusterConfig.MockAdminClient = s.mockAdminClient

	passiveClusterConfig.ClusterMetadata.MasterClusterName = s.passiveClusterName
	delete(passiveClusterConfig.ClusterMetadata.ClusterInformation, "cluster-c") // ndc_clusters.yaml has 3 clusters, but we only need 2 for this test
	cluster, err := s.testClusterFactory.NewCluster(s.T(), passiveClusterConfig, log.With(s.logger, tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.passtiveCluster = cluster

	s.registerNamespace()
}

func (s *ReplicationMigrationBackTestSuite) TearDownSuite() {
	if s.generator != nil {
		s.generator.Reset()
	}
	s.controller.Finish()
	s.NoError(s.passtiveCluster.TearDownCluster())
}

func (s *ReplicationMigrationBackTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

// Test scenario: simulate workflowId that has 2 different runs and workflows are replicating into passive cluster.
// The workflow history events' version is passive cluster. Without the support of migration back,
// workflow replication will fail. While with support of migration back, workflow replication will succeed and
// both run will exist in passive cluster and in a completed status.
func (s *ReplicationMigrationBackTestSuite) TestHistoryReplication_MigrationBackCase() {
	workflowId := "ndc-test-migration-back"
	version := int64(2) // this version has to point to passive cluster to trigger migration back case
	runId1 := uuid.New()
	runId2 := uuid.New()
	run1Slices := s.getEventSlices(version, 0) // run1 is older than run2
	run2Slices := s.getEventSlices(version, 10)

	history, err := tests.EventBatchesToVersionHistory(
		nil,
		[]*historypb.History{{Events: run1Slices[0]}, {Events: run1Slices[1]}, {Events: run1Slices[2]}},
	)
	// when handle migration back case, passive will need to fetch the history from active cluster
	s.mockActiveGetRawHistoryApiCalls(workflowId, runId1, run1Slices, history, version)
	s.mockActiveGetRawHistoryApiCalls(workflowId, runId2, run2Slices, history, version)

	s.NoError(err)

	// replicate run1's 1st batch
	s.standByReplicationTasksChan <- s.createHistoryEventReplicationTaskFromHistoryEventBatch( // supply history replication task one by one
		s.namespaceID.String(),
		workflowId,
		runId1,
		run1Slices[0],
		nil,
		history.Items,
	)

	time.Sleep(1 * time.Second) // wait for 1 sec to let the run1 events replicated

	// replicate run2
	s.standByReplicationTasksChan <- s.createHistoryEventReplicationTaskFromHistoryEventBatch( // supply history replication task one by one
		s.namespaceID.String(),
		workflowId,
		runId2,
		run2Slices[0],
		nil,
		history.Items,
	)

	time.Sleep(1 * time.Second) // wait for 1 sec to let the run2 events replicated

	res1, err := s.passtiveCluster.GetAdminClient().DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId1,
		},
	})
	s.NoError(err)

	res2, err := s.passtiveCluster.GetAdminClient().DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId2,
		},
	})

	s.NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, res1.DatabaseMutableState.ExecutionState.State)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, res2.DatabaseMutableState.ExecutionState.State)
}

func (s *ReplicationMigrationBackTestSuite) serializeEvents(events []*historypb.HistoryEvent) *commonpb.DataBlob {
	blob, err := s.serializer.SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	return blob
}

func (s *ReplicationMigrationBackTestSuite) mockActiveGetRawHistoryApiCalls(
	workflowID string,
	runID string,
	eventBatches [][]*historypb.HistoryEvent,
	history *historyspb.VersionHistory,
	version int64,
) {
	nextToken := []byte(runID + "-next-page-token-1")
	s.mockActiveGetRawHistoryResponse(workflowID, runID, 2, version, 5, version, nil, &adminservice.GetWorkflowExecutionRawHistoryResponse{
		NextPageToken: nextToken,
		HistoryBatches: []*commonpb.DataBlob{
			s.serializeEvents(eventBatches[1]),
		},
		VersionHistory: history,
	}, nil).Times(1)

	s.mockActiveGetRawHistoryResponse(workflowID, runID, 2, version, 5, version, nextToken, &adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches: []*commonpb.DataBlob{
			s.serializeEvents(eventBatches[2]),
		},
		VersionHistory: history,
	}, nil).Times(1)
}

func (s *ReplicationMigrationBackTestSuite) mockActiveGetRawHistoryResponse(
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
	token []byte,
	returnResponse *adminservice.GetWorkflowExecutionRawHistoryResponse,
	returnError error,
) *gomock.Call {
	return s.mockAdminClient["cluster-a"].(*adminservicemock.MockAdminServiceClient).EXPECT().
		GetWorkflowExecutionRawHistory(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        endEventID,
			EndEventVersion:   endEventVersion,
			MaximumPageSize:   100,
			NextPageToken:     token,
		}).Return(returnResponse, returnError)
}

func (s *ReplicationMigrationBackTestSuite) getEventSlices(version int64, timeDrift time.Duration) [][]*historypb.HistoryEvent {
	taskqueue := "taskqueue"
	workflowType := "workflowType"
	identity := "identity"
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventTime: timestamppb.New(time.Now().Add(timeDrift * time.Second).UTC()),
			Version:   version,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			TaskId:    34603008,
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
			EventTime: timestamppb.New(time.Now().Add(timeDrift * time.Second).UTC()),
			Version:   version,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			TaskId:    34603009,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				StartToCloseTimeout: durationpb.New(1000 * time.Second),
				Attempt:             1,
			}},
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId:   3,
			EventTime: timestamppb.New(time.Now().Add(timeDrift * time.Second).UTC()),
			Version:   version,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			TaskId:    34603018,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: 2,
				Identity:         identity,
				RequestId:        uuid.New(),
			}},
		},
	}
	slice3 := []*historypb.HistoryEvent{
		{
			EventId:   4,
			EventTime: timestamppb.New(time.Now().Add(timeDrift * time.Second).UTC()),
			Version:   version,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			TaskId:    34603023,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: 2,
				StartedEventId:   3,
				Identity:         identity,
			}},
		},
		{
			EventId:   5,
			EventTime: timestamppb.New(time.Now().Add(timeDrift * time.Second).UTC()),
			Version:   version,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			TaskId:    34603024,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: 4,
				Result:                       nil,
			}},
		},
	}
	eventsSlices := [][]*historypb.HistoryEvent{slice1, slice2, slice3}
	return eventsSlices
}

func (s *ReplicationMigrationBackTestSuite) registerNamespace() {
	s.namespace = namespace.Name("test-simple-workflow-ndc-" + common.GenerateRandomString(5))
	passiveFrontend := s.passtiveCluster.GetFrontendClient() //
	replicationConfig := []*replicationpb.ClusterReplicationConfig{
		{ClusterName: clusterName[0]},
		{ClusterName: clusterName[1]},
	}
	_, err := passiveFrontend.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        s.namespace.String(),
		IsGlobalNamespace:                true,
		Clusters:                         replicationConfig,
		ActiveClusterName:                clusterName[0],
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

func (s *ReplicationMigrationBackTestSuite) GetReplicationMessagesMock() (*adminservice.StreamWorkflowReplicationMessagesResponse, error) {
	task := <-s.standByReplicationTasksChan
	taskID := atomic.AddInt64(&s.standByTaskID, 1)
	task.SourceTaskId = taskID
	tasks := []*repicationpb.ReplicationTask{task}

	replicationMessage := &repicationpb.WorkflowReplicationMessages{
		ReplicationTasks:       tasks,
		ExclusiveHighWatermark: taskID + 1,
	}

	return &adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: replicationMessage,
		},
	}, nil
}

func (s *ReplicationMigrationBackTestSuite) createHistoryEventReplicationTaskFromHistoryEventBatch(
	namespaceId string,
	workflowId string,
	runId string,
	events []*historypb.HistoryEvent,
	newRunEvents []*historypb.HistoryEvent,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) *repicationpb.ReplicationTask {
	eventBlob, err := s.serializer.SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	var newRunEventBlob *commonpb.DataBlob
	if newRunEvents != nil {
		newRunEventBlob, err = s.serializer.SerializeEvents(newRunEvents, enumspb.ENCODING_TYPE_PROTO3)
		s.NoError(err)
	}
	s.NoError(err)
	taskType := enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK
	replicationTask := &repicationpb.ReplicationTask{
		TaskType: taskType,
		Attributes: &repicationpb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &repicationpb.HistoryTaskAttributes{
				NamespaceId:         namespaceId,
				WorkflowId:          workflowId,
				RunId:               runId,
				VersionHistoryItems: versionHistoryItems,
				Events:              eventBlob,
				NewRunEvents:        newRunEventBlob,
			}},
	}
	return replicationTask
}
