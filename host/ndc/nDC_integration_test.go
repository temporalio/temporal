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
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	replicationpb "go.temporal.io/api/replication/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	adminClient "go.temporal.io/server/client/admin"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/host"
)

type (
	nDCIntegrationTestSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
		active     *host.TestCluster
		generator  test.Generator
		serializer persistence.PayloadSerializer
		logger     log.Logger

		namespace                   string
		namespaceID                 string
		version                     int64
		versionIncrement            int64
		mockAdminClient             map[string]adminClient.Client
		standByReplicationTasksChan chan *replicationspb.ReplicationTask
		standByTaskID               int64
	}
)

var (
	clusterName              = []string{"active", "standby", "other"}
	clusterReplicationConfig = []*replicationpb.ClusterReplicationConfig{
		{ClusterName: clusterName[0]},
		{ClusterName: clusterName[1]},
		{ClusterName: clusterName[2]},
	}
)

func TestNDCIntegrationTestSuite(t *testing.T) {

	flag.Parse()
	suite.Run(t, new(nDCIntegrationTestSuite))
}

func (s *nDCIntegrationTestSuite) SetupSuite() {
	zapLogger, err := zap.NewDevelopment()
	// cannot use s.Nil since it is not initialized
	s.Require().NoError(err)
	s.serializer = persistence.NewPayloadSerializer()
	s.logger = loggerimpl.NewLogger(zapLogger)

	fileName := "../testdata/ndc_integration_test_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := ioutil.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	clusterConfigs[0].WorkerConfig = &host.WorkerConfig{}
	clusterConfigs[1].WorkerConfig = &host.WorkerConfig{}

	s.standByReplicationTasksChan = make(chan *replicationspb.ReplicationTask, 100)

	s.standByTaskID = 0
	s.mockAdminClient = make(map[string]adminClient.Client)
	controller := gomock.NewController(s.T())
	mockStandbyClient := adminservicemock.NewMockAdminServiceClient(controller)
	mockStandbyClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).DoAndReturn(s.GetReplicationMessagesMock).AnyTimes()
	mockOtherClient := adminservicemock.NewMockAdminServiceClient(controller)
	mockOtherClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetReplicationMessagesResponse{
			MessagesByShard: make(map[int32]*replicationspb.ReplicationMessages),
		}, nil).AnyTimes()
	s.mockAdminClient["standby"] = mockStandbyClient
	s.mockAdminClient["other"] = mockOtherClient
	clusterConfigs[0].MockAdminClient = s.mockAdminClient

	cluster, err := host.NewCluster(clusterConfigs[0], s.logger.WithTags(tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.active = cluster

	s.registerNamespace()

	s.version = clusterConfigs[1].ClusterMetadata.ClusterInformation[clusterConfigs[1].ClusterMetadata.CurrentClusterName].InitialFailoverVersion
	s.versionIncrement = clusterConfigs[0].ClusterMetadata.FailoverVersionIncrement
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.version)
}

func (s *nDCIntegrationTestSuite) GetReplicationMessagesMock(
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
			MessagesByShard: map[int32]*replicationspb.ReplicationMessages{0: replicationMessage},
		}, nil
	default:
		return &adminservice.GetReplicationMessagesResponse{
			MessagesByShard: make(map[int32]*replicationspb.ReplicationMessages),
		}, nil
	}
}

func (s *nDCIntegrationTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.version)
}

func (s *nDCIntegrationTestSuite) TearDownSuite() {
	if s.generator != nil {
		s.generator.Reset()
	}
	s.active.TearDownCluster()
}

func (s *nDCIntegrationTestSuite) TestSingleBranch() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-single-branch-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// active has initial version 0
	historyClient := s.active.GetHistoryClient()

	versions := []int64{101, 1, 201, 301, 401, 601, 501, 801, 1001, 901, 701, 1101}
	for _, version := range versions {
		runID := uuid.New()
		var historyBatch []*historypb.History
		s.generator = test.InitializeHistoryEventGenerator(s.namespace, version)

		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
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

		err := s.verifyEventHistory(workflowID, runID, historyBatch)
		s.Require().NoError(err)
	}
}

func (s *nDCIntegrationTestSuite) verifyEventHistory(
	workflowID string,
	runID string,
	historyBatch []*historypb.History,
) error {
	// get replicated history events from passive side
	passiveClient := s.active.GetFrontendClient()
	replicatedHistory, err := passiveClient.GetWorkflowExecutionHistory(
		host.NewContext(),
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
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

	if err != nil {
		return fmt.Errorf("failed to get history event from passive side: %v", err)
	}

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
		if enumspb.EventType(originEvent.GetEventType()) != event.GetEventType() {
			return fmt.Errorf("the replicated event (%v) and the origin event (%v) are not the same",
				originEvent.GetEventType().String(), event.GetEventType().String())
		}
	}

	return nil
}

func (s *nDCIntegrationTestSuite) TestMultipleBranches() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-multiple-branches-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// active has initial version 0
	historyClient := s.active.GetHistoryClient()

	versions := []int64{101, 1, 201}
	for _, version := range versions {
		runID := uuid.New()

		var baseBranch []*historypb.History
		baseGenerator := test.InitializeHistoryEventGenerator(s.namespace, version)
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
		branchVersionHistory1 := baseVersionHistory.Duplicate()
		branchGenerator1 := baseGenerator.DeepCopy()
		for i := 0; i < 10 && branchGenerator1.HasNextVertex(); i++ {
			events := branchGenerator1.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			branch1 = append(branch1, historyEvents)
		}
		branchVersionHistory1 = s.eventBatchesToVersionHistory(branchVersionHistory1, branch1)

		var branch2 []*historypb.History
		branchVersionHistory2 := baseVersionHistory.Duplicate()
		branchGenerator2 := baseGenerator.DeepCopy()
		branchGenerator2.SetVersion(branchGenerator2.GetVersion() + 1)
		for i := 0; i < 10 && branchGenerator2.HasNextVertex(); i++ {
			events := branchGenerator2.GetNextVertices()
			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			branch2 = append(branch2, historyEvents)
		}
		branchVersionHistory2 = s.eventBatchesToVersionHistory(branchVersionHistory2, branch2)

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
	}
}

func (s *nDCIntegrationTestSuite) TestHandcraftedMultipleBranches() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-handcrafted-multiple-branches-test" + uuid.New()
	runID := uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"
	identity := "worker-identity"

	// active has initial version 0
	historyClient := s.active.GetHistoryClient()

	eventsBatch1 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   1,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                       &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                           nil,
					WorkflowRunTimeoutSeconds:       1000,
					WorkflowTaskTimeoutSeconds:      1000,
					FirstWorkflowTaskBackoffSeconds: 100,
					Initiator:                       enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
			{
				EventId:   2,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   3,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         identity,
				}},
			},
			{
				EventId:   5,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId:  4,
					ActivityId:                    "0",
					ActivityType:                  &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                         nil,
					ScheduleToCloseTimeoutSeconds: 20,
					ScheduleToStartTimeoutSeconds: 20,
					StartToCloseTimeoutSeconds:    20,
					HeartbeatTimeoutSeconds:       20,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   7,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 1",
					Input:      payloads.EncodeString("some signal details 1"),
					Identity:   identity,
				}},
			},
			{
				EventId:   9,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   10,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   12,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 2",
					Input:      payloads.EncodeString("some signal details 2"),
					Identity:   identity,
				}},
			},
			{
				EventId:   13,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
			{
				EventId:   14,
				Version:   21,
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
				Version:   31,
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
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
					ScheduledEventId: 13,
					StartedEventId:   14,
					TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				}},
			},
			{
				EventId:   16,
				Version:   30,
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
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   18,
				Version:   30,
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
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
			{
				EventId:   20,
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					WorkflowTaskCompletedEventId: 19,
					Failure:                      failure.NewServerFailure("some random reason", false),
				}},
			},
		}},
	}

	versionHistory1 := s.eventBatchesToVersionHistory(nil, eventsBatch1)

	versionHistory2, err := versionHistory1.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(14, 21),
	)
	s.NoError(err)
	versionHistory2 = s.eventBatchesToVersionHistory(versionHistory2, eventsBatch2)

	versionHistory3, err := versionHistory1.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(14, 21),
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
}

func (s *nDCIntegrationTestSuite) TestHandcraftedMultipleBranchesWithZombieContinueAsNew() {

	s.setupRemoteFrontendClients()
	workflowID := "ndc-handcrafted-multiple-branches-with-continue-as-new-test" + uuid.New()
	runID := uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"
	identity := "worker-identity"

	// active has initial version 0
	historyClient := s.active.GetHistoryClient()

	eventsBatch1 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   1,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                       &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                           nil,
					WorkflowRunTimeoutSeconds:       1000,
					WorkflowTaskTimeoutSeconds:      1000,
					FirstWorkflowTaskBackoffSeconds: 100,
					Initiator:                       enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
			{
				EventId:   2,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   3,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         identity,
				}},
			},
			{
				EventId:   5,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId:  4,
					ActivityId:                    "0",
					ActivityType:                  &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                         nil,
					ScheduleToCloseTimeoutSeconds: 20,
					ScheduleToStartTimeoutSeconds: 20,
					StartToCloseTimeoutSeconds:    20,
					HeartbeatTimeoutSeconds:       20,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   7,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 1",
					Input:      payloads.EncodeString("some signal details 1"),
					Identity:   identity,
				}},
			},
			{
				EventId:   9,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   10,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   12,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 2",
					Input:      payloads.EncodeString("some signal details 2"),
					Identity:   identity,
				}},
			},
			{
				EventId:   13,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
			{
				EventId:   14,
				Version:   21,
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
				Version:   32,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
			{
				EventId:   16,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
					NewExecutionRunId:            uuid.New(),
					WorkflowType:                 &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                        nil,
					WorkflowRunTimeoutSeconds:    1000,
					WorkflowTaskTimeoutSeconds:   1000,
					WorkflowTaskCompletedEventId: 19,
					Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
		}},
	}

	versionHistory1 := s.eventBatchesToVersionHistory(nil, eventsBatch1)

	versionHistory2, err := versionHistory1.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(14, 21),
	)
	s.NoError(err)
	versionHistory2 = s.eventBatchesToVersionHistory(versionHistory2, eventsBatch2)

	versionHistory3, err := versionHistory1.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(14, 21),
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
}

func (s *nDCIntegrationTestSuite) TestEventsReapply_ZombieWorkflow() {

	workflowID := "ndc-single-branch-test" + uuid.New()

	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	// active has initial version 0
	historyClient := s.active.GetHistoryClient()

	version := int64(101)
	runID := uuid.New()
	historyBatch := []*historypb.History{}
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, version)

	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
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

	version = int64(1)
	runID = uuid.New()
	historyBatch = []*historypb.History{}
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, version)

	// verify two batches of zombie workflow are call reapply API
	s.mockAdminClient["standby"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).Times(2)
	for i := 0; i < 2 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
		historyBatch = append(historyBatch, historyEvents)
	}

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
}

func (s *nDCIntegrationTestSuite) TestEventsReapply_UpdateNonCurrentBranch() {

	workflowID := "ndc-single-branch-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"
	version := int64(101)
	isWorkflowFinished := false

	historyClient := s.active.GetHistoryClient()

	s.generator = test.InitializeHistoryEventGenerator(s.namespace, version)
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

	newGenerator := s.generator.DeepCopy()
	newBranch := []*historypb.History{}
	newVersionHistory := versionHistory.Duplicate()
	newGenerator.SetVersion(newGenerator.GetVersion() + 1) // simulate events from other cluster
	for i := 0; i < 4 && newGenerator.HasNextVertex(); i++ {
		events := newGenerator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			history := event.GetData().(*historypb.HistoryEvent)
			taskID = history.GetTaskId()
			historyEvents.Events = append(historyEvents.Events, history)
		}
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

	s.mockAdminClient["standby"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).Times(1)
	// Handcraft a stale signal event
	baseBranchLastEventBatch := baseBranch[len(baseBranch)-1].GetEvents()
	baseBranchLastEvent := baseBranchLastEventBatch[len(baseBranchLastEventBatch)-1]
	staleBranch := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   baseBranchLastEvent.GetEventId() + 1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
					Timestamp: time.Now().UnixNano(),
					Version:   baseBranchLastEvent.GetVersion(), // dummy event from other cluster
					TaskId:    taskID,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
						SignalName: "signal",
						Input:      payloads.EncodeBytes([]byte{}),
						Identity:   "ndc_integration_test",
					}},
				},
			},
		},
	}
	staleVersionHistory := s.eventBatchesToVersionHistory(versionHistory.Duplicate(), staleBranch)
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

func (s *nDCIntegrationTestSuite) TestAdminGetWorkflowExecutionRawHistoryV2() {

	workflowID := "ndc-re-send-test" + uuid.New()
	runID := uuid.New()
	workflowType := "ndc-re-send-workflow-type"
	taskqueue := "event-generator-taskQueue"
	identity := "ndc-re-send-test"

	historyClient := s.active.GetHistoryClient()
	adminClient := s.active.GetAdminClient()
	getHistory := func(
		namespace string,
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
		return adminClient.GetWorkflowExecutionRawHistoryV2(host.NewContext(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace:         namespace,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
					TaskQueue:                       &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                           nil,
					WorkflowRunTimeoutSeconds:       1000,
					WorkflowTaskTimeoutSeconds:      1000,
					FirstWorkflowTaskBackoffSeconds: 100,
					Initiator:                       enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
				}},
			},
			{
				EventId:   2,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   3,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         identity,
				}},
			},
			{
				EventId:   5,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId:  4,
					ActivityId:                    "0",
					ActivityType:                  &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                         nil,
					ScheduleToCloseTimeoutSeconds: 20,
					ScheduleToStartTimeoutSeconds: 20,
					StartToCloseTimeoutSeconds:    20,
					HeartbeatTimeoutSeconds:       20,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   7,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 1",
					Input:      payloads.EncodeString("some signal details 1"),
					Identity:   identity,
				}},
			},
			{
				EventId:   9,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   10,
				Version:   21,
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
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   12,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "some signal name 2",
					Input:      payloads.EncodeString("some signal details 2"),
					Identity:   identity,
				}},
			},
			{
				EventId:   13,
				Version:   21,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
			{
				EventId:   14,
				Version:   21,
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
				Version:   31,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
					Identity:         identity,
				}},
			},
			{
				EventId:   16,
				Version:   31,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					WorkflowTaskCompletedEventId:  4,
					ActivityId:                    "0",
					ActivityType:                  &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskqueue},
					Input:                         nil,
					ScheduleToCloseTimeoutSeconds: 20,
					ScheduleToStartTimeoutSeconds: 20,
					StartToCloseTimeoutSeconds:    20,
					HeartbeatTimeoutSeconds:       20,
				}},
			},
		}},
	}

	eventsBatch3 := []*historypb.History{
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   15,
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
					ScheduledEventId: 13,
					StartedEventId:   14,
					TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				}},
			},
			{
				EventId:   16,
				Version:   30,
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
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: taskqueue},
					StartToCloseTimeoutSeconds: 1000,
					Attempt:                    1,
				}},
			},
		}},
		{Events: []*historypb.HistoryEvent{
			{
				EventId:   18,
				Version:   30,
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
				Version:   30,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 8,
					StartedEventId:   9,
					Identity:         identity,
				}},
			},
			{
				EventId:   20,
				Version:   30,
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
				Version:   32,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
					RetryState: enumspb.RETRY_STATE_TIMEOUT,
				}},
			},
		}},
	}

	versionHistory1 := s.eventBatchesToVersionHistory(nil, eventsBatch1)

	versionHistory2, err := versionHistory1.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(14, 21),
	)
	s.NoError(err)
	versionHistory2 = s.eventBatchesToVersionHistory(versionHistory2, eventsBatch2)

	versionHistory3, err := versionHistory1.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(14, 21),
	)
	s.NoError(err)
	versionHistory3 = s.eventBatchesToVersionHistory(versionHistory3, eventsBatch3)

	versionHistory4, err := versionHistory2.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(16, 31),
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
			workflowID,
			runID,
			14,
			21,
			20,
			30,
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
			workflowID,
			runID,
			17,
			30,
			17,
			32,
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
			workflowID,
			runID,
			14,
			21,
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
			workflowID,
			runID,
			common.EmptyEventID,
			common.EmptyVersion,
			17,
			32,
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

func (s *nDCIntegrationTestSuite) registerNamespace() {
	s.namespace = "test-simple-workflow-ndc-" + common.GenerateRandomString(5)
	client1 := s.active.GetFrontendClient() // active
	_, err := client1.RegisterNamespace(host.NewContext(), &workflowservice.RegisterNamespaceRequest{
		Name:              s.namespace,
		IsGlobalNamespace: true,
		Clusters:          clusterReplicationConfig,
		// make the active cluster `standby` and replicate to `active` cluster
		ActiveClusterName:                    clusterName[1],
		WorkflowExecutionRetentionPeriodDays: 1,
	})
	s.Require().NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: s.namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.namespaceID = resp.GetNamespaceInfo().GetId()
	// Wait for namespace cache to pick the change
	time.Sleep(2 * cache.NamespaceCacheRefreshInterval)

	s.logger.Info("Registered namespace", tag.WorkflowNamespace(s.namespace), tag.WorkflowNamespaceID(s.namespaceID))
}

func (s *nDCIntegrationTestSuite) generateNewRunHistory(
	event *historypb.HistoryEvent,
	namespace string,
	workflowID string,
	runID string,
	version int64,
	workflowType string,
	taskQueue string,
) *serialization.DataBlob {

	// TODO temporary code to generate first event & version history
	//  we should generate these as part of modeled based testing

	if event.GetWorkflowExecutionContinuedAsNewEventAttributes() == nil {
		return nil
	}

	event.GetWorkflowExecutionContinuedAsNewEventAttributes().NewExecutionRunId = uuid.New()

	newRunFirstEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		Timestamp: time.Now().UnixNano(),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Version:   version,
		TaskId:    1,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:            &commonpb.WorkflowType{Name: workflowType},
			ParentWorkflowNamespace: namespace,
			ParentWorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: uuid.New(),
				RunId:      uuid.New(),
			},
			ParentInitiatedEventId: event.GetEventId(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowRunTimeoutSeconds:            10,
			WorkflowTaskTimeoutSeconds:           10,
			ContinuedExecutionRunId:              runID,
			Initiator:                            enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
			OriginalExecutionRunId:               runID,
			Identity:                             "NDC-test",
			FirstExecutionRunId:                  runID,
			Attempt:                              1,
			WorkflowExecutionExpirationTimestamp: time.Now().Add(time.Minute).UnixNano(),
		}},
	}

	eventBlob, err := s.serializer.SerializeBatchEvents([]*historypb.HistoryEvent{newRunFirstEvent}, common.EncodingTypeProto3)
	s.NoError(err)

	return eventBlob
}

func (s *nDCIntegrationTestSuite) toProtoDataBlob(
	blob *serialization.DataBlob,
) *commonpb.DataBlob {

	if blob == nil {
		return nil
	}

	var encodingType enumspb.EncodingType
	switch blob.GetEncoding() {
	case common.EncodingTypeProto3:
		encodingType = enumspb.ENCODING_TYPE_PROTO3
	case common.EncodingTypeJSON,
		common.EncodingTypeGob,
		common.EncodingTypeUnknown,
		common.EncodingTypeEmpty:
		panic(fmt.Sprintf("unsupported encoding type: %v", blob.GetEncoding()))
	default:
		panic(fmt.Sprintf("unknown encoding type: %v", blob.GetEncoding()))
	}

	return &commonpb.DataBlob{
		EncodingType: encodingType,
		Data:         blob.Data,
	}
}

func (s *nDCIntegrationTestSuite) generateEventBlobs(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	batch *historypb.History,
) (*serialization.DataBlob, *serialization.DataBlob) {
	// TODO temporary code to generate next run first event
	//  we should generate these as part of modeled based testing
	lastEvent := batch.Events[len(batch.Events)-1]
	newRunEventBlob := s.generateNewRunHistory(
		lastEvent, s.namespace, workflowID, runID, lastEvent.GetVersion(), workflowType, taskqueue,
	)
	// must serialize events batch after attempt on continue as new as generateNewRunHistory will
	// modify the NewExecutionRunId attr
	eventBlob, err := s.serializer.SerializeBatchEvents(batch.Events, common.EncodingTypeProto3)
	s.NoError(err)
	return eventBlob, newRunEventBlob
}

func (s *nDCIntegrationTestSuite) applyEvents(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	versionHistory *persistence.VersionHistory,
	eventBatches []*historypb.History,
	historyClient host.HistoryClient,
) {
	for _, batch := range eventBatches {
		eventBlob, newRunEventBlob := s.generateEventBlobs(workflowID, runID, workflowType, taskqueue, batch)
		req := &historyservice.ReplicateEventsV2Request{
			NamespaceId: s.namespaceID,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistoryItems: s.toProtoVersionHistoryItems(versionHistory),
			Events:              s.toProtoDataBlob(eventBlob),
			NewRunEvents:        s.toProtoDataBlob(newRunEventBlob),
		}

		resp, err := historyClient.ReplicateEventsV2(host.NewContext(), req)
		s.NoError(err, "Failed to replicate history event")
		s.Equal(&historyservice.ReplicateEventsV2Response{}, resp)
		resp, err = historyClient.ReplicateEventsV2(host.NewContext(), req)
		s.NoError(err, "Failed to dedup replicate history event")
		s.Equal(&historyservice.ReplicateEventsV2Response{}, resp)
	}
}

func (s *nDCIntegrationTestSuite) applyEventsThroughFetcher(
	workflowID string,
	runID string,
	workflowType string,
	taskqueue string,
	versionHistory *persistence.VersionHistory,
	eventBatches []*historypb.History,
) {
	for _, batch := range eventBatches {
		eventBlob, newRunEventBlob := s.generateEventBlobs(workflowID, runID, workflowType, taskqueue, batch)

		taskType := enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK
		replicationTask := &replicationspb.ReplicationTask{
			TaskType:     taskType,
			SourceTaskId: 1,
			Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				TaskId:              1,
				NamespaceId:         s.namespaceID,
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: s.toProtoVersionHistoryItems(versionHistory),
				Events:              s.toProtoDataBlob(eventBlob),
				NewRunEvents:        s.toProtoDataBlob(newRunEventBlob),
			}},
		}

		s.standByReplicationTasksChan <- replicationTask
		// this is to test whether dedup works
		s.standByReplicationTasksChan <- replicationTask
	}
}

func (s *nDCIntegrationTestSuite) eventBatchesToVersionHistory(
	versionHistory *persistence.VersionHistory,
	eventBatches []*historypb.History,
) *persistence.VersionHistory {

	// TODO temporary code to generate version history
	//  we should generate version as part of modeled based testing
	if versionHistory == nil {
		versionHistory = persistence.NewVersionHistory(nil, nil)
	}
	for _, batch := range eventBatches {
		for _, event := range batch.Events {
			err := versionHistory.AddOrUpdateItem(
				persistence.NewVersionHistoryItem(
					event.GetEventId(),
					event.GetVersion(),
				))
			s.NoError(err)
		}
	}

	return versionHistory
}

func (s *nDCIntegrationTestSuite) toProtoVersionHistoryItems(
	versionHistory *persistence.VersionHistory,
) []*historyspb.VersionHistoryItem {
	if versionHistory == nil {
		return nil
	}

	return versionHistory.ToProto().Items
}

func (s *nDCIntegrationTestSuite) setupRemoteFrontendClients() {
	s.mockAdminClient["standby"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).AnyTimes()
	s.mockAdminClient["other"].(*adminservicemock.MockAdminServiceClient).EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).AnyTimes()
}
