package xdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	streamBasedReplicationTestSuite struct {
		xdcBaseSuite
		controller    *gomock.Controller
		namespaceName string
		namespaceID   string
		serializer    serialization.Serializer
		generator     test.Generator
		once          sync.Once
		laneStreams   bool
	}
)

func TestStreamBasedReplicationTestSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
		laneStreams             bool
	}{
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
		{
			name:                    "EnableTransitionHistory_LaneStreams",
			enableTransitionHistory: true,
			laneStreams:             true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &streamBasedReplicationTestSuite{
				namespaceName: "replication-test-" + common.GenerateRandomString(5),
				laneStreams:   tc.laneStreams,
			}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *streamBasedReplicationTestSuite) SetupSuite() {
	s.controller = gomock.NewController(s.T())
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():                   true,
		dynamicconfig.EnableReplicationTaskBatching.Key():             true,
		dynamicconfig.EnableWorkflowTaskStampIncrementOnFailure.Key(): true,
		dynamicconfig.EnableSeparateReplicationEnableFlag.Key():       true,
	}
	if s.laneStreams {
		// Replication lanes split HIGH priority traffic, which requires the tiered stack.
		s.dynamicConfigOverrides[dynamicconfig.EnableReplicationTaskTieredProcessing.Key()] = true
	}
	s.logger = log.NewTestLogger()
	s.serializer = serialization.NewSerializer()

	s.setupSuite(
		testcore.WithDCRedirectionPolicy(config.DCRedirectionPolicy{Policy: "noop"}),
		testcore.WithClusterHistoryTaskRecorder(),
		testcore.WithReplicationStreamRecorder(),
	)
	if s.laneStreams {
		s.switchToLaneStreams()
	}
}

// switchToLaneStreams selects the lane stream implementation for every stream between the
// clusters, with lanes enabled on both ends, and waits until each cluster has created lane
// senders and receivers.
func (s *streamBasedReplicationTestSuite) switchToLaneStreams() {
	captures := make([]*metricstest.Capture, len(s.clusters))
	for i, c := range s.clusters {
		captureHandler, ok := c.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
		s.Require().True(ok, "cluster metrics handler does not support capture")
		captures[i] = captureHandler.StartCapture()
		s.T().Cleanup(func() { captureHandler.StopCapture(captures[i]) })
	}
	for _, c := range s.clusters {
		c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamReceiverLanesEnabled, true)
		c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamSenderLanesEnabled, true)
		s.selectStreamImplementations(c, "lane", "lane")
	}
	// SetupSuite's T lives for the whole suite, so wait on a background context: a context
	// bound to the suite's T would fail the suite once the suite outlasts its timeout.
	await.Require(context.Background(), s.T(), func(t *await.T) {
		require.True(t, s.laneStreamsCreated(captures), "clusters did not create lane stream senders and receivers")
	}, 30*time.Second, 200*time.Millisecond)
}

// laneStreamsCreated reports whether every capture recorded both a lane stream sender and a
// lane stream receiver.
func (s *streamBasedReplicationTestSuite) laneStreamsCreated(captures []*metricstest.Capture) bool {
	for _, capture := range captures {
		var laneSender, laneReceiver bool
		for _, recording := range capture.SnapshotMetric(metrics.ReplicationStreamImplementationSelected.Name()) {
			if recording.Tags["replication_stream_implementation"] != "lane" {
				continue
			}
			switch recording.Tags["operation"] {
			case laneTestSenderOperation:
				laneSender = true
			case laneTestReceiverOperation:
				laneReceiver = true
			default:
			}
		}
		if !laneSender || !laneReceiver {
			return false
		}
	}
	return true
}

func (s *streamBasedReplicationTestSuite) TearDownSuite() {
	// Dump recorders once at the end of all tests
	s.dumpRecorders()

	if s.generator != nil {
		s.generator.Reset()
	}
	s.controller.Finish()
	s.tearDownSuite()
}

func (s *streamBasedReplicationTestSuite) SetupTest() {
	s.setupTest()

	s.once.Do(func() {
		ctx := context.Background()
		_, err := s.clusters[0].FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace: s.namespaceName,
			Clusters:  s.clusterReplicationConfig(),
			// The first cluster is the active cluster.
			ActiveClusterName: s.clusters[0].ClusterName(),
			// Needed so that the namespace is replicated.
			IsGlobalNamespace: true,
			// This is a required parameter.
			WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
		})
		s.Require().NoError(err)
		err = s.waitUntilNamespaceReplicated(ctx, s.namespaceName)
		s.Require().NoError(err)

		nsRes, _ := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: s.namespaceName,
		})

		s.namespaceID = nsRes.NamespaceInfo.GetId()
		s.generator = test.InitializeHistoryEventGenerator("namespace", "ns-id", 1)
	})
	for _, cluster := range s.clusters {
		recorder := cluster.GetHistoryTaskRecorder()
		s.Require().NotNil(recorder)
	}
}

// getRecorder returns the HistoryTaskRecorder for the specified cluster index.
// Returns nil if the cluster doesn't have a recorder.
func (s *streamBasedReplicationTestSuite) getRecorder(clusterIdx int) *testcore.HistoryTaskRecorder {
	if clusterIdx < len(s.clusters) {
		return s.clusters[clusterIdx].GetHistoryTaskRecorder()
	}
	return nil
}

func (s *streamBasedReplicationTestSuite) TestReplicateHistoryEvents_ForceReplicationScenario() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	var versions []int64
	if s.enableTransitionHistory {
		// Use versions for cluster1 (active) so we can update workflows
		// Use same versions to prevent workflow tasks from being failed due to WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND
		versions = []int64{1, 1, 1, 1, 1, 1, 1, 1, 1}
	} else {
		versions = []int64{2, 12, 22, 32, 2, 1, 5, 8, 9}
	}

	// let's import some events into cluster0
	historyClient0 := s.clusters[0].HistoryClient()
	executions := s.importTestEvents(historyClient0, namespace.Name(s.namespaceName), namespace.ID(s.namespaceID), versions)

	// let's trigger replication by calling GenerateLastHistoryReplicationTasks. This is also used by force replication logic
	for _, execution := range executions {
		_, err := historyClient0.GenerateLastHistoryReplicationTasks(ctx, &historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId: s.namespaceID,
			Execution:   execution,
			ArchetypeId: chasm.WorkflowArchetypeID,
		})
		s.NoError(err)
	}

	s.waitForClusterSynced()
	for _, execution := range executions {
		err := s.assertHistoryEvents(ctx, s.namespaceID, execution.GetWorkflowId(), execution.GetRunId())
		s.NoError(err)
	}
}

func (s *streamBasedReplicationTestSuite) importTestEvents(
	historyClient historyservice.HistoryServiceClient,
	namespaceName namespace.Name,
	namespaceId namespace.ID,
	versions []int64,
) []*commonpb.WorkflowExecution {
	executions := []*commonpb.WorkflowExecution{}
	s.generator.Reset()
	isCloseEvent := func(event *historypb.HistoryEvent) bool {
		eventType := event.GetEventType()
		if eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED ||
			eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED ||
			eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED ||
			eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED ||
			eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW ||
			eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
			return true
		}
		return false
	}
	var runID string
	for _, version := range versions {
		workflowID := "xdc-stream-replication-test-" + uuid.NewString()
		runID = uuid.NewString()

		var historyBatch []*historypb.History
		s.generator = test.InitializeHistoryEventGenerator(namespaceName, namespaceId, version)
		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()

			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			if isCloseEvent(historyEvents.Events[len(historyEvents.Events)-1]) {
				historyEvents.Events = historyEvents.Events[:len(historyEvents.Events)-1]
			}
			historyBatch = append(historyBatch, historyEvents)
		}

		versionHistory, err := testcore.EventBatchesToVersionHistory(nil, historyBatch)
		s.NoError(err)
		s.importEvents(
			workflowID,
			runID,
			versionHistory,
			historyBatch,
			historyClient,
			true,
		)

		if s.enableTransitionHistory {
			// signal the workflow to make sure the TransitionHistory is updated
			signalName := "my signal"
			signalInput := payloads.EncodeString("my signal input")
			client0 := s.clusters[0].FrontendClient() // active
			_, err = client0.SignalWorkflowExecution(context.Background(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace:         s.namespaceName,
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				SignalName:        signalName,
				Input:             signalInput,
				Identity:          "worker1",
			})
			s.NoError(err)
		}

		executions = append(executions, &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID})
	}
	return executions
}

func (s *streamBasedReplicationTestSuite) waitUntilNamespaceReplicated(
	ctx context.Context,
	namespaceName string,
) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := s.clusters[1].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
				Namespace: namespaceName,
			})
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func (s *streamBasedReplicationTestSuite) assertHistoryEvents(
	ctx context.Context,
	namespaceId string,
	workflowId string,
	runId string,
) error {
	mockClientBean := client.NewMockBean(s.controller)
	mockClientBean.
		EXPECT().
		GetRemoteAdminClient("cluster1").
		Return(s.clusters[0].AdminClient(), nil).
		AnyTimes()
	mockClientBean.EXPECT().GetRemoteAdminClient("cluster2").Return(s.clusters[1].AdminClient(), nil).AnyTimes()

	cluster1Fetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		s.serializer,
		s.logger,
	)
	cluster2Fetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		s.serializer,
		s.logger,
	)
	iterator1 := cluster1Fetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		ctx, "cluster1", namespace.ID(namespaceId), workflowId, runId, 0, 1, 0, 0)
	iterator2 := cluster2Fetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		ctx, "cluster2", namespace.ID(namespaceId), workflowId, runId, 0, 1, 0, 0)
	for iterator1.HasNext() {
		s.True(iterator2.HasNext())
		batch1, err := iterator1.Next()
		s.NoError(err)
		batch2, err := iterator2.Next()
		s.NoError(err)
		getMsg := func() string {
			events1, _ := s.serializer.DeserializeEvents(batch1.RawEventBatch)
			events2, _ := s.serializer.DeserializeEvents(batch2.RawEventBatch)
			return fmt.Sprintf("Not equal \nevents1: %v \nevents2: %v", events1, events2)
		}
		s.Equal(batch1.RawEventBatch, batch2.RawEventBatch, getMsg())
		s.Equal(batch1.VersionHistory.Items, batch2.VersionHistory.Items)

	}
	s.False(iterator2.HasNext())
	return nil
}

func (s *streamBasedReplicationTestSuite) importEvents(
	workflowID string,
	runID string,
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
	historyClient historyservice.HistoryServiceClient,
	verifyWorkflowNotExists bool,
) {
	if len(eventBatches) == 0 {
		return
	}

	historyClient = history.NewRetryableClient(
		historyClient,
		common.CreateHistoryClientRetryPolicy(func() bool { return false }),
		common.IsResourceExhausted,
	)
	var token []byte
	for _, batch := range eventBatches {
		blob, err := s.serializer.SerializeEvents(batch.Events)
		s.NoError(err)
		req := &historyservice.ImportWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistory: versionHistory,
			HistoryBatches: []*commonpb.DataBlob{blob},
			Token:          token,
		}
		resp, err := historyClient.ImportWorkflowExecution(context.Background(), req)
		s.NoError(err, "Failed to import history event")
		token = resp.Token
	}

	if verifyWorkflowNotExists {
		_, err := historyClient.GetMutableState(context.Background(), &historyservice.GetMutableStateRequest{
			NamespaceId: s.namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
		s.IsType(&serviceerror.NotFound{}, err)
	}

	req := &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistory: versionHistory,
		HistoryBatches: []*commonpb.DataBlob{},
		Token:          token,
	}
	resp, err := historyClient.ImportWorkflowExecution(context.Background(), req)
	s.NoError(err, "Failed to import history event")
	s.Nil(resp.Token)
}

func (s *streamBasedReplicationTestSuite) TestForceReplicateResetWorkflow_BaseWorkflowNotFound() {
	ns := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	}
	resp, err := client0.DescribeNamespace(testcore.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	// Start a workflow
	id := "force-replicate-reset-test"
	wt := "force-replicate-reset-test-type"
	tl := "force-replicate-reset-test-tq"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           ns,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Process start event in cluster0
	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)

	resetResp, err := client0.ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		Reason:                    "test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)

	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)

	_, err = client0.DeleteWorkflowExecution(testcore.NewContext(), &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)

	// Wipe the local copies on the passive cluster. The frontend DeleteWorkflowExecution API rejects
	// deletions on a cluster that is passive for the workflow (they would not be replicated), so go
	// through the history service directly, which is the same path replication apply uses. The admin
	// force-delete API is not usable here: it deletes the DB rows without clearing the workflow cache,
	// so the target keeps serving the deleted runs from cache.
	client1 := s.clusters[1].FrontendClient()
	_, err = s.clusters[1].HistoryClient().DeleteWorkflowExecution(testcore.NewContext(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: resp.NamespaceInfo.GetId(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)
	_, err = s.clusters[1].HistoryClient().DeleteWorkflowExecution(testcore.NewContext(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: resp.NamespaceInfo.GetId(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp.GetRunId(),
		},
	})
	s.NoError(err)

	time.Sleep(time.Second)

	_, err = client1.DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp.GetRunId(),
		},
	})
	s.Error(err)

	_, err = s.clusters[0].HistoryClient().GenerateLastHistoryReplicationTasks(testcore.NewContext(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: resp.NamespaceInfo.GetId(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp.GetRunId(),
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)

	for range 5 {
		wfExec, err := client1.DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      resetResp.GetRunId(),
			},
		})
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, wfExec.WorkflowExecutionInfo.Status)
		return
	}
	s.Fail("Cannot replicate reset workflow to target cluster.")
}

func (s *streamBasedReplicationTestSuite) TestResetWorkflow_SyncWorkflowState() {
	ns := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	}
	resp, err := client0.DescribeNamespace(testcore.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	// Start a workflow
	id := "reset-test"
	wt := "reset-test-type"
	tl := "reset-test-tq"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           ns,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Process start event in cluster0
	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)

	resetResp1, err := client0.ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		Reason:                    "test",
		WorkflowTaskFinishEventId: 4,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)

	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)

	resetResp2, err := client0.ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp1.GetRunId(),
		},
		Reason:                    "test",
		WorkflowTaskFinishEventId: 7,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)

	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		return err == nil
	},
		time.Second*10,
		time.Second)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      resetResp1.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		return err == nil
	},
		time.Second*10,
		time.Second)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      resetResp2.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		return err == nil
	},
		time.Second*10,
		time.Second)

	// Delete reset workflows
	_, err = s.clusters[1].AdminClient().DeleteWorkflowExecution(testcore.NewContext(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	_, err = s.clusters[1].AdminClient().DeleteWorkflowExecution(testcore.NewContext(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp1.GetRunId(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	_, err = s.clusters[1].AdminClient().DeleteWorkflowExecution(testcore.NewContext(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp2.GetRunId(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)

	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		var expectedErr *serviceerror.NotFound
		return errors.As(err, &expectedErr)
	},
		time.Second*10,
		time.Second)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      resetResp1.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		var expectedErr *serviceerror.NotFound
		return errors.As(err, &expectedErr)
	},
		time.Second*10,
		time.Second)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      resetResp2.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		var expectedErr *serviceerror.NotFound
		return errors.As(err, &expectedErr)
	},
		time.Second*10,
		time.Second)

	_, err = s.clusters[0].HistoryClient().GenerateLastHistoryReplicationTasks(testcore.NewContext(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: resp.NamespaceInfo.GetId(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		return err == nil
	},
		time.Second*10,
		time.Second)

	_, err = s.clusters[0].HistoryClient().GenerateLastHistoryReplicationTasks(testcore.NewContext(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: resp.NamespaceInfo.GetId(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp1.GetRunId(),
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      resetResp1.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		return err == nil
	},
		time.Second*10,
		time.Second)

	_, err = s.clusters[0].HistoryClient().GenerateLastHistoryReplicationTasks(testcore.NewContext(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: resp.NamespaceInfo.GetId(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp2.GetRunId(),
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(
			testcore.NewContext(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      resetResp2.GetRunId(),
				},
				Archetype: chasm.WorkflowArchetype,
			})
		return err == nil
	},
		time.Second*10,
		time.Second)
}

func (s *streamBasedReplicationTestSuite) TestCloseTransferTaskAckedReplication() {
	// Test works for both SyncVersionedTransitionTask (with transition history)
	// and SyncWorkflowStateTask (without transition history)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	ns := s.createNamespaceInCluster0(false)
	s.T().Logf("Created local namespace '%s' on cluster 0 (active)", ns)

	for _, cluster := range s.clusters {
		recorder := cluster.GetReplicationStreamRecorder()
		recorder.Clear()
	}
	s.T().Log("Cleared replication stream recorders on all clusters")

	workflowID := "test-replication-" + uuid.NewString()
	sourceClient := s.clusters[0].FrontendClient()
	startResp, err := sourceClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.Require().NoError(err, "Failed to start workflow execution")
	s.T().Logf("Started workflow '%s' (RunID: %s) on cluster 0", workflowID, startResp.GetRunId())

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	//nolint:staticcheck // TODO: replace with taskpoller.TaskPoller
	poller := &testcore.TaskPoller{
		Client:              sourceClient,
		Namespace:           ns,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		Identity:            "worker-identity",
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.Require().NoError(err, "Failed to poll and process workflow task")
	s.T().Log("Completed workflow execution via worker poll")

	s.Eventually(func() bool {
		resp, err := sourceClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      startResp.GetRunId(),
			},
		})
		if err != nil {
			return false
		}
		return resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, 100*time.Millisecond)
	s.T().Log("Verified workflow reached COMPLETED status")

	namespaceResp, err := sourceClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.Require().NoError(err, "Failed to describe namespace")
	namespaceID := namespaceResp.NamespaceInfo.GetId()

	historyClient := s.clusters[0].HistoryClient()
	mutableStateResp, err := historyClient.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      startResp.GetRunId(),
		},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	s.Require().NoError(err, "Failed to describe mutable state")
	s.Require().NotNil(mutableStateResp.GetDatabaseMutableState(), "Database mutable state is nil")
	s.T().Logf("Retrieved mutable state for workflow (NamespaceID: %s)", namespaceID)

	//nolint:forbidigo // waiting for close transfer task to be acked - no alternative available
	time.Sleep(5 * time.Second)
	s.T().Log("Waited 5 seconds for close transfer task to be acknowledged internally")

	targetClient := s.clusters[1].FrontendClient()
	_, err = targetClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.Require().Error(err, "Namespace should not exist on target cluster before promotion")

	_, err = targetClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      startResp.GetRunId(),
		},
	})
	s.Require().Error(err, "Workflow should not exist on target cluster before promotion")
	s.T().Log("Verified namespace and workflow do NOT exist on cluster 1 (standby) before promotion")

	_, err = sourceClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:        ns,
		PromoteNamespace: true,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: s.clusters[0].ClusterName()},
				{ClusterName: s.clusters[1].ClusterName()},
			},
		},
	})
	s.Require().NoError(err, "Failed to promote namespace to global")
	s.T().Logf("Promoted namespace '%s' from local to global (added cluster 1)", ns)

	s.Eventually(func() bool {
		_, err := targetClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: ns,
		})
		return err == nil
	}, 60*time.Second, time.Second)
	s.T().Log("Verified namespace replicated to cluster 1")

	sourceAdminClient := s.clusters[0].AdminClient()
	_, err = sourceAdminClient.GenerateLastHistoryReplicationTasks(ctx, &adminservice.GenerateLastHistoryReplicationTasksRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      startResp.GetRunId(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.Require().NoError(err, "Failed to generate last history replication tasks")
	s.T().Log("Generated last history replication tasks to force replication of completed workflow")

	recorder := s.clusters[0].GetReplicationStreamRecorder()

	// Check for flags based on transition history mode
	if s.enableTransitionHistory {
		// With transition history: check SyncVersionedTransitionTask
		s.T().Log("Checking replication stream for close transfer task acknowledgment in versioned transition artifact...")
		s.Eventually(func() bool {
			for _, msg := range recorder.GetMessages() {
				if msg.Direction != testcore.DirectionServerSend {
					continue
				}

				resp := testcore.ExtractReplicationMessages(msg.Request)
				if resp == nil {
					continue
				}

				for _, task := range resp.GetReplicationTasks() {
					if syncAttrs := task.GetSyncVersionedTransitionTaskAttributes(); syncAttrs != nil {
						if artifact := syncAttrs.GetVersionedTransitionArtifact(); artifact != nil {
							if artifact.GetIsCloseTransferTaskAcked() && artifact.GetIsForceReplication() {
								return true
							}
						}
					}
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond)
		s.T().Log("Verified IsCloseTransferTaskAcked and IsForceReplication flags in SyncVersionedTransitionTask")
	} else {
		// Without transition history: check SyncWorkflowStateTask
		s.T().Log("Checking replication stream for close transfer task acknowledgment in workflow state attributes...")
		s.Eventually(func() bool {
			for _, msg := range recorder.GetMessages() {
				if msg.Direction != testcore.DirectionServerSend {
					continue
				}

				resp := testcore.ExtractReplicationMessages(msg.Request)
				if resp == nil {
					continue
				}

				for _, task := range resp.GetReplicationTasks() {
					if workflowStateAttrs := task.GetSyncWorkflowStateTaskAttributes(); workflowStateAttrs != nil {
						if workflowStateAttrs.GetIsCloseTransferTaskAcked() && workflowStateAttrs.GetIsForceReplication() {
							return true
						}
					}
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond)
		s.T().Log("Verified IsCloseTransferTaskAcked and IsForceReplication flags in SyncWorkflowStateTask")
	}

	// Wait for replication to complete to the passive cluster
	s.T().Log("Waiting for workflow to replicate to cluster 1 (passive)...")
	s.Eventually(func() bool {
		resp, err := targetClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      startResp.GetRunId(),
			},
		})
		if err != nil {
			return false
		}
		return resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, time.Second)
	s.T().Log("Verified workflow replicated to cluster 1 (passive) with COMPLETED status")

	// Write captured replication messages to log files for both clusters
	for _, cluster := range s.clusters {
		recorder := cluster.GetReplicationStreamRecorder()
		if err := recorder.WriteToLog(); err != nil {
			s.T().Logf("Failed to write replication stream log for cluster %s: %v", cluster.ClusterName(), err)
		}
	}
}

// TestPassiveActivityRetryTimerReplication verifies that activity retry timers are correctly generated
// on the active cluster and replicated to the passive/standby cluster during activity retries.
func (s *streamBasedReplicationTestSuite) TestPassiveActivityRetryTimerReplication() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	recorder0 := s.getRecorder(0) // active cluster
	recorder1 := s.getRecorder(1) // standby cluster
	s.Require().NotNil(recorder0)
	s.Require().NotNil(recorder1)

	workflowID := "task-recorder-test-" + uuid.NewString()
	taskQueue := "task-recorder-tq"

	// Get namespace ID for task filtering
	namespaceResp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: s.namespaceName,
	})
	s.NoError(err)
	namespaceID := namespaceResp.NamespaceInfo.GetId()

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: s.namespaceName,
	})
	s.NoError(err)
	defer sdkClient.Close()

	var activityAttempts atomic.Int32

	// Workflow with activity that retries with 3 second intervals
	simpleWorkflow := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    3 * time.Second, // 3 seconds between retries, needed to ensure tasks can replicate w/out lag
				MaximumInterval:    3 * time.Second,
				BackoffCoefficient: 1.0,
				MaximumAttempts:    3, // Fail twice, succeed on 3rd attempt
			},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result string
		err := workflow.ExecuteActivity(ctx, "test-activity").Get(ctx, &result)
		if err != nil {
			return "", err
		}
		return "completed: " + result, nil
	}

	// Activity that sleeps 3 seconds and fails twice, succeeds on 3rd attempt
	simpleActivity := func(ctx context.Context) (string, error) {
		attempt := activityAttempts.Add(1)
		if attempt < 3 {
			return "", fmt.Errorf("failed attempt %d", attempt)
		}
		return "success", nil
	}

	sdkWorker := worker.New(sdkClient, taskQueue, worker.Options{})
	sdkWorker.RegisterWorkflowWithOptions(simpleWorkflow, workflow.RegisterOptions{Name: "test-workflow"})
	sdkWorker.RegisterActivityWithOptions(simpleActivity, activity.RegisterOptions{Name: "test-activity"})

	err = sdkWorker.Start()
	s.NoError(err)
	defer sdkWorker.Stop()

	// Override dynamic config to disable eager activity execution since we are asserting transfer active task creation in this test.
	cleanup := s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.EnableActivityEagerExecution, false)
	defer cleanup()

	workflowRun, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 30 * time.Second,
	}, "test-workflow")
	s.NoError(err)
	runID := workflowRun.GetRunID()

	var workflowResult string
	err = workflowRun.Get(ctx, &workflowResult)
	s.NoError(err)

	// Get the scheduled event ID from workflow history
	historyIter := sdkClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var scheduledEventID int64
	for historyIter.HasNext() {
		event, err := historyIter.Next()
		s.NoError(err)
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			scheduledEventID = event.GetEventId()
			attrs := event.GetActivityTaskScheduledEventAttributes()
			// Verify this is the activity we expect
			s.Equal("test-activity", attrs.GetActivityType().GetName(), "Should be our test activity")
			break
		}
	}
	s.Require().NotZero(scheduledEventID, "Should have found ActivityTaskScheduled event")

	// Wait for async task generation and replication to standby
	s.Require().Eventually(func() bool {
		standbyTasks := recorder1.CountTasksForWorkflow(
			tasks.CategoryTimer,
			namespaceID,
			workflowID,
			runID,
			func(rt testcore.RecordedTask) bool {
				return rt.TaskType == enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER.String()
			},
		)
		return standbyTasks >= 2
	}, 10*time.Second, 200*time.Millisecond, "Standby cluster should eventually have replicated retry timers")

	// Verify active cluster generated exactly 1 TransferActivityTask with matching event ID
	activeTransferActivityTasks := recorder0.CountTasksForWorkflow(
		tasks.CategoryTransfer,
		namespaceID,
		workflowID,
		runID,
		func(rt testcore.RecordedTask) bool {
			if rt.TaskType != enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK.String() {
				return false
			}
			// Check if the task's event ID matches the activity's scheduled event ID
			if activityTask, ok := rt.Task.(*tasks.ActivityTask); ok {
				return activityTask.ScheduledEventID == scheduledEventID
			}
			return false
		},
	)
	s.Equal(1, activeTransferActivityTasks, "Active cluster should have exactly 1 TransferActivityTask for this activity")

	// Verify active cluster generated exactly 2 ActivityRetryTimer tasks (for 2 failed attempts) with matching event ID
	activeRetryTimers := recorder0.CountTasksForWorkflow(
		tasks.CategoryTimer,
		namespaceID,
		workflowID,
		runID,
		func(rt testcore.RecordedTask) bool {
			if rt.TaskType != enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER.String() {
				return false
			}
			// Check if the timer's event ID matches the activity's scheduled event ID
			if timerTask, ok := rt.Task.(*tasks.ActivityRetryTimerTask); ok {
				return timerTask.EventID == scheduledEventID
			}
			return false
		},
	)
	s.Equal(2, activeRetryTimers, "Active cluster should have exactly 2 ActivityRetryTimer tasks for this activity")

	// Verify standby cluster has exactly 1 TransferActivityTask with matching event ID (same as active)
	standbyTransferActivityTasks := recorder1.CountTasksForWorkflow(
		tasks.CategoryTransfer,
		namespaceID,
		workflowID,
		runID,
		func(rt testcore.RecordedTask) bool {
			if rt.TaskType != enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK.String() {
				return false
			}
			// Check if the task's event ID matches the activity's scheduled event ID
			if activityTask, ok := rt.Task.(*tasks.ActivityTask); ok {
				return activityTask.ScheduledEventID == scheduledEventID
			}
			return false
		},
	)
	s.Equal(1, standbyTransferActivityTasks, "Standby cluster should have exactly 1 TransferActivityTask for this activity (same as active)")

	// Verify standby cluster has exactly 2 ActivityRetryTimer tasks with matching event ID (same as active)
	standbyRetryTimers := recorder1.CountTasksForWorkflow(
		tasks.CategoryTimer,
		namespaceID,
		workflowID,
		runID,
		func(rt testcore.RecordedTask) bool {
			if rt.TaskType != enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER.String() {
				return false
			}
			// Check if the timer's event ID matches the activity's scheduled event ID
			if timerTask, ok := rt.Task.(*tasks.ActivityRetryTimerTask); ok {
				return timerTask.EventID == scheduledEventID
			}
			return false
		},
	)
	s.Equal(2, standbyRetryTimers, "Standby cluster should have exactly 2 ActivityRetryTimer tasks for this activity (same as active)")
}

func (s *streamBasedReplicationTestSuite) TestWorkflowTaskFailureStampReplication() {
	// This test validates stamp increments on standby cluster via replication.
	// NOTE: This test cannot work with DisableTransitionHistory because transient workflow
	// task failures (attempt >= 2) don't create history events, so stamp increments from
	// transient failures never replicate to the standby cluster. The standby only sees
	// stamp increments from non-transient failures (attempt == 1).
	if !s.enableTransitionHistory {
		s.T().Skip("Skipping TestWorkflowTaskFailureStampReplication: transient workflow task failures don't replicate with event based replication")
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	recorder0 := s.getRecorder(0) // active cluster
	recorder1 := s.getRecorder(1) // standby cluster
	s.Require().NotNil(recorder0)
	s.Require().NotNil(recorder1)

	workflowID := "workflow-task-failure-test-" + uuid.NewString()
	taskQueue := "workflow-task-failure-tq"

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: s.namespaceName,
	})
	s.NoError(err)
	defer sdkClient.Close()

	// Counter for workflow task attempts
	var attemptCount atomic.Int32

	// Workflow that fails 3 times then succeeds on 4th attempt
	failingWorkflow := func(ctx workflow.Context) (string, error) {
		attempt := attemptCount.Add(1)
		if attempt <= 3 {
			// Panic to cause workflow task timeout (will retry after 10 seconds)
			panic(fmt.Sprintf("intentional workflow task failure on attempt %d", attempt))
		}
		// Succeed on 4th attempt
		return "completed", nil
	}

	s.T().Logf("Starting worker...")
	sdkWorker := worker.New(sdkClient, taskQueue, worker.Options{})
	sdkWorker.RegisterWorkflowWithOptions(failingWorkflow, workflow.RegisterOptions{Name: "simple-workflow"})

	err = sdkWorker.Start()
	s.NoError(err)
	defer sdkWorker.Stop()

	s.T().Logf("Starting workflow that will fail 3 times...")
	workflowRun, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowRunTimeout:       60 * time.Second,
		WorkflowTaskTimeout:      10 * time.Second,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "simple-workflow")
	s.NoError(err)
	s.T().Logf("Started workflow (RunID: %s)", workflowRun.GetRunID())

	var workflowResult string
	err = workflowRun.Get(ctx, &workflowResult)
	s.NoError(err)
	s.Equal("completed", workflowResult)
	s.T().Logf("Workflow completed successfully after 3 failures: %s (RunID: %s)", workflowResult, workflowRun.GetRunID())

	//nolint:forbidigo // Wait a bit for replication to complete to the standby cluster
	time.Sleep(2 * time.Second)

	// Verify stamp increments on ACTIVE cluster
	s.T().Logf("Verifying stamp increments on active cluster...")
	s.verifyWorkflowTaskStamps(recorder0, "active", workflowID, workflowRun.GetRunID(), s.namespaceID)

	// Verify stamp increments on STANDBY cluster (passive side)
	s.T().Logf("Verifying stamp increments on standby cluster...")
	s.verifyWorkflowTaskStamps(recorder1, "standby", workflowID, workflowRun.GetRunID(), s.namespaceID)

	s.T().Logf("✓ All stamp assertions passed! Stamps are correctly incremented on both active and standby clusters.")
}

func (s *streamBasedReplicationTestSuite) verifyWorkflowTaskStamps(
	recorder *testcore.HistoryTaskRecorder,
	clusterName string,
	workflowID string,
	runID string,
	namespaceID string,
) {
	s.Require().NotEmpty(namespaceID, "NamespaceID must not be empty")
	s.Require().NotEmpty(workflowID, "WorkflowID must not be empty")
	s.Require().NotEmpty(runID, "RunID must not be empty")

	type taskWithStamp struct {
		attempt int32
		stamp   int32
	}

	var transferTasks []taskWithStamp
	var timeoutTasks []taskWithStamp

	filter := testcore.TaskFilter{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	transferRecorded := recorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTransfer, filter)
	for _, recorded := range transferRecorded {
		if wfTask, ok := recorded.Task.(*tasks.WorkflowTask); ok {
			transferTasks = append(transferTasks, taskWithStamp{
				stamp: wfTask.Stamp,
			})
		}
	}

	timerRecorded := recorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter)
	for _, recorded := range timerRecorded {
		if timeoutTask, ok := recorded.Task.(*tasks.WorkflowTaskTimeoutTask); ok {
			timeoutTasks = append(timeoutTasks, taskWithStamp{
				attempt: timeoutTask.ScheduleAttempt,
				stamp:   timeoutTask.Stamp,
			})
		}
	}

	s.Require().Len(transferTasks, 4, "Expected 4 TransferWorkflowTask tasks on %s cluster", clusterName)
	s.Require().Len(timeoutTasks, 4, "Expected 4 WorkflowTaskTimeout tasks on %s cluster", clusterName)

	for i, task := range transferTasks {
		expectedStamp := int32(i)
		s.Equal(expectedStamp, task.stamp,
			"TransferWorkflowTask #%d on %s cluster: expected stamp=%d, got stamp=%d",
			i+1, clusterName, expectedStamp, task.stamp)
	}
	s.T().Logf("✓ %s cluster: All %d TransferWorkflowTask stamps are correct (0→1→2→3)",
		clusterName, len(transferTasks))

	for _, task := range timeoutTasks {
		expectedStamp := task.attempt - 1
		s.Equal(expectedStamp, task.stamp,
			"WorkflowTaskTimeout attempt %d on %s cluster: expected stamp=%d, got stamp=%d",
			task.attempt, clusterName, expectedStamp, task.stamp)
	}
	s.T().Logf("✓ %s cluster: All %d WorkflowTaskTimeout stamps are correct (attempts 1-4 → stamps 0-3)",
		clusterName, len(timeoutTasks))
}

// dumpRecorders writes task queues and replication streams to /tmp/xdc for debugging
func (s *streamBasedReplicationTestSuite) dumpRecorders() {
	// Ensure output directory exists
	outputDir := "/tmp/xdc"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		s.T().Logf("Failed to create output directory %s: %v", outputDir, err)
		return
	}

	recorder0 := s.getRecorder(0)
	recorder1 := s.getRecorder(1)

	// Dump task queues using built-in WriteToLog with cluster names
	if recorder0 != nil && len(s.clusters) > 0 {
		clusterName := s.clusters[0].ClusterName()
		activeTaskFile := fmt.Sprintf("/tmp/xdc/task_queue_%s_active.json", clusterName)
		if err := recorder0.WriteToLog(activeTaskFile); err != nil {
			s.T().Logf("Failed to write active cluster tasks: %v", err)
		} else {
			s.T().Logf("Wrote active cluster tasks to %s", activeTaskFile)
		}
	}

	if recorder1 != nil && len(s.clusters) > 1 {
		clusterName := s.clusters[1].ClusterName()
		standbyTaskFile := fmt.Sprintf("/tmp/xdc/task_queue_%s_passive.json", clusterName)
		if err := recorder1.WriteToLog(standbyTaskFile); err != nil {
			s.T().Logf("Failed to write standby cluster tasks: %v", err)
		} else {
			s.T().Logf("Wrote standby cluster tasks to %s", standbyTaskFile)
		}
	}

	// Dump replication streams using built-in WriteToLog
	for i, cluster := range s.clusters {
		recorder := cluster.GetReplicationStreamRecorder()
		if recorder == nil {
			continue
		}

		// NOTE: This is a strong assumption on how we should have our test set replication direction
		clusterRole := "active"
		if i > 0 {
			clusterRole = "passive"
		}
		replicationFile := fmt.Sprintf("/tmp/xdc/replication_stream_%s_%s.log", cluster.ClusterName(), clusterRole)
		recorder.SetOutputFile(replicationFile)
		if err := recorder.WriteToLog(); err != nil {
			s.T().Logf("Failed to write cluster %d replication stream: %v", i, err)
		} else {
			s.T().Logf("Wrote cluster %d replication stream to %s", i, replicationFile)
		}
	}
}

// TestLaneIsolation isolates the suite's namespace onto a replication lane, checks that its
// tasks replicate on the lane, and checks that the lane retires once the namespace is no
// longer isolated.
func (s *streamBasedReplicationTestSuite) TestLaneIsolation() {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	stopIsolating := s.isolateSuiteNamespace(1000)
	workflowIDs := s.startWorkflowsUntilLaneSendsTasks()
	s.requireWorkflowsReplicated(workflowIDs[len(workflowIDs)-1:])

	stopIsolating()
	await.Require(context.Background(), s.T(), func(t *await.T) {
		require.True(t, s.sawLaneMessage(s.clusters[0], func(messages *replicationspb.WorkflowReplicationMessages) bool {
			return messages.GetLaneInfo().GetRetire()
		}), "the lane did not retire")
	}, 60*time.Second, time.Second)
}

// TestLaneKillSwitch_SenderLanesDisabled turns sender lanes off while a lane has a backlog.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_SenderLanesDisabled() {
	s.runLaneKillSwitchDrill(func() {
		for _, c := range s.clusters {
			c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamSenderLanesEnabled, false)
		}
	})
}

// TestLaneKillSwitch_LegacySender switches senders back to the legacy implementation while
// a lane has a backlog.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_LegacySender() {
	s.runLaneKillSwitchDrill(func() {
		for _, c := range s.clusters {
			s.selectStreamImplementations(c, "legacy", "lane")
		}
	})
}

// TestLaneKillSwitch_LegacyReceiver switches receivers back to the legacy implementation while a
// lane has a backlog, as when the receiving cluster rolls back to a binary without the lane
// implementation.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_LegacyReceiver() {
	s.runLaneKillSwitchDrill(func() {
		for _, c := range s.clusters {
			s.selectStreamImplementations(c, "lane", "legacy")
		}
	})
}

// TestLaneKillSwitch_OneCluster applies each kill switch only on the cluster that runs the side
// it controls, as an operator overriding a single cell would: sender switches on the active
// cluster, receiver switches on the standby.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_OneCluster() {
	for _, killSwitch := range []laneKillSwitch{
		s.senderLanesDisabled(),
		s.legacySender(),
		s.receiverLanesDisabled(),
		s.legacyReceiver(),
	} {
		s.Run(killSwitch.name, func() {
			s.runLaneKillSwitchDrill(func() {
				s.applyAndAwaitRestart(killSwitch)
			})
		})
	}
}

// TestLaneKillSwitch_Sequences applies every kill switch, one after another, while a lane has a
// backlog: in the runbook's order, and in reverse, as an operator starting from the receiver
// would. Each step that restarts a stream waits for the restart, so every intermediate config
// runs before the next step.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_Sequences() {
	awaited := func(killSwitch laneKillSwitch) func() {
		return func() { s.applyAndAwaitRestart(killSwitch) }
	}
	for _, tc := range []struct {
		name  string
		steps func(stopIsolating func()) []func()
	}{
		{
			name: "Runbook",
			steps: func(stopIsolating func()) []func() {
				return []func(){
					stopIsolating,
					awaited(s.senderLanesDisabled()),
					awaited(s.legacySender()),
					awaited(s.receiverLanesDisabled()),
				}
			},
		},
		{
			name: "Reversed",
			steps: func(stopIsolating func()) []func() {
				return []func(){
					awaited(s.receiverLanesDisabled()),
					awaited(s.legacySender()),
					// The sender is legacy by now, and the lane setting does not restart it.
					func() { s.senderLanesDisabled().apply() },
					stopIsolating,
				}
			},
		},
	} {
		s.Run(tc.name, func() {
			workflowIDs, stopIsolating := s.startLaneBacklog()
			defer stopIsolating()
			for _, step := range tc.steps(stopIsolating) {
				step()
			}
			s.requireWorkflowsReplicated(workflowIDs)
		})
	}
}

// TestLaneKillSwitch_Flapping turns a sender kill switch on and off again several times while a
// lane has a backlog, waiting for each restart, and ends with the switch on.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_Flapping() {
	for _, killSwitch := range []laneKillSwitch{s.senderLanesDisabled(), s.legacySender()} {
		s.Run(killSwitch.name, func() {
			workflowIDs, stopIsolating := s.startLaneBacklog()
			defer stopIsolating()
			for range 3 {
				revert := s.applyAndAwaitRestart(killSwitch)
				s.awaitStreamRestart(killSwitch.cluster, killSwitch.operation, revert)
				workflowIDs = append(workflowIDs, s.startLaneTestWorkflow(context.Background()))
			}
			s.applyAndAwaitRestart(killSwitch)
			s.requireWorkflowsReplicated(workflowIDs)
		})
	}
}

// TestLaneReenableAfterLegacySender switches the sender to legacy while a lane has a backlog,
// then back to the lane implementation. A new lane must form from the reader state that the
// legacy sender persisted, and no workflow may be lost across either switch.
func (s *streamBasedReplicationTestSuite) TestLaneReenableAfterLegacySender() {
	workflowIDs, stopSlowLane := s.startLaneBacklog()
	legacySender := s.legacySender()
	restoreLaneSender := s.applyAndAwaitRestart(legacySender)
	s.requireWorkflowsReplicated(workflowIDs)

	stopSlowLane()
	// Isolating again clears the recorded messages, so a lane message seen below was sent after
	// the lane implementation came back.
	stopIsolating := s.isolateSuiteNamespace(1000)
	defer stopIsolating()
	s.awaitStreamRestart(legacySender.cluster, legacySender.operation, restoreLaneSender)
	workflowIDs = append(workflowIDs, s.startWorkflowsUntilLaneSendsTasks()...)
	s.requireWorkflowsReplicated(workflowIDs)
}

// TestLaneShardReload reloads every history shard on both clusters, first while a lane is active
// and then while a lane has a backlog. The active lane must keep sending after the reload, and a
// kill switch applied after the reload must still recover the backlog.
func (s *streamBasedReplicationTestSuite) TestLaneShardReload() {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	active := s.clusters[0]
	s.Run("ActiveLane", func() {
		stopIsolating := s.isolateSuiteNamespace(1000)
		defer stopIsolating()
		workflowIDs := s.startWorkflowsUntilLaneSendsTasks()
		s.awaitStreamRestart(active, laneTestSenderOperation, s.reloadHistoryShards)
		for _, c := range s.clusters {
			c.GetReplicationStreamRecorder().Clear()
		}
		workflowIDs = append(workflowIDs, s.startWorkflowsUntilLaneSendsTasks()...)
		s.requireWorkflowsReplicated(workflowIDs)
	})
	s.Run("Backlog", func() {
		s.runLaneKillSwitchDrill(func() {
			s.awaitStreamRestart(active, laneTestSenderOperation, s.reloadHistoryShards)
			s.applyAndAwaitRestart(s.senderLanesDisabled())
		})
	})
}

// TestLaneKillSwitch_ReceiverLanesDisabled turns receiver lane support off while a lane has a
// backlog, as when the receiving cluster rolls back to a binary without lanes.
func (s *streamBasedReplicationTestSuite) TestLaneKillSwitch_ReceiverLanesDisabled() {
	s.runLaneKillSwitchDrill(func() {
		for _, c := range s.clusters {
			c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamReceiverLanesEnabled, false)
		}
	})
}

// TestLanePause makes the standby pause the suite namespace's lane on every ack. The lane must
// hold its tasks without the standby ever pausing the default HIGH lane, and send them once the
// standby resumes it.
func (s *streamBasedReplicationTestSuite) TestLanePause() {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	active, standby := s.clusters[0], s.clusters[1]
	// A limit below zero pauses a lane even with no outstanding task.
	resume := standby.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationReceiverLaneMaxOutstandingTaskCount, -1)
	defer resume()
	stopIsolating := s.isolateSuiteNamespace(1000)
	defer stopIsolating()
	// The lane sends its first task before the standby acks it, and pauses on that ack.
	workflowIDs := s.startWorkflowsUntilLaneSendsTasks()
	for range 5 {
		workflowIDs = append(workflowIDs, s.startLaneTestWorkflow(context.Background()))
	}
	await.Require(context.Background(), s.T(), func(t *await.T) {
		require.True(t, s.sawLaneAckMatching(active, func(state *replicationspb.SyncReplicationState) bool {
			for _, laneState := range state.GetLaneStates() {
				if laneState.GetFlowControlCommand() == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
					return true
				}
			}
			return false
		}), "the standby did not pause the lane")
	}, 30*time.Second, 200*time.Millisecond)
	s.False(s.sawLaneAckMatching(active, func(state *replicationspb.SyncReplicationState) bool {
		return state.GetHighPriorityState().GetFlowControlCommand() == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	}), "pausing the lane paused the default HIGH lane")
	// The standby keeps acking, so the stream is live, yet the paused lane does not send the
	// last workflow.
	acks := s.ackCount(active)
	await.Require(context.Background(), s.T(), func(t *await.T) {
		require.GreaterOrEqual(t, s.ackCount(active), acks+3, "the standby stopped acking")
	}, 30*time.Second, 200*time.Millisecond)
	_, err := standby.FrontendClient().DescribeWorkflowExecution(context.Background(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespaceName,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowIDs[len(workflowIDs)-1]},
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound, "a paused lane sent its tasks")

	resume()
	s.requireWorkflowsReplicated(workflowIDs)
}

// TestLaneReceiverLimit sets the standby's lane limit to zero: a stream with no lane opens none,
// and a stream with a lane backlog drops the lane and re-sends its tasks on the default lane.
func (s *streamBasedReplicationTestSuite) TestLaneReceiverLimit() {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	active, standby := s.clusters[0], s.clusters[1]
	acceptNoLanes := func() (revert func()) {
		s.awaitStreamRestart(standby, laneTestReceiverOperation, func() {
			revert = standby.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamReceiverMaxLanes, 0)
		})
		return revert
	}
	s.Run("NoLanes", func() {
		revert := acceptNoLanes()
		defer revert()
		stopIsolating := s.isolateSuiteNamespace(1000)
		defer stopIsolating()
		var workflowIDs []string
		for range 5 {
			workflowIDs = append(workflowIDs, s.startLaneTestWorkflow(context.Background()))
		}
		s.requireWorkflowsReplicated(workflowIDs)
		s.False(s.sawLaneMessage(active, func(messages *replicationspb.WorkflowReplicationMessages) bool {
			return messages.GetLaneInfo() != nil
		}), "the active cluster opened a lane that the standby does not accept")
	})
	s.Run("Backlog", func() {
		var revert func()
		defer func() { revert() }()
		s.runLaneKillSwitchDrill(func() { revert = acceptNoLanes() })
	})
}

// TestLaneRateChange raises the rate of a lane that holds a backlog it cannot send within the
// test's wait. The new rate must apply on the lane's next send, without a stream restart.
func (s *streamBasedReplicationTestSuite) TestLaneRateChange() {
	workflowIDs, stopSlowLane := s.startLaneBacklog()
	defer stopSlowLane()
	stopFastLane := s.isolateSuiteNamespace(1000)
	defer stopFastLane()
	s.requireWorkflowsReplicated(workflowIDs)
}

// TestLaneInvalidConfig makes the isolation config invalid while a lane holds a backlog. The
// open-source policy then asks to retire every lane at once: the lane must send its backlog
// without its rate limit and retire.
func (s *streamBasedReplicationTestSuite) TestLaneInvalidConfig() {
	workflowIDs, stopSlowLane := s.startLaneBacklog()
	defer stopSlowLane()
	for _, c := range s.clusters {
		invalid := []dynamicconfig.ReplicationLaneIsolationEntry{{NamespaceID: s.namespaceID, TasksPerSecond: 1000}}
		defer c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationLaneIsolation, invalid)()
	}
	s.requireWorkflowsReplicated(workflowIDs)
	await.Require(context.Background(), s.T(), func(t *await.T) {
		require.True(t, s.sawLaneMessage(s.clusters[0], func(messages *replicationspb.WorkflowReplicationMessages) bool {
			return messages.GetLaneInfo().GetRetire()
		}), "the lane did not retire")
	}, 60*time.Second, time.Second)
}

// runLaneKillSwitchDrill isolates the suite's namespace on a slow lane, builds a backlog the
// lane cannot send within the test's wait, applies a kill switch, and requires every
// workflow started while isolated to replicate. That only happens if the default lane
// re-sends the lane's backlog.
func (s *streamBasedReplicationTestSuite) runLaneKillSwitchDrill(applyKillSwitch func()) {
	workflowIDs, stopIsolating := s.startLaneBacklog()
	defer stopIsolating()
	applyKillSwitch()
	s.requireWorkflowsReplicated(workflowIDs)
}

// startLaneBacklog isolates the suite's namespace on a slow lane and starts workflows until the
// lane holds a backlog it cannot send within the test's wait. It returns the workflows and a
// function that stops isolating the namespace.
func (s *streamBasedReplicationTestSuite) startLaneBacklog() ([]string, func()) {
	if !s.laneStreams {
		s.T().Skip("replication lanes require lane streams")
	}
	stopIsolating := s.isolateSuiteNamespace(0.2)
	workflowIDs := s.startWorkflowsUntilLaneSendsTasks()
	for range 20 {
		workflowIDs = append(workflowIDs, s.startLaneTestWorkflow(context.Background()))
	}
	return workflowIDs, stopIsolating
}

const (
	laneTestSenderOperation   = "ReplicationStreamSender"
	laneTestReceiverOperation = "ReplicationStreamReceiver"
)

// laneKillSwitch is a kill switch for the suite namespace's lane, applied on the cluster that
// runs the side it controls.
type laneKillSwitch struct {
	name    string
	cluster *testcore.TestCluster
	// operation names the stream the switch restarts: laneTestSenderOperation or
	// laneTestReceiverOperation.
	operation string
	// apply turns the switch on and returns a function that turns it off.
	apply func() (revert func())
}

func (s *streamBasedReplicationTestSuite) senderLanesDisabled() laneKillSwitch {
	active := s.clusters[0]
	return laneKillSwitch{
		name:      "SenderLanesDisabled",
		cluster:   active,
		operation: laneTestSenderOperation,
		apply: func() func() {
			return active.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamSenderLanesEnabled, false)
		},
	}
}

func (s *streamBasedReplicationTestSuite) legacySender() laneKillSwitch {
	active := s.clusters[0]
	return laneKillSwitch{
		name:      "LegacySender",
		cluster:   active,
		operation: laneTestSenderOperation,
		apply:     func() func() { return s.selectStreamImplementations(active, "legacy", "lane") },
	}
}

func (s *streamBasedReplicationTestSuite) receiverLanesDisabled() laneKillSwitch {
	standby := s.clusters[1]
	return laneKillSwitch{
		name:      "ReceiverLanesDisabled",
		cluster:   standby,
		operation: laneTestReceiverOperation,
		apply: func() func() {
			return standby.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamReceiverLanesEnabled, false)
		},
	}
}

func (s *streamBasedReplicationTestSuite) legacyReceiver() laneKillSwitch {
	standby := s.clusters[1]
	return laneKillSwitch{
		name:      "LegacyReceiver",
		cluster:   standby,
		operation: laneTestReceiverOperation,
		apply:     func() func() { return s.selectStreamImplementations(standby, "lane", "legacy") },
	}
}

// applyAndAwaitRestart turns killSwitch on and waits until its cluster restarts the stream it
// controls, so the next step starts from the new config. It returns the function that turns
// killSwitch off.
func (s *streamBasedReplicationTestSuite) applyAndAwaitRestart(killSwitch laneKillSwitch) (revert func()) {
	s.awaitStreamRestart(killSwitch.cluster, killSwitch.operation, func() { revert = killSwitch.apply() })
	return revert
}

// awaitStreamRestart runs change and waits until cluster c creates a replication stream sender
// or receiver, as operation names it. Every stream creation records the implementation it
// selected, so a new recording shows that the stream restarted after change.
func (s *streamBasedReplicationTestSuite) awaitStreamRestart(c *testcore.TestCluster, operation string, change func()) {
	captureHandler, ok := c.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
	s.Require().True(ok, "cluster metrics handler does not support capture")
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)
	change()
	await.Require(context.Background(), s.T(), func(t *await.T) {
		for _, recording := range capture.SnapshotMetric(metrics.ReplicationStreamImplementationSelected.Name()) {
			if recording.Tags["operation"] == operation {
				return
			}
		}
		require.Fail(t, fmt.Sprintf("cluster %s did not restart a %s", c.ClusterName(), operation))
	}, 30*time.Second, 200*time.Millisecond)
}

// selectStreamImplementations selects the sender and receiver implementations for every stream
// between cluster c and the other clusters, and returns a function that restores the previous
// selection.
func (s *streamBasedReplicationTestSuite) selectStreamImplementations(c *testcore.TestCluster, sender, receiver string) func() {
	var senderRules, receiverRules []dynamicconfig.ReplicationStreamImplementationRule
	for _, remote := range s.clusters {
		if remote.ClusterName() == c.ClusterName() {
			continue
		}
		senderRules = append(senderRules, dynamicconfig.ReplicationStreamImplementationRule{
			RemoteCluster: remote.ClusterName(), Implementation: sender, ShardPercent: 100,
		})
		receiverRules = append(receiverRules, dynamicconfig.ReplicationStreamImplementationRule{
			RemoteCluster: remote.ClusterName(), Implementation: receiver, ShardPercent: 100,
		})
	}
	return c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationStreamImplementation, dynamicconfig.ReplicationStreamImplementationSettings{
		Sender:   senderRules,
		Receiver: receiverRules,
	})
}

// reloadHistoryShards closes every history shard on both clusters and then loads each one again
// from persistence, as a history restart or shard move would. A closed shard otherwise stays
// unloaded until it is used or the shard controller's next acquire, and replication streams
// exist only for loaded shards.
func (s *streamBasedReplicationTestSuite) reloadHistoryShards() {
	ctx := testcore.NewContext()
	for _, c := range s.clusters {
		for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
			_, err := c.AdminClient().CloseShard(ctx, &adminservice.CloseShardRequest{ShardId: shardID})
			s.Require().NoError(err)
		}
	}
	for _, c := range s.clusters {
		for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
			_, err := c.AdminClient().DescribeHistoryHost(ctx, &adminservice.DescribeHistoryHostRequest{ShardId: shardID})
			s.Require().NoError(err)
		}
	}
}

// isolateSuiteNamespace isolates the suite's namespace on every stream between the clusters,
// and returns a function that stops isolating it.
func (s *streamBasedReplicationTestSuite) isolateSuiteNamespace(tasksPerSecond float64) func() {
	for _, c := range s.clusters {
		c.GetReplicationStreamRecorder().Clear()
	}
	var stops []func()
	for _, c := range s.clusters {
		var entries []dynamicconfig.ReplicationLaneIsolationEntry
		for _, remote := range s.clusters {
			if remote.ClusterName() != c.ClusterName() {
				entries = append(entries, dynamicconfig.ReplicationLaneIsolationEntry{
					RemoteCluster:  remote.ClusterName(),
					NamespaceID:    s.namespaceID,
					TasksPerSecond: tasksPerSecond,
				})
			}
		}
		stops = append(stops, c.OverrideDynamicConfig(s.T(), dynamicconfig.ReplicationLaneIsolation, entries))
	}
	return func() {
		for _, stop := range stops {
			stop()
		}
	}
}

// startWorkflowsUntilLaneSendsTasks starts workflows on the active cluster until its sender
// sends replication tasks on a lane and the standby acks the lane, and returns their IDs. Lanes
// are created on an ack, so the first workflows may replicate on the default lane. Requiring the
// standby's lane ack also waits out a sender that a previous test's config left without lanes,
// whose stopped predecessor may still be retrying lane sends on its dead stream.
func (s *streamBasedReplicationTestSuite) startWorkflowsUntilLaneSendsTasks() []string {
	active := s.clusters[0]
	var workflowIDs []string
	await.Require(context.Background(), s.T(), func(t *await.T) {
		workflowIDs = append(workflowIDs, s.startLaneTestWorkflow(t.Context()))
		require.True(t, s.sawLaneMessage(active, func(messages *replicationspb.WorkflowReplicationMessages) bool {
			return messages.GetLaneInfo().GetLaneId() != "" && len(messages.GetReplicationTasks()) > 0
		}), "no replication tasks were sent on a lane")
		require.True(t, s.sawLaneAck(active), "the standby did not ack a lane")
	}, 60*time.Second, time.Second)
	return workflowIDs
}

func (s *streamBasedReplicationTestSuite) startLaneTestWorkflow(ctx context.Context) string {
	workflowID := "lane-test-" + uuid.NewString()
	_, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    s.namespaceName,
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "lane-test-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "lane-test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.Require().NoError(err)
	return workflowID
}

// requireWorkflowsReplicated waits until every workflow exists on the standby cluster.
func (s *streamBasedReplicationTestSuite) requireWorkflowsReplicated(workflowIDs []string) {
	await.Require(context.Background(), s.T(), func(t *await.T) {
		for _, workflowID := range workflowIDs {
			_, err := s.clusters[1].FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespaceName,
				Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			})
			require.NoError(t, err, "workflow %s did not replicate", workflowID)
		}
	}, 60*time.Second, time.Second)
}

// sawLaneMessage reports whether cluster sender sent a replication stream message that
// matches. Only messages the cluster's servers sent count, so a message on the stream in the
// other direction never satisfies a check about this cluster's sender. A message counts as soon
// as the sender tries to send it, even on a stream that has already ended; sawLaneAck shows
// delivery.
func (s *streamBasedReplicationTestSuite) sawLaneMessage(sender *testcore.TestCluster, match func(*replicationspb.WorkflowReplicationMessages) bool) bool {
	for _, captured := range sender.GetReplicationStreamRecorder().GetMessages() {
		if captured.Direction != testcore.DirectionServerSend {
			continue
		}
		// The recorder keeps what a server sent in Request.
		var messages *replicationspb.WorkflowReplicationMessages
		switch resp := captured.Request.(type) {
		case *adminservice.StreamWorkflowReplicationMessagesResponse:
			messages = resp.GetMessages()
		case *historyservice.StreamWorkflowReplicationMessagesResponse:
			messages = resp.GetMessages()
		default:
		}
		if messages != nil && match(messages) {
			return true
		}
	}
	return false
}

// ackCount returns how many acks cluster sender has received.
func (s *streamBasedReplicationTestSuite) ackCount(sender *testcore.TestCluster) int {
	count := 0
	s.sawLaneAckMatching(sender, func(*replicationspb.SyncReplicationState) bool {
		count++
		return false
	})
	return count
}

// sawLaneAckMatching reports whether cluster sender received an ack that matches.
func (s *streamBasedReplicationTestSuite) sawLaneAckMatching(sender *testcore.TestCluster, match func(*replicationspb.SyncReplicationState) bool) bool {
	for _, captured := range sender.GetReplicationStreamRecorder().GetMessages() {
		if captured.Direction != testcore.DirectionServerRecv {
			continue
		}
		var state *replicationspb.SyncReplicationState
		switch req := captured.Response.(type) {
		case *adminservice.StreamWorkflowReplicationMessagesRequest:
			state = req.GetSyncReplicationState()
		case *historyservice.StreamWorkflowReplicationMessagesRequest:
			state = req.GetSyncReplicationState()
		default:
		}
		if state != nil && match(state) {
			return true
		}
	}
	return false
}

// sawLaneAck reports whether cluster sender received an ack that carries lane watermarks. A
// receiver acks a lane only after lane messages reach it, so the ack shows that a live stream
// delivered lane traffic.
func (s *streamBasedReplicationTestSuite) sawLaneAck(sender *testcore.TestCluster) bool {
	return s.sawLaneAckMatching(sender, func(state *replicationspb.SyncReplicationState) bool {
		return len(state.GetLaneStates()) > 0
	})
}
