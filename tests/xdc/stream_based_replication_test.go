package xdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
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
	}
)

func TestStreamBasedReplicationTestSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &streamBasedReplicationTestSuite{
				namespaceName: "replication-test-" + common.GenerateRandomString(5),
			}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *streamBasedReplicationTestSuite) SetupSuite() {
	s.controller = gomock.NewController(s.T())
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():       true,
		dynamicconfig.EnableReplicationTaskBatching.Key(): true,
	}
	s.logger = log.NewTestLogger()
	s.serializer = serialization.NewSerializer()
	s.setupSuite(
		testcore.WithFxOptionsForService(primitives.AllServices,
			fx.Decorate(
				func() config.DCRedirectionPolicy {
					return config.DCRedirectionPolicy{Policy: "noop"}
				},
			),
		),
	)
}

func (s *streamBasedReplicationTestSuite) TearDownSuite() {
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
		workflowID := "xdc-stream-replication-test-" + uuid.New()
		runID = uuid.New()

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

	serializer := serialization.NewSerializer()
	cluster1Fetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		serializer,
		s.logger,
	)
	cluster2Fetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		serializer,
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
		common.CreateHistoryClientRetryPolicy(),
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
		RequestId:           uuid.New(),
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
		RequestId:                 uuid.New(),
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

	client1 := s.clusters[1].FrontendClient()
	_, err = client1.DeleteWorkflowExecution(testcore.NewContext(), &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)
	_, err = client1.DeleteWorkflowExecution(testcore.NewContext(), &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
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
	})
	s.NoError(err)

	for i := 0; i < 5; i++ {
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
		RequestId:           uuid.New(),
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
		RequestId:                 uuid.New(),
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
		RequestId:                 uuid.New(),
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
	})
	s.NoError(err)
	_, err = s.clusters[1].AdminClient().DeleteWorkflowExecution(testcore.NewContext(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp1.GetRunId(),
		},
	})
	s.NoError(err)
	_, err = s.clusters[1].AdminClient().DeleteWorkflowExecution(testcore.NewContext(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp2.GetRunId(),
		},
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
			})
		return err == nil
	},
		time.Second*10,
		time.Second)
}
