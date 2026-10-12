package ndc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	NDCReplicationTaskBatchingTestSuite struct {
		*require.Assertions
		protorequire.ProtoAssertions
		suite.Suite

		testClusterFactory          testcore.TestClusterFactory
		standByReplicationTasksChan chan *replicationspb.ReplicationTask
		mockAdminClient             map[string]adminservice.AdminServiceClient
		namespace                   namespace.Name
		namespaceID                 namespace.ID
		standByTaskID               atomic.Int64
		autoIncrementTaskID         int64
		passiveClusterName          string

		controller      *gomock.Controller
		passtiveCluster *testcore.TestCluster
		generator       test.Generator
		serializer      serialization.Serializer
		logger          log.Logger
	}
)

func TestNDCReplicationTaskBatching(t *testing.T) {
	// TODO: doesn't work yet: t.Parallel()
	suite.Run(t, new(NDCReplicationTaskBatchingTestSuite))
}

func (s *NDCReplicationTaskBatchingTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.serializer = serialization.NewSerializer()
	s.testClusterFactory = testcore.NewTestClusterFactory()
	s.passiveClusterName = "cluster-b"

	clusterConfigs := clustersConfig("cluster-a", "cluster-b")
	passiveClusterConfig := clusterConfigs[1]
	passiveClusterConfig.WorkerConfig = testcore.WorkerConfig{DisableWorker: true}
	passiveClusterConfig.DynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():       true,
		dynamicconfig.EnableReplicationTaskBatching.Key(): true,
	}
	s.controller = gomock.NewController(s.T())
	mockActiveStreamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)

	// below is to mock stream client, so we can directly put replication tasks into passive cluster without involving active cluster
	mockActiveStreamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockActiveStreamClient.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesResponse, error) {
		return s.GetReplicationMessagesMock()
	}).AnyTimes()
	mockActiveStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	s.standByReplicationTasksChan = make(chan *replicationspb.ReplicationTask, 100)

	mockActiveClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockActiveClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockActiveStreamClient, nil).AnyTimes()
	mockActiveClient.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any()).Return(&adminservice.ReapplyEventsResponse{}, nil).AnyTimes()
	s.mockAdminClient = map[string]adminservice.AdminServiceClient{
		"cluster-a": mockActiveClient,
	}
	passiveClusterConfig.MockAdminClient = s.mockAdminClient

	passiveClusterConfig.ClusterMetadata.MasterClusterName = s.passiveClusterName
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

func (s *NDCReplicationTaskBatchingTestSuite) TestHistoryReplicationTaskAndThenRetrieve() {
	versions := []int64{1, 1, 21, 31, 301, 401, 601, 501, 801, 1001, 901, 701, 1101}
	executions := make(map[workflow.Execution][]*historypb.History)
	for _, version := range versions {
		workflowID := "replication-message-test" + uuid.NewString()
		runID := uuid.NewString()
		var historyBatch []*historypb.History
		s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, version)
		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()

			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historyBatch = append(historyBatch, historyEvents)
			history, err := testcore.EventBatchesToVersionHistory(nil, historyBatch)
			s.NoError(err)
			s.standByReplicationTasksChan <- s.createHistoryEventReplicationTaskFromHistoryEventBatch( // supply history replication task one by one
				s.namespaceID.String(),
				workflowID,
				runID,
				historyEvents.Events,
				nil,
				history.Items,
			)
		}
		execution := workflow.Execution{
			ID:    workflowID,
			RunID: runID,
		}
		executions[execution] = historyBatch
	}
	await.Require(testcontext.For(s.T()), s.T(), func(t *await.T) {
		for execution, historyBatch := range executions {
			s.assertHistoryEvents(t, execution, historyBatch)
		}
	}, 30*time.Second, 200*time.Millisecond)
}

func (s *NDCReplicationTaskBatchingTestSuite) assertHistoryEvents(
	t *await.T,
	execution workflow.Execution,
	historyBatch []*historypb.History,
) {
	t.Helper()
	mockClientBean := client.NewMockBean(s.controller)
	mockClientBean.
		EXPECT().
		GetRemoteAdminClient(s.passiveClusterName).
		Return(s.passtiveCluster.AdminClient(), nil).
		AnyTimes()

	passiveClusterFetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		s.serializer,
		s.logger,
	)

	passiveIterator := passiveClusterFetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		t.Context(), s.passiveClusterName, s.namespaceID, execution.ID, execution.RunID, 0, 1, 0, 0)

	var actualBatches []*commonpb.DataBlob
	for passiveIterator.HasNext() {
		passiveBatch, err := passiveIterator.Next()
		require.NoError(t, err)
		actualBatches = append(actualBatches, passiveBatch.RawEventBatch)
	}
	expectedBatches := make([]*commonpb.DataBlob, 0, len(historyBatch))
	for _, batch := range historyBatch {
		inputBatch, err := s.serializer.SerializeEvents(batch.Events)
		require.NoError(t, err)
		expectedBatches = append(expectedBatches, inputBatch)
	}
	protorequire.ProtoSliceEqual(t, expectedBatches, actualBatches)
}

func (s *NDCReplicationTaskBatchingTestSuite) registerNamespace() {
	s.namespace = namespace.Name("test-simple-workflow-ndc-" + common.GenerateRandomString(5))
	passiveFrontend := s.passtiveCluster.FrontendClient() //
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
	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

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
	taskID := s.standByTaskID.Add(1)
	task.SourceTaskId = taskID
	tasks := []*replicationspb.ReplicationTask{task}

	replicationMessage := &replicationspb.WorkflowReplicationMessages{
		ReplicationTasks:       tasks,
		ExclusiveHighWatermark: taskID + 1,
	}

	return &adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: replicationMessage,
		},
	}, nil
}

func (s *NDCReplicationTaskBatchingTestSuite) createHistoryEventReplicationTaskFromHistoryEventBatch(
	namespaceId string,
	workflowId string,
	runId string,
	events []*historypb.HistoryEvent,
	newRunEvents []*historypb.HistoryEvent,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) *replicationspb.ReplicationTask {
	eventBlob, err := s.serializer.SerializeEvents(events)
	var newRunEventBlob *commonpb.DataBlob
	if newRunEvents != nil {
		newRunEventBlob, err = s.serializer.SerializeEvents(newRunEvents)
		s.NoError(err)
	}
	s.NoError(err)
	taskType := enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK
	replicationTask := &replicationspb.ReplicationTask{
		TaskType: taskType,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
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
