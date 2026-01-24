package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	stateRebuilderSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockEventsCache     *events.MockCache
		mockTaskRefresher   *workflow.MockTaskRefresher
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionManager *persistence.MockExecutionManager
		logger               log.Logger

		namespaceID namespace.ID
		workflowID  string
		runID       string
		now         time.Time

		nDCStateRebuilder *StateRebuilderImpl
	}
)

func TestStateRebuilderSuite(t *testing.T) {
	s := new(stateRebuilderSuite)
	suite.Run(t, s)
}

func (s *stateRebuilderSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskRefresher = workflow.NewMockTaskRefresher(s.controller)
	config := tests.NewDynamicConfig()
	config.EnableTransitionHistory = dynamicconfig.GetBoolPropertyFn(true)
	config.ExternalPayloadsEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: 10,
			RangeId: 1,
		}.Build(),
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.workflowID = "some random workflow ID"
	s.runID = uuid.NewString()
	s.now = time.Now().UTC()
	s.nDCStateRebuilder = NewStateRebuilder(
		s.mockShard, s.logger,
	)
	s.nDCStateRebuilder.taskRefresher = s.mockTaskRefresher
}

func (s *stateRebuilderSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *stateRebuilderSuite) TestInitializeBuilders() {
	mutableState, stateBuilder := s.nDCStateRebuilder.initializeBuilders(tests.GlobalNamespaceEntry, tests.WorkflowKey, s.now)
	s.NotNil(mutableState)
	s.NotNil(stateBuilder)
	s.NotNil(mutableState.GetExecutionInfo().GetVersionHistories())
}

func (s *stateRebuilderSuite) TestApplyEvents() {

	requestID := uuid.NewString()
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:                                 1,
			EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:                                  2,
			EventType:                                enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{},
		}.Build(),
	}

	workflowKey := definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID)

	mockStateRebuilder := workflow.NewMockMutableStateRebuilder(s.controller)
	mockStateRebuilder.EXPECT().ApplyEvents(
		gomock.Any(),
		s.namespaceID,
		requestID,
		protomock.Eq(commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		}.Build()),
		[][]*historypb.HistoryEvent{events},
		[]*historypb.HistoryEvent(nil),
		"",
	).Return(nil, nil)

	err := s.nDCStateRebuilder.applyEvents(context.Background(), workflowKey, mockStateRebuilder, events, requestID)
	s.NoError(err)
}

func (s *stateRebuilderSuite) TestPagination() {
	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")

	event1 := historypb.HistoryEvent_builder{
		EventId:                                 1,
		EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	event2 := historypb.HistoryEvent_builder{
		EventId:                              2,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	event3 := historypb.HistoryEvent_builder{
		EventId:                            3,
		EventType:                          enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
	}.Build()
	event4 := historypb.HistoryEvent_builder{
		EventId:                              4,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
	}.Build()
	event5 := historypb.HistoryEvent_builder{
		EventId:                              5,
		EventType:                            enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{},
	}.Build()
	history1 := []*historypb.History{historypb.History_builder{Events: []*historypb.HistoryEvent{event1, event2, event3}}.Build()}
	transactionID1 := int64(10)
	history2 := []*historypb.History{historypb.History_builder{Events: []*historypb.HistoryEvent{event4, event5}}.Build()}
	transactionID2 := int64(20)
	expectedHistory := append(history1, history2...)
	expectedTransactionIDs := []int64{transactionID1, transactionID2}
	pageToken := []byte("some random token")

	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{transactionID1},
		NextPageToken:  pageToken,
		Size:           12345,
	}, nil)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{transactionID2},
		NextPageToken:  nil,
		Size:           67890,
	}, nil)

	paginationFn := s.nDCStateRebuilder.getPaginationFn(context.Background(), firstEventID, nextEventID, branchToken, tests.Namespace.String())
	iter := collection.NewPagingIterator(paginationFn)

	var result []HistoryBlobsPaginationItem
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item)
	}
	var historyResult []*historypb.History
	var transactionIDsResult []int64
	for _, item := range result {
		historyResult = append(historyResult, item.History)
		transactionIDsResult = append(transactionIDsResult, item.TransactionID)
	}

	s.Equal(expectedHistory, historyResult)
	s.Equal(expectedTransactionIDs, transactionIDsResult)
}

func (s *stateRebuilderSuite) TestRebuild() {
	requestID := uuid.NewString()
	version := int64(12)
	lastEventID := int64(2)
	branchToken := []byte("other random branch token")
	targetBranchToken := []byte("some other random branch token")

	targetNamespaceID := namespace.ID(uuid.NewString())
	targetNamespace := namespace.Name("other random namespace name")
	targetWorkflowID := "other random workflow ID"
	targetRunID := uuid.NewString()

	firstEventID := common.FirstEventID
	nextEventID := lastEventID + 1
	payloadsWithExternalReference1 := payloads.EncodeString("some random input")
	payloadsWithExternalReference1.GetPayloads()[0].SetExternalPayloads([]*commonpb.Payload_ExternalPayloadDetails{
		commonpb.Payload_ExternalPayloadDetails_builder{
			SizeBytes: 1024,
		}.Build(),
	})
	events1 := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId:   1,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowType:             commonpb.WorkflowType_builder{Name: "some random workflow type"}.Build(),
			TaskQueue:                taskqueuepb.TaskQueue_builder{Name: "some random workflow type"}.Build(),
			Input:                    payloadsWithExternalReference1,
			WorkflowExecutionTimeout: durationpb.New(123 * time.Second),
			WorkflowRunTimeout:       durationpb.New(233 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(45 * time.Second),
			Identity:                 "some random identity",
		}.Build(),
	}.Build()}
	payloadsWithExternalReference2 := payloads.EncodeString("some random input")
	payloadsWithExternalReference2.GetPayloads()[0].SetExternalPayloads([]*commonpb.Payload_ExternalPayloadDetails{
		commonpb.Payload_ExternalPayloadDetails_builder{
			SizeBytes: 2048,
		}.Build(),
	})
	events2 := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId:   2,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: "some random signal name",
			Input:      payloadsWithExternalReference2,
			Identity:   "some random identity",
		}.Build(),
	}.Build()}
	history1 := []*historypb.History{historypb.History_builder{Events: events1}.Build()}
	history2 := []*historypb.History{historypb.History_builder{Events: events2}.Build()}
	pageToken := []byte("some random pagination token")

	historySize1 := 12345
	historySize2 := 67890
	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{10},
		NextPageToken:  pageToken,
		Size:           historySize1,
	}, nil)
	expectedLastFirstTransactionID := int64(20)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{expectedLastFirstTransactionID},
		NextPageToken:  nil,
		Size:           historySize2,
	}, nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(targetNamespaceID).Return(namespace.NewGlobalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: targetNamespaceID.String(), Name: targetNamespace.String()}.Build(),
		&persistencespb.NamespaceConfig{},
		persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		}.Build(),
		1234,
	), nil).AnyTimes()
	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	rebuildMutableState, rebuildStats, err := s.nDCStateRebuilder.Rebuild(
		context.Background(),
		s.now,
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID),
		branchToken,
		lastEventID,
		util.Ptr(version),
		definition.NewWorkflowKey(targetNamespaceID.String(), targetWorkflowID, targetRunID),
		targetBranchToken,
		requestID,
	)
	s.NoError(err)
	s.NotNil(rebuildMutableState)
	rebuildExecutionInfo := rebuildMutableState.GetExecutionInfo()
	s.Equal(targetNamespaceID, namespace.ID(rebuildExecutionInfo.GetNamespaceId()))
	s.Equal(targetWorkflowID, rebuildExecutionInfo.GetWorkflowId())
	s.Equal(targetRunID, rebuildMutableState.GetExecutionState().GetRunId())
	s.Equal(int64(historySize1+historySize2), rebuildStats.HistorySize)
	s.Equal(int64(1024+2048), rebuildStats.ExternalPayloadSize)
	s.Equal(int64(2), rebuildStats.ExternalPayloadCount)
	s.ProtoEqual(versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(
			targetBranchToken,
			[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
		),
	), rebuildMutableState.GetExecutionInfo().GetVersionHistories())
	s.Equal(timestamp.TimeValue(rebuildMutableState.GetExecutionState().GetStartTime()), s.now)
	s.Equal(expectedLastFirstTransactionID, rebuildExecutionInfo.GetLastFirstEventTxnId())
}

func (s *stateRebuilderSuite) TestRebuildWithCurrentMutableState() {
	requestID := uuid.NewString()
	version := int64(12)
	lastEventID := int64(2)
	branchToken := []byte("other random branch token")
	targetBranchToken := []byte("some other random branch token")

	targetNamespaceID := namespace.ID(uuid.NewString())
	targetNamespace := namespace.Name("other random namespace name")
	targetWorkflowID := "other random workflow ID"
	targetRunID := uuid.NewString()

	firstEventID := common.FirstEventID
	nextEventID := lastEventID + 1
	events1 := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId:   1,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowType:             commonpb.WorkflowType_builder{Name: "some random workflow type"}.Build(),
			TaskQueue:                taskqueuepb.TaskQueue_builder{Name: "some random workflow type"}.Build(),
			Input:                    payloads.EncodeString("some random input"),
			WorkflowExecutionTimeout: durationpb.New(123 * time.Second),
			WorkflowRunTimeout:       durationpb.New(233 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(45 * time.Second),
			Identity:                 "some random identity",
		}.Build(),
	}.Build()}
	events2 := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId:   2,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}.Build(),
	}.Build()}
	history1 := []*historypb.History{historypb.History_builder{Events: events1}.Build()}
	history2 := []*historypb.History{historypb.History_builder{Events: events2}.Build()}
	pageToken := []byte("some random pagination token")

	historySize1 := 12345
	historySize2 := 67890
	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{10},
		NextPageToken:  pageToken,
		Size:           historySize1,
	}, nil)
	expectedLastFirstTransactionID := int64(20)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{expectedLastFirstTransactionID},
		NextPageToken:  nil,
		Size:           historySize2,
	}, nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(targetNamespaceID).Return(namespace.NewGlobalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: targetNamespaceID.String(), Name: targetNamespace.String()}.Build(),
		&persistencespb.NamespaceConfig{},
		persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		}.Build(),
		1234,
	), nil).AnyTimes()

	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	currentMutableState := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			TransitionHistory: []*persistencespb.VersionedTransition{
				persistencespb.VersionedTransition_builder{
					TransitionCount:          10,
					NamespaceFailoverVersion: 12,
				}.Build(),
			},
		}.Build(),
	}.Build()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, int64(12)).Return(cluster.TestCurrentClusterName).AnyTimes()
	rebuildMutableState, rebuildStats, err := s.nDCStateRebuilder.RebuildWithCurrentMutableState(
		context.Background(),
		s.now,
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID),
		branchToken,
		lastEventID,
		util.Ptr(version),
		definition.NewWorkflowKey(targetNamespaceID.String(), targetWorkflowID, targetRunID),
		targetBranchToken,
		requestID,
		currentMutableState,
	)
	s.NoError(err)
	s.NotNil(rebuildMutableState)
	rebuildExecutionInfo := rebuildMutableState.GetExecutionInfo()
	s.Equal(targetNamespaceID, namespace.ID(rebuildExecutionInfo.GetNamespaceId()))
	s.Equal(targetWorkflowID, rebuildExecutionInfo.GetWorkflowId())
	s.Equal(targetRunID, rebuildMutableState.GetExecutionState().GetRunId())
	s.Equal(int64(historySize1+historySize2), rebuildStats.HistorySize)
	s.ProtoEqual(versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(
			targetBranchToken,
			[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
		),
	), rebuildMutableState.GetExecutionInfo().GetVersionHistories())
	s.Equal(timestamp.TimeValue(rebuildMutableState.GetExecutionState().GetStartTime()), s.now)
	s.Equal(expectedLastFirstTransactionID, rebuildExecutionInfo.GetLastFirstEventTxnId())
	s.Equal(int64(11), rebuildExecutionInfo.GetTransitionHistory()[0].GetTransitionCount())
}
