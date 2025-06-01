package eventhandler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/mock/gomock"
)

type (
	historyEventHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata

		historyEventHandler *historyEventsHandlerImpl
		shardController     *shard.MockController
		eventImporter       *MockEventImporter
	}
)

func TestHistoryEventHandlerSuite(t *testing.T) {
	s := new(historyEventHandlerSuite)
	suite.Run(t, s)
}

func (s *historyEventHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *historyEventHandlerSuite) TearDownSuite() {

}

func (s *historyEventHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.eventImporter = NewMockEventImporter(s.controller)
	s.historyEventHandler = &historyEventsHandlerImpl{
		s.clusterMetadata,
		s.eventImporter,
		s.shardController,
		log.NewNoopLogger(),
	}
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_RemoteOnly() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	historyEvents2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
	}
	historyEvents := [][]*historypb.HistoryEvent{historyEvents1, historyEvents2}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	).Times(1)

	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_LocalAndRemote_HandleLocalThenRemote() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes() // current cluster ID is 1
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000)).AnyTimes()

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 5, Version: 3},
			{EventId: 10, Version: 1001},
			{EventId: 13, Version: 1002},
			{EventId: 15, Version: 1003},
		},
	}
	localHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{EventId: 5, Version: 3},
		},
		{
			{EventId: 6, Version: 1001},
		},
		{
			{EventId: 7, Version: 1001},
			{EventId: 8, Version: 1001},
			{EventId: 9, Version: 1001},
			{EventId: 10, Version: 1001},
		},
	}
	remoteHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{EventId: 11, Version: 2},
			{EventId: 12, Version: 2},
			{EventId: 13, Version: 2},
		},
		{
			{EventId: 14, Version: 1003},
			{EventId: 15, Version: 1003},
		},
	}
	initialHistoryEvents := append(localHistoryEvents, remoteHistoryEvents...)
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(2)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(2)
	engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
	}).Return(nil, serviceerror.NewNotFound("Mutable state not found")).Times(1)
	s.eventImporter.EXPECT().ImportHistoryEventsFromBeginning(
		gomock.Any(),
		remoteCluster,
		workflowKey,
		int64(10),
		int64(1001),
	).Return(nil).Times(1)
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		remoteHistoryEvents,
		nil,
		"",
	).Times(1)

	err := s.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		initialHistoryEvents,
		nil,
		"",
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleLocalHistoryEvents_AlreadyExist() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	mutableState := &historyservice.GetMutableStateResponse{
		VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{
			Items: []*historyspb.VersionHistoryItem{
				{EventId: 10, Version: 1},
			},
		}}},
	}

	engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
	}).Return(mutableState, nil).Times(1)

	err := s.historyEventHandler.handleLocalGeneratedEvent(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_LocalOnly_ImportAllLocalAndCommit() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2, Version: 3},
			{EventId: 5, Version: 5},
			{EventId: 7, Version: 1001},
		},
	}

	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)

	gomock.InOrder(
		engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
		}).Return(nil, serviceerror.NewNotFound("Mutable state not found")).Times(1),
		s.eventImporter.EXPECT().ImportHistoryEventsFromBeginning(
			gomock.Any(),
			remoteCluster,
			workflowKey,
			int64(7),
			int64(1001),
		).Return(nil).Times(1),
	)

	err := s.historyEventHandler.handleLocalGeneratedEvent(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
	)
	s.Nil(err)
}

func (s *historyEventHandlerSuite) TestHandleHistoryEvents_LocalOnly_ExistButNotEnoughEvents_DataLose() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	s.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2, Version: 3},
			{EventId: 5, Version: 5},
			{EventId: 7, Version: 1001},
		},
	}

	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)

	gomock.InOrder(
		engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
		}).Return(&historyservice.GetMutableStateResponse{
			VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 6, Version: 1001},
				},
			}}},
		}, nil).Times(1),
	)

	err := s.historyEventHandler.handleLocalGeneratedEvent(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
	)
	s.NotNil(err)
}
