package eventhandler

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
)

type (
	futureEventsHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
		testProcessToolBox
		replication.ProcessToolBox

		futureEventHandler RemoteGeneratedEventsHandler
	}
)

func TestFutureEventsHandlerSuite(t *testing.T) {
	s := new(futureEventsHandlerSuite)
	suite.Run(t, s)
}

func (s *futureEventsHandlerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *futureEventsHandlerSuite) TearDownSuite() {

}

func (s *futureEventsHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.testProcessToolBox, s.ProcessToolBox = initializeToolBox(s.controller)
	s.futureEventHandler = NewFutureEventsHandler(
		s.ProcessToolBox,
	)
}

func (s *futureEventsHandlerSuite) TestHandleFutureHistoryEvents() {
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
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
	).Times(1)

	err := s.futureEventHandler.HandleRemoteGeneratedHistoryEvents(
		context.Background(),
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
	)
	s.Nil(err)
}
