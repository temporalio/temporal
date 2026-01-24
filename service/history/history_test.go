package history

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/api/forcedeleteworkflowexecution"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	historyAPISuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockExecutionMgr   *persistence.MockExecutionManager
		mockVisibilityMgr  *manager.MockVisibilityManager
		mockNamespaceCache *namespace.MockRegistry

		chasmRegistry *chasm.Registry
		logger        log.Logger
	}
)

func TestHistoryAPISuite(t *testing.T) {
	s := new(historyAPISuite)
	suite.Run(t, s)
}

func (s *historyAPISuite) SetupSuite() {

}

func (s *historyAPISuite) TearDownSuite() {
}

func (s *historyAPISuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockExecutionMgr = persistence.NewMockExecutionManager(s.controller)
	s.mockVisibilityMgr = manager.NewMockVisibilityManager(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.logger = log.NewTestLogger()

	s.chasmRegistry = chasm.NewRegistry(s.logger)
	err := s.chasmRegistry.Register(chasmworkflow.NewLibrary())
	s.NoError(err)
}

func (s *historyAPISuite) TearDownTest() {
	s.controller.Finish()
}
func (s *historyAPISuite) TestDeleteWorkflowExecution_DeleteCurrentExecution() {
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "workflowID",
	}.Build()

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := historyservice.ForceDeleteWorkflowExecutionRequest_builder{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: adminservice.DeleteWorkflowExecutionRequest_builder{
			Execution: execution,
			Archetype: chasm.WorkflowArchetype,
		}.Build(),
	}.Build()

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	resp, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		shardID,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.Nil(resp)
	s.Error(err)

	mutableState := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			VersionHistories: historyspb.VersionHistories_builder{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{BranchToken: []byte("branch1")}.Build(),
					historyspb.VersionHistory_builder{BranchToken: []byte("branch2")}.Build(),
					historyspb.VersionHistory_builder{BranchToken: []byte("branch3")}.Build(),
					historyspb.VersionHistory_builder{BranchToken: []byte{}}.Build(),
				},
			}.Build(),
		}.Build(),
	}.Build()

	runID := uuid.NewString()
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.NewString(),
		RunID:          runID,
		State:          enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(3)

	_, err = forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		shardID,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
}

func (s *historyAPISuite) TestDeleteWorkflowExecution_LoadMutableStateFailed() {
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "workflowID",
		RunId:      uuid.NewString(),
	}.Build()

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := historyservice.ForceDeleteWorkflowExecutionRequest_builder{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: adminservice.DeleteWorkflowExecutionRequest_builder{
			Execution: execution,
			Archetype: chasm.WorkflowArchetype,
		}.Build(),
	}.Build()

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		shardID,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
}
