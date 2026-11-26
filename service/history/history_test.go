package history

import (
	"context"
	"errors"
	"testing"
	"time"

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
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/api/forcedeleteworkflowexecution"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	historyAPISuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockExecutionMgr   *persistence.MockExecutionManager
		mockVisibilityMgr  *manager.MockVisibilityManager
		mockNamespaceCache *namespace.MockRegistry
		shardContext       *shard.ContextTest
		workflowCache      wcache.Cache

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

	s.shardContext = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	s.workflowCache = wcache.NewHostLevelCache(
		s.shardContext.GetConfig(),
		s.logger,
		metrics.NoopMetricsHandler,
		testhooks.TestHooks{},
	)

	s.chasmRegistry = chasm.NewRegistry(s.logger)
	err := s.chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry()))
	s.NoError(err)
}

func (s *historyAPISuite) TearDownTest() {
	s.controller.Finish()
}
func (s *historyAPISuite) TestDeleteWorkflowExecution_DeleteCurrentExecution() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
	}

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
			Archetype: chasm.WorkflowArchetype,
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	resp, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.Nil(resp)
	s.Error(err)

	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{BranchToken: []byte("branch1")},
					{BranchToken: []byte("branch2")},
					{BranchToken: []byte("branch3")},
					{BranchToken: []byte{}},
				},
			},
		},
	}

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
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(3)

	_, err = forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
}

func (s *historyAPISuite) TestDeleteWorkflowExecution_LoadMutableStateFailed() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.NewString(),
	}

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
			Archetype: chasm.WorkflowArchetype,
		},
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
}

func (s *historyAPISuite) TestForceDeleteWorkflowExecution_VisibilityDeleteCarriesTimes() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.NewString(),
	}
	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
			Archetype: chasm.WorkflowArchetype,
		},
	}
	startTime := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)
	initialSkip := 2 * time.Hour

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
					InitialSkippedDuration: durationpb.New(initialSkip),
				},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				StartTime: timestamppb.New(startTime),
			},
		},
	}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
			s.Equal(startTime, request.StartTime)
			s.Equal(startTime.Add(-initialSkip), request.CreateTime)
			return nil
		},
	)

	_, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
}

func (s *historyAPISuite) TestForceDeleteWorkflowExecution_LoadMutableStateFailed_TimesUnknown() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.NewString(),
	}
	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
			Archetype: chasm.WorkflowArchetype,
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
			s.True(request.StartTime.IsZero())
			s.True(request.CreateTime.IsZero())
			return nil
		},
	)

	_, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
}

func (s *historyAPISuite) TestForceDeleteWorkflowExecution_ClearsCachedMutableState() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.NewString(),
	}
	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
			Archetype: chasm.WorkflowArchetype,
		},
	}
	cachedMutableState := s.cacheMutableState(&execution)
	cachedMutableState.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
	cachedMutableState.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistencespb.WorkflowMutableState{},
	}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.DeleteWorkflowExecutionRequest) error {
			// No reader can load the execution into the cache while it is being deleted.
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, _, err := s.workflowCache.GetOrCreateChasmExecution(
				ctx,
				s.shardContext,
				tests.NamespaceID,
				&execution,
				chasm.WorkflowArchetypeID,
				locks.PriorityHigh,
			)
			s.ErrorIs(err, consts.ErrResourceExhaustedBusyWorkflow)
			return nil
		},
	)
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.NoError(err)
	s.Nil(s.cachedMutableState(&execution))
}

func (s *historyAPISuite) TestForceDeleteWorkflowExecution_DeleteFailed_ClearsCachedMutableState() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.NewString(),
	}
	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
			Archetype: chasm.WorkflowArchetype,
		},
	}
	cachedMutableState := s.cacheMutableState(&execution)
	cachedMutableState.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
	cachedMutableState.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask()

	deleteErr := errors.New("execution delete failed")
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistencespb.WorkflowMutableState{},
	}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(deleteErr)

	_, err := forcedeleteworkflowexecution.Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowCache,
		s.chasmRegistry,
		s.mockExecutionMgr,
		s.mockVisibilityMgr,
		s.logger,
	)
	s.ErrorIs(err, deleteErr)
	s.Nil(s.cachedMutableState(&execution))
}

// cacheMutableState caches a copy of the execution the way a replication task or
// DescribeMutableState does when it loads the execution before the force delete.
func (s *historyAPISuite) cacheMutableState(execution *commonpb.WorkflowExecution) *historyi.MockMutableState {
	wfContext, release, err := s.workflowCache.GetOrCreateChasmExecution(
		context.Background(),
		s.shardContext,
		tests.NamespaceID,
		execution,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	)
	s.NoError(err)
	mutableState := historyi.NewMockMutableState(s.controller)
	mutableState.EXPECT().IsDirty().Return(false)
	wfContext.(*workflow.ContextImpl).MutableState = mutableState
	release(nil)
	return mutableState
}

// cachedMutableState returns the mutable state the next reader of the execution gets from the cache.
func (s *historyAPISuite) cachedMutableState(execution *commonpb.WorkflowExecution) historyi.MutableState {
	wfContext, release, err := s.workflowCache.GetOrCreateChasmExecution(
		context.Background(),
		s.shardContext,
		tests.NamespaceID,
		execution,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	)
	s.NoError(err)
	defer release(nil)
	return wfContext.(*workflow.ContextImpl).MutableState
}
