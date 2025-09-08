package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type chasmEngineSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockEngine            *historyi.MockEngine
	mockShardController   *shard.MockController
	mockExecutionManager  *persistence.MockExecutionManager
	mockNamespaceRegistry *namespace.MockRegistry
	mockClusterMetadata   *cluster.MockMetadata

	namespaceEntry *namespace.Namespace
	entityCache    wcache.Cache
	registry       *chasm.Registry
	config         *configs.Config

	engine *ChasmEngine
}

func TestChasmEngineSuite(t *testing.T) {
	suite.Run(t, new(chasmEngineSuite))
}

func (s *chasmEngineSuite) SetupTest() {
	s.initAssertions()

	s.controller = gomock.NewController(s.T())
	s.mockShardController = shard.NewMockController(s.controller)
	s.mockEngine = historyi.NewMockEngine(s.controller)

	s.config = tests.NewDynamicConfig()
	s.config.EnableChasm = dynamicconfig.GetBoolPropertyFn(true)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
	)
	s.entityCache = wcache.NewHostLevelCache(
		s.mockShard.GetConfig(),
		s.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
	)
	s.namespaceEntry = tests.GlobalNamespaceEntry

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache
	s.mockShardController.EXPECT().GetShardByID(gomock.Any()).Return(s.mockShard, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(cluster.TestCurrentClusterInitialFailoverVersion, tests.Version).Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(s.namespaceEntry.ID()).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(s.namespaceEntry.Name()).Return(s.namespaceEntry, nil).AnyTimes()

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.registry = chasm.NewRegistry(s.mockShard.GetLogger())
	err = s.registry.Register(&testChasmLibrary{})
	s.NoError(err)
	s.mockShard.SetChasmRegistry(s.registry)

	s.mockShard.SetEngineForTesting(s.mockEngine)
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	s.engine = newChasmEngine(
		s.entityCache,
		s.registry,
		s.config,
	)
	s.engine.SetShardController(s.mockShardController)
}

func (s *chasmEngineSuite) SetupSubTest() {
	s.initAssertions()
}

func (s *chasmEngineSuite) initAssertions() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

func (s *chasmEngineSuite) TestNewEntity_BrandNew() {
	tv := testvars.New(s.T())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    "",
		},
	)
	newActivityID := tv.ActivityID()

	var runID string
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			s.validateCreateRequest(request, newActivityID, "", 0)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	entityKey, serializedRef, err := s.engine.NewEntity(
		context.Background(),
		ref,
		s.newTestEntityFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyRejectDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.NoError(err)
	expectedEntityKey := chasm.EntityKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		EntityID:    runID,
	}
	s.Equal(expectedEntityKey, entityKey)
	s.validateNewEntityResponseRef(serializedRef, expectedEntityKey)
}

func (s *chasmEngineSuite) TestNewEntity_RequestIDDedup() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    "",
		},
	)
	newActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		s.currentRunConditionFailedErr(
			tv,
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	).Times(1)

	entityKey, serializedRef, err := s.engine.NewEntity(
		context.Background(),
		ref,
		s.newTestEntityFn(newActivityID),
		chasm.WithRequestID(tv.RequestID()),
	)
	s.NoError(err)

	expectedEntityKey := chasm.EntityKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		EntityID:    tv.RunID(),
	}
	s.Equal(expectedEntityKey, entityKey)
	s.validateNewEntityResponseRef(serializedRef, expectedEntityKey)
}

func (s *chasmEngineSuite) TestNewEntity_ReusePolicy_AllowDuplicate() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    "",
		},
	)
	newActivityID := tv.ActivityID()
	currentRunConditionFailedErr := s.currentRunConditionFailedErr(
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	var runID string
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			s.validateCreateRequest(request, newActivityID, tv.RunID(), currentRunConditionFailedErr.LastWriteVersion)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	entityKey, serializedRef, err := s.engine.NewEntity(
		context.Background(),
		ref,
		s.newTestEntityFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.NoError(err)

	expectedEntityKey := chasm.EntityKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		EntityID:    runID,
	}
	s.Equal(expectedEntityKey, entityKey)
	s.validateNewEntityResponseRef(serializedRef, expectedEntityKey)
}

func (s *chasmEngineSuite) TestNewEntity_ReusePolicy_FailedOnly_Success() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    "",
		},
	)
	newActivityID := tv.ActivityID()
	currentRunConditionFailedErr := s.currentRunConditionFailedErr(
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	)

	var runID string
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			s.validateCreateRequest(request, newActivityID, tv.RunID(), currentRunConditionFailedErr.LastWriteVersion)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	entityKey, serializedRef, err := s.engine.NewEntity(
		context.Background(),
		ref,
		s.newTestEntityFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.NoError(err)

	expectedEntityKey := chasm.EntityKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		EntityID:    runID,
	}
	s.Equal(expectedEntityKey, entityKey)
	s.validateNewEntityResponseRef(serializedRef, expectedEntityKey)
}

func (s *chasmEngineSuite) TestNewEntity_ReusePolicy_FailedOnly_Fail() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    "",
		},
	)
	newActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		s.currentRunConditionFailedErr(
			tv,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		),
	).Times(1)

	_, _, err := s.engine.NewEntity(
		context.Background(),
		ref,
		s.newTestEntityFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
}

func (s *chasmEngineSuite) TestNewEntity_ReusePolicy_RejectDuplicate() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    "",
		},
	)
	newActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		s.currentRunConditionFailedErr(
			tv,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		),
	).Times(1)

	_, _, err := s.engine.NewEntity(
		context.Background(),
		ref,
		s.newTestEntityFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyRejectDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
}

func (s *chasmEngineSuite) newTestEntityFn(
	activityID string,
) func(ctx chasm.MutableContext) (chasm.Component, error) {
	return func(ctx chasm.MutableContext) (chasm.Component, error) {
		return &testComponent{
			ActivityInfo: &persistencespb.ActivityInfo{
				ActivityId: activityID,
			},
		}, nil
	}
}

func (s *chasmEngineSuite) validateCreateRequest(
	request *persistence.CreateWorkflowExecutionRequest,
	expectedActivityID string,
	expectedPreviousRunID string,
	expectedPreviousLastWriteVersion int64,
) {
	if expectedPreviousRunID == "" && expectedPreviousLastWriteVersion == 0 {
		s.Equal(persistence.CreateWorkflowModeBrandNew, request.Mode)
	} else {
		s.Equal(persistence.CreateWorkflowModeUpdateCurrent, request.Mode)
		s.Equal(expectedPreviousRunID, request.PreviousRunID)
		s.Equal(expectedPreviousLastWriteVersion, request.PreviousLastWriteVersion)
	}

	s.Len(request.NewWorkflowSnapshot.ChasmNodes, 1)
	updatedNode, ok := request.NewWorkflowSnapshot.ChasmNodes[""]
	s.True(ok)

	activityInfo := &persistencespb.ActivityInfo{}
	err := serialization.Decode(updatedNode.Data, activityInfo)
	s.NoError(err)
	s.Equal(expectedActivityID, activityInfo.ActivityId)
}

func (s *chasmEngineSuite) validateNewEntityResponseRef(
	serializedRef []byte,
	expectedEntityKey chasm.EntityKey,
) {
	deserializedRef, err := chasm.DeserializeComponentRef(serializedRef)
	s.NoError(err)
	s.Equal(expectedEntityKey, deserializedRef.EntityKey)

	archetype, err := deserializedRef.Archetype(s.registry)
	s.NoError(err)
	s.Equal("TestLibrary.test_component", archetype.String())
}

func (s *chasmEngineSuite) currentRunConditionFailedErr(
	tv *testvars.TestVars,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) *persistence.CurrentWorkflowConditionFailedError {
	return &persistence.CurrentWorkflowConditionFailedError{
		RequestIDs: map[string]*persistencespb.RequestIDInfo{
			tv.RequestID(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   0,
			},
		},
		RunID:            tv.RunID(),
		State:            state,
		Status:           status,
		LastWriteVersion: s.namespaceEntry.FailoverVersion() - 1,
	}
}

func (s *chasmEngineSuite) TestUpdateComponent_Success() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    tv.RunID(),
		},
	)
	newActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(ref.EntityKey, &persistencespb.ActivityInfo{
				ActivityId: "",
			}),
		}, nil).Times(1)
	s.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.UpdateWorkflowExecutionRequest,
		) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.Len(request.UpdateWorkflowMutation.UpsertChasmNodes, 1)
			updatedNode, ok := request.UpdateWorkflowMutation.UpsertChasmNodes[""]
			s.True(ok)

			activityInfo := &persistencespb.ActivityInfo{}
			err := serialization.Decode(updatedNode.Data, activityInfo)
			s.NoError(err)
			s.Equal(newActivityID, activityInfo.ActivityId)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	).Times(1)

	// TODO: validate returned component once Ref() method of chasm tree is implememented.
	_, err := s.engine.UpdateComponent(
		context.Background(),
		ref,
		func(
			ctx chasm.MutableContext,
			component chasm.Component,
		) error {
			tc, ok := component.(*testComponent)
			s.True(ok)
			tc.ActivityInfo.ActivityId = newActivityID
			return nil
		},
	)
	s.NoError(err)
}

func (s *chasmEngineSuite) TestReadComponent_Success() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.EntityKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			EntityID:    tv.RunID(),
		},
	)
	expectedActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(ref.EntityKey, &persistencespb.ActivityInfo{
				ActivityId: expectedActivityID,
			}),
		}, nil).Times(1)

	err := s.engine.ReadComponent(
		context.Background(),
		ref,
		func(
			ctx chasm.Context,
			component chasm.Component,
		) error {
			tc, ok := component.(*testComponent)
			s.True(ok)
			s.Equal(expectedActivityID, tc.ActivityInfo.ActivityId)
			return nil
		},
	)
	s.NoError(err)
}

func (s *chasmEngineSuite) buildPersistenceMutableState(
	key chasm.EntityKey,
	componentState proto.Message,
) *persistencespb.WorkflowMutableState {
	return &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: key.NamespaceID,
			WorkflowId:  key.BusinessID,
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{},
				},
			},
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
					TransitionCount:          10,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     key.EntityID,
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(s.mockShard.GetTimeSource().Now().Add(-1 * time.Minute)),
		},
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
						TransitionCount:          1,
					},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
						TransitionCount:          10,
					},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							Type: "TestLibrary.test_component",
						},
					},
				},
				Data: s.serializeComponentState(componentState),
			},
		},
	}
}

func (s *chasmEngineSuite) serializeComponentState(
	state proto.Message,
) *commonpb.DataBlob {
	blob, err := serialization.ProtoEncode(state)
	s.NoError(err)
	return blob
}

type testComponent struct {
	chasm.UnimplementedComponent

	ActivityInfo *persistencespb.ActivityInfo
}

func (l *testComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

type testChasmLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *testChasmLibrary) Name() string {
	return "TestLibrary"
}

func (l *testChasmLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*testComponent]("test_component"),
	}
}
