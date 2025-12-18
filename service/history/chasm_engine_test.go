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
	executionCache wcache.Cache
	registry       *chasm.Registry
	config         *configs.Config
	archetypeID    chasm.ArchetypeID

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
	s.config.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
	)
	s.executionCache = wcache.NewHostLevelCache(
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

	var ok bool
	s.archetypeID, ok = s.registry.ComponentIDFor(&testComponent{})
	s.True(ok)

	s.mockShard.SetEngineForTesting(s.mockEngine)
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	s.engine = newChasmEngine(
		s.executionCache,
		s.registry,
		s.config,
		NewChasmNotifier(),
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

func (s *chasmEngineSuite) TestNewExecution_BrandNew() {
	tv := testvars.New(s.T())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()

	var runID string
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			s.validateCreateRequest(request, s.archetypeID, newActivityID, "", 0)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	executionKey, serializedRef, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyRejectDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.NoError(err)
	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       runID,
	}
	s.Equal(expectedExecutionKey, executionKey)
	s.validateNewExecutionResponseRef(serializedRef, expectedExecutionKey)
}

func (s *chasmEngineSuite) TestNewExecution_RequestIDDedup() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
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

	executionKey, serializedRef, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithRequestID(tv.RequestID()),
	)
	s.NoError(err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}
	s.Equal(expectedExecutionKey, executionKey)
	s.validateNewExecutionResponseRef(serializedRef, expectedExecutionKey)
}

func (s *chasmEngineSuite) TestNewExecution_ReusePolicy_AllowDuplicate() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
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
			s.validateCreateRequest(request, s.archetypeID, newActivityID, tv.RunID(), currentRunConditionFailedErr.LastWriteVersion)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	executionKey, serializedRef, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.NoError(err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       runID,
	}
	s.Equal(expectedExecutionKey, executionKey)
	s.validateNewExecutionResponseRef(serializedRef, expectedExecutionKey)
}

func (s *chasmEngineSuite) TestNewExecution_ReusePolicy_FailedOnly_Success() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
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
			s.validateCreateRequest(request, s.archetypeID, newActivityID, tv.RunID(), currentRunConditionFailedErr.LastWriteVersion)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	executionKey, serializedRef, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.NoError(err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       runID,
	}
	s.Equal(expectedExecutionKey, executionKey)
	s.validateNewExecutionResponseRef(serializedRef, expectedExecutionKey)
}

func (s *chasmEngineSuite) TestNewExecution_ReusePolicy_FailedOnly_Fail() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
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

	_, _, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.ErrorAs(err, new(*chasm.ExecutionAlreadyStartedError))
}

func (s *chasmEngineSuite) TestNewExecution_ReusePolicy_RejectDuplicate() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
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

	_, _, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyRejectDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	s.ErrorAs(err, new(*chasm.ExecutionAlreadyStartedError))
}

func (s *chasmEngineSuite) TestNewExecution_ConflictPolicy_UseExisting() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()
	// Current run is still running, conflict policy will be used.
	currentRunConditionFailedErr := s.currentRunConditionFailedErr(
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)

	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)

	executionKey, serializedRef, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyUseExisting,
		),
	)
	s.NoError(err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}
	s.Equal(expectedExecutionKey, executionKey)
	s.validateNewExecutionResponseRef(serializedRef, expectedExecutionKey)
}

func (s *chasmEngineSuite) TestNewExecution_ConflictPolicy_TerminateExisting() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()
	// Current run is still running, conflict policy will be used.
	currentRunConditionFailedErr := s.currentRunConditionFailedErr(
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)

	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)

	_, _, err := s.engine.NewExecution(
		context.Background(),
		ref,
		s.newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyTerminateExisting,
		),
	)
	s.ErrorAs(err, new(*serviceerror.Unimplemented))
}

func (s *chasmEngineSuite) newTestExecutionFn(
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
	expectedArchetypeID chasm.ArchetypeID,
	expectedActivityID string,
	expectedPreviousRunID string,
	expectedPreviousLastWriteVersion int64,
) {
	s.Equal(expectedArchetypeID, request.ArchetypeID)

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

func (s *chasmEngineSuite) validateNewExecutionResponseRef(
	serializedRef []byte,
	expectedExecutionKey chasm.ExecutionKey,
) {
	deserializedRef, err := chasm.DeserializeComponentRef(serializedRef)
	s.NoError(err)
	s.Equal(expectedExecutionKey, deserializedRef.ExecutionKey)

	archetypeID, err := deserializedRef.ArchetypeID(s.registry)
	s.NoError(err)
	fqn, ok := s.registry.ComponentFqnByID(archetypeID)
	s.True(ok)
	s.Equal("TestLibrary.test_component", fqn)
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
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       tv.RunID(),
		},
	)
	newActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(ref.ExecutionKey, &persistencespb.ActivityInfo{
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
	s.mockEngine.EXPECT().NotifyChasmExecution(ref.ExecutionKey, gomock.Any()).Return().Times(1)

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
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       tv.RunID(),
		},
	)
	expectedActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(ref.ExecutionKey, &persistencespb.ActivityInfo{
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

// TestPollComponent_Success_NoWait tests the behavior of PollComponent when the predicate is
// satisfied at the outset.
func (s *chasmEngineSuite) TestPollComponent_Success_NoWait() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       tv.RunID(),
		},
	)
	expectedActivityID := tv.ActivityID()

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(ref.ExecutionKey, &persistencespb.ActivityInfo{
				ActivityId: expectedActivityID,
			}),
		}, nil).Times(1)

	newSerializedRef, err := s.engine.PollComponent(
		context.Background(),
		ref,
		func(ctx chasm.Context, component chasm.Component) (bool, error) {
			return true, nil
		},
	)
	s.NoError(err)

	newRef, err := chasm.DeserializeComponentRef(newSerializedRef)
	s.NoError(err)
	s.Equal(ref.BusinessID, newRef.BusinessID)
}

// TestPollComponent_EmptyRunID_Wait tests that PollComponent correctly resolves an empty RunID
// before subscribing for notifications. This is a regression test: previously, PollComponent
// would subscribe using the empty RunID from the request, but notifications are sent with the
// resolved RunID, so the subscription would never receive any notification and the poll would
// time out.
func (s *chasmEngineSuite) TestPollComponent_EmptyRunID_Wait() {
	tv := testvars.New(s.T())
	actualRunID := tv.Any().RunID()
	activityID := tv.ActivityID()

	// The poll request uses empty RunID.
	pollRef := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "", // empty - must be resolved before subscribing
		},
	)

	// The update request uses the actual RunID (as would happen in practice).
	resolvedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       actualRunID,
	}
	updateRef := chasm.NewComponentRef[*testComponent](resolvedExecutionKey)

	// GetCurrentExecution is called to resolve the empty RunID.
	s.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetCurrentExecutionResponse{
			RunID: actualRunID,
		}, nil).AnyTimes()

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(resolvedExecutionKey, &persistencespb.ActivityInfo{}),
		}, nil).Times(1)
	s.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(tests.UpdateWorkflowExecutionResponse, nil).Times(1)
	// The notification is sent with the resolved RunID (not empty).
	s.mockEngine.EXPECT().NotifyChasmExecution(resolvedExecutionKey, gomock.Any()).DoAndReturn(
		func(key chasm.ExecutionKey, ref []byte) {
			s.engine.notifier.Notify(key)
		},
	).Times(1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pollErr := make(chan error)
	pollResult := make(chan []byte)
	go func() {
		newSerializedRef, err := s.engine.PollComponent(
			ctx,
			pollRef,
			func(ctx chasm.Context, component chasm.Component) (bool, error) {
				tc, ok := component.(*testComponent)
				s.True(ok)
				// Predicate satisfied when activityID is set.
				return tc.ActivityInfo.ActivityId == activityID, nil
			},
		)
		pollErr <- err
		pollResult <- newSerializedRef
	}()

	// Give the poll goroutine time to start and subscribe.
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// Update the component to satisfy the predicate. This triggers a notification
	// with the resolved RunID. With the bug, PollComponent subscribed to an empty
	// RunID key, so it won't receive this notification and will time out.
	_, err := s.engine.UpdateComponent(
		context.Background(),
		updateRef,
		func(ctx chasm.MutableContext, component chasm.Component) error {
			tc, ok := component.(*testComponent)
			s.True(ok)
			tc.ActivityInfo.ActivityId = activityID
			return nil
		},
	)
	s.NoError(err)

	// With the fix, the poll should complete quickly. With the bug, it times out.
	err = <-pollErr
	s.NoError(err, "PollComponent should not time out - if it did, the subscription key likely has empty RunID")

	newSerializedRef := <-pollResult
	s.NotNil(newSerializedRef)

	newRef, err := chasm.DeserializeComponentRef(newSerializedRef)
	s.NoError(err)
	s.Equal(tv.WorkflowID(), newRef.BusinessID)
	s.Equal(actualRunID, newRef.RunID)
}

// TestPollComponent_Success_Wait tests the waiting behavior of PollComponent.
func (s *chasmEngineSuite) TestPollComponent_Success_Wait() {
	// The predicate is not satisfied at the outset, so the call blocks waiting for notifications.
	// UpdateComponent is used twice to update the execution in a way which does not satisfy the
	// predicate, and a final third time in a way that does satisfy the predicate, causing the
	// long-poll to return.
	const numUpdatesTotal = 3
	const updateAtWhichSatisfied = 2 // 0-indexed, so 3rd update

	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	activityID := tv.ActivityID()
	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       tv.RunID(),
		},
	)
	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(ref.ExecutionKey, &persistencespb.ActivityInfo{}),
		}, nil).
		Times(1) // subsequent reads during UpdateComponent and PollComponent are from cache
	s.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(tests.UpdateWorkflowExecutionResponse, nil).
		Times(numUpdatesTotal)
	s.mockEngine.EXPECT().NotifyChasmExecution(ref.ExecutionKey, gomock.Any()).DoAndReturn(
		func(key chasm.ExecutionKey, ref []byte) {
			s.engine.notifier.Notify(key)
		},
	).Times(numUpdatesTotal)

	pollErr := make(chan error)
	pollResult := make(chan []byte)
	pollComponent := func() {
		newSerializedRef, err := s.engine.PollComponent(
			context.Background(),
			ref,
			func(ctx chasm.Context, component chasm.Component) (bool, error) {
				tc, ok := component.(*testComponent)
				s.True(ok)
				satisfied := tc.ActivityInfo.ActivityId == activityID
				return satisfied, nil
			},
		)
		pollErr <- err
		pollResult <- newSerializedRef
	}
	updateComponent := func(satisfyPredicate bool) {
		_, err := s.engine.UpdateComponent(
			context.Background(),
			ref,
			func(ctx chasm.MutableContext, component chasm.Component) error {
				tc, ok := component.(*testComponent)
				s.True(ok)
				if satisfyPredicate {
					tc.ActivityInfo.ActivityId = activityID
				}
				return nil
			},
		)
		s.NoError(err)
	}
	assertEmptyChan := func(ch chan []byte) {
		select {
		case <-ch:
			s.FailNow("expected channel to be empty")
		default:
		}
	}

	// Start a PollComponent call. It will not return until the third execution update.
	go pollComponent()

	// Perform two execution updates that do not satisfy the predicate followed by one that does.
	for range 2 {
		updateComponent(false)
		time.Sleep(100 * time.Millisecond) //nolint:forbidigo
		assertEmptyChan(pollResult)
	}
	updateComponent(true)
	// The poll call has returned.
	s.NoError(<-pollErr)
	newSerializedRef := <-pollResult
	s.NotNil(newSerializedRef)

	newRef, err := chasm.DeserializeComponentRef(newSerializedRef)
	s.NoError(err)
	s.Equal(tests.NamespaceID.String(), newRef.NamespaceID)
	s.Equal(tv.WorkflowID(), newRef.BusinessID)
	s.Equal(tv.RunID(), newRef.RunID)

	newActivityID := make(chan string, 1)
	err = s.engine.ReadComponent(
		context.Background(),
		newRef,
		func(
			ctx chasm.Context,
			component chasm.Component,
		) error {
			tc, ok := component.(*testComponent)
			s.True(ok)
			newActivityID <- tc.ActivityInfo.ActivityId
			return nil
		},
	)
	s.NoError(err)
	s.Equal(activityID, <-newActivityID)
}

// TestPollComponent_StaleState tests that PollComponent returns a user-friendly Unavailable error
// when the submitted component reference is ahead of persisted state (e.g. due to namespace
// failover).
func (s *chasmEngineSuite) TestPollComponent_StaleState() {
	tv := testvars.New(s.T())
	tv = tv.WithRunID(tv.Any().RunID())

	executionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}

	testComponentTypeID, ok := s.mockShard.ChasmRegistry().ComponentIDFor(&testComponent{})
	s.True(ok)

	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: s.buildPersistenceMutableState(executionKey, &persistencespb.ActivityInfo{}),
		}, nil).AnyTimes()

	pRef := &persistencespb.ChasmComponentRef{
		NamespaceId: executionKey.NamespaceID,
		BusinessId:  executionKey.BusinessID,
		RunId:       executionKey.RunID,
		ArchetypeId: uint32(testComponentTypeID),
		ExecutionVersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 1, // ahead of persisted state
			TransitionCount:          testTransitionCount,
		},
	}
	staleToken, err := pRef.Marshal()
	s.NoError(err)
	staleRef, err := chasm.DeserializeComponentRef(staleToken)
	s.NoError(err)

	_, err = s.engine.PollComponent(
		context.Background(),
		staleRef,
		func(ctx chasm.Context, component chasm.Component) (bool, error) {
			s.Fail("predicate should not be called with stale ref")
			return false, nil
		},
	)
	s.Error(err)
	var unavailable *serviceerror.Unavailable
	s.ErrorAs(err, &unavailable)
	s.Equal("please retry", unavailable.Message)
}

func (s *chasmEngineSuite) buildPersistenceMutableState(
	key chasm.ExecutionKey,
	componentState proto.Message,
) *persistencespb.WorkflowMutableState {

	testComponentTypeID, ok := s.mockShard.ChasmRegistry().ComponentIDFor(&testComponent{})
	s.True(ok)

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
					TransitionCount:          testTransitionCount,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     key.RunID,
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
						TransitionCount:          testTransitionCount,
					},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: testComponentTypeID,
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

const (
	testComponentPausedSAName   = "PausedSA"
	testComponentPausedMemoName = "PausedMemo"
	testTransitionCount         = 10
)

var (
	testComponentPausedSearchAttribute = chasm.NewSearchAttributeBool(testComponentPausedSAName, chasm.SearchAttributeFieldBool01)

	_ chasm.VisibilitySearchAttributesProvider = (*testComponent)(nil)
	_ chasm.VisibilityMemoProvider             = (*testComponent)(nil)
)

type testComponent struct {
	chasm.UnimplementedComponent

	ActivityInfo *persistencespb.ActivityInfo
}

func (l *testComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (l *testComponent) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		testComponentPausedSearchAttribute.Value(l.ActivityInfo.Paused),
	}
}

func (l *testComponent) Memo(_ chasm.Context) proto.Message {
	return &persistencespb.WorkflowExecutionState{
		RunId: l.ActivityInfo.ActivityId,
	}
}

func newTestComponentStateBlob(info *persistencespb.ActivityInfo) *commonpb.DataBlob {
	data, _ := info.Marshal()
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         data,
	}
}

type testChasmLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *testChasmLibrary) Name() string {
	return "TestLibrary"
}

func (l *testChasmLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*testComponent]("test_component",
			chasm.WithSearchAttributes(testComponentPausedSearchAttribute)),
	}
}
