package updateworkflowoptions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type noopVersionMembershipCache struct{}

func (noopVersionMembershipCache) Get(
	_ string,
	_ string,
	_ enumspb.TaskQueueType,
	_ string,
	_ string,
) (isMember bool, ok bool) {
	return false, false
}

func (noopVersionMembershipCache) Put(
	_ string,
	_ string,
	_ enumspb.TaskQueueType,
	_ string,
	_ string,
	_ bool,
) {
}

var (
	emptyOptions            = &workflowpb.WorkflowExecutionOptions{}
	unpinnedOverrideOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	}
	pinnedOverrideOptionsA = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.A",
		},
	}
	pinnedOverrideOptionsB = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.B",
		},
	}
)

func TestMergeOptions_VersionOverrideMask(t *testing.T) {
	updateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}}
	input := emptyOptions

	// Merge unpinned into empty options
	merged, err := mergeWorkflowExecutionOptions(input, unpinnedOverrideOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, unpinnedOverrideOptions, merged)

	// Merge pinned_A into unpinned options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, pinnedOverrideOptionsA, merged)

	// Merge pinned_B into pinned_A options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsB, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, pinnedOverrideOptionsB, merged)

	// Unset versioning override
	merged, err = mergeWorkflowExecutionOptions(input, emptyOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, emptyOptions, merged)
}

func TestMergeOptions_PartialMask(t *testing.T) {
	bothUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior", "versioning_override.deployment"}}
	behaviorOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior"}}
	deploymentOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.deployment"}}

	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, behaviorOnlyUpdateMask)
	assert.Error(t, err)

	_, err = mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, deploymentOnlyUpdateMask)
	assert.Error(t, err)

	merged, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, bothUpdateMask)
	assert.NoError(t, err)
	assert.EqualExportedValues(t, unpinnedOverrideOptions, merged)
}

func TestMergeOptions_EmptyMask(t *testing.T) {
	emptyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{}}
	input := pinnedOverrideOptionsB

	// Don't merge anything
	merged, err := mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, emptyUpdateMask)
	assert.NoError(t, err)
	assert.EqualExportedValues(t, input, merged)

	// Don't merge anything
	merged, err = mergeWorkflowExecutionOptions(input, nil, emptyUpdateMask)
	assert.NoError(t, err)
	assert.EqualExportedValues(t, input, merged)
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	asteriskUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, asteriskUpdateMask)
	assert.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	fooUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, fooUpdateMask)
	assert.Error(t, err)
}

type (
	// updateWorkflowOptionsSuite contains tests for the UpdateWorkflowOptions API.
	updateWorkflowOptionsSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		shardContext      *historyi.MockShardContext
		namespaceRegistry *namespace.MockRegistry

		workflowCache              *wcache.MockCache
		workflowConsistencyChecker api.WorkflowConsistencyChecker

		currentContext      *historyi.MockWorkflowContext
		currentMutableState *historyi.MockMutableState
		mockMatchingClient  *matchingservicemock.MockMatchingServiceClient
	}
)

func TestUpdateWorkflowOptionsSuite(t *testing.T) {
	s := new(updateWorkflowOptionsSuite)
	suite.Run(t, s)
}

func (s *updateWorkflowOptionsSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.namespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.namespaceRegistry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).Return(tests.GlobalNamespaceEntry, nil)

	s.shardContext = historyi.NewMockShardContext(s.controller)
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
	s.shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true)))

	// mock a mutable state with an existing versioning override
	s.currentMutableState = historyi.NewMockMutableState(s.controller)
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
		VersioningInfo: &workflowpb.WorkflowExecutionVersioningInfo{
			VersioningOverride: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
				PinnedVersion: "X.123",
			},
		},
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()

	s.currentContext = historyi.NewMockWorkflowContext(s.controller)
	s.currentContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.currentMutableState, nil)

	s.workflowCache = wcache.NewMockCache(s.controller)
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh).
		Return(s.currentContext, wcache.NoopReleaseFn, nil)

	s.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(
		s.shardContext,
		s.workflowCache,
	)

	s.mockMatchingClient = matchingservicemock.NewMockMatchingServiceClient(s.controller)
}

func (s *updateWorkflowOptionsSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *updateWorkflowOptionsSuite) TestInvoke_Success() {

	expectedOverrideOptions := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version: &deploymentpb.WorkerDeploymentVersion{
						DeploymentName: "X",
						BuildId:        "A",
					},
				},
			},
		},
	}
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMatchingClient.EXPECT().CheckTaskQueueVersionMembership(
		gomock.Any(),
		gomock.Any(),
	).Return(&matchingservice.CheckTaskQueueVersionMembershipResponse{
		IsMember: true,
	}, nil)
	s.currentMutableState.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(expectedOverrideOptions.VersioningOverride, false, "", nil, nil, "", expectedOverrideOptions.Priority).Return(&historypb.HistoryEvent{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), s.shardContext).Return(nil)

	updateReq := &historyservice.UpdateWorkflowExecutionOptionsRequest{
		NamespaceId: tests.NamespaceID.String(),
		UpdateRequest: &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace: tests.Namespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			},
			WorkflowExecutionOptions: expectedOverrideOptions,
			UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
		},
	}

	resp, err := Invoke(
		context.Background(),
		updateReq,
		s.shardContext,
		s.workflowConsistencyChecker,
		s.mockMatchingClient,
		noopVersionMembershipCache{}, // cache not meant to be used in this test
	)
	s.NoError(err)
	s.NotNil(resp)
	proto.Equal(expectedOverrideOptions, resp.GetWorkflowExecutionOptions())
}
