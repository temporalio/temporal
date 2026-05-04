package updateworkflowoptions

import (
	"context"
	"testing"
	"time"

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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type noopVersionCache struct{}

func (noopVersionCache) Get(
	_ string,
	_ string,
	_ enumspb.TaskQueueType,
	_ string,
	_ string,
) (isMember bool, shouldSkipReactivation bool, revisionNumber int64, ok bool) {
	return false, false, 0, false
}

func (noopVersionCache) Put(
	_ string,
	_ string,
	_ enumspb.TaskQueueType,
	_ string,
	_ string,
	_ bool,
	_ bool,
	_ int64,
) {
}

// noopReactivationSignaler is a no-op signaler function for tests
func noopReactivationSignaler(_ context.Context, _ *namespace.Namespace, _, _ string, _ int64) error {
	return nil
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
	require.EqualExportedValues(t, unpinnedOverrideOptions, merged)

	// Merge pinned_A into unpinned options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, pinnedOverrideOptionsA, merged)

	// Merge pinned_B into pinned_A options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsB, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, pinnedOverrideOptionsB, merged)

	// Unset versioning override
	merged, err = mergeWorkflowExecutionOptions(input, emptyOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, emptyOptions, merged)
}

func TestMergeOptions_PartialMask(t *testing.T) {
	bothUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior", "versioning_override.deployment"}}
	behaviorOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior"}}
	deploymentOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.deployment"}}

	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, behaviorOnlyUpdateMask)
	require.Error(t, err)

	_, err = mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, deploymentOnlyUpdateMask)
	require.Error(t, err)

	merged, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, bothUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, unpinnedOverrideOptions, merged)
}

func TestMergeOptions_EmptyMask(t *testing.T) {
	emptyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{}}
	input := pinnedOverrideOptionsB

	// Don't merge anything
	merged, err := mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, emptyUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, input, merged)

	// Don't merge anything
	merged, err = mergeWorkflowExecutionOptions(input, nil, emptyUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, input, merged)
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	asteriskUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, asteriskUpdateMask)
	require.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	fooUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, fooUpdateMask)
	require.Error(t, err)
}

func TestMergeOptions_TimeSkippingConfig(t *testing.T) {
	tscMask := &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}}
	cfgA := &workflowpb.TimeSkippingConfig{Enabled: true}
	cfgB := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(time.Hour)},
	}

	tcs := []struct {
		name        string
		current     *workflowpb.WorkflowExecutionOptions
		update      *workflowpb.WorkflowExecutionOptions
		wantChanged bool
		wantConfig  *workflowpb.TimeSkippingConfig
	}{
		// nil update means "don't touch" even when mask is present
		{
			name:        "nil update - existing config preserved",
			current:     &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgA},
			update:      &workflowpb.WorkflowExecutionOptions{},
			wantChanged: false,
			wantConfig:  cfgA,
		},
		// non-nil update replaces and is detected as a change
		{
			name:        "new config - changed",
			current:     &workflowpb.WorkflowExecutionOptions{},
			update:      &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgB},
			wantChanged: true,
			wantConfig:  cfgB,
		},
		// identical config is not detected as a change
		{
			name:        "same config - no change",
			current:     &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgB},
			update:      &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgB},
			wantChanged: false,
			wantConfig:  cfgB,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			original := proto.Clone(tc.current).(*workflowpb.WorkflowExecutionOptions)
			merged, err := mergeWorkflowExecutionOptions(tc.current, tc.update, tscMask)
			require.NoError(t, err)
			require.True(t, proto.Equal(tc.wantConfig, merged.GetTimeSkippingConfig()),
				"config mismatch: want %v, got %v", tc.wantConfig, merged.GetTimeSkippingConfig())
			require.Equal(t, tc.wantChanged, !proto.Equal(merged, original))
		})
	}
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
	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()

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
	s.currentMutableState.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(expectedOverrideOptions.VersioningOverride, false, "", nil, nil, "", expectedOverrideOptions.Priority, expectedOverrideOptions.TimeSkippingConfig).Return(&historypb.HistoryEvent{}, nil)
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
		noopVersionCache{},       // cache not meant to be used in this test
		noopReactivationSignaler, // signaler not meant to be used in this test
	)
	s.NoError(err)
	s.NotNil(resp)
	proto.Equal(expectedOverrideOptions, resp.GetWorkflowExecutionOptions())
}

func TestMergeAndApply_TimeSkippingConfig(t *testing.T) {
	oneHour := durationpb.New(time.Hour)
	twoHours := durationpb.New(2 * time.Hour)
	thirtyMin := durationpb.New(30 * time.Minute)

	testCases := []struct {
		name           string
		initialConfig  *workflowpb.TimeSkippingConfig
		updateOptions  *workflowpb.WorkflowExecutionOptions
		updateMask     *fieldmaskpb.FieldMask
		expectedConfig *workflowpb.TimeSkippingConfig
	}{
		{
			name: "update max_skipped_duration preserves enabled",
			initialConfig: &workflowpb.TimeSkippingConfig{
				Enabled: true,
				Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
					MaxSkippedDuration: oneHour,
				},
			},
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
					Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
						MaxSkippedDuration: twoHours,
					},
				},
			},
			updateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config.max_skipped_duration"}},
			expectedConfig: &workflowpb.TimeSkippingConfig{
				Enabled: true,
				Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
					MaxSkippedDuration: twoHours,
				},
			},
		},
		{
			name: "change bound type to max_elapsed_duration preserves enabled",
			initialConfig: &workflowpb.TimeSkippingConfig{
				Enabled: true,
				Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
					MaxSkippedDuration: oneHour,
				},
			},
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
					Bound: &workflowpb.TimeSkippingConfig_MaxElapsedDuration{
						MaxElapsedDuration: thirtyMin,
					},
				},
			},
			updateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config.max_elapsed_duration"}},
			expectedConfig: &workflowpb.TimeSkippingConfig{
				Enabled: true,
				Bound: &workflowpb.TimeSkippingConfig_MaxElapsedDuration{
					MaxElapsedDuration: thirtyMin,
				},
			},
		},
		{
			name:          "enable from nil config",
			initialConfig: nil,
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &workflowpb.TimeSkippingConfig{Enabled: true},
			},
			updateMask:     &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config.enabled"}},
			expectedConfig: &workflowpb.TimeSkippingConfig{Enabled: true},
		},
		{
			name:          "disable from enabled config",
			initialConfig: &workflowpb.TimeSkippingConfig{Enabled: true},
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &workflowpb.TimeSkippingConfig{Enabled: false},
			},
			updateMask:     &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config.enabled"}},
			expectedConfig: &workflowpb.TimeSkippingConfig{Enabled: false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ms := historyi.NewMockMutableState(ctrl)
			ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
					Config: tc.initialConfig,
				},
			}).AnyTimes()
			ms.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, "", nil, gomock.Any()).Return(&historypb.HistoryEvent{}, nil)

			result, hasChanges, err := MergeAndApply(ms, tc.updateOptions, tc.updateMask, "")
			require.NoError(t, err)
			require.True(t, hasChanges)
			require.True(t, proto.Equal(tc.expectedConfig, result.GetTimeSkippingConfig()))
		})
	}
}
