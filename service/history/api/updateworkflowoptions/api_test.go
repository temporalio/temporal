package updateworkflowoptions

import (
	"context"
	"fmt"
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

// tscProtoEq is a gomock.Matcher for *commonpb.TimeSkippingConfig that uses proto.Equal
// instead of reflect.DeepEqual, which fails on proto messages with differing internal state.
type tscProtoEq struct{ expected *commonpb.TimeSkippingConfig }

func (m tscProtoEq) Matches(x any) bool {
	got, _ := x.(*commonpb.TimeSkippingConfig)
	if m.expected == nil && got == nil {
		return true
	}
	if m.expected == nil || got == nil {
		return false
	}
	return proto.Equal(m.expected, got)
}

func (m tscProtoEq) String() string {
	return fmt.Sprintf("proto.Equal(%v)", m.expected)
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
	oneTimeOverrideOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_OneTime{
				OneTime: &workflowpb.VersioningOverride_OneTimeOverride{
					TargetDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
						DeploymentName: "X",
						BuildId:        "C",
					},
				},
			},
		},
	}
)

func TestMergeOptions_VersionOverrideMask(t *testing.T) {
	updateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}}
	input := emptyOptions

	// Merge unpinned into empty options
	merged, optionsToReapply, err := mergeWorkflowExecutionOptions(input, unpinnedOverrideOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, unpinnedOverrideOptions, merged)
	require.False(t, optionsToReapply.hasChanges())

	// Merge pinned_A into unpinned options
	merged, optionsToReapply, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, pinnedOverrideOptionsA, merged)
	require.False(t, optionsToReapply.hasChanges())

	// Merge pinned_B into pinned_A options
	merged, optionsToReapply, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsB, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, pinnedOverrideOptionsB, merged)
	require.False(t, optionsToReapply.hasChanges())

	// Unset versioning override
	merged, optionsToReapply, err = mergeWorkflowExecutionOptions(input, emptyOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	require.EqualExportedValues(t, emptyOptions, merged)
	require.False(t, optionsToReapply.hasChanges())
}

func TestMergeOptions_PartialMask(t *testing.T) {
	allUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior", "versioning_override.deployment"}}
	behaviorOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior"}}
	deploymentOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.deployment"}}

	_, _, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, behaviorOnlyUpdateMask)
	require.Error(t, err)

	_, _, err = mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, deploymentOnlyUpdateMask)
	require.Error(t, err)

	merged, _, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, allUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, unpinnedOverrideOptions, merged)

	// partial mask for time skipping config will return invalid argument error
	timeSkippingPartialMask := &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config.enabled"}}
	_, _, err = mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, timeSkippingPartialMask)
	require.Error(t, err)

}

func TestMergeOptions_VersionOverrideNestedMask(t *testing.T) {
	testCases := []struct {
		name string
		mask *fieldmaskpb.FieldMask
	}{
		{
			name: "one_time field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.one_time"}},
		},
		{
			name: "one_time target version field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.one_time.target_deployment_version"}},
		},
		{
			name: "one_time target version deployment name field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.one_time.target_deployment_version.deployment_name"}},
		},
		{
			name: "one_time target version build id field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.one_time.target_deployment_version.build_id"}},
		},
		{
			name: "pinned oneof field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.pinned"}},
		},
		{
			name: "pinned nested version field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.pinned.version"}},
		},
		{
			name: "auto_upgrade oneof field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.auto_upgrade"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := proto.Clone(pinnedOverrideOptionsB).(*workflowpb.WorkflowExecutionOptions)
			requested := proto.Clone(oneTimeOverrideOptions).(*workflowpb.WorkflowExecutionOptions)

			_, _, err := mergeWorkflowExecutionOptions(input, requested, tc.mask)
			require.Error(t, err)
		})
	}
}

func TestMergeOptions_EmptyMask(t *testing.T) {
	emptyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{}}
	input := pinnedOverrideOptionsB

	// Don't merge anything
	merged, optionsToReapply, err := mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, emptyUpdateMask)
	require.NoError(t, err)
	require.False(t, optionsToReapply.hasChanges())
	require.EqualExportedValues(t, input, merged)

	// Don't merge anything
	merged, optionsToReapply, err = mergeWorkflowExecutionOptions(input, nil, emptyUpdateMask)
	require.NoError(t, err)
	require.False(t, optionsToReapply.hasChanges())
	require.EqualExportedValues(t, input, merged)
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	asteriskUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	_, _, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, asteriskUpdateMask)
	require.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	fooUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
	_, _, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, fooUpdateMask)
	require.Error(t, err)
}

func TestMergeOptions_TimeSkippingConfig(t *testing.T) {

	// assuming the TSC is always in the mask -> meaning the user always updates the TSC
	tscMask := &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}}
	cfgA := &commonpb.TimeSkippingConfig{Enabled: true}
	cfgB := &commonpb.TimeSkippingConfig{
		Enabled:     true,
		FastForward: durationpb.New(time.Hour),
	}
	cfgC := &commonpb.TimeSkippingConfig{Enabled: false}

	tcs := []struct {
		name                 string
		mergeInto            *workflowpb.WorkflowExecutionOptions
		mergeFrom            *workflowpb.WorkflowExecutionOptions
		configForUpdateEvent *commonpb.TimeSkippingConfig
		configHasChanged     bool
	}{
		{
			name:                 "nil update - clears the TSC",
			mergeInto:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgA},
			mergeFrom:            nil,
			configForUpdateEvent: nil,
			configHasChanged:     true,
		},
		{
			name:                 "same config with side-effect field change",
			mergeInto:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgB},
			mergeFrom:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgB},
			configForUpdateEvent: cfgB,
			configHasChanged:     true,
		},
		{
			name:                 "new config with side-effect field change",
			mergeInto:            &workflowpb.WorkflowExecutionOptions{},
			mergeFrom:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgB},
			configForUpdateEvent: cfgB,
			configHasChanged:     true,
		},
		{
			name:                 "same config with no side-effect field change",
			mergeInto:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgA},
			mergeFrom:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgA},
			configForUpdateEvent: cfgA,
			configHasChanged:     false,
		},
		{
			name:                 "new config to disable time skipping",
			mergeInto:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgA},
			mergeFrom:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgC},
			configForUpdateEvent: cfgC,
			configHasChanged:     true,
		},
		{
			name:                 "new config to enable time skipping",
			mergeInto:            &workflowpb.WorkflowExecutionOptions{},
			mergeFrom:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfgA},
			configForUpdateEvent: cfgA,
			configHasChanged:     true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			merged, sideEffects, err := mergeWorkflowExecutionOptions(tc.mergeInto, tc.mergeFrom, tscMask)
			require.NoError(t, err)
			require.True(t, proto.Equal(tc.configForUpdateEvent, merged.GetTimeSkippingConfig()))
			require.Equal(t, tc.configHasChanged, sideEffects.timeSkippingConfigHasChanged)
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
	s.currentMutableState.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
		expectedOverrideOptions.VersioningOverride, false, "", nil, nil, "",
		expectedOverrideOptions.Priority, expectedOverrideOptions.TimeSkippingConfig, false, nil).Return(&historypb.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().Now().Return(time.Time{})
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

func TestMergeAndApply(t *testing.T) {
	oneHour := durationpb.New(time.Hour)
	newOverride := &workflowpb.VersioningOverride{Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE}

	tscMask := &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}}
	versioningMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}}

	tcs := []struct {
		name          string
		initialConfig *commonpb.TimeSkippingConfig
		updateOptions *workflowpb.WorkflowExecutionOptions
		updateMask    *fieldmaskpb.FieldMask

		// use version as an unrelated option to test its impacts on TSC
		expectVersioningOverride *workflowpb.VersioningOverride
		unsetVersion             bool

		// the values in WorkflowExecutionOptionsUpdatedEventAttributes
		expectChanges    bool
		expectTSCInEvent *commonpb.TimeSkippingConfig
		expectTSCUpdated bool

		// only checks the result when needed
		needCheckResultTSC bool
		resultTSC          *commonpb.TimeSkippingConfig
	}{
		{
			name:          "basic: enable from nil config",
			initialConfig: nil,
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true},
			},
			updateMask:               tscMask,
			expectChanges:            true,
			expectVersioningOverride: nil,
			unsetVersion:             true,
			expectTSCInEvent:         &commonpb.TimeSkippingConfig{Enabled: true},
			expectTSCUpdated:         true,
			resultTSC:                &commonpb.TimeSkippingConfig{Enabled: true},
			needCheckResultTSC:       true,
		},
		{
			name:          "basic: disable from enabled config",
			initialConfig: &commonpb.TimeSkippingConfig{Enabled: true},
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: false},
			},
			updateMask:               tscMask,
			expectChanges:            true,
			expectVersioningOverride: nil,
			unsetVersion:             true,
			expectTSCInEvent:         &commonpb.TimeSkippingConfig{Enabled: false},
			expectTSCUpdated:         true,
			resultTSC:                &commonpb.TimeSkippingConfig{Enabled: false},
			needCheckResultTSC:       true,
		},
		{
			name:          "basic: fast-forward from nil",
			initialConfig: nil,
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			},
			updateMask:               tscMask,
			expectChanges:            true,
			expectVersioningOverride: nil,
			unsetVersion:             true,
			expectTSCInEvent:         &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			expectTSCUpdated:         true,
			resultTSC:                &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			needCheckResultTSC:       true,
		},
		{
			name:          "reapply: TSC updated with same fast-forward",
			initialConfig: &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			updateOptions: &workflowpb.WorkflowExecutionOptions{
				TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			},
			updateMask:               tscMask,
			expectVersioningOverride: nil,
			unsetVersion:             true,

			expectChanges:      true,
			expectTSCInEvent:   &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			expectTSCUpdated:   true,
			resultTSC:          &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			needCheckResultTSC: true,
		},
		{
			name:                     "TSC no change: version update with fast-forward untouched",
			initialConfig:            &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			updateOptions:            &workflowpb.WorkflowExecutionOptions{VersioningOverride: newOverride},
			updateMask:               versioningMask,
			expectVersioningOverride: newOverride,
			unsetVersion:             false,
			// as we always put the merged TSC into the event, even if it is the same as the initial config
			expectChanges:      true,
			expectTSCInEvent:   &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			expectTSCUpdated:   false,
			needCheckResultTSC: true,
			resultTSC:          &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
		},
		{
			name:                     "TSC no change: TSC without fast-forward are updated with same value",
			initialConfig:            &commonpb.TimeSkippingConfig{Enabled: true},
			updateOptions:            &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true}},
			updateMask:               tscMask,
			expectVersioningOverride: nil,
			unsetVersion:             false,
			expectChanges:            false,
		},
		{
			name:                     "nil allowed: nil with mask clears the TSC",
			initialConfig:            &commonpb.TimeSkippingConfig{Enabled: true, FastForward: oneHour},
			updateOptions:            &workflowpb.WorkflowExecutionOptions{VersioningOverride: newOverride},
			updateMask:               &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config", "versioning_override"}},
			expectChanges:            true,
			expectVersioningOverride: newOverride,
			unsetVersion:             false,
			expectTSCInEvent:         nil,
			expectTSCUpdated:         true,
			resultTSC:                nil,
			needCheckResultTSC:       true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ms := historyi.NewMockMutableState(ctrl)
			ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				TimeSkippingInfo: &persistencespb.TimeSkippingInfo{Config: tc.initialConfig},
			}).AnyTimes()

			if tc.expectChanges {
				ms.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
					tc.expectVersioningOverride,
					tc.unsetVersion,
					"",
					nil,
					nil,
					"",
					nil,
					tscProtoEq{tc.expectTSCInEvent},
					tc.expectTSCUpdated,
					nil,
				).Return(&historypb.HistoryEvent{}, nil).Times(1)
			}

			result, hasChanges, err := MergeAndApply(ms, tc.updateOptions, tc.updateMask, "")
			require.NoError(t, err)
			if tc.expectChanges {
				require.True(t, hasChanges)
				if tc.needCheckResultTSC {
					require.True(t, proto.Equal(tc.resultTSC, result.GetTimeSkippingConfig()))
				}
			} else {
				require.False(t, hasChanges)
			}
		})
	}
}
