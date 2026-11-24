package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

// The higher parent HLC supplies the base user data. A present clock beats a missing clock.
// Equal or both-missing clocks select incoming; logical counters break equal wall times.
func TestSelectTaskQueueUserData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		currentClock        *clockspb.HybridLogicalClock
		incomingClock       *clockspb.HybridLogicalClock
		wantSelectionReason taskQueueUserDataSelectionReason
	}{
		{
			name:                "current_higher_parent_clock",
			currentClock:        &clockspb.HybridLogicalClock{WallClock: 20},
			incomingClock:       &clockspb.HybridLogicalClock{WallClock: 10},
			wantSelectionReason: taskQueueUserDataSelectionReasonCurrentParentClock,
		},
		{
			name:                "incoming_higher_parent_clock",
			currentClock:        &clockspb.HybridLogicalClock{WallClock: 10},
			incomingClock:       &clockspb.HybridLogicalClock{WallClock: 20},
			wantSelectionReason: taskQueueUserDataSelectionReasonIncomingParentClock,
		},
		{
			name:                "only_current_has_a_clock",
			currentClock:        &clockspb.HybridLogicalClock{WallClock: 10},
			wantSelectionReason: taskQueueUserDataSelectionReasonCurrentParentClock,
		},
		{
			name:                "only_incoming_has_a_clock",
			incomingClock:       &clockspb.HybridLogicalClock{WallClock: 10},
			wantSelectionReason: taskQueueUserDataSelectionReasonIncomingParentClock,
		},
		{
			name:                "both_clocks_missing_use_incoming",
			wantSelectionReason: taskQueueUserDataSelectionReasonIncomingFallback,
		},
		{
			name:                "equal_clocks_use_incoming",
			currentClock:        &clockspb.HybridLogicalClock{WallClock: 10},
			incomingClock:       &clockspb.HybridLogicalClock{WallClock: 10},
			wantSelectionReason: taskQueueUserDataSelectionReasonIncomingFallback,
		},
		{
			name:                "current_higher_logical_counter",
			currentClock:        &clockspb.HybridLogicalClock{WallClock: 10, Version: 1},
			incomingClock:       &clockspb.HybridLogicalClock{WallClock: 10},
			wantSelectionReason: taskQueueUserDataSelectionReasonCurrentParentClock,
		},
		{
			name:                "incoming_higher_logical_counter",
			currentClock:        &clockspb.HybridLogicalClock{WallClock: 10},
			incomingClock:       &clockspb.HybridLogicalClock{WallClock: 10, Version: 1},
			wantSelectionReason: taskQueueUserDataSelectionReasonIncomingParentClock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			current := &persistencespb.TaskQueueUserData{Clock: test.currentClock}
			incoming := &persistencespb.TaskQueueUserData{Clock: test.incomingClock}

			baseUserData, otherUserData, selectionReason := selectTaskQueueUserData(current, incoming)

			require.Equal(t, test.wantSelectionReason, selectionReason)
			if test.wantSelectionReason == taskQueueUserDataSelectionReasonCurrentParentClock {
				require.Same(t, current, baseUserData)
				require.Same(t, incoming, otherUserData)
			} else {
				require.Same(t, incoming, baseUserData)
				require.Same(t, current, otherUserData)
			}
		})
	}
}

// The higher routing-config revision wins, and the complete winning config is retained.
// Identical routing configs produce no conflict.
func TestMergeTaskQueueUserDataDeploymentsRoutingRevisionSelection(t *testing.T) {
	t.Parallel()

	newUserData := func(routingConfig *deploymentpb.RoutingConfig) *persistencespb.TaskQueueUserData {
		return taskQueueUserDataWithDeployment(
			nil,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			"deployment",
			&persistencespb.WorkerDeploymentData{
				RoutingConfig: routingConfig,
			},
		)
	}

	tests := []struct {
		name               string
		baseRoutingConfig  *deploymentpb.RoutingConfig
		otherRoutingConfig *deploymentpb.RoutingConfig
		wantRoutingConfig  *deploymentpb.RoutingConfig
	}{
		{
			name:               "base_higher_revision_wins",
			baseRoutingConfig:  newTestRoutingConfig(6, "base-build"),
			otherRoutingConfig: newTestRoutingConfig(5, "other-build"),
			wantRoutingConfig:  newTestRoutingConfig(6, "base-build"),
		},
		{
			name:               "other_higher_revision_wins",
			baseRoutingConfig:  newTestRoutingConfig(5, "base-build"),
			otherRoutingConfig: newTestRoutingConfig(6, "other-build"),
			wantRoutingConfig:  newTestRoutingConfig(6, "other-build"),
		},
		{
			name:               "identical_routing_config_has_no_conflict",
			baseRoutingConfig:  newTestRoutingConfig(5, "build"),
			otherRoutingConfig: newTestRoutingConfig(5, "build"),
			wantRoutingConfig:  newTestRoutingConfig(5, "build"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseUserData := newUserData(test.baseRoutingConfig)
			otherUserData := newUserData(test.otherRoutingConfig)

			merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

			require.Empty(t, conflicts)
			mergedDeployment := merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].
				GetDeploymentData().GetDeploymentsData()["deployment"]
			protorequire.ProtoEqual(t, test.wantRoutingConfig, mergedDeployment.GetRoutingConfig())
		})
	}
}

// The higher version revision wins, and the complete winning version data is retained.
// Identical version data produces no conflict.
func TestMergeTaskQueueUserDataDeploymentsVersionRevisionSelection(t *testing.T) {
	t.Parallel()

	newUserData := func(version *deploymentspb.WorkerDeploymentVersionData) *persistencespb.TaskQueueUserData {
		return taskQueueUserDataWithDeployment(
			nil,
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			"deployment",
			&persistencespb.WorkerDeploymentData{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
					"build": version,
				},
			},
		)
	}

	tests := []struct {
		name         string
		baseVersion  *deploymentspb.WorkerDeploymentVersionData
		otherVersion *deploymentspb.WorkerDeploymentVersionData
		wantVersion  *deploymentspb.WorkerDeploymentVersionData
	}{
		{
			name:         "base_higher_revision_wins",
			baseVersion:  newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			otherVersion: newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
			wantVersion:  newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
		},
		{
			name:         "other_higher_revision_wins",
			baseVersion:  newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			otherVersion: newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
			wantVersion:  newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
		},
		{
			name:         "identical_version_data_has_no_conflict",
			baseVersion:  newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			otherVersion: newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			wantVersion:  newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseUserData := newUserData(test.baseVersion)
			otherUserData := newUserData(test.otherVersion)

			merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

			require.Empty(t, conflicts)
			mergedDeployment := merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].
				GetDeploymentData().GetDeploymentsData()["deployment"]
			protorequire.ProtoEqual(t, test.wantVersion, mergedDeployment.GetVersions()["build"])
		})
	}
}

// Base {A} and other user data {B} produce {A, B}, preserving both deployments' contents.
func TestMergeTaskQueueUserDataDeploymentsUnionsDeployments(t *testing.T) {
	t.Parallel()

	baseDeployment := &persistencespb.WorkerDeploymentData{RoutingConfig: newTestRoutingConfig(0, "base-build")}
	otherDeployment := &persistencespb.WorkerDeploymentData{RoutingConfig: newTestRoutingConfig(0, "other-build")}
	baseUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"base-deployment",
		baseDeployment,
	)
	otherUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"other-deployment",
		otherDeployment,
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, &persistencespb.DeploymentData{
		DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
			"base-deployment":  baseDeployment,
			"other-deployment": otherDeployment,
		},
	}, merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData())
}

// Within one deployment, base {build-A} and other user data {build-B} retain both versions.
// Versions with revision zero are retained.
func TestMergeTaskQueueUserDataDeploymentsUnionsVersions(t *testing.T) {
	t.Parallel()

	baseVersion := newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	otherVersion := newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
	baseUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"base-build": baseVersion,
			},
		},
	)
	otherUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"other-build": otherVersion,
			},
		},
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, &persistencespb.WorkerDeploymentData{
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"base-build":  baseVersion,
			"other-build": otherVersion,
		},
	}, merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["deployment"])
}

// An existing deployment with a nil version map accepts versions from the other user data, including revision zero.
func TestMergeTaskQueueUserDataDeploymentsInitializesVersionMap(t *testing.T) {
	t.Parallel()

	baseUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{},
	)
	otherUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"build": newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED),
			},
		},
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, otherUserData, merged)
}

// Merging deployments leaves the base clock, config, fairness, VersioningData,
// legacy versions, and unversioned ramp data unchanged.
//
//nolint:staticcheck // Verifies compatibility for deprecated user data fields.
func TestMergeTaskQueueUserDataDeploymentsPreservesNonDeploymentFieldsFromBase(t *testing.T) {
	t.Parallel()

	workflowType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	baseClock := &clockspb.HybridLogicalClock{WallClock: 20, Version: 1, ClusterId: 2}
	baseConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"base": 2}}
	otherConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"other": 3}}
	baseLegacyVersion := newTestLegacyDeploymentVersionData("base")
	otherLegacyVersion := newTestLegacyDeploymentVersionData("other")
	baseRampData := &deploymentspb.DeploymentVersionData{RampPercentage: 25}
	baseVersioningData := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{{SetIds: []string{"base"}}},
	}

	baseUserData := &persistencespb.TaskQueueUserData{
		Clock:          baseClock,
		VersioningData: baseVersioningData,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				Config:        baseConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V1,
				DeploymentData: &persistencespb.DeploymentData{
					Versions:            []*deploymentspb.DeploymentVersionData{baseLegacyVersion},
					UnversionedRampData: baseRampData,
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"base": {RoutingConfig: newTestRoutingConfig(1, "base")},
					},
				},
			},
		},
	}
	otherUserData := &persistencespb.TaskQueueUserData{
		Clock:          &clockspb.HybridLogicalClock{WallClock: 10},
		VersioningData: &persistencespb.VersioningData{VersionSets: []*persistencespb.CompatibleVersionSet{{SetIds: []string{"other"}}}},
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				Config:        otherConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V2,
				DeploymentData: &persistencespb.DeploymentData{
					Versions:            []*deploymentspb.DeploymentVersionData{otherLegacyVersion},
					UnversionedRampData: &deploymentspb.DeploymentVersionData{RampPercentage: 50},
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"other": {RoutingConfig: newTestRoutingConfig(1, "other")},
					},
				},
			},
		},
	}
	want := &persistencespb.TaskQueueUserData{
		Clock:          baseClock,
		VersioningData: baseVersioningData,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				Config:        baseConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V1,
				DeploymentData: &persistencespb.DeploymentData{
					Versions:            []*deploymentspb.DeploymentVersionData{baseLegacyVersion},
					UnversionedRampData: baseRampData,
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"base":  {RoutingConfig: newTestRoutingConfig(1, "base")},
						"other": {RoutingConfig: newTestRoutingConfig(1, "other")},
					},
				},
			},
		},
	}

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, want, merged)
}

// A task queue type found only in the other user data contributes its deployment map.
// Its config, fairness, and legacy deployment fields are not imported.
//
//nolint:staticcheck // Verifies compatibility for deprecated user data fields.
func TestMergeTaskQueueUserDataDeploymentsImportsOnlyDeploymentsForNewTaskQueueType(t *testing.T) {
	t.Parallel()

	activityType := int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	otherDeployment := &persistencespb.WorkerDeploymentData{
		RoutingConfig: newTestRoutingConfig(0, "build"),
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"build": newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED),
		},
	}
	baseUserData := &persistencespb.TaskQueueUserData{}
	otherUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		"deployment",
		otherDeployment,
	)
	otherActivityData := otherUserData.PerType[activityType]
	otherActivityData.Config = &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"other": 3}}
	otherActivityData.FairnessState = enumsspb.FAIRNESS_STATE_V2
	otherActivityData.DeploymentData.Versions = []*deploymentspb.DeploymentVersionData{newTestLegacyDeploymentVersionData("legacy")}
	otherActivityData.DeploymentData.UnversionedRampData = &deploymentspb.DeploymentVersionData{RampPercentage: 25}
	want := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_ACTIVITY, "deployment", otherDeployment)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, want, merged)
}

// Equal revisions with different payloads retain the base value and report conflict details.
// Routing-config and version conflicts are both returned when they occur together.
func TestMergeTaskQueueUserDataDeploymentsReportsEqualRevisionConflicts(t *testing.T) {
	t.Parallel()

	baseRoutingConfig := newTestRoutingConfig(3, "base")
	otherRoutingConfig := newTestRoutingConfig(3, "other")
	baseVersion := newTestWorkerDeploymentVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	otherVersion := newTestWorkerDeploymentVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
	routingConflict := taskQueueUserDataMergeConflict{
		conflictType:   taskQueueUserDataMergeConflictTypeRoutingConfig,
		taskQueueType:  int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		deploymentName: "deployment",
		revisionNumber: 3,
	}
	versionConflict := taskQueueUserDataMergeConflict{
		conflictType:   taskQueueUserDataMergeConflictTypeVersionData,
		taskQueueType:  int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		deploymentName: "deployment",
		buildID:        "build",
		revisionNumber: 4,
	}
	tests := []struct {
		name            string
		baseDeployment  *persistencespb.WorkerDeploymentData
		otherDeployment *persistencespb.WorkerDeploymentData
		wantConflicts   []taskQueueUserDataMergeConflict
	}{
		{
			name:            "routing_config_conflict",
			baseDeployment:  &persistencespb.WorkerDeploymentData{RoutingConfig: baseRoutingConfig},
			otherDeployment: &persistencespb.WorkerDeploymentData{RoutingConfig: otherRoutingConfig},
			wantConflicts:   []taskQueueUserDataMergeConflict{routingConflict},
		},
		{
			name: "version_conflict",
			baseDeployment: &persistencespb.WorkerDeploymentData{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"build": baseVersion},
			},
			otherDeployment: &persistencespb.WorkerDeploymentData{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"build": otherVersion},
			},
			wantConflicts: []taskQueueUserDataMergeConflict{versionConflict},
		},
		{
			name: "routing_config_and_version_conflicts",
			baseDeployment: &persistencespb.WorkerDeploymentData{
				RoutingConfig: baseRoutingConfig,
				Versions:      map[string]*deploymentspb.WorkerDeploymentVersionData{"build": baseVersion},
			},
			otherDeployment: &persistencespb.WorkerDeploymentData{
				RoutingConfig: otherRoutingConfig,
				Versions:      map[string]*deploymentspb.WorkerDeploymentVersionData{"build": otherVersion},
			},
			wantConflicts: []taskQueueUserDataMergeConflict{routingConflict, versionConflict},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseUserData := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "deployment", test.baseDeployment)
			otherUserData := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "deployment", test.otherDeployment)

			merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

			require.Equal(t, test.wantConflicts, conflicts)
			protorequire.ProtoEqual(t, baseUserData, merged)
		})
	}
}

// Two nil inputs produce empty user data. Missing base containers are initialized for deployments from the other user data.
// Task queue types without deployments in the other user data contribute nothing.
//
//nolint:staticcheck // Verifies compatibility for deprecated user data fields.
func TestMergeTaskQueueUserDataDeploymentsHandlesEmptyUserDataAndContainers(t *testing.T) {
	t.Parallel()

	workflowType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	otherWithDeployment := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{},
	)
	otherWithNilDeployment := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "nil-deployment", nil)
	tests := []struct {
		name          string
		baseUserData  *persistencespb.TaskQueueUserData
		otherUserData *persistencespb.TaskQueueUserData
		want          *persistencespb.TaskQueueUserData
	}{
		{
			name: "both_user_data_nil",
			want: &persistencespb.TaskQueueUserData{},
		},
		{
			name:          "nil_base_initializes_containers",
			otherUserData: otherWithDeployment,
			want:          otherWithDeployment,
		},
		{
			name:         "nil_other_retains_base",
			baseUserData: otherWithDeployment,
			want:         otherWithDeployment,
		},
		{
			name: "nil_task_queue_type_initializes_containers",
			baseUserData: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{workflowType: nil},
			},
			otherUserData: otherWithDeployment,
			want:          otherWithDeployment,
		},
		{
			name: "nil_deployment_entry_is_preserved",
			baseUserData: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{workflowType: nil},
			},
			otherUserData: otherWithNilDeployment,
			want:          otherWithNilDeployment,
		},
		{
			name:         "other_type_without_deployments_is_not_imported",
			baseUserData: &persistencespb.TaskQueueUserData{},
			otherUserData: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					workflowType: {
						Config:        &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"other": 3}},
						FairnessState: enumsspb.FAIRNESS_STATE_V2,
						DeploymentData: &persistencespb.DeploymentData{
							Versions:            []*deploymentspb.DeploymentVersionData{newTestLegacyDeploymentVersionData("legacy")},
							UnversionedRampData: &deploymentspb.DeploymentVersionData{RampPercentage: 25},
						},
					},
				},
			},
			want: &persistencespb.TaskQueueUserData{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			merged, conflicts := mergeTaskQueueUserDataDeployments(test.baseUserData, test.otherUserData)

			require.Empty(t, conflicts)
			protorequire.ProtoEqual(t, test.want, merged)
		})
	}
}

// Merging leaves both inputs unchanged. Mutating an input or the merged result does not
// change the other values, including nested maps and proto messages.
func TestMergeTaskQueueUserDataDeploymentsClonesInputs(t *testing.T) {
	t.Parallel()

	baseClock := &clockspb.HybridLogicalClock{WallClock: 20}
	baseVersion := newTestWorkerDeploymentVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	otherRoutingConfig := newTestRoutingConfig(0, "revision-zero")
	otherVersion := newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED)
	baseUserData := taskQueueUserDataWithDeployment(
		baseClock,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"base-build": baseVersion},
		},
	)
	otherUserData := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: otherRoutingConfig,
			Versions:      map[string]*deploymentspb.WorkerDeploymentVersionData{"other-build": otherVersion},
		},
	)
	baseBeforeMerge := common.CloneProto(baseUserData)
	otherBeforeMerge := common.CloneProto(otherUserData)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseUserData, otherUserData)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, baseBeforeMerge, baseUserData)
	protorequire.ProtoEqual(t, otherBeforeMerge, otherUserData)
	wantMerged := common.CloneProto(taskQueueUserDataWithDeployment(
		baseClock,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: otherRoutingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"base-build":  baseVersion,
				"other-build": otherVersion,
			},
		},
	))

	baseClock.WallClock = 30
	baseVersion.RevisionNumber = 30
	otherRoutingConfig.CurrentDeploymentVersion.BuildId = "mutated-input"
	otherVersion.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
	protorequire.ProtoEqual(t, wantMerged, merged)

	baseAfterMutation := common.CloneProto(baseUserData)
	otherAfterMutation := common.CloneProto(otherUserData)
	deployment := merged.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"]
	merged.Clock.WallClock = 40
	deployment.RoutingConfig.CurrentDeploymentVersion.BuildId = "mutated-result"
	deployment.Versions["base-build"].RevisionNumber = 40
	deployment.Versions["other-build"].Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE
	deployment.Versions["new-build"] = &deploymentspb.WorkerDeploymentVersionData{}
	protorequire.ProtoEqual(t, baseAfterMutation, baseUserData)
	protorequire.ProtoEqual(t, otherAfterMutation, otherUserData)
}

func newTestRoutingConfig(revision int64, buildID string) *deploymentpb.RoutingConfig {
	return &deploymentpb.RoutingConfig{
		RevisionNumber: revision,
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: "deployment",
			BuildId:        buildID,
		},
	}
}

func newTestWorkerDeploymentVersionData(
	revision int64,
	status enumspb.WorkerDeploymentVersionStatus,
) *deploymentspb.WorkerDeploymentVersionData {
	return &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: revision,
		Status:         status,
	}
}

func newTestLegacyDeploymentVersionData(buildID string) *deploymentspb.DeploymentVersionData {
	return &deploymentspb.DeploymentVersionData{
		Version: &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: "legacy",
			BuildId:        buildID,
		},
	}
}
