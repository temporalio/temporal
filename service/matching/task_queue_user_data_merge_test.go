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

// The higher parent HLC supplies the base snapshot. A present clock beats a missing clock.
// Equal or both-missing clocks select incoming; logical counters break equal wall times.
func TestSelectTaskQueueUserDataSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		currentClock        *clockspb.HybridLogicalClock
		incomingClock       *clockspb.HybridLogicalClock
		wantSelectionReason taskQueueUserDataSnapshotSelectionReason
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

			baseSnapshot, mergeSourceSnapshot, selectionReason := selectTaskQueueUserDataSnapshots(current, incoming)

			require.Equal(t, test.wantSelectionReason, selectionReason)
			if test.wantSelectionReason == taskQueueUserDataSelectionReasonCurrentParentClock {
				require.Same(t, current, baseSnapshot)
				require.Same(t, incoming, mergeSourceSnapshot)
			} else {
				require.Same(t, incoming, baseSnapshot)
				require.Same(t, current, mergeSourceSnapshot)
			}
		})
	}
}

// The higher routing-config revision wins, and the complete winning config is retained.
// Identical routing configs produce no conflict.
func TestMergeTaskQueueUserDataDeploymentsRoutingRevisionSelection(t *testing.T) {
	t.Parallel()

	snapshot := func(routingConfig *deploymentpb.RoutingConfig) *persistencespb.TaskQueueUserData {
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
		name                     string
		baseRoutingConfig        *deploymentpb.RoutingConfig
		mergeSourceRoutingConfig *deploymentpb.RoutingConfig
		wantRoutingConfig        *deploymentpb.RoutingConfig
	}{
		{
			name:                     "base_higher_revision_wins",
			baseRoutingConfig:        newTestRoutingConfig(6, "base-build"),
			mergeSourceRoutingConfig: newTestRoutingConfig(5, "merge-source-build"),
			wantRoutingConfig:        newTestRoutingConfig(6, "base-build"),
		},
		{
			name:                     "merge_source_higher_revision_wins",
			baseRoutingConfig:        newTestRoutingConfig(5, "base-build"),
			mergeSourceRoutingConfig: newTestRoutingConfig(6, "merge-source-build"),
			wantRoutingConfig:        newTestRoutingConfig(6, "merge-source-build"),
		},
		{
			name:                     "identical_routing_config_has_no_conflict",
			baseRoutingConfig:        newTestRoutingConfig(5, "build"),
			mergeSourceRoutingConfig: newTestRoutingConfig(5, "build"),
			wantRoutingConfig:        newTestRoutingConfig(5, "build"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseSnapshot := snapshot(test.baseRoutingConfig)
			mergeSourceSnapshot := snapshot(test.mergeSourceRoutingConfig)

			merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

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

	snapshot := func(version *deploymentspb.WorkerDeploymentVersionData) *persistencespb.TaskQueueUserData {
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
		name               string
		baseVersion        *deploymentspb.WorkerDeploymentVersionData
		mergeSourceVersion *deploymentspb.WorkerDeploymentVersionData
		wantVersion        *deploymentspb.WorkerDeploymentVersionData
	}{
		{
			name:               "base_higher_revision_wins",
			baseVersion:        newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			mergeSourceVersion: newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
			wantVersion:        newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
		},
		{
			name:               "merge_source_higher_revision_wins",
			baseVersion:        newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			mergeSourceVersion: newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
			wantVersion:        newTestWorkerDeploymentVersionData(6, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
		},
		{
			name:               "identical_version_data_has_no_conflict",
			baseVersion:        newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			mergeSourceVersion: newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			wantVersion:        newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseSnapshot := snapshot(test.baseVersion)
			mergeSourceSnapshot := snapshot(test.mergeSourceVersion)

			merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

			require.Empty(t, conflicts)
			mergedDeployment := merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].
				GetDeploymentData().GetDeploymentsData()["deployment"]
			protorequire.ProtoEqual(t, test.wantVersion, mergedDeployment.GetVersions()["build"])
		})
	}
}

// Base {A} and merge source {B} produce {A, B}, preserving both deployments' contents.
func TestMergeTaskQueueUserDataDeploymentsUnionsDeployments(t *testing.T) {
	t.Parallel()

	baseDeployment := &persistencespb.WorkerDeploymentData{RoutingConfig: newTestRoutingConfig(0, "base-build")}
	mergeSourceDeployment := &persistencespb.WorkerDeploymentData{RoutingConfig: newTestRoutingConfig(0, "merge-source-build")}
	baseSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"base-deployment",
		baseDeployment,
	)
	mergeSourceSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"merge-source-deployment",
		mergeSourceDeployment,
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, &persistencespb.DeploymentData{
		DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
			"base-deployment":         baseDeployment,
			"merge-source-deployment": mergeSourceDeployment,
		},
	}, merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData())
}

// Within one deployment, base {build-A} and merge source {build-B} retain both versions.
// Versions with revision zero are retained.
func TestMergeTaskQueueUserDataDeploymentsUnionsVersions(t *testing.T) {
	t.Parallel()

	baseVersion := newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	mergeSourceVersion := newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
	baseSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"base-build": baseVersion,
			},
		},
	)
	mergeSourceSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"merge-source-build": mergeSourceVersion,
			},
		},
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, &persistencespb.WorkerDeploymentData{
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"base-build":         baseVersion,
			"merge-source-build": mergeSourceVersion,
		},
	}, merged.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["deployment"])
}

// An existing deployment with a nil version map accepts source versions, including revision zero.
func TestMergeTaskQueueUserDataDeploymentsInitializesVersionMap(t *testing.T) {
	t.Parallel()

	baseSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{},
	)
	mergeSourceSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"build": newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED),
			},
		},
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, mergeSourceSnapshot, merged)
}

// Merging deployments leaves the base clock, config, fairness, VersioningData,
// legacy versions, and unversioned ramp data unchanged.
//
//nolint:staticcheck // Verifies compatibility for deprecated snapshot fields.
func TestMergeTaskQueueUserDataDeploymentsPreservesNonDeploymentFieldsFromBase(t *testing.T) {
	t.Parallel()

	workflowType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	baseClock := &clockspb.HybridLogicalClock{WallClock: 20, Version: 1, ClusterId: 2}
	baseConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"base": 2}}
	mergeSourceConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"merge-source": 3}}
	baseLegacyVersion := newTestLegacyDeploymentVersionData("base")
	mergeSourceLegacyVersion := newTestLegacyDeploymentVersionData("merge-source")
	baseRampData := &deploymentspb.DeploymentVersionData{RampPercentage: 25}
	baseVersioningData := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{{SetIds: []string{"base"}}},
	}

	baseSnapshot := &persistencespb.TaskQueueUserData{
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
	mergeSourceSnapshot := &persistencespb.TaskQueueUserData{
		Clock:          &clockspb.HybridLogicalClock{WallClock: 10},
		VersioningData: &persistencespb.VersioningData{VersionSets: []*persistencespb.CompatibleVersionSet{{SetIds: []string{"merge-source"}}}},
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				Config:        mergeSourceConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V2,
				DeploymentData: &persistencespb.DeploymentData{
					Versions:            []*deploymentspb.DeploymentVersionData{mergeSourceLegacyVersion},
					UnversionedRampData: &deploymentspb.DeploymentVersionData{RampPercentage: 50},
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"merge-source": {RoutingConfig: newTestRoutingConfig(1, "merge-source")},
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
						"base":         {RoutingConfig: newTestRoutingConfig(1, "base")},
						"merge-source": {RoutingConfig: newTestRoutingConfig(1, "merge-source")},
					},
				},
			},
		},
	}

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, want, merged)
}

// A task queue type found only in the merge source contributes its deployment map.
// Its config, fairness, and legacy deployment fields are not imported.
//
//nolint:staticcheck // Verifies compatibility for deprecated snapshot fields.
func TestMergeTaskQueueUserDataDeploymentsImportsOnlyDeploymentsForNewTaskQueueType(t *testing.T) {
	t.Parallel()

	activityType := int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	mergeSourceDeployment := &persistencespb.WorkerDeploymentData{
		RoutingConfig: newTestRoutingConfig(0, "build"),
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"build": newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED),
		},
	}
	baseSnapshot := &persistencespb.TaskQueueUserData{}
	mergeSourceSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		"deployment",
		mergeSourceDeployment,
	)
	mergeSourceActivityData := mergeSourceSnapshot.PerType[activityType]
	mergeSourceActivityData.Config = &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"merge-source": 3}}
	mergeSourceActivityData.FairnessState = enumsspb.FAIRNESS_STATE_V2
	mergeSourceActivityData.DeploymentData.Versions = []*deploymentspb.DeploymentVersionData{newTestLegacyDeploymentVersionData("legacy")}
	mergeSourceActivityData.DeploymentData.UnversionedRampData = &deploymentspb.DeploymentVersionData{RampPercentage: 25}
	want := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_ACTIVITY, "deployment", mergeSourceDeployment)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, want, merged)
}

// Equal revisions with different payloads retain the base value and report conflict details.
// Routing-config and version conflicts are both returned when they occur together.
func TestMergeTaskQueueUserDataDeploymentsReportsEqualRevisionConflicts(t *testing.T) {
	t.Parallel()

	baseRoutingConfig := newTestRoutingConfig(3, "base")
	mergeSourceRoutingConfig := newTestRoutingConfig(3, "merge-source")
	baseVersion := newTestWorkerDeploymentVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	mergeSourceVersion := newTestWorkerDeploymentVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
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
		name                  string
		baseDeployment        *persistencespb.WorkerDeploymentData
		mergeSourceDeployment *persistencespb.WorkerDeploymentData
		wantConflicts         []taskQueueUserDataMergeConflict
	}{
		{
			name:                  "routing_config_conflict",
			baseDeployment:        &persistencespb.WorkerDeploymentData{RoutingConfig: baseRoutingConfig},
			mergeSourceDeployment: &persistencespb.WorkerDeploymentData{RoutingConfig: mergeSourceRoutingConfig},
			wantConflicts:         []taskQueueUserDataMergeConflict{routingConflict},
		},
		{
			name: "version_conflict",
			baseDeployment: &persistencespb.WorkerDeploymentData{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"build": baseVersion},
			},
			mergeSourceDeployment: &persistencespb.WorkerDeploymentData{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"build": mergeSourceVersion},
			},
			wantConflicts: []taskQueueUserDataMergeConflict{versionConflict},
		},
		{
			name: "routing_config_and_version_conflicts",
			baseDeployment: &persistencespb.WorkerDeploymentData{
				RoutingConfig: baseRoutingConfig,
				Versions:      map[string]*deploymentspb.WorkerDeploymentVersionData{"build": baseVersion},
			},
			mergeSourceDeployment: &persistencespb.WorkerDeploymentData{
				RoutingConfig: mergeSourceRoutingConfig,
				Versions:      map[string]*deploymentspb.WorkerDeploymentVersionData{"build": mergeSourceVersion},
			},
			wantConflicts: []taskQueueUserDataMergeConflict{routingConflict, versionConflict},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			baseSnapshot := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "deployment", test.baseDeployment)
			mergeSourceSnapshot := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "deployment", test.mergeSourceDeployment)

			merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

			require.Equal(t, test.wantConflicts, conflicts)
			protorequire.ProtoEqual(t, baseSnapshot, merged)
		})
	}
}

// Two nil snapshots produce empty data. Missing base containers are initialized for source deployments.
// Source task queue types without deployments contribute nothing.
//
//nolint:staticcheck // Verifies compatibility for deprecated snapshot fields.
func TestMergeTaskQueueUserDataDeploymentsHandlesEmptySnapshotsAndContainers(t *testing.T) {
	t.Parallel()

	workflowType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	sourceWithDeployment := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{},
	)
	sourceWithNilDeployment := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "nil-deployment", nil)
	tests := []struct {
		name                string
		baseSnapshot        *persistencespb.TaskQueueUserData
		mergeSourceSnapshot *persistencespb.TaskQueueUserData
		want                *persistencespb.TaskQueueUserData
	}{
		{
			name: "both_snapshots_nil",
			want: &persistencespb.TaskQueueUserData{},
		},
		{
			name:                "nil_base_initializes_containers",
			mergeSourceSnapshot: sourceWithDeployment,
			want:                sourceWithDeployment,
		},
		{
			name:         "nil_source_retains_base",
			baseSnapshot: sourceWithDeployment,
			want:         sourceWithDeployment,
		},
		{
			name: "nil_task_queue_type_initializes_containers",
			baseSnapshot: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{workflowType: nil},
			},
			mergeSourceSnapshot: sourceWithDeployment,
			want:                sourceWithDeployment,
		},
		{
			name: "nil_deployment_entry_is_preserved",
			baseSnapshot: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{workflowType: nil},
			},
			mergeSourceSnapshot: sourceWithNilDeployment,
			want:                sourceWithNilDeployment,
		},
		{
			name:         "source_type_without_deployments_is_not_imported",
			baseSnapshot: &persistencespb.TaskQueueUserData{},
			mergeSourceSnapshot: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					workflowType: {
						Config:        &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"merge-source": 3}},
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

			merged, conflicts := mergeTaskQueueUserDataDeployments(test.baseSnapshot, test.mergeSourceSnapshot)

			require.Empty(t, conflicts)
			protorequire.ProtoEqual(t, test.want, merged)
		})
	}
}

// Merging leaves both inputs unchanged. Mutating an input or the merged result does not
// change the other snapshots, including nested maps and proto messages.
func TestMergeTaskQueueUserDataDeploymentsClonesInputs(t *testing.T) {
	t.Parallel()

	baseClock := &clockspb.HybridLogicalClock{WallClock: 20}
	baseVersion := newTestWorkerDeploymentVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	mergeSourceRoutingConfig := newTestRoutingConfig(0, "revision-zero")
	mergeSourceVersion := newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED)
	baseSnapshot := taskQueueUserDataWithDeployment(
		baseClock,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"base-build": baseVersion},
		},
	)
	mergeSourceSnapshot := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: mergeSourceRoutingConfig,
			Versions:      map[string]*deploymentspb.WorkerDeploymentVersionData{"merge-source-build": mergeSourceVersion},
		},
	)
	baseBeforeMerge := common.CloneProto(baseSnapshot)
	mergeSourceBeforeMerge := common.CloneProto(mergeSourceSnapshot)

	merged, conflicts := mergeTaskQueueUserDataDeployments(baseSnapshot, mergeSourceSnapshot)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, baseBeforeMerge, baseSnapshot)
	protorequire.ProtoEqual(t, mergeSourceBeforeMerge, mergeSourceSnapshot)
	wantMerged := common.CloneProto(taskQueueUserDataWithDeployment(
		baseClock,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: mergeSourceRoutingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"base-build":         baseVersion,
				"merge-source-build": mergeSourceVersion,
			},
		},
	))

	baseClock.WallClock = 30
	baseVersion.RevisionNumber = 30
	mergeSourceRoutingConfig.CurrentDeploymentVersion.BuildId = "mutated-input"
	mergeSourceVersion.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
	protorequire.ProtoEqual(t, wantMerged, merged)

	baseAfterMutation := common.CloneProto(baseSnapshot)
	mergeSourceAfterMutation := common.CloneProto(mergeSourceSnapshot)
	deployment := merged.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"]
	merged.Clock.WallClock = 40
	deployment.RoutingConfig.CurrentDeploymentVersion.BuildId = "mutated-result"
	deployment.Versions["base-build"].RevisionNumber = 40
	deployment.Versions["merge-source-build"].Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE
	deployment.Versions["new-build"] = &deploymentspb.WorkerDeploymentVersionData{}
	protorequire.ProtoEqual(t, baseAfterMutation, baseSnapshot)
	protorequire.ProtoEqual(t, mergeSourceAfterMutation, mergeSourceSnapshot)
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
