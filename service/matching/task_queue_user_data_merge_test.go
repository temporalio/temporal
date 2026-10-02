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

func TestMergeTaskQueueUserDataDeploymentsUnionsByRevision(t *testing.T) {
	taskQueueType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	preferredRouting := mergeTestRoutingConfig(5, "preferred-routing")
	otherRouting := mergeTestRoutingConfig(6, "other-routing")
	preferredHigher := mergeTestVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	otherLower := mergeTestVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
	preferredLower := mergeTestVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE)
	otherHigher := mergeTestVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)
	preferredOnly := mergeTestVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED)
	otherOnly := mergeTestVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED)

	preferred := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			taskQueueType: {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"shared": {
							RoutingConfig: preferredRouting,
							Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
								"preferred-higher": preferredHigher,
								"other-higher":     preferredLower,
								"preferred-only":   preferredOnly,
							},
						},
						"preferred-deployment": {
							RoutingConfig: mergeTestRoutingConfig(1, "preferred-deployment"),
						},
					},
				},
			},
		},
	}
	other := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			taskQueueType: {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"shared": {
							RoutingConfig: otherRouting,
							Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
								"preferred-higher": otherLower,
								"other-higher":     otherHigher,
								"other-only":       otherOnly,
							},
						},
						"other-deployment": {
							RoutingConfig: mergeTestRoutingConfig(1, "other-deployment"),
						},
					},
				},
			},
		},
	}

	merged, conflicts := mergeTaskQueueUserDataDeployments(preferred, other)

	require.Empty(t, conflicts)
	deployments := merged.GetPerType()[taskQueueType].GetDeploymentData().GetDeploymentsData()
	require.Contains(t, deployments, "preferred-deployment")
	require.Contains(t, deployments, "other-deployment")
	shared := deployments["shared"]
	protorequire.ProtoEqual(t, otherRouting, shared.GetRoutingConfig())
	protorequire.ProtoEqual(t, preferredHigher, shared.GetVersions()["preferred-higher"])
	protorequire.ProtoEqual(t, otherHigher, shared.GetVersions()["other-higher"])
	protorequire.ProtoEqual(t, preferredOnly, shared.GetVersions()["preferred-only"])
	protorequire.ProtoEqual(t, otherOnly, shared.GetVersions()["other-only"])
}

func TestMergeTaskQueueUserDataDeploymentsPreservesSnapshotFields(t *testing.T) {
	workflowType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	activityType := int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	preferredClock := &clockspb.HybridLogicalClock{WallClock: 20, Version: 1, ClusterId: 2}
	preferredConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"preferred": 2}}
	otherConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"other": 3}}
	preferredLegacyVersion := mergeTestLegacyVersion("preferred")
	otherLegacyVersion := mergeTestLegacyVersion("other")
	preferredVersioningData := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{{SetIds: []string{"preferred"}}},
	}

	preferred := &persistencespb.TaskQueueUserData{
		Clock:          preferredClock,
		VersioningData: preferredVersioningData,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				Config:        preferredConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V1,
				DeploymentData: &persistencespb.DeploymentData{
					//nolint:staticcheck // This verifies that legacy snapshot fields are not unioned.
					Versions: []*deploymentspb.DeploymentVersionData{preferredLegacyVersion},
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"preferred": {RoutingConfig: mergeTestRoutingConfig(1, "preferred")},
					},
				},
			},
		},
	}
	other := &persistencespb.TaskQueueUserData{
		Clock:          &clockspb.HybridLogicalClock{WallClock: 10},
		VersioningData: &persistencespb.VersioningData{VersionSets: []*persistencespb.CompatibleVersionSet{{SetIds: []string{"other"}}}},
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				Config:        otherConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V2,
				DeploymentData: &persistencespb.DeploymentData{
					//nolint:staticcheck // This verifies that legacy snapshot fields are not unioned.
					Versions: []*deploymentspb.DeploymentVersionData{otherLegacyVersion},
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"other": {RoutingConfig: mergeTestRoutingConfig(1, "other")},
					},
				},
			},
			activityType: {
				Config:        otherConfig,
				FairnessState: enumsspb.FAIRNESS_STATE_V2,
				DeploymentData: &persistencespb.DeploymentData{
					//nolint:staticcheck // This verifies that legacy snapshot fields are not imported with a missing task queue type.
					Versions: []*deploymentspb.DeploymentVersionData{otherLegacyVersion},
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"other": {RoutingConfig: mergeTestRoutingConfig(1, "other")},
					},
				},
			},
		},
	}

	merged, conflicts := mergeTaskQueueUserDataDeployments(preferred, other)

	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, preferredClock, merged.GetClock())
	protorequire.ProtoEqual(t, preferredVersioningData, merged.GetVersioningData())
	workflowData := merged.GetPerType()[workflowType]
	protorequire.ProtoEqual(t, preferredConfig, workflowData.GetConfig())
	require.Equal(t, enumsspb.FAIRNESS_STATE_V1, workflowData.GetFairnessState())
	//nolint:staticcheck // This verifies that legacy snapshot fields are not unioned.
	protorequire.ProtoEqual(t, preferredLegacyVersion, workflowData.GetDeploymentData().GetVersions()[0])
	require.Contains(t, workflowData.GetDeploymentData().GetDeploymentsData(), "preferred")
	require.Contains(t, workflowData.GetDeploymentData().GetDeploymentsData(), "other")

	activityData := merged.GetPerType()[activityType]
	require.Nil(t, activityData.GetConfig())
	require.Equal(t, enumsspb.FAIRNESS_STATE_UNSPECIFIED, activityData.GetFairnessState())
	//nolint:staticcheck // This verifies that legacy snapshot fields are not imported with a missing task queue type.
	require.Empty(t, activityData.GetDeploymentData().GetVersions())
	require.Contains(t, activityData.GetDeploymentData().GetDeploymentsData(), "other")
}

func TestMergeTaskQueueUserDataDeploymentsReportsEqualRevisionConflicts(t *testing.T) {
	taskQueueType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	preferredRouting := mergeTestRoutingConfig(3, "preferred")
	otherRouting := mergeTestRoutingConfig(3, "other")
	preferredVersion := mergeTestVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	otherVersion := mergeTestVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
	identicalVersion := mergeTestVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)
	preferred := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TaskQueueType(taskQueueType),
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: preferredRouting,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"conflict":  preferredVersion,
				"identical": identicalVersion,
			},
		},
	)
	other := taskQueueUserDataWithDeployment(
		nil,
		enumspb.TaskQueueType(taskQueueType),
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: otherRouting,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"conflict":  otherVersion,
				"identical": common.CloneProto(identicalVersion),
			},
		},
	)

	merged, conflicts := mergeTaskQueueUserDataDeployments(preferred, other)

	require.ElementsMatch(t, []taskQueueUserDataMergeConflict{
		{
			conflictType:   taskQueueUserDataMergeConflictTypeRoutingConfig,
			taskQueueType:  taskQueueType,
			deploymentName: "deployment",
			revisionNumber: 3,
		},
		{
			conflictType:   taskQueueUserDataMergeConflictTypeVersionData,
			taskQueueType:  taskQueueType,
			deploymentName: "deployment",
			buildID:        "conflict",
			revisionNumber: 4,
		},
	}, conflicts)
	deployment := merged.GetPerType()[taskQueueType].GetDeploymentData().GetDeploymentsData()["deployment"]
	protorequire.ProtoEqual(t, preferredRouting, deployment.GetRoutingConfig())
	protorequire.ProtoEqual(t, preferredVersion, deployment.GetVersions()["conflict"])
	protorequire.ProtoEqual(t, identicalVersion, deployment.GetVersions()["identical"])
}

func TestMergeTaskQueueUserDataDeploymentsHandlesNilValuesAndClonesInputs(t *testing.T) {
	empty, conflicts := mergeTaskQueueUserDataDeployments(nil, nil)
	require.Empty(t, conflicts)
	protorequire.ProtoEqual(t, &persistencespb.TaskQueueUserData{}, empty)

	workflowType := int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	activityType := int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	preferredClock := &clockspb.HybridLogicalClock{WallClock: 20}
	preferredVersion := mergeTestVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
	otherRouting := mergeTestRoutingConfig(0, "revision-zero")
	otherVersion := mergeTestVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED)
	preferred := &persistencespb.TaskQueueUserData{
		Clock: preferredClock,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"deployment": {
							Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
								"preferred": preferredVersion,
							},
						},
					},
				},
			},
			activityType: nil,
		},
	}
	other := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			workflowType: {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"deployment": {
							RoutingConfig: otherRouting,
							Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
								"nil":   otherVersion,
								"other": mergeTestVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED),
							},
						},
					},
				},
			},
			activityType: {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{"nil-deployment": nil},
				},
			},
		},
	}

	merged, conflicts := mergeTaskQueueUserDataDeployments(preferred, other)

	require.Empty(t, conflicts)
	deployment := merged.GetPerType()[workflowType].GetDeploymentData().GetDeploymentsData()["deployment"]
	require.NotNil(t, deployment.GetRoutingConfig())
	require.Zero(t, deployment.GetRoutingConfig().GetRevisionNumber())
	require.NotNil(t, deployment.GetVersions()["nil"])
	require.Zero(t, deployment.GetVersions()["nil"].GetRevisionNumber())
	require.Contains(t, deployment.GetVersions(), "other")
	nilDeployment, exists := merged.GetPerType()[activityType].GetDeploymentData().GetDeploymentsData()["nil-deployment"]
	require.True(t, exists)
	require.Nil(t, nilDeployment)

	require.NotSame(t, preferred, merged)
	require.NotSame(t, preferredClock, merged.GetClock())
	require.NotSame(t, preferredVersion, deployment.GetVersions()["preferred"])
	require.NotSame(t, otherRouting, deployment.GetRoutingConfig())
	require.NotSame(t, otherVersion, deployment.GetVersions()["nil"])

	preferredClock.WallClock = 30
	preferredVersion.RevisionNumber = 30
	otherRouting.RevisionNumber = 30
	otherVersion.RevisionNumber = 30
	require.Equal(t, int64(20), merged.GetClock().GetWallClock())
	require.Equal(t, int64(2), deployment.GetVersions()["preferred"].GetRevisionNumber())
	require.Zero(t, deployment.GetRoutingConfig().GetRevisionNumber())
	require.Zero(t, deployment.GetVersions()["nil"].GetRevisionNumber())

	deployment.GetRoutingConfig().RevisionNumber = 40
	deployment.GetVersions()["preferred"].RevisionNumber = 40
	deployment.GetVersions()["nil"].RevisionNumber = 40
	require.Equal(t, int64(30), otherRouting.GetRevisionNumber())
	require.Equal(t, int64(30), preferredVersion.GetRevisionNumber())
	require.Equal(t, int64(30), otherVersion.GetRevisionNumber())
}

func mergeTestRoutingConfig(revision int64, buildID string) *deploymentpb.RoutingConfig {
	return &deploymentpb.RoutingConfig{
		RevisionNumber: revision,
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: "deployment",
			BuildId:        buildID,
		},
	}
}

func mergeTestVersionData(
	revision int64,
	status enumspb.WorkerDeploymentVersionStatus,
) *deploymentspb.WorkerDeploymentVersionData {
	return &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: revision,
		Status:         status,
	}
}

func mergeTestLegacyVersion(buildID string) *deploymentspb.DeploymentVersionData {
	return &deploymentspb.DeploymentVersionData{
		Version: &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: "legacy",
			BuildId:        buildID,
		},
	}
}
