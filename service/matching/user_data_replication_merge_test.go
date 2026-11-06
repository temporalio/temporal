package matching

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
)

func taskQueueUserDataWithDeployment(
	clock *clockspb.HybridLogicalClock,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	deployment *persistencespb.WorkerDeploymentData,
) *persistencespb.TaskQueueUserData {
	return &persistencespb.TaskQueueUserData{
		Clock: clock,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			int32(taskQueueType): {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						deploymentName: deployment,
					},
				},
			},
		},
	}
}

// Replicating identical user data leaves persisted data unchanged and emits no conflict diagnostics.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventIdenticalUserData() {
	captureHandler := s.captureDroppedOnEngine()
	current := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 20, Version: 1, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"},
				RevisionNumber:           5,
			},
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"build": {RevisionNumber: 5, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
			},
		},
	)
	current.VersioningData = &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{mkNewSet("v1-build", current.Clock)},
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("v2-target"), current.Clock, nil),
		},
		RedirectRules: []*persistencespb.RedirectRule{
			mkRedirectRulePersistence(mkRedirectRule("v2-source", "v2-target"), current.Clock, nil),
		},
	}
	currentPerType := current.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)]
	currentPerType.Config = &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"tenant": 2}}
	//nolint:staticcheck // Verifies compatibility for the deprecated fairness state.
	currentPerType.FairnessState = enumsspb.FAIRNESS_STATE_V2
	incoming := common.CloneProto(current)
	taskQueue := uuid.NewString()
	s.seedTaskQueueUserData(taskQueue, current)
	metricCapture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(metricCapture)
	logCapture := s.logger.StartCapture()
	defer s.logger.StopCapture(logCapture)

	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

	protorequire.ProtoEqual(s.T(), current, got)
	s.Require().Empty(metricCapture.SnapshotMetric(metrics.TaskQueueUserDataReplicationEqualRevisionConflicts.Name()))
	for _, record := range logCapture.Snapshot() {
		s.Require().False(record.Level == testlogger.Warn && record.Message == "found conflicting user data with same revision number during replication")
	}
}

// After failover, delayed replication can contribute valid versions and higher revisions despite its older parent clock.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventDelayedReplicationAfterFailoverPreservesVersions() {
	currentClock := &clockspb.HybridLogicalClock{WallClock: 200, ClusterId: 1}
	incomingClock := &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 1}
	currentRouting := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "shared", BuildId: "current-routing"},
		RevisionNumber:           5,
	}
	incomingRouting := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "shared", BuildId: "incoming-routing"},
		RevisionNumber:           6,
	}
	currentVersion := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 8,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}
	incomingVersion := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 7,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}
	current := taskQueueUserDataWithDeployment(
		currentClock,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"shared",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: currentRouting,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": currentVersion,
				"C": {RevisionNumber: 0, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED},
			},
		},
	)
	currentDeployments := current.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()
	currentDeployments["current-only"] = &persistencespb.WorkerDeploymentData{
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"current-only": {RevisionNumber: 1},
		},
	}
	currentDeployments["zero-routing"] = &persistencespb.WorkerDeploymentData{}

	incomingZeroRouting := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "zero-routing", BuildId: "zero"},
		RevisionNumber:           0,
	}
	incoming := taskQueueUserDataWithDeployment(
		incomingClock,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"shared",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: incomingRouting,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": incomingVersion,
				"B": {RevisionNumber: 0, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE},
			},
		},
	)
	incomingDeployments := incoming.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()
	incomingDeployments["incoming-only"] = &persistencespb.WorkerDeploymentData{
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"incoming-only": {RevisionNumber: 1},
		},
	}
	incomingDeployments["zero-routing"] = &persistencespb.WorkerDeploymentData{RoutingConfig: incomingZeroRouting}

	taskQueue := uuid.NewString()
	s.seedTaskQueueUserData(taskQueue, current)
	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

	protorequire.ProtoEqual(s.T(), currentClock, got.GetClock())
	gotDeployments := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()
	s.Require().Len(gotDeployments, 4)
	s.Contains(gotDeployments, "current-only")
	s.Contains(gotDeployments, "incoming-only")
	gotShared := gotDeployments["shared"]
	protorequire.ProtoEqual(s.T(), incomingRouting, gotShared.GetRoutingConfig())
	s.Require().Len(gotShared.GetVersions(), 3)
	protorequire.ProtoEqual(s.T(), currentVersion, gotShared.GetVersions()["A"])
	s.Contains(gotShared.GetVersions(), "B")
	s.Contains(gotShared.GetVersions(), "C")
	protorequire.ProtoEqual(s.T(), incomingZeroRouting, gotDeployments["zero-routing"].GetRoutingConfig())
}

// Newer snapshots replace membership; delayed snapshots merge versions by revision without replacing snapshot fields.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventReorderedAndRepeatedUserData() {
	older := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(1, "A"),
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
			},
		},
	)
	older.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = &taskqueuepb.TaskQueueConfig{
		FairnessWeightOverrides: map[string]float32{"older": 1},
	}
	newer := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(2, "A"),
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": newTestWorkerDeploymentVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			},
		},
	)
	newer.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = &taskqueuepb.TaskQueueConfig{
		FairnessWeightOverrides: map[string]float32{"newer": 1},
	}
	disjoint := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 30, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"B": newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE),
			},
		},
	)
	disjoint.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = &taskqueuepb.TaskQueueConfig{
		FairnessWeightOverrides: map[string]float32{"disjoint": 1},
	}
	want := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 30, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(2, "A"),
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": newTestWorkerDeploymentVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
				"B": newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE),
			},
		},
	)
	want.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = &taskqueuepb.TaskQueueConfig{
		FairnessWeightOverrides: map[string]float32{"disjoint": 1},
	}
	wantAfterOlderReplay := common.CloneProto(want)
	wantAfterOlderReplayDeployment := wantAfterOlderReplay.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"]
	wantAfterOlderReplayDeployment.RoutingConfig = newTestRoutingConfig(1, "A")
	wantAfterOlderReplayDeployment.Versions["A"] = newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
	tests := []struct {
		name             string
		userDataUpdates  []*persistencespb.TaskQueueUserData
		wantBeforeRepeat *persistencespb.TaskQueueUserData
		wantAfterRepeat  *persistencespb.TaskQueueUserData
	}{
		{
			name:             "ordered updates then stale user data",
			userDataUpdates:  []*persistencespb.TaskQueueUserData{older, newer, disjoint, older},
			wantBeforeRepeat: disjoint,
			wantAfterRepeat:  wantAfterOlderReplay,
		},
		{
			name:             "newest parent first then stale user data",
			userDataUpdates:  []*persistencespb.TaskQueueUserData{disjoint, newer, older, older},
			wantBeforeRepeat: want,
			wantAfterRepeat:  want,
		},
		{
			name:             "newer version before stale then repeated subset",
			userDataUpdates:  []*persistencespb.TaskQueueUserData{newer, older, disjoint, newer},
			wantBeforeRepeat: disjoint,
			wantAfterRepeat:  want,
		},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			taskQueue := uuid.NewString()
			s.seedTaskQueueUserData(taskQueue, test.userDataUpdates[0])
			var got *persistencespb.TaskQueueUserData
			for _, incoming := range test.userDataUpdates[1 : len(test.userDataUpdates)-1] {
				got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
			}
			protorequire.ProtoEqual(s.T(), test.wantBeforeRepeat, got)

			got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, test.userDataUpdates[len(test.userDataUpdates)-1])
			protorequire.ProtoEqual(s.T(), test.wantAfterRepeat, got)
		})
	}
}

// Each deployment forgets one old version and registers one new version; the passive must retain the same bounded set.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventRepeatedDeletionsDoNotAccumulateVersions() {
	const retainedVersions = 12
	const rollouts = 50
	initialCurrentBuildID := fmt.Sprintf("build-%03d", retainedVersions-1)
	s.matchingEngine.config.EnableTaskQueueUserDataRevisionMerge = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	active := &persistencespb.TaskQueueUserData{
		Clock:   &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 1},
		PerType: make(map[int32]*persistencespb.TaskQueueTypeUserData),
	}
	for _, taskType := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
		versions := make(map[string]*deploymentspb.WorkerDeploymentVersionData)
		for i := 0; i < retainedVersions; i++ {
			versions[fmt.Sprintf("build-%03d", i)] = newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED)
		}
		versions[initialCurrentBuildID].Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT
		active.PerType[int32(taskType)] = taskQueueUserDataWithDeployment(nil, taskType, "deployment", &persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(1, initialCurrentBuildID),
			Versions:      versions,
		}).PerType[int32(taskType)]
	}
	taskQueue := testvars.New(s.T()).TaskQueue().Name
	s.seedTaskQueueUserData(taskQueue, common.CloneProto(active))

	for nextBuild := retainedVersions; nextBuild < retainedVersions+rollouts; nextBuild++ {
		active = common.CloneProto(active)
		active.Clock.WallClock++
		for _, perType := range active.PerType {
			deployment := perType.DeploymentData.DeploymentsData["deployment"]
			previous := deployment.Versions[fmt.Sprintf("build-%03d", nextBuild-1)]
			previous.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
			previous.RevisionNumber++
			delete(deployment.Versions, fmt.Sprintf("build-%03d", nextBuild-retainedVersions))
			buildID := fmt.Sprintf("build-%03d", nextBuild)
			deployment.Versions[buildID] = newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
			deployment.RoutingConfig = newTestRoutingConfig(int64(nextBuild+1), buildID)
		}

		got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, active)

		protorequire.ProtoEqual(s.T(), active, got)
	}
}

//nolint:staticcheck // SA1019 verifies that deprecated deployment fields are selected by the parent clock.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventImportsOnlyDeploymentDataForMissingType() {
	current := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"workflow-deployment",
		&persistencespb.WorkerDeploymentData{},
	)
	incoming := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		"activity-deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: &deploymentpb.RoutingConfig{RevisionNumber: 0},
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"activity-build": {RevisionNumber: 0},
			},
		},
	)
	activityData := incoming.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)]
	activityData.Config = &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"incoming": 1}}
	activityData.FairnessState = enumsspb.FAIRNESS_STATE_V2
	activityData.DeploymentData.Versions = []*deploymentspb.DeploymentVersionData{
		{Version: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "legacy", BuildId: "activity-build"}},
	}
	activityData.DeploymentData.UnversionedRampData = &deploymentspb.DeploymentVersionData{RampPercentage: 25}
	incoming.PerType[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)] = &persistencespb.TaskQueueTypeUserData{
		Config:        &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"nexus": 1}},
		FairnessState: enumsspb.FAIRNESS_STATE_V2,
	}

	taskQueue := uuid.NewString()
	s.seedTaskQueueUserData(taskQueue, current)
	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

	gotActivity := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)]
	s.Require().NotNil(gotActivity)
	s.Nil(gotActivity.GetConfig())
	s.Equal(enumsspb.FAIRNESS_STATE_UNSPECIFIED, gotActivity.GetFairnessState())
	s.Empty(gotActivity.GetDeploymentData().GetVersions())
	s.Nil(gotActivity.GetDeploymentData().GetUnversionedRampData())
	s.Contains(gotActivity.GetDeploymentData().GetDeploymentsData(), "activity-deployment")
	s.NotContains(got.GetPerType(), int32(enumspb.TASK_QUEUE_TYPE_NEXUS))
}

// Normal deletion must remove a forgotten version from the passive rather than retaining it in a union.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventNormalDeletionRemovesVersionFromPassive() {
	passive := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(5, "B"),
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
				"B": newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			},
		},
	)
	// Both replicas start with {A, B}; the active then forgets A and advances its clock.
	active := common.CloneProto(passive)
	active.Clock = &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1}
	delete(active.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"].Versions, "A")

	taskQueue := uuid.NewString()
	s.seedTaskQueueUserData(taskQueue, passive)
	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, active)

	protorequire.ProtoEqual(s.T(), active, got)
}

//nolint:staticcheck // SA1019 verifies that deprecated deployment fields are selected by the parent clock.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventUsesParentClockForNonDeploymentFields() {
	newUserData := func(
		clock *clockspb.HybridLogicalClock,
		label string,
		fairnessState enumsspb.FairnessState,
		markerClock int64,
	) *persistencespb.TaskQueueUserData {
		return &persistencespb.TaskQueueUserData{
			Clock: clock,
			VersioningData: &persistencespb.VersioningData{
				AssignmentRules: []*persistencespb.AssignmentRule{{
					CreateTimestamp: &clockspb.HybridLogicalClock{WallClock: markerClock, ClusterId: 1},
				}},
				RedirectRules: []*persistencespb.RedirectRule{{
					CreateTimestamp: &clockspb.HybridLogicalClock{WallClock: markerClock, ClusterId: 1},
				}},
			},
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					Config:        &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{label: 1}},
					FairnessState: fairnessState,
					DeploymentData: &persistencespb.DeploymentData{
						Versions: []*deploymentspb.DeploymentVersionData{{
							Version: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "legacy", BuildId: label},
						}},
						UnversionedRampData: &deploymentspb.DeploymentVersionData{RampPercentage: float32(markerClock)},
					},
				},
			},
		}
	}

	tests := []struct {
		name          string
		currentClock  *clockspb.HybridLogicalClock
		incomingClock *clockspb.HybridLogicalClock
		currentWins   bool
	}{
		{
			name:          "newer current clock",
			currentClock:  &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			incomingClock: &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
			currentWins:   true,
		},
		{
			name:          "newer incoming clock",
			currentClock:  &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
			incomingClock: &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		},
		{
			name:         "only current is clocked",
			currentClock: &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			currentWins:  true,
		},
		{
			name:          "only incoming is clocked",
			incomingClock: &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		},
		{
			name: "both are clockless",
		},
		{
			name:          "equal clocks use incoming fallback",
			currentClock:  &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			incomingClock: &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			current := newUserData(test.currentClock, "current", enumsspb.FAIRNESS_STATE_V1, 1)
			incoming := newUserData(test.incomingClock, "incoming", enumsspb.FAIRNESS_STATE_V2, 2)
			want := incoming
			if test.currentWins {
				want = current
			}
			taskQueue := uuid.NewString()
			s.seedTaskQueueUserData(taskQueue, current)

			got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

			protorequire.ProtoEqual(s.T(), want.GetClock(), got.GetClock())
			gotPerType := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)]
			wantPerType := want.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)]
			protorequire.ProtoEqual(s.T(), wantPerType.GetConfig(), gotPerType.GetConfig())
			s.Equal(wantPerType.GetFairnessState(), gotPerType.GetFairnessState())
			protorequire.ProtoElementsMatch(s.T(), wantPerType.GetDeploymentData().GetVersions(), gotPerType.GetDeploymentData().GetVersions())
			protorequire.ProtoEqual(s.T(), wantPerType.GetDeploymentData().GetUnversionedRampData(), gotPerType.GetDeploymentData().GetUnversionedRampData())
			protorequire.ProtoElementsMatch(s.T(), want.GetVersioningData().GetAssignmentRules(), got.GetVersioningData().GetAssignmentRules())
			protorequire.ProtoElementsMatch(s.T(), want.GetVersioningData().GetRedirectRules(), got.GetVersioningData().GetRedirectRules())
		})
	}
}

func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventResolvesEqualRevisionConflictByParentClock() {
	const conflictLogMessage = "found conflicting user data with same revision number during replication"
	captureHandler := s.captureDroppedOnEngine()
	tests := []struct {
		name           string
		currentClock   *clockspb.HybridLogicalClock
		incomingClock  *clockspb.HybridLogicalClock
		wantStatus     enumspb.WorkerDeploymentVersionStatus
		wantSide       string
		wantResolution string
		wantNoConflict bool
	}{
		{
			name:           "newer current clock",
			currentClock:   &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			incomingClock:  &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
			wantStatus:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			wantSide:       "current",
			wantResolution: "current_parent_clock",
		},
		{
			name:           "newer incoming clock",
			currentClock:   &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
			incomingClock:  &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			wantStatus:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			wantSide:       "incoming",
			wantResolution: "incoming_parent_clock",
			wantNoConflict: true,
		},
		{
			name:           "only current is clocked",
			currentClock:   &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			wantStatus:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			wantSide:       "current",
			wantResolution: "current_parent_clock",
		},
		{
			name:           "only incoming is clocked",
			incomingClock:  &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			wantStatus:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			wantSide:       "incoming",
			wantResolution: "incoming_parent_clock",
		},
		{
			name:           "both are clockless",
			wantStatus:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			wantSide:       "incoming",
			wantResolution: "incoming_clock_fallback",
		},
		{
			name:           "equal clocks use incoming fallback",
			currentClock:   &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			incomingClock:  &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			wantStatus:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			wantSide:       "incoming",
			wantResolution: "incoming_clock_fallback",
			wantNoConflict: true,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			current := taskQueueUserDataWithDeployment(
				test.currentClock,
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				"deployment",
				&persistencespb.WorkerDeploymentData{
					Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
						"build": {RevisionNumber: 5, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
					},
				},
			)
			incoming := taskQueueUserDataWithDeployment(
				test.incomingClock,
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				"deployment",
				&persistencespb.WorkerDeploymentData{
					Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
						"build": {RevisionNumber: 5, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING},
					},
				},
			)
			taskQueue := uuid.NewString()
			s.seedTaskQueueUserData(taskQueue, current)
			metricCapture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(metricCapture)
			logCapture := s.logger.StartCapture()
			defer s.logger.StopCapture(logCapture)

			got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

			gotVersion := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["deployment"].GetVersions()["build"]
			s.Equal(test.wantStatus, gotVersion.GetStatus())
			recordings := metricCapture.SnapshotMetric(metrics.TaskQueueUserDataReplicationEqualRevisionConflicts.Name())
			if test.wantNoConflict {
				s.Require().Empty(recordings)
				return
			}
			s.Require().Len(recordings, 1)
			s.Equal(int64(1), recordings[0].Value)
			s.Len(recordings[0].Tags, 4)
			s.Equal(s.ns.Name().String(), recordings[0].Tags["namespace"])
			s.Equal(enumspb.TASK_QUEUE_TYPE_WORKFLOW.String(), recordings[0].Tags[metrics.TaskTypeTagName])
			s.Equal("version_data", recordings[0].Tags["conflict_type"])
			s.Equal(test.wantResolution, recordings[0].Tags["resolution"])
			logCapture.RequireContains(s.T(), testlogger.CapturedLogPattern{
				Level:   testlogger.Warn,
				Message: conflictLogMessage,
				Tags: map[string]any{
					"wf-namespace":    s.ns.Name().String(),
					"wf-namespace-id": s.ns.ID().String(),
					"task-queue-name": taskQueue,
					"task-queue-type": enumspb.TASK_QUEUE_TYPE_WORKFLOW.String(),
					"deployment":      "deployment",
					"build-id":        "build",
					"conflict-type":   "version_data",
					"revision":        int64(5),
					"selected-side":   test.wantSide,
					"resolution":      test.wantResolution,
					"current-clock":   testlogger.AnyTagValue,
					"incoming-clock":  testlogger.AnyTagValue,
				},
			})
		})
	}

	s.Run("routing config conflict", func() {
		currentRouting := &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "current"},
			RevisionNumber:           5,
		}
		incomingRouting := &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "incoming"},
			RevisionNumber:           5,
		}
		current := taskQueueUserDataWithDeployment(
			&clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			"deployment",
			&persistencespb.WorkerDeploymentData{RoutingConfig: currentRouting},
		)
		incoming := taskQueueUserDataWithDeployment(
			&clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			"deployment",
			&persistencespb.WorkerDeploymentData{RoutingConfig: incomingRouting},
		)
		taskQueue := uuid.NewString()
		s.seedTaskQueueUserData(taskQueue, current)
		metricCapture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(metricCapture)
		logCapture := s.logger.StartCapture()
		defer s.logger.StopCapture(logCapture)

		got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

		gotRouting := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["deployment"].GetRoutingConfig()
		protorequire.ProtoEqual(s.T(), currentRouting, gotRouting)
		recordings := metricCapture.SnapshotMetric(metrics.TaskQueueUserDataReplicationEqualRevisionConflicts.Name())
		s.Require().Len(recordings, 1)
		s.Equal("routing_config", recordings[0].Tags["conflict_type"])
		s.Equal("current_parent_clock", recordings[0].Tags["resolution"])
		logCapture.RequireContains(s.T(), testlogger.CapturedLogPattern{
			Level:   testlogger.Warn,
			Message: conflictLogMessage,
			Tags: map[string]any{
				"deployment":     "deployment",
				"conflict-type":  "routing_config",
				"revision":       int64(5),
				"selected-side":  "current",
				"resolution":     "current_parent_clock",
				"current-clock":  testlogger.AnyTagValue,
				"incoming-clock": testlogger.AnyTagValue,
			},
		})
	})

	s.Run("identical values do not emit diagnostics", func() {
		currentDeployment := &persistencespb.WorkerDeploymentData{
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"},
				RevisionNumber:           5,
			},
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"build": {RevisionNumber: 5, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
			},
		}
		current := taskQueueUserDataWithDeployment(
			&clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			"deployment",
			currentDeployment,
		)
		incoming := taskQueueUserDataWithDeployment(
			&clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			"deployment",
			common.CloneProto(currentDeployment),
		)
		taskQueue := uuid.NewString()
		s.seedTaskQueueUserData(taskQueue, current)
		metricCapture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(metricCapture)
		logCapture := s.logger.StartCapture()
		defer s.logger.StopCapture(logCapture)

		s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

		s.Empty(metricCapture.SnapshotMetric(metrics.TaskQueueUserDataReplicationEqualRevisionConflicts.Name()))
		for _, record := range logCapture.Snapshot() {
			s.False(record.Level == testlogger.Warn && record.Message == conflictLogMessage)
		}
	})
}

func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventClonesMergedDeploymentData() {
	incomingRouting := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "shared", BuildId: "incoming"},
		RevisionNumber:           2,
	}
	incomingVersion := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 2,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}
	incomingOnly := &persistencespb.WorkerDeploymentData{
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"incoming-only": {RevisionNumber: 0},
		},
	}
	current := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"shared",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: &deploymentpb.RoutingConfig{RevisionNumber: 1},
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": {RevisionNumber: 1},
			},
		},
	)
	incoming := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"shared",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: incomingRouting,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": incomingVersion,
			},
		},
	)
	incomingDeployments := incoming.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()
	incomingDeployments["incoming-only"] = incomingOnly
	wantRouting := common.CloneProto(incomingRouting)
	wantVersion := common.CloneProto(incomingVersion)
	wantIncomingOnly := common.CloneProto(incomingOnly)

	taskQueue := uuid.NewString()
	s.seedTaskQueueUserData(taskQueue, current)
	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
	gotDeployments := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()
	gotShared := gotDeployments["shared"]

	s.NotSame(incomingRouting, gotShared.GetRoutingConfig())
	s.NotSame(incomingVersion, gotShared.GetVersions()["A"])
	s.NotSame(incomingOnly, gotDeployments["incoming-only"])
	incomingRouting.RevisionNumber++
	incomingVersion.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
	incomingOnly.Versions["mutated"] = &deploymentspb.WorkerDeploymentVersionData{}
	protorequire.ProtoEqual(s.T(), wantRouting, gotShared.GetRoutingConfig())
	protorequire.ProtoEqual(s.T(), wantVersion, gotShared.GetVersions()["A"])
	protorequire.ProtoEqual(s.T(), wantIncomingOnly, gotDeployments["incoming-only"])
}

func TestTaskQueueUserDataRevisionMergeConfig(t *testing.T) {
	t.Parallel()

	client := dynamicconfig.NewMemoryClient()
	config := NewConfig(dynamicconfig.NewCollection(client, log.NewNoopLogger()))
	require.True(t, config.EnableTaskQueueUserDataRevisionMerge("disabled"))
	t.Cleanup(client.OverrideSetting(dynamicconfig.MatchingEnableTaskQueueUserDataRevisionMerge, []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: "disabled"}, Value: false},
	}))
	require.False(t, config.EnableTaskQueueUserDataRevisionMerge("disabled"))
	require.True(t, config.EnableTaskQueueUserDataRevisionMerge("other"))
}

// Clockless compatibility, HLC tie-breakers, and disabling revision merging follow their explicit policies.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventMergeFlagAndClockCompatibility() {
	current := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "deployment", &persistencespb.WorkerDeploymentData{
		RoutingConfig: newTestRoutingConfig(5, "shared"),
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"shared":       newTestWorkerDeploymentVersionData(8, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			"current-only": newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
		},
	})
	current.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = &taskqueuepb.TaskQueueConfig{
		FairnessWeightOverrides: map[string]float32{"current": 1},
	}
	incoming := taskQueueUserDataWithDeployment(nil, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "deployment", &persistencespb.WorkerDeploymentData{
		RoutingConfig: newTestRoutingConfig(6, "shared"),
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"shared":        newTestWorkerDeploymentVersionData(7, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING),
			"incoming-only": newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE),
		},
	})
	incoming.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = &taskqueuepb.TaskQueueConfig{
		FairnessWeightOverrides: map[string]float32{"incoming": 1},
	}
	mergedCurrent := common.CloneProto(current)
	mergedCurrentDeployment := mergedCurrent.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"]
	mergedCurrentDeployment.RoutingConfig = newTestRoutingConfig(6, "shared")
	mergedCurrentDeployment.Versions["incoming-only"] = newTestWorkerDeploymentVersionData(0, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE)
	mergedIncoming := common.CloneProto(mergedCurrent)
	mergedIncoming.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config = common.CloneProto(incoming.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].Config)

	older := &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1}
	newer := &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1}
	laterCounter := &clockspb.HybridLogicalClock{WallClock: 10, Version: 1, ClusterId: 1}
	laterCluster := &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 2}
	tests := []struct {
		name          string
		enabled       bool
		currentClock  *clockspb.HybridLogicalClock
		incomingClock *clockspb.HybridLogicalClock
		want          *persistencespb.TaskQueueUserData
		wantClock     *clockspb.HybridLogicalClock
	}{
		{"merge_disabled_newer_incoming_replaces", false, older, newer, incoming, newer},
		{"merge_disabled_equal_clocks_replace", false, older, older, incoming, older},
		{"merge_disabled_older_incoming_is_discarded", false, newer, older, current, newer},
		{"merge_disabled_missing_current_clock", false, nil, newer, incoming, newer},
		{"merge_disabled_missing_incoming_clock", false, newer, nil, current, newer},
		{"merge_disabled_both_clocks_missing", false, nil, nil, incoming, nil},
		{"equal_clocks_replace", true, older, older, incoming, older},
		{"missing_current_clock_merges", true, nil, newer, mergedIncoming, newer},
		{"missing_incoming_clock_merges", true, newer, nil, mergedCurrent, newer},
		{"both_clocks_missing_merge", true, nil, nil, mergedIncoming, nil},
		{"newer_incoming_counter_replaces", true, older, laterCounter, incoming, laterCounter},
		{"older_incoming_counter_merges", true, laterCounter, older, mergedCurrent, laterCounter},
		{"newer_incoming_cluster_replaces", true, older, laterCluster, incoming, laterCluster},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			s.matchingEngine.config.EnableTaskQueueUserDataRevisionMerge = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(test.enabled)
			currentData := common.CloneProto(current)
			currentData.Clock = common.CloneProto(test.currentClock)
			incomingData := common.CloneProto(incoming)
			incomingData.Clock = common.CloneProto(test.incomingClock)
			want := common.CloneProto(test.want)
			want.Clock = common.CloneProto(test.wantClock)
			currentBefore := common.CloneProto(currentData)
			incomingBefore := common.CloneProto(incomingData)
			taskQueue := testvars.New(s.T()).TaskQueue().Name
			s.seedTaskQueueUserData(taskQueue, currentData)

			got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incomingData)

			protorequire.ProtoEqual(s.T(), want, got)
			protorequire.ProtoEqual(s.T(), currentBefore, currentData)
			protorequire.ProtoEqual(s.T(), incomingBefore, incomingData)
			incomingData.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"].Versions["incoming-only"].RevisionNumber++
			protorequire.ProtoEqual(s.T(), want, got)
		})
	}
}

// A fresh authoritative snapshot removes accumulated versions, whole deployments, and missing task queue types.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventDeletionAndForceReplicationClearAccumulatedVersions() {
	s.matchingEngine.config.EnableTaskQueueUserDataRevisionMerge = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	current := &persistencespb.TaskQueueUserData{
		Clock:   &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 1},
		PerType: make(map[int32]*persistencespb.TaskQueueTypeUserData),
	}
	for _, taskType := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
		versions := map[string]*deploymentspb.WorkerDeploymentVersionData{
			"current": newTestWorkerDeploymentVersionData(10, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
		}
		for i := 0; i < 606; i++ {
			versions[fmt.Sprintf("drained-%03d", i)] = newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED)
		}
		current.PerType[int32(taskType)] = taskQueueUserDataWithDeployment(nil, taskType, "deployment", &persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(20, "current"),
			Versions:      versions,
		}).PerType[int32(taskType)]
		current.PerType[int32(taskType)].DeploymentData.DeploymentsData["forgotten-deployment"] = &persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"forgotten": newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
			},
		}
	}
	current.PerType[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)] = &persistencespb.TaskQueueTypeUserData{}
	incoming := common.CloneProto(current)
	incoming.Clock = &clockspb.HybridLogicalClock{WallClock: 200, ClusterId: 1}
	delete(incoming.PerType, int32(enumspb.TASK_QUEUE_TYPE_NEXUS))
	for _, perType := range incoming.PerType {
		delete(perType.DeploymentData.DeploymentsData, "forgotten-deployment")
		versions := perType.DeploymentData.DeploymentsData["deployment"].Versions
		for i := 11; i < 606; i++ {
			delete(versions, fmt.Sprintf("drained-%03d", i))
		}
	}
	taskQueue := testvars.New(s.T()).TaskQueue().Name
	s.seedTaskQueueUserData(taskQueue, current)

	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
	protorequire.ProtoEqual(s.T(), incoming, got)
	got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
	protorequire.ProtoEqual(s.T(), incoming, got)

	// A delayed snapshot can restore absent entries without advancing the parent clock.
	got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, current)
	wantRestored := common.CloneProto(current)
	wantRestored.Clock = common.CloneProto(incoming.Clock)
	delete(wantRestored.PerType, int32(enumspb.TASK_QUEUE_TYPE_NEXUS))
	protorequire.ProtoEqual(s.T(), wantRestored, got)

	// Force replication of the same-clock authoritative snapshot clears those entries again.
	got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
	protorequire.ProtoEqual(s.T(), incoming, got)

	// Subsequent newer snapshots keep propagating version removals.
	incoming.Clock = &clockspb.HybridLogicalClock{WallClock: 300, ClusterId: 1}
	got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
	protorequire.ProtoEqual(s.T(), incoming, got)

	// Removing the remaining per-type data must also propagate an empty snapshot.
	empty := &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 400, ClusterId: 1}}
	got = s.applyTaskQueueUserDataReplicationEvent(taskQueue, empty)
	protorequire.ProtoEqual(s.T(), empty, got)
}

// Accepted limitation: a newer incoming parent clock wins even if its snapshot omits a valid target version.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventNewerClockSnapshotCanDiscardValidTargetVersion() {
	target := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(5, "B"),
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"B": newTestWorkerDeploymentVersionData(5, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			},
		},
	)
	// A delayed source snapshot can have a higher clock because of clock skew while still missing B.
	incoming := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 30, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			RoutingConfig: newTestRoutingConfig(4, "A"),
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"A": newTestWorkerDeploymentVersionData(4, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			},
		},
	)
	taskQueue := testvars.New(s.T()).TaskQueue().Name
	s.seedTaskQueueUserData(taskQueue, target)

	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

	protorequire.ProtoEqual(s.T(), incoming, got)
}

// Replacing deployment membership must not bypass legacy build ID merging or parent-clock selection of V2 rules.
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventNewerSnapshotPreservesLegacyVersioning() {
	s.matchingEngine.config.EnableTaskQueueUserDataRevisionMerge = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	currentClock := &clockspb.HybridLogicalClock{WallClock: 10, ClusterId: 1}
	incomingClock := &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 1}
	current := taskQueueUserDataWithDeployment(currentClock, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "forgotten", &persistencespb.WorkerDeploymentData{
		Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"forgotten": newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
		},
	})
	current.VersioningData = &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{mkNewSet("old-v1-build", currentClock)},
		AssignmentRules: []*persistencespb.AssignmentRule{
			mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("old-v2-target"), currentClock, nil),
		},
	}
	incoming := &persistencespb.TaskQueueUserData{
		Clock: incomingClock,
		VersioningData: &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{mkNewSet("new-v1-build", incomingClock)},
			AssignmentRules: []*persistencespb.AssignmentRule{
				mkAssignmentRulePersistence(mkAssignmentRuleWithoutRamp("new-v2-target"), incomingClock, nil),
			},
			RedirectRules: []*persistencespb.RedirectRule{
				mkRedirectRulePersistence(mkRedirectRule("source", "new-v2-target"), incomingClock, nil),
			},
		},
	}
	want := common.CloneProto(incoming)
	want.VersioningData.VersionSets = []*persistencespb.CompatibleVersionSet{
		mkNewSet("old-v1-build", currentClock),
		mkNewSet("new-v1-build", incomingClock),
	}
	s.mockVisibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{}, nil)
	taskQueue := testvars.New(s.T()).TaskQueue().Name
	s.seedTaskQueueUserData(taskQueue, current)

	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

	protorequire.ProtoEqual(s.T(), want, got)
}
