package matching

import (
	"github.com/google/uuid"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
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

func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventUnionsDeploymentDataByRevision() {
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

//nolint:staticcheck // SA1019 verifies that deprecated deployment fields remain snapshot-scoped
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

func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventRetainsEntryMissingFromNewerSnapshot() {
	currentVersion := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 5,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	}
	current := taskQueueUserDataWithDeployment(
		&clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 1},
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		"deployment",
		&persistencespb.WorkerDeploymentData{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{"A": currentVersion},
		},
	)
	incomingClock := &clockspb.HybridLogicalClock{WallClock: 200, ClusterId: 1}
	incomingConfig := &taskqueuepb.TaskQueueConfig{FairnessWeightOverrides: map[string]float32{"newer": 1}}
	incoming := &persistencespb.TaskQueueUserData{
		Clock: incomingClock,
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {Config: incomingConfig},
		},
	}

	taskQueue := uuid.NewString()
	s.seedTaskQueueUserData(taskQueue, current)
	got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)

	protorequire.ProtoEqual(s.T(), incomingClock, got.GetClock())
	protorequire.ProtoEqual(s.T(), incomingConfig, got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetConfig())
	gotDeployment := got.GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["deployment"]
	s.Require().NotNil(gotDeployment)
	protorequire.ProtoEqual(s.T(), currentVersion, gotDeployment.GetVersions()["A"])
}

//nolint:staticcheck // SA1019 verifies that deprecated deployment fields remain snapshot-scoped
func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventUsesParentClockForSnapshotFields() {
	snapshot := func(
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
			current := snapshot(test.currentClock, "current", enumsspb.FAIRNESS_STATE_V1, 1)
			incoming := snapshot(test.incomingClock, "incoming", enumsspb.FAIRNESS_STATE_V2, 2)
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
	const conflictLogMessage = "task queue user data replication encountered an equal-revision conflict"
	captureHandler := s.captureDroppedOnEngine()
	tests := []struct {
		name           string
		currentClock   *clockspb.HybridLogicalClock
		incomingClock  *clockspb.HybridLogicalClock
		wantStatus     enumspb.WorkerDeploymentVersionStatus
		wantSide       string
		wantResolution string
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
					"wf-namespace":       s.ns.Name().String(),
					"wf-namespace-id":    s.ns.ID().String(),
					"wf-task-queue-name": taskQueue,
					"wf-task-queue-type": enumspb.TASK_QUEUE_TYPE_WORKFLOW.String(),
					"deployment":         "deployment",
					"build-id":           "build",
					"conflict-type":      "version_data",
					"revision":           int64(5),
					"selected-side":      test.wantSide,
					"resolution":         test.wantResolution,
					"current-clock":      testlogger.AnyTagValue,
					"incoming-clock":     testlogger.AnyTagValue,
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
