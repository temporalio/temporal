package matching

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	namespaceID        = "ns-id"
	namespaceName      = "ns-name"
	taskQueueName      = "my-test-tq"
	defaultPriorityTag = "3" // MatchingTaskPriorityTag(DefaultPriorityKey) with PriorityLevels=5
)

type PartitionManagerTestSuite struct {
	suite.Suite
	protorequire.ProtoAssertions

	newMatcher     bool
	fairness       bool
	controller     *gomock.Controller
	userDataMgr    *mockUserDataManager
	partitionMgr   *taskQueuePartitionManagerImpl
	matchingClient *matchingservicemock.MockMatchingServiceClient
	ns             *namespace.Namespace
}

// TODO(pri): cleanup; delete this
func TestTaskQueuePartitionManager_Classic_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &PartitionManagerTestSuite{newMatcher: false})
}

func TestTaskQueuePartitionManager_Pri_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &PartitionManagerTestSuite{newMatcher: true})
}

func TestTaskQueuePartitionManager_Fair_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &PartitionManagerTestSuite{newMatcher: true, fairness: true})
}

func (s *PartitionManagerTestSuite) SetupTest() {
	s.ProtoAssertions = protorequire.New(s.T())
	s.controller = gomock.NewController(s.T())
	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)

	ns, registry := createMockNamespaceCache(s.controller, namespace.Name(namespaceName))
	s.ns = ns
	config := defaultTestConfig()
	if s.fairness {
		useFairness(config)
	} else if s.newMatcher {
		useNewMatcher(config)
	}

	s.matchingClient = matchingservicemock.NewMockMatchingServiceClient(s.controller)
	engine := createTestMatchingEngine(logger, s.controller, config, s.matchingClient, registry)

	f, err := tqid.NewTaskQueueFamily(namespaceID, taskQueueName)
	s.NoError(err)
	partition := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()
	tqConfig := newTaskQueueConfig(partition.TaskQueue(), engine.config, ns.Name())
	s.userDataMgr = &mockUserDataManager{}

	pm, err := newTaskQueuePartitionManager(engine, ns, partition, tqConfig, logger, logger, metrics.NoopMetricsHandler, s.userDataMgr)
	s.NoError(err)
	s.partitionMgr = pm
	engine.Start()
	pm.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = pm.WaitUntilInitialized(ctx)
	s.NoError(err)
}

func (s *PartitionManagerTestSuite) TestAddTask_Forwarded() {
	_, _, err := s.partitionMgr.AddTask(context.Background(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  "wf",
		},
		forwardInfo: &taskqueuespb.TaskForwardInfo{SourcePartition: "another-partition"},
	})
	s.Equal(errRemoteSyncMatchFailed, err)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_NoVersionDirective() {
	s.validateAddTask("", false, nil, nil)
	s.validatePollTask("", false)

	// a poller with non-empty build ID should go to unversioned queue when useVersioning=false
	s.validateAddTask("", false, nil, nil)
	s.validatePollTask("buildXYZ", false)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_AssignedTask() {
	bld := "buildXYZ"
	s.validateAddTask("", false, nil, worker_versioning.MakeBuildIdDirective(bld))
	s.validatePollTask(bld, true)
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_MultipleBuildIds() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// adding multiple tasks to queues with different buildIds
	bld1 := "build1"
	bld2 := "build2"
	s.validateAddTask("", false, nil, worker_versioning.MakeBuildIdDirective(bld1))
	s.validateAddTask("", false, nil, worker_versioning.MakeBuildIdDirective(bld2))
	buildIds := make(map[string]bool)
	buildIds[bld1] = true
	buildIds[bld2] = true

	// validating TQ Stats
	resp, err := s.partitionMgr.Describe(ctx, buildIds, false, true, true, false)
	s.NoError(err)
	s.Equal(2, len(resp.VersionsInfoInternal))

	// validate PhysicalTaskQueueInfo structures
	info1 := resp.VersionsInfoInternal[bld1].GetPhysicalTaskQueueInfo()
	s.NotNil(info1)
	info2 := resp.VersionsInfoInternal[bld2].GetPhysicalTaskQueueInfo()
	s.NotNil(info2)
	// check rate manually
	s.Greater(info1.TaskQueueStats.TasksAddRate, float32(0))
	s.Greater(info2.TaskQueueStats.TasksAddRate, float32(0))
	// reset so we can compare the rest exactly
	info1.TaskQueueStats.TasksAddRate = 0
	info1.TaskQueueStatsByPriorityKey[3].TasksAddRate = 0
	info2.TaskQueueStats.TasksAddRate = 0
	info2.TaskQueueStatsByPriorityKey[3].TasksAddRate = 0

	expectedPhysicalTQInfo := &taskqueuespb.PhysicalTaskQueueInfo{
		Pollers: nil, // no pollers polling
		TaskQueueStats: &taskqueuepb.TaskQueueStats{
			ApproximateBacklogAge:   durationpb.New(0),
			ApproximateBacklogCount: 1,
		},
		TaskQueueStatsByPriorityKey: map[int32]*taskqueuepb.TaskQueueStats{
			3: &taskqueuepb.TaskQueueStats{
				ApproximateBacklogAge:   durationpb.New(0),
				ApproximateBacklogCount: 1,
			},
		},
	}
	s.ProtoEqual(expectedPhysicalTQInfo, resp.VersionsInfoInternal[bld1].PhysicalTaskQueueInfo)
	s.ProtoEqual(expectedPhysicalTQInfo, resp.VersionsInfoInternal[bld2].PhysicalTaskQueueInfo)

	// adding pollers
	s.validatePollTask(bld1, true)
	s.validatePollTask(bld2, true)

	// fresher call of the describe API
	resp, err = s.partitionMgr.Describe(ctx, buildIds, false, true, true, true)
	s.NoError(err)

	// validate TQ internal statistics (not exposed via public API)
	var status0 *taskqueuespb.InternalTaskQueueStatus
	if s.fairness {
		status0 = &taskqueuespb.InternalTaskQueueStatus{
			FairReadLevel:           fairLevel{pass: 1000, id: 1}.toProto(),
			FairAckLevel:            fairLevel{}.toProto(),
			TaskIdBlock:             &taskqueuepb.TaskIdBlock{StartId: 2, EndId: 100000},
			LoadedTasks:             1,
			FairMaxReadLevel:        fairLevel{pass: 1000, id: 1}.toProto(),
			ApproximateBacklogCount: 1,
		}
	} else {
		status0 = &taskqueuespb.InternalTaskQueueStatus{
			ReadLevel:               1,
			AckLevel:                0,
			TaskIdBlock:             &taskqueuepb.TaskIdBlock{StartId: 2, EndId: 100000},
			LoadedTasks:             1,
			MaxReadLevel:            1,
			ApproximateBacklogCount: 1,
		}
	}

	status1 := resp.VersionsInfoInternal[bld1].PhysicalTaskQueueInfo.GetInternalTaskQueueStatus()
	s.Equal(1, len(status1))
	s.ProtoEqual(status0, status1[0])
	status2 := resp.VersionsInfoInternal[bld2].PhysicalTaskQueueInfo.GetInternalTaskQueueStatus()
	s.Equal(1, len(status2))
	s.ProtoEqual(status0, status2[0])
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_UnloadedVersionedQueues() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// adding a task to a versioned queue
	bld := "buildXYZ"
	s.validateAddTask("", false, nil, worker_versioning.MakeBuildIdDirective(bld))
	buildIds := make(map[string]bool)
	buildIds[bld] = true

	// task is backlogged in the source queue so it is loaded by now
	sourceQ, err := s.partitionMgr.getVersionedQueue(ctx, "", bld, nil, false)
	s.NoError(err)
	s.NotNil(sourceQ)

	// unload sourceQ
	s.partitionMgr.unloadPhysicalQueue(sourceQ, unloadCauseUnspecified)

	s.describeStatsEventually(buildIds, false, false, false, func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool {
		// 1 task in the backlog
		stats := resp.VersionsInfoInternal[bld].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		return stats != nil && stats.GetApproximateBacklogCount() == 1
	})
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_CurrentAndRampingSplitDefaultBacklog() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const (
		deploymentName = "foo"
		currentBuildID = "A"
		rampingBuildID = "B"
		rampPct        = float32(30)
	)

	// Seed user data with deployment routing config that marks current/ramping.
	s.userDataMgr.data = &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							deploymentName: {
								RoutingConfig: &deploymentpb.RoutingConfig{
									CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
										DeploymentName: deploymentName,
										BuildId:        currentBuildID,
									},
									RampingDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
										DeploymentName: deploymentName,
										BuildId:        rampingBuildID,
									},
									RampingVersionPercentage:            rampPct,
									RampingVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
									RampingVersionPercentageChangedTime: timestamppb.New(time.Now().Add(-1 * time.Hour)),
									CurrentVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
								},
								Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
									currentBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
									rampingBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
								},
							},
						},
					},
				},
			},
		},
	}

	err := s.partitionMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	dQueue := s.partitionMgr.defaultQueue()
	// Backlog 10 tasks in the unversioned/default queue.
	for i := 0; i < 10; i++ {
		err := dQueue.SpoolTask(&persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  fmt.Sprintf("wf-%d", i),
		})
		s.Require().NoError(err)
	}

	currentQ, err := s.partitionMgr.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    currentBuildID,
	}, true)
	s.NoError(err)

	// Make this a pinned task so that it goes to the current versioned queue.
	err = currentQ.SpoolTask(&persistencespb.TaskInfo{
		NamespaceId: namespaceID,
		RunId:       "run",
		WorkflowId:  "wf-current-extra",
		VersionDirective: &taskqueuespb.TaskVersionDirective{
			Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
			DeploymentVersion: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        currentBuildID,
			},
			RevisionNumber: 1,
		},
	})
	s.Require().NoError(err)

	buildIds := map[string]bool{
		worker_versioning.BuildIDToStringV32(deploymentName, currentBuildID): true,
		worker_versioning.BuildIDToStringV32(deploymentName, rampingBuildID): true,
		"": true, // also request unversioned stats; it should have the attributed portion removed
	}

	currentKey := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
	)
	rampingKey := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: rampingBuildID},
	)

	s.describeStatsEventually(buildIds, false, false, false, func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool {
		currentStats := resp.VersionsInfoInternal[currentKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		rampingStats := resp.VersionsInfoInternal[rampingKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		unversionedStats := resp.VersionsInfoInternal[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats()

		// 7 unversioned tasks + 1 pinned task
		if currentStats.GetApproximateBacklogCount() != 8 {
			return false
		}
		// 3 unversioned tasks
		if rampingStats.GetApproximateBacklogCount() != 3 {
			return false
		}
		// 0 unversioned tasks after subtraction
		return unversionedStats.GetApproximateBacklogCount() == 0
	})
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_OneUnversionedTask_OverAttributesToCurrentAndRamping() {
	const (
		deploymentName = "foo"
		currentBuildID = "A"
		rampingBuildID = "B"
		rampPct        = float32(30)
	)

	// Seed user data with deployment routing config that marks current/ramping.
	s.userDataMgr.data = &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							deploymentName: {
								RoutingConfig: &deploymentpb.RoutingConfig{
									CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
										DeploymentName: deploymentName,
										BuildId:        currentBuildID,
									},
									RampingDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
										DeploymentName: deploymentName,
										BuildId:        rampingBuildID,
									},
									RampingVersionPercentage:            rampPct,
									RampingVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
									RampingVersionPercentageChangedTime: timestamppb.New(time.Now().Add(-1 * time.Hour)),
									CurrentVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
								},
								Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
									currentBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
									rampingBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := s.partitionMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	// Backlog exactly 1 task in the unversioned/default queue.
	err = s.partitionMgr.defaultQueue().SpoolTask(&persistencespb.TaskInfo{
		NamespaceId: namespaceID,
		RunId:       "run",
		WorkflowId:  "wf-single",
	})
	s.Require().NoError(err)

	buildIds := map[string]bool{
		worker_versioning.BuildIDToStringV32(deploymentName, currentBuildID): true,
		worker_versioning.BuildIDToStringV32(deploymentName, rampingBuildID): true,
		"": true, // also request unversioned stats; it should have the attributed portion removed
	}

	currentKey := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
	)
	rampingKey := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: rampingBuildID},
	)

	s.describeStatsEventually(buildIds, false, false, false, func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool {
		currentStats := resp.VersionsInfoInternal[currentKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		rampingStats := resp.VersionsInfoInternal[rampingKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		unversionedStats := resp.VersionsInfoInternal[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats()

		return currentStats.GetApproximateBacklogCount() == 1 &&
			rampingStats.GetApproximateBacklogCount() == 1 &&
			unversionedStats.GetApproximateBacklogCount() == 0
	})
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_OnlyCurrentNoRampingTakesAllUnversionedBacklog() {
	const (
		deploymentName = "foo"
		currentBuildID = "A"
	)

	// Seed user data with deployment routing config that marks only current (no ramping).
	s.userDataMgr.data = &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							deploymentName: {
								RoutingConfig: &deploymentpb.RoutingConfig{
									CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
										DeploymentName: deploymentName,
										BuildId:        currentBuildID,
									},
									RampingDeploymentVersion:            nil,
									RampingVersionPercentage:            0,
									CurrentVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
									RampingVersionPercentageChangedTime: timestamppb.New(time.Now().Add(-1 * time.Hour)),
								},
								Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
									currentBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := s.partitionMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	dQueue := s.partitionMgr.defaultQueue()
	// Backlog 10 tasks in the unversioned/default queue.
	for i := 0; i < 10; i++ {
		err = dQueue.SpoolTask(&persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  fmt.Sprintf("wf-only-current-%d", i),
		})
		s.Require().NoError(err)
	}

	buildIds := map[string]bool{
		worker_versioning.BuildIDToStringV32(deploymentName, currentBuildID): true,
		"": true,
	}

	currentKey := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
	)

	s.describeStatsEventually(buildIds, false, false, false, func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool {
		currentStats := resp.VersionsInfoInternal[currentKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		unversionedStats := resp.VersionsInfoInternal[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		if currentStats == nil || unversionedStats == nil {
			return false
		}
		// With only current (no ramping), all unversioned backlog is attributed to current.
		return currentStats.GetApproximateBacklogCount() == 10 && unversionedStats.GetApproximateBacklogCount() == 0
	})
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_OnlyRampingNoCurrentSplitsUnversionedBacklog() {
	const (
		deploymentName = "foo"
		rampingBuildID = "B"
		rampPct        = float32(30)
	)

	// Seed user data with deployment routing config that marks only ramping (no current).
	s.userDataMgr.data = &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							deploymentName: {
								RoutingConfig: &deploymentpb.RoutingConfig{
									CurrentDeploymentVersion: nil,
									RampingDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
										DeploymentName: deploymentName,
										BuildId:        rampingBuildID,
									},
									RampingVersionPercentage:            rampPct,
									CurrentVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
									RampingVersionChangedTime:           timestamppb.New(time.Now().Add(-1 * time.Hour)),
									RampingVersionPercentageChangedTime: timestamppb.New(time.Now().Add(-1 * time.Hour)),
								},
								Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
									rampingBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := s.partitionMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	dQueue := s.partitionMgr.defaultQueue()
	// Backlog 10 tasks in the unversioned/default queue.
	for i := 0; i < 10; i++ {
		err = dQueue.SpoolTask(&persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  fmt.Sprintf("wf-only-ramp-%d", i),
		})
		s.Require().NoError(err)
	}

	buildIds := map[string]bool{
		worker_versioning.BuildIDToStringV32(deploymentName, rampingBuildID): true,
		"": true,
	}

	rampingKey := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: rampingBuildID},
	)

	s.describeStatsEventually(buildIds, false, false, false, func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool {
		rampingStats := resp.VersionsInfoInternal[rampingKey].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		unversionedStats := resp.VersionsInfoInternal[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		return rampingStats.GetApproximateBacklogCount() == 3 && unversionedStats.GetApproximateBacklogCount() == 7
	})
}

func (s *PartitionManagerTestSuite) TestDescribeTaskQueuePartition_UnversionedDoesNotDoubleCount() {
	// Current is unversioned (no current/ramping deployment versions set).
	s.userDataMgr.data = &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							"foo": {
								RoutingConfig: &deploymentpb.RoutingConfig{
									CurrentDeploymentVersion: nil,
									RampingDeploymentVersion: nil,
									RampingVersionPercentage: 0,
								},
							},
						},
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := s.partitionMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	dQueue := s.partitionMgr.defaultQueue()
	// Backlog 5 tasks in the unversioned/default queue.
	for i := 0; i < 5; i++ {
		err = dQueue.SpoolTask(&persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  fmt.Sprintf("wf-uv-%d", i),
		})
		s.Require().NoError(err)
	}

	s.describeStatsEventually(map[string]bool{"": true}, false, false, false, func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool {
		uvStats := resp.VersionsInfoInternal[""].GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		return uvStats != nil && uvStats.GetApproximateBacklogCount() == 5
	})
}

// latestLogicalBacklogCount returns the most recent approximate_backlog_count
// recording for the given worker_version and task_priority tag values.
func latestLogicalBacklogCount(snap map[string][]*metricstest.CapturedRecording, workerVersion, priorityTag string) (float64, bool) {
	var latest float64
	found := false
	for _, rec := range snap[metrics.ApproximateBacklogCount.Name()] {
		if rec.Tags["worker_version"] == workerVersion && rec.Tags[metrics.TaskPriorityTagName] == priorityTag {
			latest = rec.Value.(float64)
			found = true
		}
	}
	return latest, found
}

// addRoutingConfigUserData sets up deployment user data with a current version and an optional
// ramping version. Pass "" for rampingBuildID to omit ramping. Note, this only adds the routing config
// for the Workflow Task Queue Type.
func (s *PartitionManagerTestSuite) addRoutingConfigUserData(deploymentName, currentBuildID, rampingBuildID string, rampPct float32) {
	routingConfig := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        currentBuildID,
		},
		CurrentVersionChangedTime: timestamppb.New(time.Now().Add(-1 * time.Hour)),
	}
	versions := map[string]*deploymentspb.WorkerDeploymentVersionData{
		currentBuildID: {RevisionNumber: 1, UpdateTime: timestamppb.Now()},
	}
	if rampingBuildID != "" {
		routingConfig.RampingDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        rampingBuildID,
		}
		routingConfig.RampingVersionPercentage = rampPct
		routingConfig.RampingVersionChangedTime = timestamppb.New(time.Now().Add(-1 * time.Hour))
		routingConfig.RampingVersionPercentageChangedTime = timestamppb.New(time.Now().Add(-1 * time.Hour))
		versions[rampingBuildID] = &deploymentspb.WorkerDeploymentVersionData{RevisionNumber: 1, UpdateTime: timestamppb.Now()}
	}
	s.userDataMgr.data = &persistencespb.VersionedTaskQueueUserData{
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							deploymentName: {
								RoutingConfig: routingConfig,
								Versions:      versions,
							},
						},
					},
				},
			},
		},
	}
}

// spoolDefaultTasks spools n tasks to the partition manager's default queue.
func (s *PartitionManagerTestSuite) spoolDefaultTasks(pm *taskQueuePartitionManagerImpl, n int) {
	dQueue := pm.defaultQueue()
	for i := 0; i < n; i++ {
		err := dQueue.SpoolTask(&persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  fmt.Sprintf("wf-%d", i),
		})
		s.Require().NoError(err)
	}
}

func (s *PartitionManagerTestSuite) TestLogicalBacklogMetrics_NoVersioning() {
	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime: 1 * time.Minute,
	})
	defer cleanup()

	s.spoolDefaultTasks(pm, 5)

	// Wait for backlog stats to stabilize, then emit logical metrics.
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		pm.fetchAndEmitLogicalBacklogMetrics(ctx)

		snap := capture.Snapshot()
		count, ok := latestLogicalBacklogCount(snap, "__unversioned__", defaultPriorityTag)
		return ok && count == float64(5)
	}, 2*time.Second, 50*time.Millisecond)
}

func (s *PartitionManagerTestSuite) TestLogicalBacklogMetrics_CurrentOnly() {
	const (
		deploymentName = "foo"
		currentBuildID = "A"
	)

	s.addRoutingConfigUserData(deploymentName, currentBuildID, "", 0)

	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime: 1 * time.Minute,
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.spoolDefaultTasks(pm, 10)

	// Create versioned queue and spool 2 pinned tasks.
	currentQ, err := pm.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    currentBuildID,
	}, true)
	s.Require().NoError(err)
	for i := 0; i < 2; i++ {
		err = currentQ.SpoolTask(&persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  fmt.Sprintf("wf-pinned-%d", i),
			VersionDirective: &taskqueuespb.TaskVersionDirective{
				Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
				DeploymentVersion: &deploymentspb.WorkerDeploymentVersion{
					DeploymentName: deploymentName,
					BuildId:        currentBuildID,
				},
				RevisionNumber: 1,
			},
		})
		s.Require().NoError(err)
	}

	currentVersionTag := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
	)

	// Wait for backlog stats to stabilize, then emit logical metrics.
	s.Require().Eventually(func() bool {
		pm.fetchAndEmitLogicalBacklogMetrics(ctx)
		snap := capture.Snapshot()

		unvCount, unvOk := latestLogicalBacklogCount(snap, "__unversioned__", defaultPriorityTag)
		curCount, curOk := latestLogicalBacklogCount(snap, currentVersionTag, defaultPriorityTag)
		return unvOk && unvCount == float64(0) &&
			curOk && curCount == float64(12)
	}, 2*time.Second, 50*time.Millisecond)
}

func (s *PartitionManagerTestSuite) TestLogicalBacklogMetrics_CurrentAndRamping() {
	const (
		deploymentName = "foo"
		currentBuildID = "A"
		rampingBuildID = "B"
		rampPct        = float32(30)
	)

	s.addRoutingConfigUserData(deploymentName, currentBuildID, rampingBuildID, rampPct)

	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime: 1 * time.Minute,
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.spoolDefaultTasks(pm, 10)

	// Create versioned queues so includeAllActive picks them up.
	_, err := pm.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    currentBuildID,
	}, true)
	s.Require().NoError(err)
	_, err = pm.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    rampingBuildID,
	}, true)
	s.Require().NoError(err)

	currentVersionTag := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
	)
	rampingVersionTag := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: rampingBuildID},
	)

	// ceil(10 * 70/100) = 7 for current, ceil(10 * 30/100) = 3 for ramping
	s.Require().Eventually(func() bool {
		iterCtx, iterCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer iterCancel()
		pm.fetchAndEmitLogicalBacklogMetrics(iterCtx)
		snap := capture.Snapshot()

		unvCount, unvOk := latestLogicalBacklogCount(snap, "__unversioned__", defaultPriorityTag)
		curCount, curOk := latestLogicalBacklogCount(snap, currentVersionTag, defaultPriorityTag)
		rmpCount, rmpOk := latestLogicalBacklogCount(snap, rampingVersionTag, defaultPriorityTag)
		return unvOk && unvCount == float64(0) &&
			curOk && curCount == float64(7) &&
			rmpOk && rmpCount == float64(3)
	}, 2*time.Second, 50*time.Millisecond)
}

func (s *PartitionManagerTestSuite) TestLogicalBacklogMetrics_SmallBacklog() {
	const (
		deploymentName = "foo"
		currentBuildID = "A"
		rampingBuildID = "B"
		rampPct        = float32(30)
	)

	s.addRoutingConfigUserData(deploymentName, currentBuildID, rampingBuildID, rampPct)

	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime: 1 * time.Minute,
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.spoolDefaultTasks(pm, 1)

	// Create versioned queues so includeAllActive picks them up.
	_, err := pm.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    currentBuildID,
	}, true)
	s.Require().NoError(err)
	_, err = pm.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    rampingBuildID,
	}, true)
	s.Require().NoError(err)

	currentVersionTag := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
	)
	rampingVersionTag := worker_versioning.ExternalWorkerDeploymentVersionToString(
		&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: rampingBuildID},
	)

	// ceil(1 * 70/100) = 1 for current, ceil(1 * 30/100) = 1 for ramping.
	// Over-attribution: both get 1, unversioned is max(0, 1-1-1) = 0.
	s.Require().Eventually(func() bool {
		iterCtx, iterCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer iterCancel()
		pm.fetchAndEmitLogicalBacklogMetrics(iterCtx)
		snap := capture.Snapshot()

		unvCount, unvOk := latestLogicalBacklogCount(snap, "__unversioned__", defaultPriorityTag)
		curCount, curOk := latestLogicalBacklogCount(snap, currentVersionTag, defaultPriorityTag)
		rmpCount, rmpOk := latestLogicalBacklogCount(snap, rampingVersionTag, defaultPriorityTag)
		return unvOk && unvCount == float64(0) &&
			curOk && curCount == float64(1) &&
			rmpOk && rmpCount == float64(1)
	}, 2*time.Second, 50*time.Millisecond)
}

func (s *PartitionManagerTestSuite) TestLogicalBacklogMetrics_BreakdownByBuildIDDisabled() {
	const (
		deploymentName = "foo"
		currentBuildID = "A"
	)

	s.addRoutingConfigUserData(deploymentName, currentBuildID, "", 0)

	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime: 1 * time.Minute,
	})
	defer cleanup()

	// Disable BreakdownMetricsByBuildID. When disabled, versioned entries would all share the
	// "__versioned__" tag, producing non-deterministic gauge values. The function should skip
	// versioned entries and only emit the unversioned metric.
	pm.config.BreakdownMetricsByBuildID = func() bool { return false }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.spoolDefaultTasks(pm, 5)

	// Create the versioned queue so it would appear in Describe output.
	_, err := pm.getVersionedQueue(ctx, "", "", &deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    currentBuildID,
	}, true)
	s.Require().NoError(err)

	// Wait for backlog stats to stabilize, then emit.
	s.Require().Eventually(func() bool {
		pm.fetchAndEmitLogicalBacklogMetrics(ctx)
		snap := capture.Snapshot()

		// Unversioned should still be emitted (with attributed count = 0 since current takes all).
		_, unvOk := latestLogicalBacklogCount(snap, "__unversioned__", defaultPriorityTag)
		if !unvOk {
			return false
		}

		// Versioned entry should NOT be emitted with the actual version tag.
		currentVersionTag := worker_versioning.ExternalWorkerDeploymentVersionToString(
			&deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: currentBuildID},
		)
		_, curOk := latestLogicalBacklogCount(snap, currentVersionTag, defaultPriorityTag)
		if curOk {
			return false // should not be present
		}

		// Also should not appear under the generic "__versioned__" tag, since we skip entirely.
		_, genericOk := latestLogicalBacklogCount(snap, "__versioned__", defaultPriorityTag)
		return !genericOk
	}, 2*time.Second, 50*time.Millisecond)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_UnassignedTask() {
	s.validateAddTask("", false, nil, worker_versioning.MakeUseAssignmentRulesDirective())
	s.validatePollTask("", false)
}

func (s *PartitionManagerTestSuite) TestPollWithRedirectRules() {
	source := "bld1"
	target := "bld2"
	versioningData := &persistencespb.VersioningData{
		RedirectRules: []*persistencespb.RedirectRule{
			{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: source,
					TargetBuildId: target,
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(source))

	s.validatePollTask(target, true)

	_, _, err := s.partitionMgr.PollTask(ctx, &pollMetadata{
		workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:       source,
			UseVersioning: true,
		},
	})
	s.Equal(serviceerror.NewNewerBuildExists(target), err)
}

func (s *PartitionManagerTestSuite) TestRedirectRuleLoadUpstream() {
	source := "bld1"
	target := "bld2"
	versioningData := &persistencespb.VersioningData{
		RedirectRules: []*persistencespb.RedirectRule{
			{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: source,
					TargetBuildId: target,
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(source))

	// task is backlogged in the source queue so it is loaded by now
	sourceQ, err := s.partitionMgr.getVersionedQueue(ctx, "", source, nil, false)
	s.NoError(err)
	s.NotNil(sourceQ)

	// unload sourceQ
	s.partitionMgr.unloadPhysicalQueue(sourceQ, unloadCauseUnspecified)

	// poll from target
	s.validatePollTask(target, true)

	// polling from target should've loaded the source as well
	sourceQ, err = s.partitionMgr.getVersionedQueue(ctx, "", source, nil, false)
	s.NoError(err)
	s.NotNil(sourceQ)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_NoVersionDirective() {
	buildId := "bld"
	versioningData := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(buildId)}}
	s.validateAddTask("", false, versioningData, nil)
	s.validatePollTask("", false)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_AssignedTask() {
	ruleBld := "rule-bld"
	versioningData := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(ruleBld)}}
	taskBld := "task-bld"
	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(taskBld))
	s.validatePollTask(taskBld, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_UnassignedTask() {
	ruleBld := "rule-bld"
	versioningData := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(ruleBld)}}
	s.validateAddTask(ruleBld, false, versioningData, worker_versioning.MakeUseAssignmentRulesDirective())
	s.validatePollTask(ruleBld, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_UnassignedTask_SyncMatch() {
	ruleBld := "rule-bld"
	versioningData := &persistencespb.VersioningData{AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(ruleBld)}}
	s.validatePollTaskSyncMatch(ruleBld, true)
	s.validateAddTask("", true, versioningData, worker_versioning.MakeUseAssignmentRulesDirective())
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_NoVersionDirective() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(ruleBld)},
		VersionSets:     []*persistencespb.CompatibleVersionSet{vs},
	}

	s.validateAddTask("", false, versioningData, nil)
	// make sure version set queue is not loaded
	s.Nil(s.partitionMgr.versionedQueues[PhysicalTaskQueueVersion{versionSet: vs.SetIds[0]}])
	s.validatePollTask("", false)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_AssignedTask() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(ruleBld)},
		VersionSets:     []*persistencespb.CompatibleVersionSet{vs},
	}

	taskBld := "task-bld"
	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(taskBld))
	// make sure version set queue is not loaded
	s.Nil(s.partitionMgr.versionedQueues[PhysicalTaskQueueVersion{versionSet: vs.SetIds[0]}])
	s.validatePollTask(taskBld, true)

	// now use the version set build ID
	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(vs.BuildIds[0].Id))
	// make sure version set queue is loaded
	s.NotNil(s.partitionMgr.versionedQueues[PhysicalTaskQueueVersion{versionSet: vs.SetIds[0]}])
	s.validatePollTask(vs.BuildIds[0].Id, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_UnassignedTask() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistencespb.VersioningData{
		AssignmentRules: []*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(ruleBld)},
		VersionSets:     []*persistencespb.CompatibleVersionSet{vs},
	}
	s.validateAddTask(ruleBld, false, versioningData, worker_versioning.MakeUseAssignmentRulesDirective())
	// make sure version set queue is not loaded
	s.Nil(s.partitionMgr.versionedQueues[PhysicalTaskQueueVersion{versionSet: vs.SetIds[0]}])
	s.validatePollTask(ruleBld, true)
}

func (s *PartitionManagerTestSuite) TestGetAllPollerInfo() {
	// no pollers
	pollers := s.partitionMgr.GetAllPollerInfo()
	s.True(len(pollers) == 0)

	// one unversioned poller
	s.pollWithIdentity("uv", "", false, false)
	pollers = s.partitionMgr.GetAllPollerInfo()
	s.True(len(pollers) == 1)

	// one versioned poller
	s.pollWithIdentity("v", "bid", true, false)
	pollers = s.partitionMgr.GetAllPollerInfo()
	s.True(len(pollers) == 2)

	// one unversioned poller with deployment options
	s.pollWithIdentity("uvdo", "bid", false, true)
	pollers = s.partitionMgr.GetAllPollerInfo()
	s.True(len(pollers) == 3)

	for _, p := range pollers {
		//nolint:staticcheck // SA1019 deprecated GetWorkerVersionCapabilities
		workerVersionCapabilities := p.GetWorkerVersionCapabilities()
		switch p.GetIdentity() {
		case "uv":
			s.False(workerVersionCapabilities.GetUseVersioning())
		case "v":
			s.True(workerVersionCapabilities.GetUseVersioning())
			s.Equal("bid", workerVersionCapabilities.GetBuildId())
		case "uvdo":
			s.NotNil(p.GetDeploymentOptions())
			s.Equal("bid", p.GetDeploymentOptions().GetBuildId())
		}
	}
}

func (s *PartitionManagerTestSuite) TestHasAnyPollerAfter() {
	// no pollers
	s.False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-5 * time.Minute)))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false, false)
	s.True(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))

	// one versioned poller
	s.pollWithIdentity("v", "bid", true, false)
	s.True(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))
}

func (s *PartitionManagerTestSuite) TestHasPollerAfter_Unversioned() {
	// no pollers
	s.False(s.partitionMgr.HasPollerAfter("", time.Now().Add(-5*time.Minute)))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false, false)
	s.True(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-500 * time.Microsecond)))
	s.True(s.partitionMgr.HasPollerAfter("", time.Now().Add(-500*time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.False(s.partitionMgr.HasPollerAfter("", time.Now().Add(-100*time.Microsecond)))

	// one versioned poller
	s.pollWithIdentity("v", "bid", true, false)
	s.False(s.partitionMgr.HasPollerAfter("", time.Now().Add(-100*time.Microsecond)))
}

func (s *PartitionManagerTestSuite) TestHasPollerAfter_Versioned() {
	// no pollers
	s.False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-5 * time.Minute)))

	// one version-set poller
	bid := "bid"
	s.pollWithIdentity("v", bid, true, false)
	s.True(s.partitionMgr.HasPollerAfter(bid, time.Now().Add(-100*time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.False(s.partitionMgr.HasPollerAfter(bid, time.Now().Add(-100*time.Microsecond)))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false, true)
	s.False(s.partitionMgr.HasPollerAfter(bid, time.Now().Add(-100*time.Microsecond)))
}

func (s *PartitionManagerTestSuite) TestLegacyDescribeTaskQueue() {
	// not testing TaskQueueStatus, as it is invalid right now and will be changed with the new LegacyDescribeTaskQueue API
	// no pollers
	resp, err := s.partitionMgr.LegacyDescribeTaskQueue(false)
	s.NoError(err)
	s.Equal(0, len(resp.DescResponse.GetPollers()))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false, false)
	resp, err = s.partitionMgr.LegacyDescribeTaskQueue(false)
	s.NoError(err)
	s.Equal(1, len(resp.DescResponse.GetPollers()))

	// one versioned poller
	s.pollWithIdentity("v", "bid", true, false)
	resp, err = s.partitionMgr.LegacyDescribeTaskQueue(false)
	s.NoError(err)
	s.Equal(2, len(resp.DescResponse.GetPollers()))

	for _, p := range resp.DescResponse.GetPollers() {
		//nolint:staticcheck // SA1019 deprecated GetWorkerVersionCapabilities
		workerVersionCapabilities := p.GetWorkerVersionCapabilities()
		switch p.GetIdentity() {
		case "uv":
			s.False(workerVersionCapabilities.GetUseVersioning())
		case "v":
			s.True(workerVersionCapabilities.GetUseVersioning())
			s.Equal("bid", workerVersionCapabilities.GetBuildId())
		}
	}
}

func (s *PartitionManagerTestSuite) TestAutoEnable() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.matchingClient.EXPECT().UpdateFairnessState(ctx, &matchingservice.UpdateFairnessStateRequest{
		NamespaceId:   s.ns.ID().String(),
		TaskQueue:     "my-test-tq",
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		FairnessState: enumsspb.FAIRNESS_STATE_V2,
	}).Times(1).Return(nil, nil)
	_, _, err := s.partitionMgr.AddTask(ctx, addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "run",
			WorkflowId:  "wf",
			Priority: &commonpb.Priority{
				PriorityKey: 3,
				FairnessKey: "myFairnessKey",
			},
		},
	})
	s.Require().NoError(err)
}

func (s *PartitionManagerTestSuite) validateAddTask(expectedBuildId string, expectedSyncMatch bool, versioningData *persistencespb.VersioningData, directive *taskqueuespb.TaskVersionDirective) {
	timeout := 1000000 * time.Millisecond
	if expectedSyncMatch {
		// trySyncMatch "eats" one second from the context timeout!
		timeout += time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	s.userDataMgr.updateVersioningData(versioningData)
	buildId, syncMatch, err := s.partitionMgr.AddTask(ctx, addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId:      namespaceID,
			RunId:            "run",
			WorkflowId:       "wf",
			VersionDirective: directive,
		},
	})
	s.NoError(err)
	s.Equal(expectedSyncMatch, syncMatch)
	s.Equal(expectedBuildId, buildId)
}

func (s *PartitionManagerTestSuite) validatePollTaskSyncMatch(buildId string, useVersioning bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		task, _, err := s.partitionMgr.PollTask(
			ctx, &pollMetadata{
				workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
					BuildId:       buildId,
					UseVersioning: useVersioning,
				},
			},
		)
		s.NoError(err)
		s.NotNil(task)
		s.NotNil(task.responseC)
		close(task.responseC)
	}()
	// give time for poller to start polling before resuming execution
	time.Sleep(10 * time.Millisecond)
}

// Poll task and assert no error and that a non-nil task is returned
func (s *PartitionManagerTestSuite) validatePollTask(buildId string, useVersioning bool) *internalTask {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	task, _, err := s.partitionMgr.PollTask(ctx, &pollMetadata{
		workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:       buildId,
			UseVersioning: useVersioning,
		},
	})
	s.NoError(err)
	s.NotNil(task)

	return task
}

// UpdatePollerData is a no-op if the poller context has no identity, so we need a context with identity for any tests that check poller info
func (s *PartitionManagerTestSuite) pollWithIdentity(pollerId, buildId string, useVersioning bool, passOptions bool) {
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), identityKey, pollerId), 100*time.Millisecond)
	defer cancel()

	pm := &pollMetadata{}
	if passOptions {
		pm.deploymentOptions = &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       "foo",
			BuildId:              buildId,
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_UNVERSIONED,
		}
		if useVersioning {
			pm.deploymentOptions.WorkerVersioningMode = enumspb.WORKER_VERSIONING_MODE_VERSIONED
		}
	} else {
		pm.workerVersionCapabilities = &commonpb.WorkerVersionCapabilities{
			BuildId:       buildId,
			UseVersioning: useVersioning,
		}
	}
	_, _, err := s.partitionMgr.PollTask(ctx, pm)

	if !errors.Is(err, errNoTasks) {
		s.Fail(fmt.Sprintf("expected errNoTasks but got %e", err))
	}
}

func createVersionSet(buildId string) *persistencespb.CompatibleVersionSet {
	clock := hlc.Zero(1)
	return &persistencespb.CompatibleVersionSet{
		SetIds: []string{hashBuildId(buildId)},
		BuildIds: []*persistencespb.BuildId{
			mkBuildId(buildId, clock),
		},
		BecameDefaultTimestamp: clock,
	}
}

func (s *PartitionManagerTestSuite) describeStatsEventually(
	buildIds map[string]bool,
	includeAllActive, reportPollers, internalTaskQueueStatus bool,
	check func(resp *matchingservice.DescribeTaskQueuePartitionResponse) bool,
) {
	// Backlog stats are sourced from async readers; wait until Describe reflects expected stable values.
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		resp, err := s.partitionMgr.Describe(ctx, buildIds, includeAllActive, true /* reportStats */, reportPollers, internalTaskQueueStatus)
		if err != nil {
			return false
		}
		return check(resp)
	}, 2*time.Second, 10*time.Millisecond)
}

// testPartitionManagerConfig holds configuration for setting up a partition manager in tests
type testPartitionManagerConfig struct {
	loadTime         time.Duration // How long ago partition was loaded
	withRecentPoller bool          // Whether to register a poller to simulate recent poller activity
}

// setupPartitionManagerWithCapture creates a partition manager with a capturing metrics handler
// and returns the manager, capture, and a cleanup function
func (s *PartitionManagerTestSuite) setupPartitionManagerWithCapture(
	config testPartitionManagerConfig,
) (*taskQueuePartitionManagerImpl, *metricstest.Capture, func()) {
	// Create capturing metrics handler
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()

	// Create a new partition manager with the capturing metrics handler
	f, err := tqid.NewTaskQueueFamily(namespaceID, taskQueueName)
	s.Require().NoError(err)
	partition := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()
	tqConfig := newTaskQueueConfig(partition.TaskQueue(), s.partitionMgr.engine.config, s.partitionMgr.ns.Name())

	pm, err := newTaskQueuePartitionManager(s.partitionMgr.engine, s.partitionMgr.ns, partition, tqConfig, s.partitionMgr.logger, s.partitionMgr.throttledLogger, metricsHandler, s.userDataMgr)
	s.Require().NoError(err)
	pm.Start()

	// Simulate partition loaded at specified time in the past
	pm.loadTime = time.Now().Add(-config.loadTime)

	// Wait until initialized
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = pm.WaitUntilInitialized(ctx)
	cancel()
	s.NoError(err)

	// Register a poller if requested
	if config.withRecentPoller {
		pollCtx, pollCancel := context.WithTimeout(
			context.WithValue(context.Background(), identityKey, "test-poller"),
			100*time.Millisecond,
		)
		_, _, _ = pm.PollTask(pollCtx, &pollMetadata{
			workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:       "",
				UseVersioning: false,
			},
		})
		pollCancel()
	}

	// Return cleanup function
	cleanup := func() {
		metricsHandler.StopCapture(capture)
		pm.Stop(unloadCauseUnspecified)
	}

	return pm, capture, cleanup
}

func (s *PartitionManagerTestSuite) TestNoRecentPollerMetric_NewlyLoadedPartitionWithNoRecentPoller() {
	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime:         1 * time.Minute,
		withRecentPoller: false,
	})
	defer cleanup()

	// Add a task to trigger the metric check
	_, _, err := pm.AddTask(context.Background(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "test-run",
			WorkflowId:  "test-workflow",
		},
	})
	s.Require().NoError(err)

	// Verify metric was NOT emitted for newly loaded partition
	snapshot := capture.Snapshot()
	recordings, exists := snapshot[metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()]
	s.False(exists && len(recordings) > 0, "Metric should not be emitted for newly loaded partition")
}

func (s *PartitionManagerTestSuite) TestNoRecentPollerMetric_NewlyLoadedPartitionWithRecentPollers() {
	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime:         1 * time.Minute,
		withRecentPoller: true,
	})
	defer cleanup()

	// Add a task to trigger the metric check
	_, _, err := pm.AddTask(context.Background(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "test-run",
			WorkflowId:  "test-workflow",
		},
	})
	s.Require().NoError(err)

	// Verify metric was NOT emitted for newly loaded partition even with pollers
	snapshot := capture.Snapshot()
	recordings, exists := snapshot[metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()]
	s.False(exists && len(recordings) > 0, "Metric should not be emitted for newly loaded partition with pollers")
}

func (s *PartitionManagerTestSuite) TestNoRecentPollerMetric_OldPartitionWithNoPollers() {
	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime:         3 * time.Minute,
		withRecentPoller: false,
	})
	defer cleanup()

	// Add a task to trigger the metric check
	_, _, err := pm.AddTask(context.Background(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "test-run",
			WorkflowId:  "test-workflow",
		},
	})
	s.Require().NoError(err)

	// Verify metric WAS emitted for old partition with no pollers
	snapshot := capture.Snapshot()
	recordings, exists := snapshot[metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()]
	s.True(exists && len(recordings) > 0, "Metric should be emitted for old partition with no pollers")
	s.Equal(int64(1), recordings[0].Value)
}

func (s *PartitionManagerTestSuite) TestNoRecentPollerMetric_OldPartitionWithRecentPollers() {
	pm, capture, cleanup := s.setupPartitionManagerWithCapture(testPartitionManagerConfig{
		loadTime:         3 * time.Minute,
		withRecentPoller: true,
	})
	defer cleanup()

	// Add a task to trigger the metric check
	_, _, err := pm.AddTask(context.Background(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			NamespaceId: namespaceID,
			RunId:       "test-run",
			WorkflowId:  "test-workflow",
		},
	})
	s.Require().NoError(err)

	// Verify metric was NOT emitted because of recent pollers being present
	snapshot := capture.Snapshot()
	recordings, exists := snapshot[metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()]
	s.False(exists, "No recordings should exist when there are no recent pollers")
	s.Empty(recordings, "Metric should not be emitted when there are recent pollers")
}

type mockUserDataManager struct {
	sync.Mutex
	data     *persistencespb.VersionedTaskQueueUserData
	onChange UserDataOnChangeFunc
}

func (m *mockUserDataManager) Start() {
	// noop
}

func (m *mockUserDataManager) Stop() {
	// noop
}

func (m *mockUserDataManager) WaitUntilInitialized(_ context.Context) error {
	return nil
}

func (m *mockUserDataManager) GetUserData() (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error) {
	m.Lock()
	defer m.Unlock()
	return m.data, nil, nil
}

func (m *mockUserDataManager) UpdateUserData(_ context.Context, _ UserDataUpdateOptions, updateFn UserDataUpdateFunc) (int64, error) {
	m.Lock()
	defer m.Unlock()
	data, _, err := updateFn(m.data.GetData())
	if err != nil {
		return 0, err
	}
	m.data = &persistencespb.VersionedTaskQueueUserData{Data: data, Version: m.data.GetVersion() + 1}
	version := m.data.Version
	if m.onChange != nil {
		go m.onChange(m.data)
	}
	return version, nil
}

func (m *mockUserDataManager) HandleGetUserDataRequest(ctx context.Context, req *matchingservice.GetTaskQueueUserDataRequest) (*matchingservice.GetTaskQueueUserDataResponse, error) {
	panic("unused")
}

func (m *mockUserDataManager) CheckTaskQueueUserDataPropagation(ctx context.Context, version int64, wfPartitions int, actPartitions int) error {
	panic("unused")
}

func (m *mockUserDataManager) LocalBacklogPriorityChanged(map[PhysicalTaskQueueVersion]int64) {
	panic("unused")
}

func (m *mockUserDataManager) updateVersioningData(data *persistencespb.VersioningData) {
	m.Lock()
	defer m.Unlock()
	m.data = &persistencespb.VersionedTaskQueueUserData{Data: &persistencespb.TaskQueueUserData{VersioningData: data}}
}

var _ userDataManager = (*mockUserDataManager)(nil)
