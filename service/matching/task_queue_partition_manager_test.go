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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	namespaceId   = "ns-id"
	namespaceName = "ns-name"
	taskQueueName = "my-test-tq"
)

type PartitionManagerTestSuite struct {
	suite.Suite
	protorequire.ProtoAssertions

	newMatcher   bool
	fairness     bool
	controller   *gomock.Controller
	userDataMgr  *mockUserDataManager
	partitionMgr *taskQueuePartitionManagerImpl
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
	config := NewConfig(dynamicconfig.NewNoopCollection())
	if s.fairness {
		useFairness(config)
	} else if s.newMatcher {
		useNewMatcher(config)
	}

	matchingClientMock := matchingservicemock.NewMockMatchingServiceClient(s.controller)
	engine := createTestMatchingEngine(logger, s.controller, config, matchingClientMock, registry)

	f, err := tqid.NewTaskQueueFamily(namespaceId, taskQueueName)
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
			NamespaceId: namespaceId,
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
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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

	// calling Describe on an unloaded physical queue
	resp, err := s.partitionMgr.Describe(ctx, buildIds, false, true, false, false)
	s.NoError(err)

	// 1 task in the backlog
	s.NotNil(resp.VersionsInfoInternal[bld].PhysicalTaskQueueInfo.TaskQueueStats)
	s.Equal(int64(1), resp.VersionsInfoInternal[bld].PhysicalTaskQueueInfo.TaskQueueStats.ApproximateBacklogCount)
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
			NamespaceId:      namespaceId,
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

type mockUserDataManager struct {
	sync.Mutex
	data *persistencespb.VersionedTaskQueueUserData
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
	return m.data.Version, nil
}

func (m *mockUserDataManager) HandleGetUserDataRequest(ctx context.Context, req *matchingservice.GetTaskQueueUserDataRequest) (*matchingservice.GetTaskQueueUserDataResponse, error) {
	panic("unused")
}

func (m *mockUserDataManager) CheckTaskQueueUserDataPropagation(ctx context.Context, version int64, wfPartitions int, actPartitions int) error {
	panic("unused")
}

func (m *mockUserDataManager) updateVersioningData(data *persistencespb.VersioningData) {
	m.Lock()
	defer m.Unlock()
	m.data = &persistencespb.VersionedTaskQueueUserData{Data: &persistencespb.TaskQueueUserData{VersioningData: data}}
}

var _ userDataManager = (*mockUserDataManager)(nil)
