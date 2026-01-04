package matching

import (
	"context"
	"math"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var rpsInf = math.Inf(1)

const (
	defaultNamespaceId = "deadbeef-0000-4567-890a-bcdef0123456"
	defaultRootTqID    = "tq"
)

type PhysicalTaskQueueManagerTestSuite struct {
	suite.Suite

	newMatcher           bool
	fairness             bool
	config               *Config
	controller           *gomock.Controller
	physicalTaskQueueKey *PhysicalTaskQueueKey
	matchingClientMock   *matchingservicemock.MockMatchingServiceClient
	tqMgr                *physicalTaskQueueManagerImpl
}

// TODO(pri): cleanup; delete this
func TestPhysicalTaskQueueManager_Classic_Suite(t *testing.T) {
	suite.Run(t, &PhysicalTaskQueueManagerTestSuite{newMatcher: false})
}

func TestPhysicalTaskQueueManager_Pri_Suite(t *testing.T) {
	suite.Run(t, &PhysicalTaskQueueManagerTestSuite{newMatcher: true})
}

func TestPhysicalTaskQueueManager_Fair_TestSuite(t *testing.T) {
	suite.Run(t, &PhysicalTaskQueueManagerTestSuite{newMatcher: true, fairness: true})
}

func (s *PhysicalTaskQueueManagerTestSuite) SetupTest() {
	s.config = defaultTestConfig()
	s.controller = gomock.NewController(s.T())
	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)

	nsName := namespace.Name("ns-name")
	ns, registry := createMockNamespaceCache(s.controller, nsName)

	engine := createTestMatchingEngine(logger, s.controller, s.config, nil, registry)
	engine.metricsHandler = metricstest.NewCaptureHandler()

	s.physicalTaskQueueKey = defaultTqId()
	prtn := s.physicalTaskQueueKey.Partition()
	tqConfig := newTaskQueueConfig(prtn.TaskQueue(), engine.config, nsName)
	onFatalErr := func(unloadCause) { s.T().Fatal("user data manager called onFatalErr") }
	udMgr := newUserDataManager(engine.taskManager, engine.matchingRawClient, onFatalErr, nil, prtn, tqConfig, engine.logger, engine.namespaceRegistry)

	prtnMgr, err := newTaskQueuePartitionManager(engine, ns, prtn, tqConfig, engine.logger, nil, metrics.NoopMetricsHandler, udMgr)
	s.NoError(err)
	engine.partitions[prtn.Key()] = prtnMgr

	if s.fairness {
		prtnMgr.config.NewMatcher = true
		prtnMgr.config.EnableFairness = true
	} else if s.newMatcher {
		prtnMgr.config.NewMatcher = true
	}

	s.tqMgr, err = newPhysicalTaskQueueManager(prtnMgr, s.physicalTaskQueueKey)
	s.NoError(err)
	prtnMgr.defaultQueueFuture.Set(s.tqMgr, nil)
}

/*
TODO(pri): rewrite or delete this test
func TestReaderSignaling(t *testing.T) {
	readerNotifications := make(chan struct{}, 1)
	clearNotifications := func() {
		for len(readerNotifications) > 0 {
			<-readerNotifications
		}
	}
	tqMgr := mustCreateTestPhysicalTaskQueueManager(t, gomock.NewController(t))

	// redirect taskReader signals into our local channel
	s.tqMgr.backlogMgr.taskReader.notifyC = readerNotifications

	s.tqMgr.Start()
	defer s.tqMgr.Stop(unloadCauseUnspecified)

	// shut down the taskReader so it doesn't steal notifications from us
	s.tqMgr.backlogMgr.taskReader.gorogrp.Cancel()
	s.tqMgr.backlogMgr.taskReader.gorogrp.Wait()

	clearNotifications()

	err := s.tqMgr.SpoolTask(&persistencespb.TaskInfo{
		CreateTime: timestamp.TimePtr(time.Now().UTC()),
	})
	require.NoError(t, err)
	require.Len(t, readerNotifications, 1,
		"Spool task should signal taskReader")

	clearNotifications()
	poller, _ := runOneShotPoller(context.Background(), tqMgr)
	defer poller.Cancel()

	task := newInternalTaskForSyncMatch(&persistencespb.TaskInfo{
		CreateTime: timestamp.TimePtr(time.Now().UTC()),
	}, nil)
	sync, err := s.tqMgr.TrySyncMatch(context.TODO(), task)
	require.NoError(t, err)
	require.True(t, sync)
	require.Len(t, readerNotifications, 0,
		"Sync match should not signal taskReader")
}
*/

func makePollMetadata(rps float64) *pollMetadata {
	return &pollMetadata{taskQueueMetadata: &taskqueuepb.TaskQueueMetadata{
		MaxTasksPerSecond: &wrapperspb.DoubleValue{
			Value: rps,
		},
	}}
}

// runOneShotPoller spawns a goroutine to call tqMgr.PollTask on the provided tqMgr.
// The second return value is a channel of either error or *internalTask.
func runOneShotPoller(ctx context.Context, tqm physicalTaskQueueManager) (*goro.Handle, chan interface{}) {
	out := make(chan interface{}, 1)
	handle := goro.NewHandle(ctx).Go(func(ctx context.Context) error {
		task, err := tqm.PollTask(ctx, makePollMetadata(rpsInf))
		if task == nil {
			out <- err
			return nil
		}
		task.finish(err, true)
		out <- task
		return nil
	})
	// tqMgr.PollTask() needs some time to attach the goro started above to the
	// internal task channel. Sorry for this but it appears unavoidable.
	time.Sleep(10 * time.Millisecond)
	return handle, out
}

func defaultTqId() *PhysicalTaskQueueKey {
	return newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
}

// TODO(pri): old matcher cleanup
func (s *PhysicalTaskQueueManagerTestSuite) TestReaderBacklogAge() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}

	// Create queue Manager and set queue state
	blm := s.tqMgr.backlogMgr.(*backlogManagerImpl)
	s.NoError(blm.taskWriter.initReadWriteState())
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(0), blm.taskAckManager.getReadLevel())

	blm.taskReader.taskBuffer <- randomTaskInfoWithAgeTaskID(time.Minute, 1)
	blm.taskReader.taskBuffer <- randomTaskInfoWithAgeTaskID(10*time.Second, 2)
	go blm.taskReader.dispatchBufferedTasks()

	s.EventuallyWithT(func(collect *assert.CollectT) {
		require.InDelta(s.T(), time.Minute, blm.taskReader.getBacklogHeadAge(), float64(time.Second))
	}, time.Second, 10*time.Millisecond)

	_, err := blm.pqMgr.PollTask(context.Background(), makePollMetadata(rpsInf))
	s.NoError(err)

	s.EventuallyWithT(func(collect *assert.CollectT) {
		require.InDelta(s.T(), 10*time.Second, blm.taskReader.getBacklogHeadAge(), float64(500*time.Millisecond))
	}, time.Second, 10*time.Millisecond)

	_, err = blm.pqMgr.PollTask(context.Background(), makePollMetadata(rpsInf))
	s.NoError(err)

	s.EventuallyWithT(func(collect *assert.CollectT) {
		require.Equalf(s.T(), time.Duration(0), blm.taskReader.getBacklogHeadAge(), "backlog age being reset because of no tasks in the buffer")
	}, time.Second, 10*time.Millisecond)
}

func randomTaskInfoWithAgeTaskID(age time.Duration, TaskID int64) *persistencespb.AllocatedTaskInfo {
	rt1 := time.Now().Add(-age)
	rt2 := rt1.Add(time.Hour)

	return &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.NewString(),
			WorkflowId:       uuid.NewString(),
			RunId:            uuid.NewString(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamppb.New(rt1),
			ExpiryTime:       timestamppb.New(rt2),
		},
		TaskId: TaskID,
	}
}

// TODO(pri): old matcher cleanup
func (s *PhysicalTaskQueueManagerTestSuite) TestLegacyDescribeTaskQueue() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}

	blm := s.tqMgr.backlogMgr.(*backlogManagerImpl)
	s.NoError(blm.taskWriter.initReadWriteState())
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(0), blm.taskAckManager.getReadLevel())

	startTaskID := int64(1)
	taskCount := int64(3)
	for i := int64(0); i < taskCount; i++ {
		blm.taskAckManager.addTask(startTaskID + i)
	}

	// Manually increase the backlog counter since it does not get incremented by taskAckManager.addTask
	// Only doing this for the purpose of this test
	blm.db.updateBacklogStats(taskCount, time.Time{})

	includeTaskStatus := false
	descResp := s.tqMgr.LegacyDescribeTaskQueue(includeTaskStatus)
	s.Equal(0, len(descResp.DescResponse.GetPollers()))
	s.Nil(descResp.DescResponse.GetTaskQueueStatus())

	includeTaskStatus = true
	taskQueueStatus := s.tqMgr.LegacyDescribeTaskQueue(includeTaskStatus).DescResponse.GetTaskQueueStatus()
	s.NotNil(taskQueueStatus)
	s.Zero(taskQueueStatus.GetAckLevel())
	s.Equal(taskCount, taskQueueStatus.GetReadLevel())
	s.Equal(taskCount, taskQueueStatus.GetBacklogCountHint())
	idBlock := taskQueueStatus.GetTaskIdBlock()
	s.Equal(int64(1), idBlock.GetStartId())
	s.Equal(s.tqMgr.config.RangeSize, idBlock.GetEndId())

	// Add a poller and complete all tasks
	pollerIdent := pollerIdentity("test-poll")
	s.tqMgr.pollerHistory.updatePollerInfo(pollerIdent, &pollMetadata{})
	for i := int64(0); i < taskCount; i++ {
		_, numAcked := blm.taskAckManager.completeTask(startTaskID + i)
		blm.db.updateBacklogStats(-numAcked, time.Time{})
	}

	descResp = s.tqMgr.LegacyDescribeTaskQueue(includeTaskStatus)
	s.Equal(1, len(descResp.DescResponse.GetPollers()))
	s.Equal(string(pollerIdent), descResp.DescResponse.Pollers[0].GetIdentity())
	s.NotEmpty(descResp.DescResponse.Pollers[0].GetLastAccessTime())

	rps := 5.0
	s.tqMgr.pollerHistory.updatePollerInfo(pollerIdent, makePollMetadata(rps))
	descResp = s.tqMgr.LegacyDescribeTaskQueue(includeTaskStatus)
	s.Equal(1, len(descResp.DescResponse.GetPollers()))
	s.Equal(string(pollerIdent), descResp.DescResponse.Pollers[0].GetIdentity())
	s.True(descResp.DescResponse.Pollers[0].GetRatePerSecond() > 4.0 && descResp.DescResponse.Pollers[0].GetRatePerSecond() < 6.0)

	taskQueueStatus = descResp.DescResponse.GetTaskQueueStatus()
	s.NotNil(taskQueueStatus)
	s.Equal(taskCount, taskQueueStatus.GetAckLevel())
	s.Zero(taskQueueStatus.GetBacklogCountHint())
}

func (s *PhysicalTaskQueueManagerTestSuite) TestCheckIdleTaskQueue() {
	// Idle
	s.tqMgr.Start()
	time.Sleep(50 * time.Millisecond) // nolint:forbidigo
	s.Equal(common.DaemonStatusStarted, atomic.LoadInt32(&s.tqMgr.status))
	s.tqMgr.Stop(unloadCauseShuttingDown)
	s.Equal(common.DaemonStatusStopped, atomic.LoadInt32(&s.tqMgr.status))

	s.SetupTest()

	// Active poll-er
	s.tqMgr.Start()
	s.tqMgr.pollerHistory.updatePollerInfo("test-poll", &pollMetadata{})
	s.Equal(1, len(s.tqMgr.GetAllPollerInfo()))
	time.Sleep(50 * time.Millisecond) // nolint:forbidigo
	s.Equal(common.DaemonStatusStarted, atomic.LoadInt32(&s.tqMgr.status))
	s.tqMgr.Stop(unloadCauseUnspecified)
	s.Equal(common.DaemonStatusStopped, atomic.LoadInt32(&s.tqMgr.status))

	s.SetupTest()

	// Active adding task
	s.tqMgr.Start()
	s.Equal(0, len(s.tqMgr.GetAllPollerInfo()))
	time.Sleep(50 * time.Millisecond) // nolint:forbidigo
	s.Equal(common.DaemonStatusStarted, atomic.LoadInt32(&s.tqMgr.status))
	s.tqMgr.Stop(unloadCauseUnspecified)
	s.Equal(common.DaemonStatusStopped, atomic.LoadInt32(&s.tqMgr.status))
}

func (s *PhysicalTaskQueueManagerTestSuite) TestAddTaskStandby() {
	ns := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
		},
		cluster.TestAlternativeClusterInitialFailoverVersion,
	)

	// we need to override the mockNamespaceCache to return a passive namespace
	mockNamespaceCache := namespace.NewMockRegistry(s.controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
	s.tqMgr.namespaceRegistry = mockNamespaceCache

	s.tqMgr.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := s.tqMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	defer s.tqMgr.Stop(unloadCauseShuttingDown)
	cancel()

	// stop taskWriter so that we can check if there's any call to it
	// otherwise the task persist process is async and hard to test
	s.tqMgr.tqCtxCancel()

	err = s.tqMgr.SpoolTask(&persistencespb.TaskInfo{
		CreateTime: timestamp.TimePtr(time.Now().UTC()),
	})
	s.Equal(errShutdown, err) // task writer was stopped above
}

func (s *PhysicalTaskQueueManagerTestSuite) TestTQMDoesFinalUpdateOnIdleUnload() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}

	s.config.MaxTaskQueueIdleTime = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(1 * time.Second)
	s.tqMgr.Start()
	defer s.tqMgr.Stop(unloadCauseShuttingDown)

	tm, _ := s.tqMgr.partitionMgr.engine.taskManager.(*testTaskManager)
	s.EventuallyWithT(func(collect *assert.CollectT) {
		// will unload due to idleness
		require.Equal(collect, 1, tm.getUpdateCount(s.physicalTaskQueueKey))
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *PhysicalTaskQueueManagerTestSuite) TestTQMDoesNotDoFinalUpdateOnOwnershipLost() {
	// TODO: use mocks instead of testTaskManager so we can do synchronization better instead of sleeps
	s.config.UpdateAckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(100 * time.Millisecond)
	s.tqMgr.Start()

	// wait for goroutines to start and to acquire rangeid lock
	time.Sleep(10 * time.Millisecond) // nolint:forbidigo

	var tm *testTaskManager
	if s.fairness {
		tm = s.tqMgr.partitionMgr.engine.fairTaskManager.(*testTaskManager) // nolint:revive
	} else {
		tm = s.tqMgr.partitionMgr.engine.taskManager.(*testTaskManager) // nolint:revive
	}
	s.Equal(0, tm.getUpdateCount(s.physicalTaskQueueKey))

	// simulate stolen lock
	ptm := tm.getQueueDataByKey(s.physicalTaskQueueKey)
	ptm.Lock()
	ptm.rangeID++
	ptm.Unlock()

	// change something to ensure it does the periodic write
	s.tqMgr.backlogMgr.getDB().updateAckLevelAndBacklogStats(0, 123456, 10, time.Time{})

	// on the next periodic write, it'll fail due to conflict and unload the task queue
	s.Eventually(func() bool {
		return s.tqMgr.tqCtx.Err() != nil
	}, 5*time.Second, 20*time.Millisecond)

	// no additional updates (the failed periodic update counts as "1")
	s.Equal(1, tm.getUpdateCount(s.physicalTaskQueueKey))
}

func (s *PhysicalTaskQueueManagerTestSuite) TestTQMInterruptsPollOnClose() {
	s.tqMgr.Start()

	pollStart := time.Now()
	pollCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, pollCh := runOneShotPoller(pollCtx, s.tqMgr)

	s.tqMgr.Stop(unloadCauseUnspecified) // should interrupt poller

	<-pollCh
	s.Less(time.Since(pollStart), 4*time.Second)
}

func (s *PhysicalTaskQueueManagerTestSuite) TestPollScalingUpOnBacklog() {
	rl := quotas.NewMockRateLimiter(s.controller)
	rl.EXPECT().AllowN(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	s.tqMgr.pollerScalingRateLimiter = rl

	fakeStats := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 100,
		ApproximateBacklogAge:   durationpb.New(1 * time.Minute),
	}

	decision := s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.NotNil(decision)
	s.GreaterOrEqual(decision.PollRequestDeltaSuggestion, int32(1))
}

func (s *PhysicalTaskQueueManagerTestSuite) TestPollScalingUpAddRateExceedsDispatchRate() {
	rl := quotas.NewMockRateLimiter(s.controller)
	rl.EXPECT().AllowN(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	s.tqMgr.pollerScalingRateLimiter = rl

	fakeStats := &taskqueuepb.TaskQueueStats{
		TasksAddRate:      100,
		TasksDispatchRate: 10,
	}

	decision := s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.NotNil(decision)
	s.GreaterOrEqual(decision.PollRequestDeltaSuggestion, int32(1))
}

func (s *PhysicalTaskQueueManagerTestSuite) TestPollScalingNoChangeOnNoBacklogFastMatch() {
	fakeStats := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 0,
		ApproximateBacklogAge:   durationpb.New(0),
	}
	decision := s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.Nil(decision)
}

func (s *PhysicalTaskQueueManagerTestSuite) TestPollScalingNonRootPartition() {
	// Non-root partitions only get to emit decisions on high backlog
	f, err := tqid.NewTaskQueueFamily(namespaceID, taskQueueName)
	s.NoError(err)
	partition := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(1)
	s.tqMgr.partitionMgr.partition = partition
	// Also disable rate limit to ensure that's not why nil is returned here
	rl := quotas.NewMockRateLimiter(s.controller)
	rl.EXPECT().AllowN(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	s.tqMgr.pollerScalingRateLimiter = rl

	fakeStats := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 100,
		ApproximateBacklogAge:   durationpb.New(1 * time.Minute),
	}
	decision := s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.NotNil(decision)
	s.GreaterOrEqual(decision.PollRequestDeltaSuggestion, int32(1))

	fakeStats.ApproximateBacklogCount = 0
	decision = s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.Nil(decision)
}

func (s *PhysicalTaskQueueManagerTestSuite) TestPollScalingDownOnLongSyncMatch() {
	fakeStats := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 0,
	}
	decision := s.tqMgr.makePollerScalingDecisionImpl(time.Now().Add(-2*time.Second), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.LessOrEqual(decision.PollRequestDeltaSuggestion, int32(-1))
}

func (s *PhysicalTaskQueueManagerTestSuite) TestPollScalingDecisionsAreRateLimited() {
	rl := quotas.NewMockRateLimiter(s.controller)
	rl.EXPECT().AllowN(gomock.Any(), gomock.Any()).Return(true).Times(1)
	rl.EXPECT().AllowN(gomock.Any(), gomock.Any()).Return(false).Times(1)
	s.tqMgr.pollerScalingRateLimiter = rl

	fakeStats := &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: 100,
		ApproximateBacklogAge:   durationpb.New(1 * time.Minute),
	}
	decision := s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.GreaterOrEqual(decision.PollRequestDeltaSuggestion, int32(1))

	decision = s.tqMgr.makePollerScalingDecisionImpl(time.Now(), func() *taskqueuepb.TaskQueueStats { return fakeStats })
	s.Nil(decision)
}
