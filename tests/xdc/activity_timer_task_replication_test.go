package xdc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tasks"
	historyworkflow "go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ActivityTimerTaskReplicationSuite covers how a standby cluster maintains an activity's
// timer task mask as updates replicate to it.
//
// The mask records which activity timeout tasks this cluster has already created. Getting
// it wrong is costly in both directions: keeping a bit whose deadline moved loses the
// timer, since CreateNextActivityTimer only inspects the earliest timer and skips it when
// the bit is set; clearing a bit whose deadline did not move regenerates a task that is
// already pending. These tests cover one direction each.
type ActivityTimerTaskReplicationSuite struct {
	xdcBaseSuite
}

func TestActivityTimerTaskReplicationSuite(t *testing.T) {
	s := new(ActivityTimerTaskReplicationSuite)
	suite.Run(t, s)
}

func (s *ActivityTimerTaskReplicationSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}
	// The timer task mask is carried across a replicated update by
	// applyUpdatesToSubStateMachines, which only runs on the state-based replication path.
	// xdcBaseSuite overrides the EnableTransitionHistory default with this field, so
	// without setting it these tests would exercise the event-based SyncActivity path and
	// never reach the logic under test.
	s.enableTransitionHistory = true
	s.setupSuite()
}

func (s *ActivityTimerTaskReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *ActivityTimerTaskReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestRetryingActivityDoesNotAccumulateStandbyTimerTasks covers the "deadline unchanged,
// keep the bit" direction: a retrying activity must not make the standby regenerate its
// schedule-to-close timeout task on every replicated update.
//
// Each attempt produces two applies on the standby, one when the activity starts and one
// when it fails and the next attempt is scheduled. Before the mask was carried across an
// apply, every one of them cleared it, and the following task refresh recreated whichever
// timer was earliest. Between attempts that is schedule-to-close: clearing the started
// state removes start-to-close and heartbeat from the sequence, and schedule-to-start is
// normalized to the schedule-to-close duration when the caller does not set it, so it is
// anchored on the advancing ScheduledTime while schedule-to-close stays on the fixed
// FirstScheduledTime. Since that deadline never moves, the regenerated tasks all carried
// an identical fire time and piled onto a single instant in the timer queue.
//
// The assertion is that the count of far-future tasks does not grow between two attempt
// checkpoints. That is the property that matters, and it is robust to however many tasks
// initial replication creates; an exact count would not be, because the baseline is two:
//
//   - On attempt 1, ScheduledTime still equals FirstScheduledTime, so the
//     schedule-to-start and schedule-to-close deadlines coincide. The tie resolves toward
//     schedule-to-start, so that task is created and then left behind as an orphan once
//     the activity starts and its timer leaves the sequence.
//   - On attempt 2, ScheduledTime has advanced, so schedule-to-close is strictly earlier
//     and is created.
//   - From attempt 3 on, the schedule-to-close bit is set and stays the earliest timer
//     between attempts, so nothing further is created.
//
// Under the old behavior the count tracked the attempt count exactly (measured: 5 at
// attempt 5, 12 at attempt 12).
func (s *ActivityTimerTaskReplicationSuite) TestRetryingActivityDoesNotAccumulateStandbyTimerTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		// Long enough that the schedule-to-close deadline stays far in the future for the
		// whole test, so it never fires and is never consumed.
		scheduleToCloseTimeout = time.Hour
		startToCloseTimeout    = 2 * time.Second
		retryInterval          = time.Second
		// Two checkpoints far enough apart that one-task-per-attempt growth is
		// unmistakable.
		firstCheckpoint  = int32(5)
		secondCheckpoint = int32(12)
		// Separates the schedule-to-close band from the per-attempt timers, which fire
		// within seconds. adminservice.Task does not carry the timeout type, so fire time
		// is the only discriminator available.
		farFutureCutoff = scheduleToCloseTimeout / 2
	)

	var startedActivityCount atomic.Int32
	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		return "", errors.New("bad-luck-please-retry")
	}

	workflowFn := func(ctx workflow.Context) error {
		var ret string
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    retryInterval,
				BackoffCoefficient: 1,
			},
		}), activityFunction).Get(ctx, &ret)
	}

	ns, workflowRun := s.startWorkflow(ctx, workflowFn, activityFunction)

	countBands := func(checkpoint int32) (farFuture, perAttempt int, ai *persistencespb.ActivityInfo) {
		ai = s.awaitStandbyAttempt(ctx, ns, workflowRun.GetID(), workflowRun.GetRunID(), checkpoint)
		cutoff := time.Now().UTC().Add(farFutureCutoff)
		for _, task := range s.activityTimeoutTasks(ctx, s.clusters[1], workflowRun.GetID()) {
			if task.GetFireTime().AsTime().After(cutoff) {
				farFuture++
			} else {
				perAttempt++
			}
		}
		return farFuture, perAttempt, ai
	}

	firstFar, firstPerAttempt, standbyActivity := countBands(firstCheckpoint)
	s.T().Logf("at attempt>=%d: farFuture=%d perAttempt=%d mask=%d",
		firstCheckpoint, firstFar, firstPerAttempt, standbyActivity.GetTimerTaskStatus())

	secondFar, secondPerAttempt, standbyActivity := countBands(secondCheckpoint)
	s.T().Logf("at attempt>=%d: farFuture=%d perAttempt=%d mask=%d (activity attempts started: %d)",
		secondCheckpoint, secondFar, secondPerAttempt, standbyActivity.GetTimerTaskStatus(),
		startedActivityCount.Load())

	s.Equal(firstFar, secondFar,
		"standby accumulated far-future activity timeout tasks across retries: %d at attempt %d, %d at attempt %d; "+
			"the timer task mask is being cleared on every replicated apply",
		firstFar, firstCheckpoint, secondFar, secondCheckpoint)

	// The opposite failure: over-preserving the mask would starve the per-attempt timers.
	// Each attempt gets a fresh start-to-close deadline, so those tasks must keep coming.
	s.Greater(secondPerAttempt, firstPerAttempt,
		"standby stopped creating per-attempt activity timeout tasks; a preserved mask bit is "+
			"suppressing timers whose deadline did move")

	// The standby must also record that a schedule-to-close task exists, and the tasks
	// that do exist must fire at the deadline the standby itself derives.
	s.NotZero(
		standbyActivity.GetTimerTaskStatus()&historyworkflow.TimerTaskStatusCreatedScheduleToClose,
		"standby mask does not record a schedule-to-close task",
	)
	expectedFireTime := standbyActivity.GetFirstScheduledTime().AsTime().
		Add(standbyActivity.GetScheduleToCloseTimeout().AsDuration())
	cutoff := time.Now().UTC().Add(farFutureCutoff)
	for _, task := range s.activityTimeoutTasks(ctx, s.clusters[1], workflowRun.GetID()) {
		if !task.GetFireTime().AsTime().After(cutoff) {
			continue
		}
		s.Equal(workflowRun.GetRunID(), task.GetRunId())
		s.WithinDuration(expectedFireTime, task.GetFireTime().AsTime(), time.Second,
			"far-future task does not fire at FirstScheduledTime+ScheduleToCloseTimeout")
	}

	// A bit must never be set for a timer that does not currently apply, since
	// CreateNextActivityTimer would then skip the earliest timer and stall the sequence.
	if standbyActivity.GetStartedEventId() == common.EmptyEventID {
		s.Zero(standbyActivity.GetTimerTaskStatus()&historyworkflow.TimerTaskStatusCreatedStartToClose,
			"start-to-close bit is set while the standby sees the activity as not started")
	}
}

// TestHeartbeatingActivityRecreatesStandbyHeartbeatTimer covers the moving-deadline case:
// a heartbeat pushes the heartbeat deadline forward, so the pending heartbeat timeout task
// becomes wrong and the standby must replace it rather than rest on the bit it already has
// set. Heartbeat is the earliest timer while the activity runs, so a standby that stopped
// replacing it would also stall the rest of the sequence.
//
// Note this does not discriminate on how the timer task mask is carried across a
// replicated update: heartbeat is handled separately, because its deadline moves on every
// heartbeat and no event drives recreation. The standby timer executor clears the
// heartbeat bit itself once the task fires and its recorded visibility time has passed
// (the only bit it ever clears), and CreateNextActivityTimer persists the deadline each
// heartbeat task was created for. So this passes whether the mask carry logic preserves or
// clears bits. It is here as an invariant guard on that separate mechanism; the mask carry
// logic is covered by TestRetryingActivityDoesNotAccumulateStandbyTimerTasks, which fails
// in both directions.
func (s *ActivityTimerTaskReplicationSuite) TestHeartbeatingActivityRecreatesStandbyHeartbeatTimer() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		heartbeatTimeout       = 4 * time.Second
		heartbeatInterval      = 500 * time.Millisecond
		startToCloseTimeout    = 60 * time.Second
		scheduleToCloseTimeout = time.Hour
		activityRunTime        = 30 * time.Second
		// Heartbeat deadlines land a few seconds out; start-to-close is a minute out and
		// schedule-to-close an hour, so this cleanly isolates the heartbeat band.
		heartbeatBandCutoff = 30 * time.Second
	)

	activityFunction := func(ctx context.Context) (string, error) {
		deadline := time.Now().Add(activityRunTime)
		for time.Now().Before(deadline) {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(heartbeatInterval):
			}
		}
		return "done", nil
	}

	workflowFn := func(ctx workflow.Context) error {
		var ret string
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			HeartbeatTimeout:       heartbeatTimeout,
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
		}), activityFunction).Get(ctx, &ret)
	}

	ns, workflowRun := s.startWorkflow(ctx, workflowFn, activityFunction)

	// Latest heartbeat timeout task the standby holds, or zero if it holds none.
	latestHeartbeatTask := func() time.Time {
		var latest time.Time
		cutoff := time.Now().UTC().Add(heartbeatBandCutoff)
		for _, task := range s.activityTimeoutTasks(ctx, s.clusters[1], workflowRun.GetID()) {
			fireTime := task.GetFireTime().AsTime()
			if fireTime.Before(cutoff) && fireTime.After(latest) {
				latest = fireTime
			}
		}
		return latest
	}

	// Wait until the standby has seen the activity start and recorded a heartbeat timer.
	var firstFireTime time.Time
	await.Require(ctx, s.T(), func(t *await.T) {
		ai := s.standbyActivity(ctx, ns, workflowRun.GetID(), workflowRun.GetRunID())
		require.NotNil(t, ai)
		require.NotEqual(t, common.EmptyEventID, ai.GetStartedEventId(), "activity not started on standby yet")
		require.NotZero(t, ai.GetTimerTaskStatus()&historyworkflow.TimerTaskStatusCreatedHeartbeat,
			"standby mask does not record a heartbeat task")
		firstFireTime = latestHeartbeatTask()
		require.False(t, firstFireTime.IsZero(), "no heartbeat timeout task on the standby")
	}, 60*time.Second, 500*time.Millisecond)
	s.T().Logf("first heartbeat task fires at %s", firstFireTime.Format(time.RFC3339Nano))

	// As heartbeats keep arriving the deadline advances, so the standby must keep
	// recreating the task rather than resting on the bit it already has set.
	var secondFireTime time.Time
	await.Require(ctx, s.T(), func(t *await.T) {
		secondFireTime = latestHeartbeatTask()
		require.False(t, secondFireTime.IsZero(), "standby has no heartbeat timeout task")
		require.True(t, secondFireTime.After(firstFireTime),
			"heartbeat timeout task did not advance past %s; the standby is not recreating it as the deadline moves",
			firstFireTime)
	}, 60*time.Second, 500*time.Millisecond)
	s.T().Logf("heartbeat task advanced to %s (+%s)",
		secondFireTime.Format(time.RFC3339Nano), secondFireTime.Sub(firstFireTime))

	// The task the standby holds must match the deadline it derives from its own state:
	// max(StartedTime, LastHeartbeatUpdateTime) + HeartbeatTimeout.
	ai := s.standbyActivity(ctx, ns, workflowRun.GetID(), workflowRun.GetRunID())
	s.NotNil(ai)
	lastHeartbeat := ai.GetStartedTime().AsTime()
	if hb := ai.GetLastHeartbeatUpdateTime().AsTime(); hb.After(lastHeartbeat) {
		lastHeartbeat = hb
	}
	s.WithinDuration(lastHeartbeat.Add(ai.GetHeartbeatTimeout().AsDuration()), latestHeartbeatTask(), 2*time.Second,
		"heartbeat task does not fire at lastHeartbeat+HeartbeatTimeout")
}

// startWorkflow registers the activity on the active cluster and starts the workflow,
// returning the namespace and the run.
func (s *ActivityTimerTaskReplicationSuite) startWorkflow(
	ctx context.Context,
	workflowFn any,
	activityFn any,
) (string, sdkclient.WorkflowRun) {
	ns := s.createGlobalNamespace()
	activeSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	s.T().Cleanup(activeSDKClient.Close)

	taskQueue := testcore.RandomizeStr("tq")
	worker := sdkworker.New(activeSDKClient, taskQueue, sdkworker.Options{})
	worker.RegisterWorkflow(workflowFn)
	worker.RegisterActivity(activityFn)
	s.NoError(worker.Start())
	s.T().Cleanup(worker.Stop)

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}, workflowFn)
	s.NoError(err)
	return ns, workflowRun
}

// standbyActivity returns the standby's view of the workflow's single pending activity, or
// nil if it does not have exactly one.
func (s *ActivityTimerTaskReplicationSuite) standbyActivity(
	ctx context.Context,
	ns, workflowID, runID string,
) *persistencespb.ActivityInfo {
	resp, err := s.clusters[1].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	if err != nil {
		return nil
	}
	activityInfos := resp.GetDatabaseMutableState().GetActivityInfos()
	if len(activityInfos) != 1 {
		return nil
	}
	for _, ai := range activityInfos {
		return ai
	}
	return nil
}

// awaitStandbyAttempt waits for the active cluster to reach minAttempt and then for the
// standby to catch up, so the applies under test have actually run there.
func (s *ActivityTimerTaskReplicationSuite) awaitStandbyAttempt(
	ctx context.Context,
	ns, workflowID, runID string,
	minAttempt int32,
) *persistencespb.ActivityInfo {
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}
	await.Require(ctx, s.T(), func(t *await.T) {
		resp, err := s.clusters[0].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: execution,
		})
		require.NoError(t, err)
		activityInfos := resp.GetDatabaseMutableState().GetActivityInfos()
		require.Len(t, activityInfos, 1)
		for _, ai := range activityInfos {
			require.GreaterOrEqual(t, ai.GetAttempt(), minAttempt)
		}
	}, 60*time.Second, 500*time.Millisecond)

	var standbyActivity *persistencespb.ActivityInfo
	await.Require(ctx, s.T(), func(t *await.T) {
		ai := s.standbyActivity(ctx, ns, workflowID, runID)
		require.NotNil(t, ai)
		require.GreaterOrEqual(t, ai.GetAttempt(), minAttempt)
		standbyActivity = ai
	}, 60*time.Second, 500*time.Millisecond)
	return standbyActivity
}

// activityTimeoutTasks returns the cluster's activity timeout tasks for the given
// workflow. The xdc suites run with a single history shard, so shard 1 holds every task.
func (s *ActivityTimerTaskReplicationSuite) activityTimeoutTasks(
	ctx context.Context,
	cluster *testcore.TestCluster,
	workflowID string,
) []*adminservice.Task {
	var result []*adminservice.Task
	var nextPageToken []byte
	for {
		resp, err := cluster.AdminClient().ListHistoryTasks(ctx, &adminservice.ListHistoryTasksRequest{
			ShardId:  1,
			Category: int32(tasks.CategoryIDTimer),
			TaskRange: &historyspb.TaskRange{
				InclusiveMinTaskKey: &historyspb.TaskKey{FireTime: timestamppb.New(time.Unix(0, 0).UTC())},
				ExclusiveMaxTaskKey: &historyspb.TaskKey{FireTime: timestamppb.New(time.Now().UTC().Add(10 * 365 * 24 * time.Hour))},
			},
			BatchSize:     1000,
			NextPageToken: nextPageToken,
		})
		s.NoError(err)

		for _, task := range resp.GetTasks() {
			if task.GetWorkflowId() != workflowID {
				continue
			}
			if task.GetTaskType() != enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT {
				continue
			}
			result = append(result, task)
		}

		nextPageToken = resp.GetNextPageToken()
		if len(nextPageToken) == 0 {
			return result
		}
	}
}
