package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type timeSkippingReplicationSuite struct {
	xdcBaseSuite
}

func TestTimeSkippingReplicationSuite(t *testing.T) {
	t.Parallel()
	s := &timeSkippingReplicationSuite{}
	suite.Run(t, s)
}

func (s *timeSkippingReplicationSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.TimeSkippingEnabled.Key(): true,
	}
	s.logger = log.NewTestLogger()
	s.setupSuite()
}

func (s *timeSkippingReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *timeSkippingReplicationSuite) SetupTest() {
	s.setupTest()
}

// describeNamespaceID looks up the namespace ID from cluster[0]; the same ID is
// shared on standby once the namespace replicates.
func (s *timeSkippingReplicationSuite) describeNamespaceID(ctx context.Context, ns string) string {
	resp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.NoError(err)
	return resp.GetNamespaceInfo().GetId()
}

// getExecutionInfoFromCluster reads the persisted ExecutionInfo from the given cluster's
// history service. Use the database (not cache) view to ensure we observe replicated state.
func (s *timeSkippingReplicationSuite) getExecutionInfoFromCluster(
	ctx context.Context,
	clusterIdx int,
	nsID, wfID, runID string,
) *persistencespb.WorkflowExecutionInfo {
	resp, err := s.clusters[clusterIdx].HistoryClient().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: nsID,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID},
		ArchetypeId: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	s.NotNil(resp.GetDatabaseMutableState())
	return resp.GetDatabaseMutableState().GetExecutionInfo()
}

// waitForTimeSkippingInfoSynced blocks until the standby cluster's TimeSkippingInfo
// agrees with the active's on Config and AccumulatedSkippedDuration. TaskRegenerationStatus
// is cluster-local (regen is re-run on standby after replication) and is not asserted.
func (s *timeSkippingReplicationSuite) waitForTimeSkippingInfoSynced(
	ctx context.Context,
	nsID, wfID, runID string,
) {
	s.waitForClusterSynced()
	s.EventuallyWithT(func(c *assert.CollectT) {
		active := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(c, active, "active TimeSkippingInfo must be present")
		require.NotNil(c, standby, "standby TimeSkippingInfo must be present")
		require.True(c, proto.Equal(active.GetConfig(), standby.GetConfig()),
			"config mismatch: active=%v standby=%v", active.GetConfig(), standby.GetConfig())
		require.Equal(c,
			active.GetAccumulatedSkippedDuration().AsDuration(),
			standby.GetAccumulatedSkippedDuration().AsDuration(),
			"accumulated skipped duration must match")
	}, replicationWaitTime, replicationCheckInterval)
}

// startSkippingWorkflow starts a workflow on the active cluster (cluster[0]) with
// the given TimeSkippingConfig and optional WorkflowStartDelay. Returns the run ID.
// The start-delay scenario triggers a skip on the very first close transaction
// (no WT yet, ExecutionTime > StartTime), giving us deterministic accumulated skip
// without needing to drive any workflow tasks.
func (s *timeSkippingReplicationSuite) startSkippingWorkflow(
	ctx context.Context,
	ns, wfID, tq string,
	runTimeout, startDelay time.Duration,
	cfg *workflowpb.TimeSkippingConfig,
) string {
	req := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          wfID,
		WorkflowType:        &commonpb.WorkflowType{Name: "ts-replication-wf"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(runTimeout),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  cfg,
	}
	if startDelay > 0 {
		req.WorkflowStartDelay = durationpb.New(startDelay)
	}
	resp, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, req)
	s.NoError(err)
	return resp.GetRunId()
}

// TestBasicSkipReplicates verifies the core replication contract for time-skipping:
// a skip transition applied on the active cluster's mutable state replicates to the
// standby with matching Config and AccumulatedSkippedDuration. TaskRegenerationStatus
// is cluster-local (the standby re-runs regen after replication) and is not asserted.
func (s *timeSkippingReplicationSuite) TestBasicSkipReplicates() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-basic-" + uuid.NewString()
	tq := "ts-repl-basic-tq-" + uuid.NewString()

	const (
		startDelay = time.Hour
		accumTol   = 100 * time.Millisecond
	)
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, startDelay,
		&workflowpb.TimeSkippingConfig{Enabled: true})

	// Wait for the start close-transaction skip to land on active.
	s.EventuallyWithT(func(c *assert.CollectT) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(c, info, "active must persist TimeSkippingInfo after start")
		require.Greater(c, info.GetAccumulatedSkippedDuration().AsDuration(), 30*time.Minute,
			"active must accumulate ~startDelay of skip on the first close transaction")
	}, 15*time.Second, 200*time.Millisecond)

	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)

	active := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
	standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
	s.True(proto.Equal(active.GetConfig(), standby.GetConfig()))
	s.Equal(
		active.GetAccumulatedSkippedDuration().AsDuration(),
		standby.GetAccumulatedSkippedDuration().AsDuration(),
	)
	s.InDelta(float64(startDelay), float64(standby.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol),
		"standby's accumulated skip should match the configured startDelay within tolerance")
}

// TestBoundReachedPropagates verifies that crossing a MaxSkippedDuration bound on
// the active — which flips Config.Enabled to false and caps AccumulatedSkippedDuration
// at the bound — replicates to the standby. After replication, standby's config
// must report Enabled=false so subsequent skip checks short-circuit there too.
func (s *timeSkippingReplicationSuite) TestBoundReachedPropagates() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-bound-" + uuid.NewString()
	tq := "ts-repl-bound-tq-" + uuid.NewString()

	const (
		maxSkip    = 30 * time.Minute
		startDelay = time.Hour
		accumTol   = 100 * time.Millisecond
	)
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, startDelay,
		&workflowpb.TimeSkippingConfig{
			Enabled: true,
			Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(maxSkip)},
		},
	)

	// Wait for the bound to be crossed on active: Enabled must flip to false and
	// accumulated must cap at ~maxSkip.
	s.EventuallyWithT(func(c *assert.CollectT) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(c, info)
		require.False(c, info.GetConfig().GetEnabled(),
			"crossing MaxSkippedDuration must flip Config.Enabled to false")
		require.InDelta(c,
			float64(maxSkip), float64(info.GetAccumulatedSkippedDuration().AsDuration()),
			float64(accumTol),
			"accumulated must be capped at maxSkip after the bound transition")
	}, 15*time.Second, 200*time.Millisecond)

	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)

	standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
	s.False(standby.GetConfig().GetEnabled(),
		"standby must observe Config.Enabled=false after bound replication")
	s.InDelta(float64(maxSkip), float64(standby.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))

	// Standby's history must contain the TimeSkippingTransitioned event marking
	// the bound crossing — proves the event itself (not just the MS field)
	// replicated from active.
	histResp, err := s.clusters[1].FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID},
		})
	s.NoError(err)
	boundTransitions := 0
	for _, ev := range histResp.GetHistory().GetEvents() {
		if ev.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			continue
		}
		if ev.GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterBound() {
			boundTransitions++
		}
	}
	s.Equal(1, boundTransitions,
		"standby history must contain exactly one TimeSkippingTransitioned event with DisabledAfterBound=true")
}

// TestFailoverPreservesAccumulatedSkip verifies that after failover, the new active
// cluster preserves the AccumulatedSkippedDuration accumulated under the previous
// active and can drive further work — the regenerated WorkflowBackoffTimerTask on
// the new active fires, producing a workflow task that completes the run.
func (s *timeSkippingReplicationSuite) TestFailoverPreservesAccumulatedSkip() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-failover-" + uuid.NewString()
	tq := "ts-repl-failover-tq-" + uuid.NewString()

	const startDelay = time.Hour
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, startDelay,
		&workflowpb.TimeSkippingConfig{Enabled: true})

	// Skip on active; replicate to standby.
	s.EventuallyWithT(func(c *assert.CollectT) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(c, info)
		require.Greater(c, info.GetAccumulatedSkippedDuration().AsDuration(), 30*time.Minute)
	}, 15*time.Second, 200*time.Millisecond)
	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)
	accumBefore := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).
		GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()

	// Fail over to cluster[1]. cluster[1].InitialFailoverVersion=2.
	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)

	// New active (cluster[1]) must preserve accumulated skip in its MS.
	afterFailover := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
	s.NotNil(afterFailover, "TimeSkippingInfo must persist on the new active after failover")
	s.Equal(accumBefore, afterFailover.GetAccumulatedSkippedDuration().AsDuration(),
		"accumulated skip must survive failover")

	// New active must be able to drive forward progress: the regenerated
	// WorkflowBackoffTimerTask on cluster[1] should fire (its visibility was
	// shifted into near-now by RegenerateTimerTasksForTimeSkipping at replicate
	// time), making a workflow task available to complete the run.
	tv := testvars.New(s.T()).WithTaskQueue(tq).WithWorkflowID(wfID)
	poller := taskpoller.New(s.T(), s.clusters[1].FrontendClient(), ns)
	_, err := poller.PollAndHandleWorkflowTask(tv,
		func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
					},
				}},
			}, nil
		})
	s.NoError(err, "new active must be able to complete a workflow task post-failover")
}

// TestActivityHeartbeatTimeoutUnderSkip exercises the activity heartbeat-timeout
// path under accumulated skip. The fix under test is the virtual→wall conversion
// at mutable_state_impl.go:6851-6852 in AddTasks: heartbeat ActivityTimeoutTask's
// VisibilityTimestamp is in the workflow's virtual frame on creation and must be
// shifted by -accumulatedSkippedDuration so the timer queue dispatches at wall
// +HeartbeatTimeout instead of wall +HeartbeatTimeout+skip.
//
// Scenario: workflow accumulates ~1h skip on the start close-tx, schedules an
// activity with HeartbeatTimeout=2s and StartToClose=30s, polls the activity (so
// it starts and the heartbeat timer is created), then never heartbeats. We
// measure wall-clock latency from PollActivityTaskQueue → WT 2 arrival:
//   - With the fix: heartbeat fires at ~2s wall → WT 2 arrives within a few
//     seconds of activity start.
//   - Without the fix: heartbeat would fire at ~2s+skip wall (~1h), StartToClose
//     (30s wall) wins the race, and WT 2 arrives ~30s after activity start.
//
// 10s is comfortably between the two regimes; we assert latency < 10s. We also
// confirm an ActivityTaskTimedOut event is present as a sanity check.
func (s *timeSkippingReplicationSuite) TestActivityHeartbeatTimeoutUnderSkip() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-heartbeat-timeout-" + uuid.NewString()
	tq := "ts-repl-heartbeat-timeout-tq-" + uuid.NewString()

	const startDelay = time.Hour
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, startDelay,
		&workflowpb.TimeSkippingConfig{Enabled: true})

	// Wait for the start-delay skip to land on active before scheduling the activity.
	s.EventuallyWithT(func(c *assert.CollectT) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(c, info, "active must persist TimeSkippingInfo after start")
		require.Greater(c, info.GetAccumulatedSkippedDuration().AsDuration(), 30*time.Minute,
			"active must accumulate ~startDelay before activity is scheduled")
	}, 15*time.Second, 200*time.Millisecond)

	tv := testvars.New(s.T()).WithTaskQueue(tq).WithWorkflowID(wfID)
	poller := taskpoller.New(s.T(), s.clusters[0].FrontendClient(), ns)
	const workerIdentity = "ts-heartbeat-worker"

	// WT 1: schedule the activity. HeartbeatTimeout (2s) << StartToClose (15s)
	// so wall-clock latency to WT 2 discriminates whether heartbeat fired first.
	_, err := poller.PollAndHandleWorkflowTask(tv,
		func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             tv.ActivityID(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
							StartToCloseTimeout:    durationpb.New(30 * time.Second),
							HeartbeatTimeout:       durationpb.New(2 * time.Second),
							RetryPolicy:            &commonpb.RetryPolicy{MaximumAttempts: 1},
						},
					},
				}},
			}, nil
		})
	s.NoError(err)

	// Poll the activity so it starts. CreateNextActivityTimer emits a heartbeat
	// ActivityTimeoutTask whose VisibilityTimestamp is converted from virtual to
	// wall by AddTasks (mutable_state_impl.go:6851-6852). We never heartbeat or
	// respond, so the heartbeat timer fires on its own.
	actTask, err := s.clusters[0].FrontendClient().PollActivityTaskQueue(ctx,
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: ns,
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  workerIdentity,
		})
	s.NoError(err)
	s.NotEmpty(actTask.GetTaskToken(), "must receive an activity task to start the activity")
	activityStartedWall := time.Now()

	// WT 2 arrives once the activity times out. With the virtual→wall conversion
	// in place, heartbeat fires at ~2s wall and WT 2 arrives shortly after. Without
	// the conversion, heartbeat would fire at ~2s+skip wall (≫15s), and StartToClose
	// (15s wall) wins the race instead — which we detect via wall-clock latency.
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
					},
				}},
			}, nil
		})
	s.NoError(err)
	wt2Latency := time.Since(activityStartedWall)

	// 10s is comfortably between heartbeat (~2s) and StartToClose (15s), with slack
	// for CI scheduler/queue overhead.
	s.Less(wt2Latency, 10*time.Second,
		"WT 2 must arrive within ~heartbeat (2s); >10s indicates StartToClose (15s) won the race — heartbeat task visibility wasn't converted to wall under accumulated skip")

	// Sanity: the activity did time out (any type), proving the timer queue advanced.
	histResp, err := s.clusters[0].FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID},
		})
	s.NoError(err)
	foundActivityTimeout := false
	for _, ev := range histResp.GetHistory().GetEvents() {
		if ev.GetActivityTaskTimedOutEventAttributes() != nil {
			foundActivityTimeout = true
			break
		}
	}
	s.True(foundActivityTimeout, "history must contain an ActivityTaskTimedOut event")

	// Replication must converge — the timeout event and skip state both make it to standby.
	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)
}
