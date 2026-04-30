package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
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
	// Drive the state-based replication path so applyIncomingTimeSkippingInfo runs.
	// Without this, events alone replicate TimeSkippingInfo via their handlers, and
	// the state-based merge function added by this commit is never exercised.
	s.enableTransitionHistory = true
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
	await.Require(ctx, s.T(), func(t *await.T) {
		active := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(t, active, "active TimeSkippingInfo must be present")
		require.NotNil(t, standby, "standby TimeSkippingInfo must be present")
		require.True(t, proto.Equal(active.GetConfig(), standby.GetConfig()),
			"config mismatch: active=%v standby=%v", active.GetConfig(), standby.GetConfig())
		require.Equal(t,
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
	cfg *commonpb.TimeSkippingConfig,
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

// completeFirstWorkflowTask polls and completes the initial workflow task on the
// active cluster with no commands, leaving the workflow open and idle. Once idle,
// a registered FastForward fires on the next close transaction (see the
// FastForward functional tests for the same pattern).
func (s *timeSkippingReplicationSuite) completeFirstWorkflowTask(ns, wfID, tq string) {
	tv := testvars.New(s.T()).WithTaskQueue(tq).WithWorkflowID(wfID)
	poller := taskpoller.New(s.T(), s.clusters[0].FrontendClient(), ns)
	_, err := poller.PollAndHandleWorkflowTask(tv,
		func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		})
	s.NoError(err)
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
		&commonpb.TimeSkippingConfig{Enabled: true})

	// Wait for the start close-transaction skip to land on active.
	await.Require(ctx, s.T(), func(t *await.T) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(t, info, "active must persist TimeSkippingInfo after start")
		require.Greater(t, info.GetAccumulatedSkippedDuration().AsDuration(), 30*time.Minute,
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

// TestFastForwardDisablePropagates verifies that completing a registered FastForward
// on the active — which flips Config.Enabled to false and accumulates the fast-forward
// duration — replicates to the standby. After replication, standby's config must
// report Enabled=false so subsequent skip checks short-circuit there too.
func (s *timeSkippingReplicationSuite) TestFastForwardDisablePropagates() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-ff-" + uuid.NewString()
	tq := "ts-repl-ff-tq-" + uuid.NewString()

	const (
		fastForward = 30 * time.Minute
		accumTol    = 30 * time.Second
	)
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, 0,
		&commonpb.TimeSkippingConfig{
			Enabled:     true,
			FastForward: durationpb.New(fastForward),
		},
	)

	// Drive the initial workflow task so the workflow goes idle; the fast-forward
	// fires on the next close transaction.
	s.completeFirstWorkflowTask(ns, wfID, tq)

	// Wait for the fast-forward to complete on active: Enabled must flip to false,
	// HasReached must be set, and accumulated must land at ~fastForward.
	await.Require(ctx, s.T(), func(t *await.T) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(t, info)
		require.False(t, info.GetConfig().GetEnabled(),
			"completing the fast-forward must flip Config.Enabled to false")
		require.True(t, info.GetFastForwardInfo().GetHasReached(), "active must set HasReached=true")
		require.InDelta(t,
			float64(fastForward), float64(info.GetAccumulatedSkippedDuration().AsDuration()),
			float64(accumTol),
			"accumulated must land at ~fastForward after the disable transition")
	}, 30*time.Second, 200*time.Millisecond)

	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)

	standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
	s.False(standby.GetConfig().GetEnabled(),
		"standby must observe Config.Enabled=false after fast-forward replication")
	s.InDelta(float64(fastForward), float64(standby.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))

	// Standby's history must contain the TimeSkippingTransitioned event marking
	// the fast-forward disable — proves the event itself (not just the MS field)
	// replicated from active.
	histResp, err := s.clusters[1].FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID},
		})
	s.NoError(err)
	disableTransitions := 0
	for _, ev := range histResp.GetHistory().GetEvents() {
		if ev.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			continue
		}
		if ev.GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterFastForward() {
			disableTransitions++
		}
	}
	s.Equal(1, disableTransitions,
		"standby history must contain exactly one TimeSkippingTransitioned event with DisabledAfterFastForward=true")
}

// TestStandbyTimeSkippingTimerTaskAcksOnReachedFastForward drives the standby
// executor's executeTimeSkippingTimerTask end-to-end. A registered FastForward
// installs a TimeSkippingTimerTask whose visibility is scheduled wall-clock time.
// On the standby this task is regenerated during state-based replication via
// task_refresher.refreshTasksForTimeSkipping.
//
// Scenario:
//
//  1. Start a workflow on active with TimeSkippingConfig {Enabled, FastForward}.
//  2. Drive the initial workflow task so the workflow goes idle; the fast-forward
//     fires, flipping Enabled=false and setting HasReached=true.
//  3. State replicates to standby. Standby's own TimeSkippingTimerTask was generated
//     by the refresh; when it fires, executeTimeSkippingTimerTask runs and observes
//     SourceEventId-match with HasReached=true → ack branch.
//
// Convergence on both clusters proves the standby's executor path is wired up,
// returns no error, and doesn't hang or crash.
func (s *timeSkippingReplicationSuite) TestStandbyTimeSkippingTimerTaskAcksOnReachedFastForward() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-ff-timer-" + uuid.NewString()
	tq := "ts-repl-ff-timer-tq-" + uuid.NewString()

	const fastForward = 30 * time.Minute
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, 0,
		&commonpb.TimeSkippingConfig{
			Enabled:     true,
			FastForward: durationpb.New(fastForward),
		},
	)

	s.completeFirstWorkflowTask(ns, wfID, tq)

	// Active reaches the fast-forward: Enabled flips to false and HasReached is set.
	// This is driven by the TimeSkippingTimerTask firing on the active.
	await.Require(ctx, s.T(), func(t *await.T) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(t, info)
		require.False(t, info.GetConfig().GetEnabled(),
			"active must flip Enabled=false after the fast-forward timer fires")
		ff := info.GetFastForwardInfo()
		require.NotNil(t, ff)
		require.True(t, ff.GetHasReached(), "active must set HasReached=true")
	}, 30*time.Second, 200*time.Millisecond)

	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)

	// Standby must converge to the same end state. The standby's own
	// TimeSkippingTimerTask was generated by refreshTasksForTimeSkipping during
	// replication; it fires through executeTimeSkippingTimerTask and acks because
	// HasReached is already true by that time.
	standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
	s.NotNil(standby)
	s.False(standby.GetConfig().GetEnabled(),
		"standby must observe Config.Enabled=false after fast-forward replication")
	ff := standby.GetFastForwardInfo()
	s.NotNil(ff)
	s.True(ff.GetHasReached(),
		"standby must observe HasReached=true; absence indicates the standby's TimeSkippingTimerTask didn't replicate the fast-forward transition correctly")
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
		&commonpb.TimeSkippingConfig{Enabled: true})

	// Skip on active; wait for replication to standby.
	await.Require(ctx, s.T(), func(t *await.T) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(t, info)
		require.Greater(t, info.GetAccumulatedSkippedDuration().AsDuration(), 30*time.Minute)
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
