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
// agrees with the active's on Config, AccumulatedSkippedDuration, and entry count.
// Per-entry TaskRegenStatus is cluster-local and explicitly excluded.
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
		require.Len(c, standby.GetTaskRegenEntries(), len(active.GetTaskRegenEntries()),
			"task regen entry count must match")
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
// standby with matching Config, AccumulatedSkippedDuration, and TaskRegenEntries
// count. Per-entry TaskRegenStatus is intentionally not asserted — it is local to
// each cluster (see TimeSkipTaskRegenEntry contract).
func (s *timeSkippingReplicationSuite) TestBasicSkipReplicates() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsID := s.describeNamespaceID(ctx, ns)
	wfID := "ts-repl-basic-" + uuid.NewString()
	tq := "ts-repl-basic-tq-" + uuid.NewString()

	const startDelay = time.Hour
	runID := s.startSkippingWorkflow(ctx, ns, wfID, tq, 24*time.Hour, startDelay,
		&workflowpb.TimeSkippingConfig{Enabled: true})

	// Wait for the start close-transaction skip to land on active.
	s.EventuallyWithT(func(c *assert.CollectT) {
		info := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
		require.NotNil(c, info, "active must persist TimeSkippingInfo after start")
		require.Greater(c, info.GetAccumulatedSkippedDuration().AsDuration(), 30*time.Minute,
			"active must accumulate ~startDelay of skip on the first close transaction")
		require.NotEmpty(c, info.GetTaskRegenEntries(), "active must record at least one regen entry")
	}, 15*time.Second, 200*time.Millisecond)

	s.waitForTimeSkippingInfoSynced(ctx, nsID, wfID, runID)

	active := s.getExecutionInfoFromCluster(ctx, 0, nsID, wfID, runID).GetTimeSkippingInfo()
	standby := s.getExecutionInfoFromCluster(ctx, 1, nsID, wfID, runID).GetTimeSkippingInfo()
	s.True(proto.Equal(active.GetConfig(), standby.GetConfig()))
	s.Equal(
		active.GetAccumulatedSkippedDuration().AsDuration(),
		standby.GetAccumulatedSkippedDuration().AsDuration(),
	)
	s.Len(standby.GetTaskRegenEntries(), len(active.GetTaskRegenEntries()))
	s.InDelta(float64(startDelay), float64(standby.GetAccumulatedSkippedDuration().AsDuration()), float64(time.Minute),
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
		accumTol   = time.Minute
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
