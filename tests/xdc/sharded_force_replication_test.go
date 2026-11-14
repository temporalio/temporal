package xdc

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ShardedForceReplicationSuite struct{ xdcBaseSuite }

func TestShardedForceReplicationSuite(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("transitionHistory=%t", enabled), func(t *testing.T) {
			s := &ShardedForceReplicationSuite{}
			s.enableTransitionHistory = enabled
			suite.Run(t, s)
		})
	}
}

func (s *ShardedForceReplicationSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.HistoryCountSuggestContinueAsNew.Key(): 20,
	}
	s.setupSuite(testcore.WithNumHistoryShards(4))
}
func (s *ShardedForceReplicationSuite) SetupTest()     { s.setupTest() }
func (s *ShardedForceReplicationSuite) TearDownSuite() { s.tearDownSuite() }

func (s *ShardedForceReplicationSuite) TestMultipleListingRanges() {
	t := s.T()
	ctx := t.Context()
	ns := s.createNamespaceInCluster0(true)
	source := s.clusters[0].FrontendClient()
	target := s.clusters[1].FrontendClient()
	const count = 48
	executions := make([]*commonpb.WorkflowExecution, 0, count)
	// Create histories before adding the destination so ordinary replication
	// cannot mask an execution omitted by the force-replication scan.
	for i := range count {
		id := fmt.Sprintf("workflow-%02d", i)
		started, err := source.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace: ns, WorkflowId: id, RequestId: id,
			WorkflowType:             &commonpb.WorkflowType{Name: "unpolled"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "unpolled"},
			WorkflowExecutionTimeout: durationpb.New(time.Hour),
		})
		require.NoError(t, err)
		execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: started.RunId}
		_, err = source.TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: ns, WorkflowExecution: execution, Reason: "source fixture",
		})
		require.NoError(t, err)
		executions = append(executions, execution)
	}
	s.waitForVisibilityCount(ctx, ns, count)
	s.waitForClusterSynced()
	s.updateNamespaceClusters(ns, 0, s.clusters)
	for _, execution := range executions {
		_, err := target.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns, Execution: execution,
		})
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound, "fixture must require force replication")
	}
	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort: s.clusters[0].Host().FrontendGRPCAddress(), Namespace: primitives.SystemLocalNamespace,
	})
	require.NoError(t, err)
	defer client.Close()
	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: "sharded-multiple-ranges", TaskQueue: primitives.MigrationShardedActivityTQ,
	}, "force-replication-sharded", migration.ShardedForceReplicationParams{
		Namespace: ns, TargetClusterName: s.clusters[1].ClusterName(),
		BatchSize: 10, MaxExecsPerShard: 5, ListWorkflowsPageSize: 2,
		ConcurrentBatchCount: 2,
	})
	require.NoError(t, err)
	firstRunID := run.GetRunID()
	require.NoError(t, run.Get(ctx, nil))

	children, continuations := 0, 0
	var verified int64
	for runID := firstRunID; runID != ""; {
		iter := client.GetWorkflowHistory(ctx, run.GetID(), runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		nextRunID, liveChildren := "", 0
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)
			switch event.EventType {
			case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
				if event.GetChildWorkflowExecutionStartedEventAttributes().WorkflowType.Name == "force-replication-sharded-worker" {
					liveChildren++
					children++
					require.Equal(t, 1, liveChildren, "listing ranges must not overlap")
				}
			case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
				attrs := event.GetChildWorkflowExecutionCompletedEventAttributes()
				if attrs.WorkflowType.Name == "force-replication-sharded-worker" {
					liveChildren--
					var result struct{ VerifiedCount int64 }
					require.NoError(t, payloads.Decode(attrs.Result, &result))
					verified += result.VerifiedCount
				}
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
				continuations++
				nextRunID = event.GetWorkflowExecutionContinuedAsNewEventAttributes().NewExecutionRunId
			default:
				continue
			}
		}
		require.Zero(t, liveChildren)
		runID = nextRunID
	}
	require.Greater(t, children, 1, "must exercise child range transitions")
	require.Positive(t, continuations, "must exercise parent continue-as-new")
	require.Equal(t, int64(count), verified)
	t.Logf("replicated %d runs through %d children and %d parent continuations", count, children, continuations)
	for _, execution := range executions {
		sourceHistory, err := source.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns, Execution: execution,
		})
		require.NoError(t, err)
		targetHistory, err := target.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns, Execution: execution,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, sourceHistory.History, targetHistory.History)
		require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			targetHistory.History.Events[len(targetHistory.History.Events)-1].EventType)
	}
}
