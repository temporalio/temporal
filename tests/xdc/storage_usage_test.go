package xdc

import (
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

// TestStorageUsagePerCluster checks that each cluster (active and passive) accrues its own stored
// byte-seconds counter instead of copying the active cluster's, that each cluster's stored counter
// matches the byte-seconds metrics it emitted, and that after failover the new active cluster keeps
// accruing from its own value.
func (s *FunctionalClustersTestSuite) TestStorageUsagePerCluster() {
	const holdTime = 1100 * time.Millisecond
	ns := s.createGlobalNamespace()
	workflowID := testcore.RandomizeStr("storage-usage")

	captures := make([]*metricstest.Capture, len(s.clusters))
	for i, c := range s.clusters {
		captureHandler, ok := c.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
		s.Require().True(ok, "cluster metrics handler does not support capture")
		capture := captureHandler.StartCapture()
		captures[i] = capture
		s.T().Cleanup(func() { captureHandler.StopCapture(capture) })
	}
	msByteSecondsMetric := func(clusterIdx int) int64 {
		var total int64
		for _, r := range captures[clusterIdx].SnapshotMetric(metrics.MutableStateByteSeconds.Name()) {
			if r.Tags["namespace"] == ns {
				total += r.Value.(int64)
			}
		}
		return total
	}
	describe := func(t require.TestingT, clusterIdx int, runID string) *persistencespb.StorageUsage {
		resp, err := s.clusters[clusterIdx].AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			Archetype: chasm.WorkflowArchetype,
		})
		require.NoError(t, err)
		return resp.GetDatabaseMutableState().GetExecutionInfo().GetStorageUsage()
	}
	// waitReplicated waits until the passive cluster has applied every event of the active cluster, so
	// each transition is applied (and accrues locally) on the passive cluster before the next one.
	waitReplicated := func(runID string) {
		nextEventID := func(t require.TestingT, clusterIdx int) int64 {
			resp, err := s.clusters[clusterIdx].AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				Archetype: chasm.WorkflowArchetype,
			})
			require.NoError(t, err)
			return resp.GetDatabaseMutableState().GetNextEventId()
		}
		expected := nextEventID(s.T(), 0)
		await.Require(s.T().Context(), s.T(), func(t *await.T) {
			require.Equal(t, expected, nextEventID(t, 1))
		}, replicationWaitTime, replicationCheckInterval)
	}
	signal := func(clusterIdx int, runID string) {
		_, err := s.clusters[clusterIdx].FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         ns,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			SignalName:        "storage-usage-signal",
		})
		s.NoError(err)
	}

	startResp, err := s.clusters[0].FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    ns,
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "storage-usage-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: testcore.RandomizeStr("storage-usage-tq"), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	runID := startResp.GetRunId()
	waitReplicated(runID)
	for range 2 {
		time.Sleep(holdTime) //nolint:forbidigo // byte-seconds accrue on wall-clock time between transitions
		signal(0, runID)
		waitReplicated(runID)
	}

	// Each cluster accrues its own counter, with either replication mode, and its stored counter is
	// the sum of the metric deltas it emitted.
	for clusterIdx := range 2 {
		await.Require(s.T().Context(), s.T(), func(t *await.T) {
			usage := describe(t, clusterIdx, runID)
			require.Positive(t, usage.GetMutableStateByteSeconds())
			require.Positive(t, usage.GetHistoryByteSeconds())
			require.Equal(t, usage.GetMutableStateByteSeconds(), msByteSecondsMetric(clusterIdx))
		}, replicationWaitTime, replicationCheckInterval)
	}
	activeUsage := describe(s.T(), 0, runID)
	passiveUsage := describe(s.T(), 1, runID)
	// Same bytes held for about the same time, offset by replication lag.
	s.GreaterOrEqual(passiveUsage.GetMutableStateByteSeconds()*4, activeUsage.GetMutableStateByteSeconds())

	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)
	beforeFailover := describe(s.T(), 1, runID)
	time.Sleep(holdTime) //nolint:forbidigo // byte-seconds accrue on wall-clock time between transitions
	signal(1, runID)

	afterFailover := describe(s.T(), 1, runID)
	s.Greater(afterFailover.GetMutableStateByteSeconds(), beforeFailover.GetMutableStateByteSeconds())
	s.Greater(afterFailover.GetHistoryByteSeconds(), beforeFailover.GetHistoryByteSeconds())
	// The new active continues from its own value.
	s.GreaterOrEqual(beforeFailover.GetMutableStateByteSeconds(), passiveUsage.GetMutableStateByteSeconds())
}
