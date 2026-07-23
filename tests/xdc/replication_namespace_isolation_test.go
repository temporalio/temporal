package xdc

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
)

// controllableNamespaceThrottler is a test NamespaceThrottler whose throttled set is
// controlled by the test. It reports every namespace the test marks as throttled, so
// the sender peels that namespace's HIGH-lane (live) traffic onto a dedicated severity
// tier.
type controllableNamespaceThrottler struct {
	mu        sync.Mutex
	throttled map[string]struct{}
}

func newControllableNamespaceThrottler() *controllableNamespaceThrottler {
	return &controllableNamespaceThrottler{throttled: make(map[string]struct{})}
}

func (t *controllableNamespaceThrottler) RecordTask(string) {}

func (t *controllableNamespaceThrottler) ThrottledNamespaceIDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.throttled))
	for ns := range t.throttled {
		out = append(out, ns)
	}
	return out
}

func (t *controllableNamespaceThrottler) CatchupQPS(string) float64 { return 0 }

func (t *controllableNamespaceThrottler) throttle(namespaceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.throttled[namespaceID] = struct{}{}
}

var _ replication.NamespaceThrottler = (*controllableNamespaceThrottler)(nil)

type replicationNamespaceIsolationTestSuite struct {
	xdcBaseSuite
	throttler *controllableNamespaceThrottler
}

func TestReplicationNamespaceIsolationTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(replicationNamespaceIsolationTestSuite))
}

func (s *replicationNamespaceIsolationTestSuite) SetupSuite() {
	s.throttler = newControllableNamespaceThrottler()
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():                   true,
		dynamicconfig.EnableReplicationTaskTieredProcessing.Key():     true,
		dynamicconfig.EnableReplicationReaderGroup.Key():              true,
		dynamicconfig.EnableReplicationNamespaceIsolation.Key():       true,
		dynamicconfig.ReplicationStreamSenderThrottledLaneCount.Key(): 2,
		// Don't artificially slow the throttled tier in the test — we assert correctness
		// (no data loss), not pacing.
		dynamicconfig.ReplicationStreamSenderCatchupQPSRatio.Key(): 1.0,
	}
	s.logger = log.NewTestLogger()
	s.setupSuite(
		// Replace the default Noop throttler with one the test controls, so isolation
		// actually engages for a chosen namespace.
		testcore.WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(func(replication.NamespaceThrottler) replication.NamespaceThrottler {
				return s.throttler
			}),
		),
	)
}

func (s *replicationNamespaceIsolationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *replicationNamespaceIsolationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestThrottledNamespaceLiveReplicationUsesTier verifies that with namespace isolation
// enabled and one namespace marked throttled, that namespace's HIGH-priority *live*
// replication is peeled onto a dedicated severity tier, while a normal namespace stays
// on the default HIGH lane — and BOTH namespaces' workflows replicate to the standby
// with no data loss. This exercises the end-to-end HIGH isolation path: capability
// negotiation, tier split at the applied watermark, rate-limited tier draining, and
// gap-safe ordered-event coverage.
func (s *replicationNamespaceIsolationTestSuite) TestThrottledNamespaceLiveReplicationUsesTier() {
	testCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Capture sender-side (cluster 0) metrics for the whole test.
	senderMetrics, ok := s.clusters[0].Host().GetMetricsHandler().(*metricstest.CaptureHandler)
	s.True(ok, "expected a capturing metrics handler on the active cluster")
	capture := senderMetrics.StartCapture()
	defer senderMetrics.StopCapture(capture)

	// Global namespaces present in BOTH clusters: workflows run in cluster 0 replicate
	// live to the standby over the HIGH lane (a stream of ordered HistoryReplication
	// tasks) — exactly the traffic HIGH isolation acts on.
	throttledNS := s.createGlobalNamespace()
	normalNS := s.createGlobalNamespace()

	// Mark throttledNS as throttled, then wait until the sender has actually peeled it
	// onto a tier BEFORE running its workflow — so its live history cannot sneak through
	// the default HIGH lane ahead of the throttle taking effect.
	s.throttler.throttle(s.getNamespaceID(testCtx, throttledNS))
	s.waitForTierEngaged(capture)

	// Run and complete a workflow in each namespace on the active cluster. Their history
	// events replicate live: throttledNS over its tier lane, normalNS over the default
	// HIGH lane.
	throttledRun := s.runCompletedWorkflow(testCtx, throttledNS, "iso-throttled-wf")
	normalRun := s.runCompletedWorkflow(testCtx, normalNS, "iso-normal-wf")

	// The throttled namespace's live replication must have been sent on a throttled tier
	// lane, not the default HIGH lane.
	s.assertTierCarriedSends(capture)

	// Both workflows must be present on the standby — proving the throttled namespace's
	// tiered HIGH-lane replication was delivered in order with no data loss.
	s.verifyReplicated(testCtx, throttledNS, "iso-throttled-wf", throttledRun)
	s.verifyReplicated(testCtx, normalNS, "iso-normal-wf", normalRun)
}

// waitForTierEngaged blocks until the sender reports at least one namespace isolated in a
// throttled tier (the replication_stream_sender_throttled_namespace_count gauge).
func (s *replicationNamespaceIsolationTestSuite) waitForTierEngaged(capture *metricstest.Capture) {
	s.Eventually(func() bool {
		for _, rec := range capture.Snapshot()["replication_stream_sender_throttled_namespace_count"] {
			if !strings.HasPrefix(rec.Tags["replicationStreamLane"], "tier-") {
				continue
			}
			if v, ok := rec.Value.(float64); ok && v >= 1 {
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "sender should peel the throttled namespace onto a tier")
}

// assertTierCarriedSends asserts that tasks were sent on a throttled tier lane. Only the
// throttled namespace is a tier member, so any tier-lane send is its live traffic.
func (s *replicationNamespaceIsolationTestSuite) assertTierCarriedSends(capture *metricstest.Capture) {
	tierSends := 0
	for _, rec := range capture.Snapshot()["replication_tasks_send"] {
		if strings.HasPrefix(rec.Tags["replicationStreamLane"], "tier-") {
			tierSends++
		}
	}
	s.Positive(tierSends, "throttled namespace's live replication should be sent on a tier lane, not the default HIGH lane")
}

func (s *replicationNamespaceIsolationTestSuite) getNamespaceID(ctx context.Context, ns string) string {
	resp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.NoError(err)
	return resp.NamespaceInfo.GetId()
}

func (s *replicationNamespaceIsolationTestSuite) runCompletedWorkflow(ctx context.Context, ns, wfID string) string {
	taskQueue := wfID + "-tq"
	client, worker := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), ns, taskQueue, wfID+"-worker")
	wfFn := func(workflow.Context) error { return nil }
	worker.RegisterWorkflow(wfFn)
	s.NoError(worker.Start())
	defer worker.Stop()

	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 wfID,
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 30 * time.Second,
	}, wfFn)
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
	return run.GetRunID()
}

func (s *replicationNamespaceIsolationTestSuite) verifyReplicated(ctx context.Context, ns, wfID, expectedRunID string) {
	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[1].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	defer client.Close()

	s.Eventually(func() bool {
		desc, err := client.DescribeWorkflowExecution(ctx, wfID, "")
		if err != nil {
			return false
		}
		return desc.WorkflowExecutionInfo.Execution.RunId == expectedRunID &&
			desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 500*time.Millisecond, "workflow %s should replicate to standby", wfID)
}
