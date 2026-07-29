package xdc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This suite contains tests of scenarios in which conflicting histories arise during history replication. To do that we
// need to create "split-brain" sitauations in which both clusters believe they are active, and to do that, we need to
// control when history and namespace event replication tasks are executed. This is achieved using injection approaches
// based on those in tests/xdc/history_replication_dlq_test.go.

type (
	// The suite creates two clusters. We use injection to create history and namespace replication task executors
	// that push their tasks into test-specific (i.e. workflow-specific) buffers.
	hrsuTestSuite struct {
		xdcBaseSuite
	}
	// Each test starts its own workflow, in its own namespace.
	hrsuTest struct {
		tv       *testvars.TestVars
		cluster1 hrsuTestCluster
		cluster2 hrsuTestCluster
		s        *hrsuTestSuite
	}
	hrsuTestCluster struct {
		testCluster *testcore.TestCluster
		client      sdkclient.Client
		// Per-test, per-cluster buffers of replication tasks.
		namespaceReplicationTasks      chan *hrsuNamespaceReplicationTask
		inboundHistoryReplicationTasks chan *hrsuTestExecutableTask
		t                              *hrsuTest
	}
	hrsuNamespaceReplicationTask struct {
		task    *replicationspb.NamespaceTaskAttributes
		execute func() error
	}
	// Used to inject a modified history event replication task executor.
	hrsuTestExecutableTask struct {
		replicationTask *replicationspb.ReplicationTask
		execute         func() error
		result          chan error
	}
)

const (
	taskBufferCapacity = 100
)

func TestHistoryReplicationSignalsAndUpdatesTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(hrsuTestSuite))
}

func (s *hrsuTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key(): true,
		// Use short interval to make long poll timeout
		dynamicconfig.HistoryLongPollExpirationInterval.Key(): 100 * time.Millisecond,
	}
	s.logger = log.NewTestLogger()
	s.setupSuite()
}

func (s *hrsuTestSuite) SetupTest() {
	s.setupTest()
}

func (s *hrsuTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *hrsuTestSuite) startHrsuTest() (*hrsuTest, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	tv := testvars.New(s.T())
	ns := tv.NamespaceName().String()
	t := hrsuTest{tv: tv, s: s}
	t.cluster1 = t.newHrsuTestCluster(ns, s.clusters[0])
	t.cluster2 = t.newHrsuTestCluster(ns, s.clusters[1])
	t.registerMultiRegionNamespace(ctx)
	return &t, ctx, cancel
}

func (t *hrsuTest) newHrsuTestCluster(ns string, cluster *testcore.TestCluster) hrsuTestCluster {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  cluster.Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(t.s.logger),
	})
	t.s.NoError(err)

	c := hrsuTestCluster{
		testCluster:                    cluster,
		client:                         sdkClient,
		namespaceReplicationTasks:      make(chan *hrsuNamespaceReplicationTask, taskBufferCapacity),
		inboundHistoryReplicationTasks: make(chan *hrsuTestExecutableTask, taskBufferCapacity),
		t:                              t,
	}

	// Buffer namespace replication tasks so tests can inspect and release them in a controlled order.
	cluster.InjectHook(
		t.s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			c.namespaceReplicationTasks <- &hrsuNamespaceReplicationTask{
				task:    task,
				execute: execute,
			}
			return nil
		}),
		namespace.Name(ns),
	)

	// Buffer history replication tasks so tests can inspect and release them in a controlled order.
	cluster.InjectHook(
		t.s.T(),
		testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, func(task *replicationspb.ReplicationTask, execute func() error) error {
			historyTask := &hrsuTestExecutableTask{
				replicationTask: task,
				execute:         execute,
				result:          make(chan error),
			}
			c.inboundHistoryReplicationTasks <- historyTask
			return <-historyTask.result
		}),
		testhooks.GlobalScope,
	)
	return c
}

// TestAcceptedUpdateCanBeCompletedAfterFailoverAndFailback tests that an update can be accepted in one cluster, and completed in a
// different cluster, after a failover.
func (s *hrsuTestSuite) TestAcceptedUpdateCanBeCompletedAfterFailoverAndFailback() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	// Cluster0 is active initially. We start an update in cluster0, run it through to acceptance, and replicate the
	// history to cluster1. Then we failover to cluster1 (where the update registry is empty) and confirm that the update
	// can be completed in the new active cluster.
	t.startAndAcceptUpdateInCluster1ThenFailoverTo2AndCompleteUpdate(ctx)
	// Finally, we start an update in cluster1, run it through to acceptance, failover back to cluster0 (which already
	// has an update registry from before the failover), and confirm that the update can be completed in cluster0.
	t.startAndAcceptUpdateInCluster2ThenFailoverTo1AndCompleteUpdate(ctx)
}

// TODO test failover before replication

func (s *hrsuTestSuite) TestUpdateCompletedAfterFailoverCannotBeCompletedAgainAfterFailback() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })
	// Cluster0 is active initially. We start an update in cluster0, run it through to acceptance, and replicate the
	// history to cluster1. Then we failover to cluster1 (where the update registry is empty) and confirm that the update
	// can be completed in the new active cluster.
	t.startAndAcceptUpdateInCluster1ThenFailoverTo2AndCompleteUpdate(ctx)
	// Now we fail back to cluster0. When this cluster was last active this update was in accepted state but,
	// nevertheless, it should not be possible to complete it, since it is already completed.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)
	t.failover2To1(ctx)
	s.NoError(t.cluster1.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster1-signal"))
	s.Error(t.cluster1.pollAndCompleteUpdate("cluster1-update-id"))
}

// TestConflictResolutionReappliesSignals creates a split-brain scenario in which both clusters believe they are active.
// Both clusters then accept a signal and write it to their own history, and the test confirms that the signal is
// reapplied during the resulting conflict resolution process.
func (s *hrsuTestSuite) TestConflictResolutionReappliesSignals() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	t.enterSplitBrainState(ctx)

	// Both clusters now believe they are active and hence both will accept a signal.

	// Send signals
	s.NoError(t.cluster1.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster1-signal"))
	s.NoError(t.cluster2.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster2-signal"))

	// cluster1 has accepted a signal
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v1 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster1-signal\""}]}}
	`, t.cluster1.getHistory(ctx))

	// cluster2 has also accepted a signal (with failover version 2 since it is endogenous to cluster1)
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	`, t.cluster2.getHistory(ctx))

	// Execute pending history replication tasks. Each cluster sends its signal to the other, but these have the same
	// event ID; this conflict is resolved by reapplying one of the signals after the other.

	// cluster2 sends its signal to cluster1. Since it has a higher failover version, it supersedes the endogenous
	// signal in cluster1.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	`, t.cluster1.getHistory(ctx))

	// cluster1 sends its signal to cluster2. Since it has a lower failover version, it is reapplied after the
	// endogenous cluster1 signal.
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	4 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster1-signal\""}]}}
	`, t.cluster2.getHistory(ctx))

	// Cluster2 sends the reapplied signal to cluster1, bringing the cluster histories into agreement.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.Equal(t.cluster1.getHistory(ctx), t.cluster2.getHistory(ctx))
}

// TestConflictResolutionReappliesUpdates creates a split-brain scenario in which both clusters believe they are active.
// Both clusters then accept an update and write it to their own history, and the test confirms that the update is
// reapplied during the resulting conflict resolution process.
func (s *hrsuTestSuite) TestConflictResolutionReappliesUpdates() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	cluster1UpdateId := "cluster1-update-id"
	cluster2UpdateId := "cluster2-update-id"

	t.enterSplitBrainStateAndAcceptUpdatesInBothClusters(ctx, cluster1UpdateId, cluster2UpdateId)
	// Execute pending history replication tasks. Each cluster sends its update to the other, triggering conflict
	// resolution.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)

	// cluster1 has received an update with failover version 2 which superseded its own update.
	s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	`, cluster2UpdateId), t.cluster1.getHistory(ctx))

	// cluster2 has reapplied the accepted update from cluster0 on top of its own update, changing it from state
	// Accepted to state Admitted, since it must be submitted to the validator on the new branch.
	s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	6 v2 WorkflowExecutionUpdateAdmitted {"Request": {"Meta": {"UpdateId": "%s"}, "Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
	7 v2 WorkflowTaskScheduled
	`, cluster2UpdateId, cluster1UpdateId), t.cluster2.getHistory(ctx))

	// Cluster2 sends the reapplied update to cluster1, bringing the cluster histories into agreement.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED)
	s.Equal(t.cluster1.getHistory(ctx), t.cluster2.getHistory(ctx))

	s.NoError(t.cluster2.pollAndCompleteUpdate(cluster2UpdateId))
	s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	6 v2 WorkflowExecutionUpdateAdmitted {"Request": {"Meta": {"UpdateId": "%s"}, "Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
	7 v2 WorkflowTaskScheduled
	8 v2 WorkflowTaskStarted
	9 v2 WorkflowTaskCompleted
   10 v2 WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "%s"}}
  `, cluster2UpdateId, cluster1UpdateId, cluster2UpdateId), t.cluster2.getHistory(ctx))
}

// TestConflictResolutionDoesNotReapplyAcceptedUpdateWithConflictingId creates a split-brain scenario in which both
// clusters believe they are active. Both clusters then accept an update and write it to their own history, but those
// updates have the same update ID. The test confirms that when the conflict is resolved, we do not reapply the
// UpdateAccepted event, since it has a conflicting ID.
func (s *hrsuTestSuite) TestConflictResolutionDoesNotReapplyAcceptedUpdateWithConflictingId() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	// Both clusters accept an update with the same ID.
	t.enterSplitBrainStateAndAcceptUpdatesInBothClusters(ctx, "update-id", "update-id")
	// Execute pending history replication tasks. Each cluster sends its update to the other, triggering conflict
	// resolution.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)

	// Cluster1 has received an accepted update with failover version 2, which superseded its own update. Cluster2 has
	// received an accepted update from cluster0 with a lower failover version. Normally, such an update would be
	// reapplied. But since it has the same update ID as the cluster0 update, and since that update is not completed,
	// we must not reapply it. The result is that both clusters have the same history; the update accepted in cluster0
	// has been dropped.
	for _, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		t.s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	`, c.getHistory(ctx))
	}
}

// TestConflictResolutionDoesNotReapplyCompleteUpdateWithConflictingId creates a split-brain scenario in which both
// clusters believe they are active. Both clusters then accept and *complete* an update and write it to their own history, but those
// updates have the same update ID. The test confirms that when the conflict is resolved, we do not reapply the
// UpdateAccepted and UpdateCompleted event, since it has a conflicting ID.
// Same as above but for completed Updates.
func (s *hrsuTestSuite) TestConflictResolutionDoesNotReapplyCompleteUpdateWithConflictingId() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	// Both clusters accept and complete an update with the same ID.
	t.enterSplitBrainStateAndCompletedUpdatesInBothClusters(ctx, "update-id", "update-id")
	// Execute pending history replication tasks. Each cluster sends its update to the other, triggering conflict
	// resolution.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)

	// Cluster1 has received an accepted update with failover version 2, which superseded its own update. Cluster2 has
	// received an accepted update from cluster0 with a lower failover version. Normally, such an update would be
	// reapplied. But since it has the same update ID as the cluster0 update, and since that update is not completed,
	// we must not reapply it. The result is that both clusters have the same history; the update accepted in cluster0
	// has been dropped.
	for _, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		t.s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	6 v2 WorkflowExecutionUpdateCompleted {"Meta":{"UpdateId":"update-id"}}
		`, c.getHistory(ctx))
	}
}

// TestConflictResolutionDoesNotReapplyAdmittedUpdateWithConflictingId creates a split-brain scenario in which both
// clusters believe they are active. Both clusters then accept an update and write it to their own history, but those
// updates have the same update ID. This time however, we perform a WorkflowReset in one of the clusters, creating an
// UpdateAdmitted event. The test confirms that when the conflict is resolved, we do not reapply the UpdateAdmitted
// event, since it has a conflicting ID.
func (s *hrsuTestSuite) TestConflictResolutionDoesNotReapplyAdmittedUpdateWithConflictingId() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	// Both clusters accept an update with the same ID.
	t.enterSplitBrainStateAndAcceptUpdatesInBothClusters(ctx, "update-id", "update-id")
	for i, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		clusterId := i + 1
		t.s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v%[1]d WorkflowTaskStarted
	4 v%[1]d WorkflowTaskCompleted
	5 v%[1]d WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster%[1]d-update-input\""}]}}}}
	`, clusterId), c.getHistory(ctx))
	}
	// Perform a reset in each cluster; this converts the UpdateAccepted events to UpdateAdmitted events.
	workflowTaskCompletedId := 4
	var resetRunIds []string
	for i, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		clusterId := i + 1
		resetRunIds = append(resetRunIds, c.resetWorkflow(ctx, int64(workflowTaskCompletedId)))
		t.s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v%[1]d WorkflowTaskStarted
	4 v%[1]d WorkflowTaskFailed
	5 v%[1]d WorkflowExecutionUpdateAdmitted {"Request": {"Meta": {"UpdateId": "update-id"}, "Input": {"Args": {"Payloads": [{"Data": "\"cluster%[1]d-update-input\""}]}}}}
	6 v%[1]d WorkflowTaskScheduled
	`, clusterId), c.getHistoryForRunId(ctx, resetRunIds[i]))
	}
	// Execute pending history replication tasks. Each cluster sends its update to the other, triggering conflict
	// resolution.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED)
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED)

	// Cluster1 has the higher failover version, so its history branch is chosen in the conflict resolution.
	activeRunId := resetRunIds[1]

	for _, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		// Cluster1 has received an admitted update with failover version 2, which superseded its own update. Cluster2 has
		// received an admitted update from cluster0 with a lower failover version. Normally, such an update would be
		// reapplied. But since it has the same update ID as the cluster0 update, and since that update is not completed,
		// we must not reapply it. The result is that both clusters have the same history; the update admitted in cluster0
		// has been dropped.
		t.s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskFailed
	5 v2 WorkflowExecutionUpdateAdmitted {"Request": {"Meta": {"UpdateId": "update-id"}, "Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	6 v2 WorkflowTaskScheduled
	`, c.getHistoryForRunId(ctx, activeRunId))
	}
}

// Start update in cluster0, run it through to acceptance, replicate it to cluster1, then failover to 2 and complete
// the update there.
func (t *hrsuTest) startAndAcceptUpdateInCluster1ThenFailoverTo2AndCompleteUpdate(ctx context.Context) {
	t.cluster1.sendUpdateAndWaitUntilStage(ctx, "cluster1-update-id", "cluster1-update-input", sdkclient.WorkflowUpdateStageAccepted)
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)

	for _, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		t.s.EqualHistoryEvents(`
		1 v1 WorkflowExecutionStarted
		2 v1 WorkflowTaskScheduled
		3 v1 WorkflowTaskStarted
		4 v1 WorkflowTaskCompleted
		5 v1 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "cluster1-update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
		`, c.getHistory(ctx))
	}

	t.failover1To2(ctx)

	// This test does not explicitly model the update handler, but since the update has been accepted yet not completed,
	// the handler must have scheduled something (e.g. a timer, an activity, a child workflow), and we need to do
	// something to create another WorkflowTaskScheduled event, so that the worker can send the update completion
	// message. We use a signal for that purpose.
	t.s.NoError(t.cluster2.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster2-signal"))

	// Complete the update in  cluster1 after the failover.
	t.s.NoError(t.cluster2.pollAndCompleteUpdate("cluster1-update-id"))

	t.s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v1 WorkflowTaskStarted
	4 v1 WorkflowTaskCompleted
	5 v1 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "cluster1-update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
	6 v2 WorkflowExecutionSignaled
	7 v2 WorkflowTaskScheduled
	8 v2 WorkflowTaskStarted
	9 v2 WorkflowTaskCompleted
   10 v2 WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "cluster1-update-id"}}
	`, t.cluster2.getHistory(ctx))
}

// Run an update in cluster1 to Accepted state, failover to cluster0, and confirm that it can be completed in cluster0.
func (t *hrsuTest) startAndAcceptUpdateInCluster2ThenFailoverTo1AndCompleteUpdate(ctx context.Context) {
	t.cluster2.sendUpdateAndWaitUntilStage(ctx, "cluster2-update-id", "cluster2-update-input", sdkclient.WorkflowUpdateStageAccepted)
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)

	for _, c := range []hrsuTestCluster{t.cluster1, t.cluster2} {
		t.s.EqualHistoryEvents(`
		1 v1 WorkflowExecutionStarted
		2 v1 WorkflowTaskScheduled
		3 v1 WorkflowTaskStarted
		4 v1 WorkflowTaskCompleted
		5 v1 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "cluster1-update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
		6 v2 WorkflowExecutionSignaled
		7 v2 WorkflowTaskScheduled
		8 v2 WorkflowTaskStarted
		9 v2 WorkflowTaskCompleted
	   10 v2 WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "cluster1-update-id"}}
	   11 v2 WorkflowTaskScheduled
	   12 v2 WorkflowTaskStarted
	   13 v2 WorkflowTaskCompleted
	   14 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "cluster2-update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	   `, c.getHistory(ctx))
	}

	t.failover2To1(ctx)

	// As above, send a signal to create a WorkflowTaskScheduled event.
	t.s.NoError(t.cluster1.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster1-signal"))
	t.s.NoError(t.cluster1.pollAndCompleteUpdate("cluster2-update-id"))

	t.s.EqualHistoryEvents(`
	1  v1 WorkflowExecutionStarted
	2  v1 WorkflowTaskScheduled
	3  v1 WorkflowTaskStarted
	4  v1 WorkflowTaskCompleted
	5  v1 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "cluster1-update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
	6  v2 WorkflowExecutionSignaled
	7  v2 WorkflowTaskScheduled
	8  v2 WorkflowTaskStarted
	9  v2 WorkflowTaskCompleted
   10  v2 WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "cluster1-update-id"}}
   11  v2 WorkflowTaskScheduled
   12  v2 WorkflowTaskStarted
   13  v2 WorkflowTaskCompleted
   14  v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "cluster2-update-id", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
   15 v11 WorkflowExecutionSignaled
   16 v11 WorkflowTaskScheduled
   17 v11 WorkflowTaskStarted
   18 v11 WorkflowTaskCompleted
   19 v11 WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "cluster2-update-id"}}
   `, t.cluster1.getHistory(ctx))
}

func (t *hrsuTest) enterSplitBrainStateAndAcceptUpdatesInBothClusters(ctx context.Context, cluster1UpdateId, cluster2UpdateId string) {
	t.enterSplitBrainState(ctx)

	// Both clusters now believe they are active and hence both will accept an update.

	// Send updates
	t.cluster1.sendUpdateAndWaitUntilStage(ctx, cluster1UpdateId, "cluster1-update-input", sdkclient.WorkflowUpdateStageAccepted)
	t.cluster2.sendUpdateAndWaitUntilStage(ctx, cluster2UpdateId, "cluster2-update-input", sdkclient.WorkflowUpdateStageAccepted)

	// cluster1 has accepted an update
	t.s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v1 WorkflowTaskStarted
	4 v1 WorkflowTaskCompleted
	5 v1 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
	`, cluster1UpdateId), t.cluster1.getHistory(ctx))

	// cluster2 has also accepted an update (events have failover version 2 since they are endogenous to cluster1)
	t.s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	`, cluster2UpdateId), t.cluster2.getHistory(ctx))
}

func (t *hrsuTest) enterSplitBrainStateAndCompletedUpdatesInBothClusters(ctx context.Context, cluster1UpdateId, cluster2UpdateId string) {
	t.enterSplitBrainState(ctx)

	// Both clusters now believe they are active and hence both will accept and complete an update.

	// Send updates
	t.cluster1.sendUpdateAndWaitUntilStage(ctx, cluster1UpdateId, "cluster1-update-input", sdkclient.WorkflowUpdateStageCompleted)
	t.cluster2.sendUpdateAndWaitUntilStage(ctx, cluster2UpdateId, "cluster2-update-input", sdkclient.WorkflowUpdateStageCompleted)

	// cluster1 has completed an update
	t.s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v1 WorkflowTaskStarted
	4 v1 WorkflowTaskCompleted
	5 v1 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster1-update-input\""}]}}}}
	6 v1 WorkflowExecutionUpdateCompleted {"Meta":{"UpdateId":"%[1]s"}}
	`, cluster1UpdateId), t.cluster1.getHistory(ctx))

	// cluster2 has also completed an update (events have failover version 2 since they are endogenous to cluster1)
	t.s.EqualHistoryEvents(fmt.Sprintf(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowTaskStarted
	4 v2 WorkflowTaskCompleted
	5 v2 WorkflowExecutionUpdateAccepted {"ProtocolInstanceId": "%s", "AcceptedRequest": {"Input": {"Args": {"Payloads": [{"Data": "\"cluster2-update-input\""}]}}}}
	6 v2 WorkflowExecutionUpdateCompleted {"Meta":{"UpdateId":"%[1]s"}}
	`, cluster2UpdateId), t.cluster2.getHistory(ctx))
}

// TODO (alex): replace this with t.s.failover()
func (t *hrsuTest) failover1To2(ctx context.Context) {
	t.s.Equal([]string{t.s.clusters[0].ClusterName(), t.s.clusters[0].ClusterName()}, t.getActiveClusters(ctx))
	t.cluster1.setActive(ctx, t.s.clusters[1].ClusterName())
	t.s.Equal([]string{t.s.clusters[1].ClusterName(), t.s.clusters[0].ClusterName()}, t.getActiveClusters(ctx))

	time.Sleep(testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	t.cluster2.executeNamespaceReplicationTasksUntil(enumsspb.NAMESPACE_OPERATION_UPDATE)
	// Wait for active cluster to be changed in namespace registry entry.
	// TODO (dan) It would be nice to find a better approach.
	time.Sleep(testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo
	t.s.Equal([]string{t.s.clusters[1].ClusterName(), t.s.clusters[1].ClusterName()}, t.getActiveClusters(ctx))
}

func (t *hrsuTest) failover2To1(ctx context.Context) {
	t.s.Equal([]string{t.s.clusters[1].ClusterName(), t.s.clusters[1].ClusterName()}, t.getActiveClusters(ctx))
	t.cluster1.setActive(ctx, t.s.clusters[0].ClusterName())
	t.s.Equal([]string{t.s.clusters[0].ClusterName(), t.s.clusters[1].ClusterName()}, t.getActiveClusters(ctx))

	time.Sleep(testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	t.cluster2.executeNamespaceReplicationTasksUntil(enumsspb.NAMESPACE_OPERATION_UPDATE)
	// Wait for active cluster to be changed in namespace registry entry.
	// TODO (dan) It would be nice to find a better approach.
	time.Sleep(testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo
	t.s.Equal([]string{t.s.clusters[0].ClusterName(), t.s.clusters[0].ClusterName()}, t.getActiveClusters(ctx))
}

func (t *hrsuTest) enterSplitBrainState(ctx context.Context) {
	// We now create a "split brain" state by setting cluster2 to active. We do not execute namespace replication tasks
	// afterward, so cluster1 does not learn of the change.
	t.s.Equal([]string{t.s.clusters[0].ClusterName(), t.s.clusters[0].ClusterName()}, t.getActiveClusters(ctx))
	t.cluster2.setActive(ctx, t.s.clusters[1].ClusterName())
	t.s.Equal([]string{t.s.clusters[0].ClusterName(), t.s.clusters[1].ClusterName()}, t.getActiveClusters(ctx))

	// TODO (dan) Why do the tests still pass with this? Does this not remove the split-brain?
	// t.cluster1.executeNamespaceReplicationTasksUntil(enumsspb.NAMESPACE_OPERATION_UPDATE)

	// Wait for active cluster to be changed in namespace registry entry.
	// TODO (dan) It would be nice to find a better approach.
	time.Sleep(testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo
}

// executeNamespaceReplicationTasksUntil executes buffered namespace event replication tasks until the specified event
// type is encountered with the specified failover version.
func (c *hrsuTestCluster) executeNamespaceReplicationTasksUntil(operation enumsspb.NamespaceOperation) {
	for {
		bufferedTask := <-c.namespaceReplicationTasks
		err := bufferedTask.execute()
		c.t.s.NoError(err)
		if bufferedTask.task.NamespaceOperation == operation {
			return
		}
	}
}

// executeHistoryReplicationTasksUntil executes buffered history event replication tasks until the specified
// event type is encountered.
func (c *hrsuTestCluster) executeHistoryReplicationTasksUntil(
	eventType enumspb.EventType,
) {
	for {
		task := <-c.inboundHistoryReplicationTasks
		events := c.t.s.executeHistoryReplicationTask(task)
		for _, event := range events {
			if event.GetEventType() == eventType {
				return
			}
		}
	}
}

func (s *hrsuTestSuite) executeHistoryReplicationTask(task *hrsuTestExecutableTask) []*historypb.HistoryEvent {
	err := task.execute()
	s.NoError(err)
	task.result <- err
	attrs := task.replicationTask.GetHistoryTaskAttributes()
	s.NotNil(attrs)
	events, err := serialization.DefaultDecoder.DeserializeEvents(attrs.Events)
	s.NoError(err)
	return events
}

// Update test utilities
func (c *hrsuTestCluster) sendUpdateAndWaitUntilStage(ctx context.Context, updateId string, arg string, stage sdkclient.WorkflowUpdateStage) {
	updateErrCh := make(chan error)
	go func() {
		_, err := c.client.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			UpdateID:     updateId,
			WorkflowID:   c.t.tv.WorkflowID(),
			RunID:        c.t.tv.RunID(),
			UpdateName:   c.t.tv.Any().String(),
			Args:         []any{arg},
			WaitForStage: stage,
		})
		updateErrCh <- err
	}()

	// Wait admitted to make sure that Update reached server, and added to registry.
	// This guarantees following poll to get an Update request message.
	c.waitUpdateAdmitted(c.t.tv, updateId)

	// Blocks until the update request causes a WFT to be dispatched; then sends the update acceptance message
	// required for the update request to return.
	switch stage {
	case sdkclient.WorkflowUpdateStageCompleted:
		err := c.pollAndAcceptCompleteUpdate(updateId)
		c.t.s.NoError(err)
	case sdkclient.WorkflowUpdateStageAccepted:
		err := c.pollAndAcceptUpdate()
		c.t.s.NoError(err)
	default:
		c.t.s.FailNow("invalid stage", stage)
	}

	c.t.s.NoError(<-updateErrCh)
}

func (c *hrsuTestCluster) waitUpdateAdmitted(tv *testvars.TestVars, updateID string) {
	c.t.s.EventuallyWithTf(func(collectT *assert.CollectT) {
		pollResp, pollErr := c.testCluster.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: tv.NamespaceName().String(),
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: tv.WorkflowExecution(),
				UpdateId:          updateID,
			},
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED},
		})
		require.NoError(collectT, pollErr)
		// This is technically "at least Admitted".
		require.GreaterOrEqual(collectT, pollResp.GetStage(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED)
	}, 5*time.Second, 10*time.Millisecond, "update %s did not reach Admitted stage", updateID)
}

func (c *hrsuTestCluster) pollAndAcceptUpdate() error {
	poller := &testcore.TaskPoller{
		Client:              c.testCluster.FrontendClient(),
		Namespace:           c.t.tv.NamespaceName().String(),
		TaskQueue:           c.t.tv.TaskQueue(),
		Identity:            c.t.tv.WorkerIdentity(),
		WorkflowTaskHandler: c.t.acceptUpdateWFTHandler,
		MessageHandler:      c.t.acceptUpdateMessageHandler,
		Logger:              c.t.s.logger,
		T:                   c.t.s.T(),
	}
	_, err := poller.PollAndProcessWorkflowTask()
	return err
}

func (c *hrsuTestCluster) pollAndCompleteUpdate(updateId string) error {
	poller := &testcore.TaskPoller{
		Client:              c.testCluster.FrontendClient(),
		Namespace:           c.t.tv.NamespaceName().String(),
		TaskQueue:           c.t.tv.TaskQueue(),
		Identity:            c.t.tv.WorkerIdentity(),
		WorkflowTaskHandler: c.t.completeUpdateWFTHandler,
		MessageHandler:      c.completeUpdateMessageHandler(updateId),
		Logger:              c.t.s.logger,
		T:                   c.t.s.T(),
	}
	_, err := poller.PollAndProcessWorkflowTask()
	return err
}

func (c *hrsuTestCluster) pollAndAcceptCompleteUpdate(updateId string) error {
	poller := &testcore.TaskPoller{
		Client:              c.testCluster.FrontendClient(),
		Namespace:           c.t.tv.NamespaceName().String(),
		TaskQueue:           c.t.tv.TaskQueue(),
		Identity:            c.t.tv.WorkerIdentity(),
		WorkflowTaskHandler: joinHandlers(c.t.acceptUpdateWFTHandler, c.t.completeUpdateWFTHandler),
		MessageHandler:      joinHandlers(c.t.acceptUpdateMessageHandler, c.completeUpdateMessageHandler(updateId)),
		Logger:              c.t.s.logger,
		T:                   c.t.s.T(),
	}
	_, err := poller.PollAndProcessWorkflowTask()
	return err
}

func (c *hrsuTestCluster) pollAndErrorWhileProcessingWorkflowTask() error {
	poller := &testcore.TaskPoller{
		Client:              c.testCluster.FrontendClient(),
		Namespace:           c.t.tv.NamespaceName().String(),
		TaskQueue:           c.t.tv.TaskQueue(),
		Identity:            c.t.tv.WorkerIdentity(),
		WorkflowTaskHandler: c.t.respondWithErrorWFTHandler,
		MessageHandler:      c.t.respondWithErrorMessageHandler,
		Logger:              c.t.s.logger,
		T:                   c.t.s.T(),
	}
	_, err := poller.PollAndProcessWorkflowTask()
	return err
}

func (t *hrsuTest) acceptUpdateMessageHandler(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
	// The WFT contains the update request as a protocol message xor an UpdateAdmittedEvent: obtain the updateId from
	// one or the other.
	var updateAdmittedEvent *historypb.HistoryEvent
	for _, e := range resp.History.Events {
		if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED {
			t.s.Nil(updateAdmittedEvent)
			updateAdmittedEvent = e
		}
	}
	updateId := ""
	if updateAdmittedEvent != nil {
		t.s.Empty(resp.Messages)
		attrs := updateAdmittedEvent.GetWorkflowExecutionUpdateAdmittedEventAttributes()
		updateId = attrs.Request.Meta.UpdateId
	} else {
		t.s.Len(resp.Messages, 1)
		msg := resp.Messages[0]
		updateId = msg.ProtocolInstanceId
	}

	return []*protocolpb.Message{
		{
			Id:                 "accept-msg-id",
			ProtocolInstanceId: updateId,
			Body: protoutils.MarshalAny(t.s.T(), &updatepb.Acceptance{
				AcceptedRequestMessageId:         "request-msg-id",
				AcceptedRequestSequencingEventId: int64(-1),
			}),
		},
	}, nil
}

func (t *hrsuTest) acceptUpdateWFTHandler(_ *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
			MessageId: "accept-msg-id",
		}},
	}}, nil
}

func (c *hrsuTestCluster) completeUpdateMessageHandler(updateId string) func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
	return func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		return []*protocolpb.Message{
			{
				Id:                 "completion-msg-id",
				ProtocolInstanceId: updateId,
				SequencingId:       nil,
				Body: protoutils.MarshalAny(c.t.s.T(), &updatepb.Response{
					Meta: &updatepb.Meta{
						UpdateId: updateId,
						Identity: c.t.tv.WorkerIdentity(),
					},
					Outcome: &updatepb.Outcome{
						Value: &updatepb.Outcome_Success{
							Success: payloads.EncodeString(c.updateResult(updateId)),
						},
					},
				}),
			},
		}, nil

	}
}

// updateResult returns the update result sent by the worker in this cluster.
func (c *hrsuTestCluster) updateResult(updateId string) string {
	return fmt.Sprintf("%s-%s-result", c.testCluster.ClusterName(), updateId)
}

func (t *hrsuTest) completeUpdateWFTHandler(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
			MessageId: "completion-msg-id",
		}},
	}}, nil
}

func (t *hrsuTest) respondWithErrorMessageHandler(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
	return []*protocolpb.Message{}, errors.New("fake error while handling workflow task (message handler)")
}

func (t *hrsuTest) respondWithErrorWFTHandler(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
	return []*commandpb.Command{}, errors.New("fake error while handling workflow task (WFT handler)")
}

func (c *hrsuTestCluster) otherCluster() *hrsuTestCluster {
	var otherCluster *hrsuTestCluster
	for _, c2 := range []*hrsuTestCluster{&c.t.cluster1, &c.t.cluster2} {
		if c2 != c {
			otherCluster = c2
		}
	}
	if otherCluster == nil {
		c.t.s.FailNow("bug in test: failed to identify other cluster")
	}
	return otherCluster
}

// gRPC utilities

func (t *hrsuTest) registerMultiRegionNamespace(ctx context.Context) {
	_, err := t.cluster1.testCluster.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        t.tv.NamespaceName().String(),
		Clusters:                         t.s.clusterReplicationConfig(),
		ActiveClusterName:                t.s.clusters[0].ClusterName(),
		IsGlobalNamespace:                true,                           // Needed so that the namespace is replicated
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24), // Required parameter
	})
	t.s.NoError(err)

	// Namespace event replication tasks are being captured; execute the pending ones to propagate the new namespace to cluster2.
	t.cluster2.executeNamespaceReplicationTasksUntil(enumsspb.NAMESPACE_OPERATION_CREATE)
	t.s.Equal([]string{t.s.clusters[0].ClusterName(), t.s.clusters[0].ClusterName()}, t.getActiveClusters(ctx))
}

func (t *hrsuTest) getActiveClusters(ctx context.Context) []string {
	return []string{t.cluster1.getActiveCluster(ctx), t.cluster2.getActiveCluster(ctx)}
}

// startWorkflow starts a workflow in the cluster and replicates the initial workflow events to the other cluster.
func (c *hrsuTestCluster) startWorkflow(ctx context.Context, workflowFn any) {
	run, err := c.client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: c.t.tv.TaskQueue().Name,
		ID:        c.t.tv.WorkflowID(),
	}, workflowFn)
	c.t.s.NoError(err)
	c.t.tv = c.t.tv.WithRunID(run.GetRunID())

	// Process history replication tasks in the other cluster until the initial workflow events are replicated.
	c.otherCluster().executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)

	for _, cluster := range []*hrsuTestCluster{&c.t.cluster1, &c.t.cluster2} {
		c.t.s.EqualHistoryEvents(`
		1 v1 WorkflowExecutionStarted
		2 v1 WorkflowTaskScheduled
		  `, cluster.getHistory(ctx))
	}
}

func (c *hrsuTestCluster) resetWorkflow(ctx context.Context, workflowTaskFinishEventId int64) string {
	resp, err := c.client.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 c.t.tv.NamespaceName().String(),
		WorkflowExecution:         c.t.tv.WorkflowExecution(),
		Reason:                    "reset",
		WorkflowTaskFinishEventId: workflowTaskFinishEventId,
	})
	c.t.s.NoError(err)
	return resp.RunId
}

func (c *hrsuTestCluster) setActive(ctx context.Context, clusterName string) {
	_, err := c.testCluster.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: c.t.tv.NamespaceName().String(),
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName,
		},
	})
	c.t.s.NoError(err)
}

func (c *hrsuTestCluster) getHistory(ctx context.Context) []*historypb.HistoryEvent {
	return c.getHistoryForRunId(ctx, c.t.tv.RunID())
}

func (c *hrsuTestCluster) getHistoryForRunId(ctx context.Context, runId string) []*historypb.HistoryEvent {
	historyResponse, err := c.testCluster.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: c.t.tv.NamespaceName().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: c.t.tv.WorkflowID(),
			RunId:      runId,
		},
	})
	c.t.s.NoError(err)
	return historyResponse.History.Events
}

func (c *hrsuTestCluster) pollWorkflowResult(ctx context.Context, runId string) *historypb.HistoryEvent {
	getHistoryWithLongPoll := func(token []byte) ([]*historypb.HistoryEvent, []byte, error) {
		responseInner, err := c.testCluster.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: c.t.tv.NamespaceName().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: c.t.tv.WorkflowID(),
				RunId:      runId,
			},
			MaximumPageSize:        1,
			WaitNewEvent:           true,
			NextPageToken:          token,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		})
		if err != nil {
			return nil, nil, err
		}
		return responseInner.History.Events, responseInner.NextPageToken, nil
	}

	var token []byte
	var allEvents []*historypb.HistoryEvent
	for {
		if ctx.Err() != nil {
			c.t.s.NoError(ctx.Err(), "context expired while waiting for workflow result")
			return nil
		}
		events, nextPageToken, err := getHistoryWithLongPoll(token)
		if err != nil {
			// Transient error (e.g. CurrentBranchChanged after conflict resolution): retry from scratch.
			token = nil
			continue
		}
		allEvents = append(allEvents, events...)
		if nextPageToken == nil {
			break
		}
		token = nextPageToken
	}

	c.t.s.Len(allEvents, 1)
	return allEvents[0]
}

func (c *hrsuTestCluster) getActiveCluster(ctx context.Context) string {
	resp, err := c.testCluster.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: c.t.tv.NamespaceName().String()})
	c.t.s.NoError(err)
	return resp.ReplicationConfig.ActiveClusterName
}

func joinHandlers[T any](handlers ...func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*T, error)) func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*T, error) {
	return func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*T, error) {
		var joinedResult []*T
		for _, handler := range handlers {
			handlerResult, err := handler(task)
			if err != nil {
				return nil, err
			}
			joinedResult = append(joinedResult, handlerResult...)
		}
		return joinedResult, nil
	}
}

// TestConflictResolutionGetResult creates a split-brain scenario in which both clusters believe they are active.
// The test confirms that the workflow result can be retrievved if conflict resolution happens (CurrentBranchChange).
func (s *hrsuTestSuite) TestConflictResolutionGetResult() {
	t, ctx, cancel := s.startHrsuTest()
	defer cancel()
	t.cluster1.startWorkflow(ctx, func(workflow.Context) error { return nil })

	t.enterSplitBrainState(ctx)

	// Both clusters now believe they are active and hence both will accept a signal.

	// Send signals
	s.NoError(t.cluster1.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster1-signal"))
	s.NoError(t.cluster2.client.SignalWorkflow(ctx, t.tv.WorkflowID(), t.tv.RunID(), "my-signal", "cluster2-signal"))

	// cluster1 has accepted a signal
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v1 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster1-signal\""}]}}
	`, t.cluster1.getHistory(ctx))

	// cluster2 has also accepted a signal (with failover version 2 since it is endogenous to cluster1)
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	`, t.cluster2.getHistory(ctx))

	// pull the workflow result from cluster1. This will block until the workflow task is completed.
	workflowResultCh := make(chan *historypb.HistoryEvent)
	workflowResultFn := func() {
		event := t.cluster1.pollWorkflowResult(ctx, t.tv.RunID())
		workflowResultCh <- event
	}
	go workflowResultFn()

	// Ensure long poll is timeout
	time.Sleep(time.Millisecond * 100) //nolint:forbidigo

	// Execute pending history replication tasks. Each cluster sends its signal to the other, but these have the same
	// event ID; this conflict is resolved by reapplying one of the signals after the other.

	// cluster2 sends its signal to cluster1. Since it has a higher failover version, it supersedes the endogenous
	// signal in cluster1.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	`, t.cluster1.getHistory(ctx))

	// cluster1 sends its signal to cluster2. Since it has a lower failover version, it is reapplied after the
	// endogenous cluster1 signal.
	t.cluster2.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.EqualHistoryEvents(`
	1 v1 WorkflowExecutionStarted
	2 v1 WorkflowTaskScheduled
	3 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	4 v2 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster1-signal\""}]}}
	`, t.cluster2.getHistory(ctx))

	// Cluster2 sends the reapplied signal to cluster1, bringing the cluster histories into agreement.
	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.Equal(t.cluster1.getHistory(ctx), t.cluster2.getHistory(ctx))

	// Complete the workflow in cluster2. This will cause the workflow result to be sent to cluste1.
	task, err := t.cluster2.testCluster.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: t.tv.NamespaceName().String(),
		TaskQueue: t.tv.TaskQueue(),
		Identity:  t.tv.WorkerIdentity(),
	})
	s.Require().NoError(err)
	_, err = t.cluster2.testCluster.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	s.Require().NoError(err)

	t.cluster1.executeHistoryReplicationTasksUntil(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
	s.Equal(t.cluster1.getHistory(ctx), t.cluster2.getHistory(ctx))

	// Make sure we can get the workflow result after the conflict resolution (CurrentBranchChange).
	event := <-workflowResultCh
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, event.GetEventType())
}
