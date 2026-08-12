package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type gradualConnectTestSuite struct {
	xdcBaseSuite
}

func TestGradualConnectTestSuite(t *testing.T) {
	t.Parallel()
	s := &gradualConnectTestSuite{}
	suite.Run(t, s)
}

func (s *gradualConnectTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.setupSuite()
}

func (s *gradualConnectTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *gradualConnectTestSuite) SetupTest() {
	s.setupTest()
}

// Exercises the gradual-connect ramp end to end: shed while the ramp is active, admitted once it
// completes.
func (s *gradualConnectTestSuite) TestNewlyConnectedClusterRampsAdmission() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Create the namespace on the active cluster only -- standby isn't a member yet.
	ns := s.createNamespaceInCluster0(true)

	active := s.clusters[0]
	standby := s.clusters[1]

	// A single ramp step: 0% until it elapses, then a deterministic jump to 100%.
	const rampStepDuration = 12 * time.Second
	for _, override := range []struct {
		setting dynamicconfig.GenericSetting
		value   any
	}{
		{dynamicconfig.EnableReplicationGradualConnect, true},
		{dynamicconfig.ReplicationGradualConnectInitialPercent, 0},
		{dynamicconfig.ReplicationGradualConnectStepPercent, 100},
		{dynamicconfig.ReplicationGradualConnectStepDuration, rampStepDuration},
	} {
		s.T().Cleanup(standby.OverrideDynamicConfig(s.T(), override.setting, override.value))
	}

	// Add standby to the namespace's cluster list.
	connectedAt := time.Now()
	s.updateNamespaceClusters(ns, 0, s.clusters)

	// Diagnostic: standby's own connect time, read directly from persisted state (not the
	// namespace cache, to rule out a cache-refresh delay).
	nsResp, err := standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: ns})
	s.Require().NoError(err)
	connectTimestamp := nsResp.Namespace.GetReplicationConfig().GetClusterConnectTime()[standby.ClusterName()]
	s.Require().NotNil(connectTimestamp, "standby should have recorded its own connect time for this namespace")
	s.Require().WithinDuration(connectedAt, connectTimestamp.AsTime(), namespaceCacheWaitTime+5*time.Second)
	// Anchor on the recorded connect time, not the test's own wall-clock read -- propagation from
	// active to standby can lag it by more than the shed check's window below.
	connectTime := connectTimestamp.AsTime()

	// Still inside the ramp window: a workflow started on active must not replicate to standby yet.
	shedWorkflowID := "gc-shed-" + uuid.NewString()
	s.gcStartAndCompleteWorkflow(ctx, active, ns, shedWorkflowID)
	s.Require().Never(func() bool {
		return s.gcWorkflowExistsOn(ctx, standby, ns, shedWorkflowID)
	}, 3*time.Second, 200*time.Millisecond, "workflow should be shed while the ramp is still active")

	// Once the ramp step has elapsed, a new workflow started on active must replicate normally.
	// Sleep-until-deadline is deliberate, not poll-until-condition: a task generated before the
	// deadline is dropped for good by the shed gate, so polling early can't substitute for it.
	time.Sleep(time.Until(connectTime.Add(rampStepDuration + 3*time.Second))) //nolint:forbidigo
	admittedWorkflowID := "gc-admit-" + uuid.NewString()
	s.gcStartAndCompleteWorkflow(ctx, active, ns, admittedWorkflowID)
	await.RequireTruef(s.T(), func() bool {
		return s.gcWorkflowExistsOn(ctx, standby, ns, admittedWorkflowID)
	}, replicationWaitTime, replicationCheckInterval, "workflow should replicate once the ramp has completed")
}

// gcStartAndCompleteWorkflow starts and completes a workflow on cluster c via the raw frontend API,
// waiting for it to reach COMPLETED before returning.
func (s *gradualConnectTestSuite) gcStartAndCompleteWorkflow(ctx context.Context, c *testcore.TestCluster, ns, workflowID string) {
	client := c.FrontendClient()
	taskQueue := "gc-tq-" + uuid.NewString()

	startResp, err := client.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "gc-test-wf-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.Require().NoError(err)
	runID := startResp.GetRunId()

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				},
			},
		}}, nil
	}
	//nolint:staticcheck // matches the existing pattern in delete_execution_replication_test.go
	poller := &testcore.TaskPoller{
		Client:              client,
		Namespace:           ns,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:            "worker",
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.Require().NoError(err)

	await.RequireTruef(s.T(), func() bool {
		resp, err := client.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		if err != nil {
			return false
		}
		return resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, time.Second, "workflow should reach COMPLETED on %s", c.ClusterName())
}

// gcWorkflowExistsOn reports whether workflowID is visible on cluster c.
func (s *gradualConnectTestSuite) gcWorkflowExistsOn(ctx context.Context, c *testcore.TestCluster, ns, workflowID string) bool {
	_, err := c.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	return err == nil
}
