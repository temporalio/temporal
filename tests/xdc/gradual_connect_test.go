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
	replicationpb "go.temporal.io/api/replication/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/worker/migration"
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

// Tests shedding, explicit ramp completion, and force-replication recovery.
func (s *gradualConnectTestSuite) TestNewlyConnectedClusterRampsAdmission() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	ns := s.createNamespaceInCluster0(true)

	active := s.clusters[0]
	standby := s.clusters[1]

	// Keep the ramp active for the duration of the test.
	const rampDuration = time.Hour
	for _, override := range []struct {
		setting dynamicconfig.GenericSetting
		value   any
	}{
		{dynamicconfig.EnableReplicationGradualConnect, true},
		{dynamicconfig.ReplicationGradualConnectInitialPercent, 0},
	} {
		s.T().Cleanup(active.OverrideDynamicConfig(s.T(), override.setting, override.value))
	}

	connectedAt := time.Now()
	s.updateNamespaceClustersWithReplicationConfigs(
		ns,
		0,
		s.clusters,
		[]*replicationpb.ClusterReplicationConfig{
			{ClusterName: active.ClusterName()},
			{
				ClusterName:             standby.ClusterName(),
				ReplicationRampDuration: durationpb.New(rampDuration),
			},
		},
	)

	// Verify the ramp reached target persistence before generating traffic.
	nsResp, err := standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: ns})
	s.Require().NoError(err)
	ramp := nsResp.Namespace.GetReplicationConfig().GetClusterReplicationRamps()[standby.ClusterName()]
	s.Require().NotNil(ramp, "standby should receive the snapshotted ramp for this connection")
	s.Require().WithinDuration(connectedAt, ramp.GetStartTime().AsTime(), namespaceCacheWaitTime+5*time.Second)
	s.Require().Equal(rampDuration, ramp.GetDuration().AsDuration())

	// Select an ID that remains outside the ramp even if the test takes almost an hour.
	var shedWorkflowID string
	for {
		shedWorkflowID = "gc-shed-" + uuid.NewString()
		if !dynamicconfig.RolloutAccepts([]byte(shedWorkflowID), 99) {
			break
		}
	}
	s.gcStartAndCompleteWorkflow(ctx, active, ns, shedWorkflowID)
	s.Require().Never(func() bool {
		return s.gcWorkflowExistsOn(ctx, standby, ns, shedWorkflowID)
	}, 3*time.Second, 200*time.Millisecond, "workflow should be shed while the ramp is still active")

	// Wait for the clear to reach target persistence before generating more traffic.
	s.updateNamespaceClustersWithReplicationConfigs(
		ns,
		0,
		s.clusters,
		[]*replicationpb.ClusterReplicationConfig{
			{ClusterName: active.ClusterName()},
			{
				ClusterName:             standby.ClusterName(),
				ReplicationRampDuration: durationpb.New(0),
			},
		},
	)
	await.RequireTruef(s.T(), func() bool {
		nsResp, err := standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: ns})
		if err != nil {
			return false
		}
		return nsResp.Namespace.GetReplicationConfig().GetClusterReplicationRamps()[standby.ClusterName()] == nil
	}, replicationWaitTime, replicationCheckInterval, "standby should remove the ramp after a zero-duration update")
	s.waitForNamespaceCacheRefresh()

	admittedWorkflowID := "gc-admit-" + uuid.NewString()
	s.gcStartAndCompleteWorkflow(ctx, active, ns, admittedWorkflowID)
	await.RequireTruef(s.T(), func() bool {
		return s.gcWorkflowExistsOn(ctx, standby, ns, admittedWorkflowID)
	}, replicationWaitTime, replicationCheckInterval, "workflow should replicate once the ramp has completed")

	// Shed tasks are acknowledged, so recovery requires force replication.
	s.waitForVisibilityCount(ctx, ns, 2)
	systemClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  active.Host().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.Require().NoError(err)
	defer systemClient.Close()
	forceRun, err := systemClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 "gc-force-replication-" + uuid.NewString(),
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Minute,
	}, "force-replication", migration.ForceReplicationParams{
		Namespace:          ns,
		OverallRps:         10,
		EnableVerification: true,
		TargetClusterName:  standby.ClusterName(),
	})
	s.Require().NoError(err)
	s.Require().NoError(forceRun.Get(ctx, nil))
	await.RequireTruef(s.T(), func() bool {
		return s.gcWorkflowExistsOn(ctx, standby, ns, shedWorkflowID)
	}, replicationWaitTime, replicationCheckInterval, "force replication should restore the workflow shed during the ramp")
}

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
	//nolint:staticcheck // TODO: replace with taskpoller.TaskPoller
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

func (s *gradualConnectTestSuite) gcWorkflowExistsOn(ctx context.Context, c *testcore.TestCluster, ns, workflowID string) bool {
	_, err := c.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	return err == nil
}
