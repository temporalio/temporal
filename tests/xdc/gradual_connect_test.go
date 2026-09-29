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
	suite.Run(t, &gradualConnectTestSuite{})
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

func (s *gradualConnectTestSuite) TestNewlyConnectedClusterRampsAndRecovers() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*testTimeout)
	defer cancel()

	ns := s.createNamespaceInCluster0(true)
	active := s.clusters[0]
	standby := s.clusters[1]
	s.T().Cleanup(active.OverrideDynamicConfig(
		s.T(),
		dynamicconfig.EnableReplicationGradualConnect,
		true,
	))

	const rampDuration = time.Hour
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

	activeNS, err := active.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: ns})
	s.Require().NoError(err)
	ramp := activeNS.Namespace.GetReplicationConfig().GetClusterReplicationRamps()[standby.ClusterName()]
	s.Require().NotNil(ramp)
	s.Equal(rampDuration, ramp.GetDuration().AsDuration())
	standbyNS, err := standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: ns})
	s.Require().NoError(err)
	s.Empty(standbyNS.Namespace.GetReplicationConfig().GetClusterReplicationRamps())

	var shedWorkflowID string
	for {
		shedWorkflowID = "gc-shed-" + uuid.NewString()
		if !dynamicconfig.RolloutAccepts([]byte(shedWorkflowID), 99) {
			break
		}
	}
	s.startAndCompleteGradualConnectWorkflow(ctx, active, ns, shedWorkflowID)
	s.Require().Never(func() bool {
		return s.gradualConnectWorkflowExists(ctx, standby, ns, shedWorkflowID)
	}, 3*time.Second, 200*time.Millisecond)

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
	activeNS, err = active.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: ns})
	s.Require().NoError(err)
	s.NotContains(activeNS.Namespace.GetReplicationConfig().GetClusterReplicationRamps(), standby.ClusterName())

	admittedWorkflowID := "gc-admit-" + uuid.NewString()
	s.startAndCompleteGradualConnectWorkflow(ctx, active, ns, admittedWorkflowID)
	await.RequireTruef(s.T(), func() bool {
		return s.gradualConnectWorkflowExists(ctx, standby, ns, admittedWorkflowID)
	}, replicationWaitTime, replicationCheckInterval, "workflow should replicate after the ramp is cleared")

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
		return s.gradualConnectWorkflowExists(ctx, standby, ns, shedWorkflowID)
	}, replicationWaitTime, replicationCheckInterval, "force replication should restore the shed workflow")
}

func (s *gradualConnectTestSuite) startAndCompleteGradualConnectWorkflow(
	ctx context.Context,
	cluster *testcore.TestCluster,
	ns string,
	workflowID string,
) {
	client := cluster.FrontendClient()
	taskQueue := "gc-tq-" + uuid.NewString()
	startResponse, err := client.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "gc-test-workflow"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.Require().NoError(err)

	//nolint:staticcheck // TODO: replace with taskpoller.TaskPoller
	poller := &testcore.TaskPoller{
		Client:    client,
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  "worker",
		WorkflowTaskHandler: func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("done"),
					},
				},
			}}, nil
		},
		Logger: s.logger,
		T:      s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.Require().NoError(err)

	await.RequireTruef(s.T(), func() bool {
		response, err := client.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      startResponse.GetRunId(),
			},
		})
		return err == nil &&
			response.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, time.Second, "workflow should complete on the source")
}

func (s *gradualConnectTestSuite) gradualConnectWorkflowExists(
	ctx context.Context,
	cluster *testcore.TestCluster,
	ns string,
	workflowID string,
) bool {
	_, err := cluster.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	return err == nil
}
