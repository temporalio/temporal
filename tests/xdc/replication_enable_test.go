package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	ReplicationEnableTestSuite struct {
		suite.Suite
		*require.Assertions
		logger   log.Logger
		clusters []*testcore.TestCluster
	}
)

func TestReplicationEnableTestSuite(t *testing.T) {
	s := new(ReplicationEnableTestSuite)
	suite.Run(t, s)
}

// SetupSuite sets up two clusters with minimal config required for replication enable/disable testing
func (s *ReplicationEnableTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()

	// Minimal dynamic config overrides required for replication enable/disable testing
	dynamicConfigOverrides := map[dynamicconfig.Key]interface{}{
		dynamicconfig.ClusterMetadataRefreshInterval.Key():        time.Second * 5,
		dynamicconfig.EnableReplicationStream.Key():               true,
		dynamicconfig.EnableSeparateReplicationEnableFlag.Key():   true,
		dynamicconfig.SendRawHistoryBetweenInternalServices.Key(): true,
	}

	clusterConfigs := []*testcore.TestClusterConfig{
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
	}

	s.clusters = make([]*testcore.TestCluster, len(clusterConfigs))
	suffix := common.GenerateRandomString(5)
	clusterNames := []string{"active_" + suffix, "standby_" + suffix}

	testClusterFactory := testcore.NewTestClusterFactory()
	for clusterIndex, clusterName := range clusterNames {
		clusterConfigs[clusterIndex].DynamicConfigOverrides = dynamicConfigOverrides
		clusterConfigs[clusterIndex].ClusterMetadata.MasterClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.CurrentClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.EnableGlobalNamespace = true
		clusterConfigs[clusterIndex].Persistence.DBName = "func_tests_" + clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{
			clusterName: {
				Enabled:                true,
				InitialFailoverVersion: int64(clusterIndex + 1),
			},
		}
		clusterConfigs[clusterIndex].EnableMetricsCapture = true

		var err error
		s.clusters[clusterIndex], err = testClusterFactory.NewCluster(s.T(), clusterConfigs[clusterIndex], log.With(s.logger, tag.ClusterName(clusterName)))
		s.Require().NoError(err)
	}

}

func (s *ReplicationEnableTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *ReplicationEnableTestSuite) TearDownSuite() {
	for _, c := range s.clusters {
		s.NoError(c.TearDownCluster())
	}
}

func (s *ReplicationEnableTestSuite) clusterReplicationConfig() []*replicationpb.ClusterReplicationConfig {
	config := make([]*replicationpb.ClusterReplicationConfig, len(s.clusters))
	for i, c := range s.clusters {
		config[i] = &replicationpb.ClusterReplicationConfig{
			ClusterName: c.ClusterName(),
		}
	}
	return config
}

// TestReplicationEnableFlow tests the complete flow of:
// 1. Connect clusters with connection enabled but replication disabled
// 2. Create namespace - verify it DOES replicate (namespace replication happens when clusters connected)
// 3. Verify namespace replicated to standby
// 4. Start and complete workflow - workflow does NOT replicate yet (workflow replication disabled)
// 5. Enable workflow replication
// 6. Start new workflow - verify it DOES replicate
// 7. Verify workflow replicated to standby
// 8. Disable replication again
// 9. Start another workflow - verify it does NOT replicate
// 10. Verify workflow does NOT exist on standby
func (s *ReplicationEnableTestSuite) TestReplicationEnableFlow() {
	ctx := context.Background()
	tv := testvars.New(s.T())

	activeCluster := s.clusters[0]
	standbyCluster := s.clusters[1]

	// Simple toy workflow that completes immediately
	toyWorkflow := func(ctx workflow.Context) (string, error) {
		return "workflow completed successfully", nil
	}

	workflowID1 := tv.WorkflowID()
	taskQueueName := tv.TaskQueue().Name
	activeNamespace := tv.NamespaceName().String()

	s.logger.Info("Step 1: Connect clusters with connection enabled but replication disabled")

	// Connect clusters to each other with replication disabled
	var err error
	_, err = activeCluster.AdminClient().AddOrUpdateRemoteCluster(
		ctx,
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               standbyCluster.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           standbyCluster.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             false,
		})
	s.Require().NoError(err)

	_, err = standbyCluster.AdminClient().AddOrUpdateRemoteCluster(
		ctx,
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               activeCluster.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           activeCluster.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             false,
		})
	s.Require().NoError(err)

	// Wait for cluster metadata to refresh (ClusterMetadataRefreshInterval is 5 seconds)
	time.Sleep(6 * time.Second) //nolint:forbidigo

	s.logger.Info("Step 2: Create namespace")

	_, err = activeCluster.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        activeNamespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                activeCluster.ClusterName(),
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * 24 * time.Hour),
	})
	s.Require().NoError(err)

	// Create SDK client and worker after namespace is created
	activeSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  activeCluster.Host().FrontendGRPCAddress(),
		Namespace: activeNamespace,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.Require().NoError(err)
	defer activeSDKClient.Close()

	worker := sdkworker.New(activeSDKClient, taskQueueName, sdkworker.Options{})
	worker.RegisterWorkflow(toyWorkflow)
	s.Require().NoError(worker.Start())
	defer worker.Stop()

	s.logger.Info("Step 3: Verify namespace DOES replicate to standby (cluster connection enabled)")

	// Namespace should replicate even when workflow replication is disabled
	// because namespace replication happens when clusters are connected
	s.Eventually(func() bool {
		_, err := standbyCluster.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: activeNamespace,
		})
		return err == nil
	}, 60*time.Second, 1*time.Second, "Namespace should replicate when clusters are connected")

	s.logger.Info("Step 4: Start and complete workflow on active cluster (before replication enabled)")

	// Start workflow using SDK client (worker is already running from Step 1)
	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: taskQueueName,
		ID:        workflowID1,
	}, toyWorkflow)
	s.Require().NoError(err)

	// Wait for workflow to complete
	var result string
	err = workflowRun.Get(ctx, &result)
	s.Require().NoError(err)
	s.Require().Equal("workflow completed successfully", result)

	// Verify workflow is in completed state on active cluster
	descResp, err := activeCluster.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: activeNamespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID1,
			RunId:      workflowRun.GetRunID(),
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)

	// Verify workflow did NOT replicate to standby cluster (replication is disabled)
	time.Sleep(2 * time.Second) //nolint:forbidigo
	_, err = standbyCluster.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: activeNamespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID1,
		},
	})
	s.Require().Error(err, "Workflow should NOT replicate to standby when replication is disabled")
	s.Require().Contains(err.Error(), "not found", "Expected workflow not found error")

	s.logger.Info("Step 5: Enable replication on both clusters")

	// Enable replication active -> standby
	_, err = activeCluster.AdminClient().AddOrUpdateRemoteCluster(
		ctx,
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               standbyCluster.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           standbyCluster.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             true, // NOW enable replication
		})
	s.Require().NoError(err)

	// Enable replication standby -> active
	_, err = standbyCluster.AdminClient().AddOrUpdateRemoteCluster(
		ctx,
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               activeCluster.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           activeCluster.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             true, // NOW enable replication
		})
	s.Require().NoError(err)

	// Wait for cluster metadata to refresh and replication streams to establish
	time.Sleep(5 * time.Second) //nolint:forbidigo

	s.logger.Info("Step 6: Start new workflow on active cluster (after replication enabled)")

	tv2 := tv.WithWorkflowIDNumber(2)
	workflowID2 := tv2.WorkflowID()

	_, err = activeSDKClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: taskQueueName,
		ID:        workflowID2,
	}, toyWorkflow)
	s.Require().NoError(err)

	s.logger.Info("Step 7: Verify new workflow DOES replicate to standby")

	s.Eventually(func() bool {
		descResp2, descErr := standbyCluster.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: activeNamespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID2},
		})
		return descErr == nil && descResp2 != nil && descResp2.WorkflowExecutionInfo.Execution.WorkflowId == workflowID2
	}, 30*time.Second, 1*time.Second, "Workflow started after enabling replication should replicate to standby")

	s.logger.Info("Step 8: Disable replication on both clusters")

	// Disable replication active -> standby
	_, err = activeCluster.AdminClient().AddOrUpdateRemoteCluster(
		ctx,
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               standbyCluster.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           standbyCluster.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             false, // Disable replication
		})
	s.Require().NoError(err)

	// Disable replication standby -> active
	_, err = standbyCluster.AdminClient().AddOrUpdateRemoteCluster(
		ctx,
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               activeCluster.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           activeCluster.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             false, // Disable replication
		})
	s.Require().NoError(err)

	// Wait for cluster metadata to refresh and replication streams to stop
	time.Sleep(6 * time.Second) //nolint:forbidigo

	s.logger.Info("Step 9: Start another workflow on active cluster (after disabling replication)")

	tv3 := tv.WithWorkflowIDNumber(3)
	workflowID3 := tv3.WorkflowID()

	_, err = activeSDKClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: taskQueueName,
		ID:        workflowID3,
	}, toyWorkflow)
	s.Require().NoError(err)

	s.logger.Info("Step 10: Verify third workflow does NOT replicate to standby")

	// Wait a bit to ensure replication would have happened if it was enabled
	time.Sleep(5 * time.Second) //nolint:forbidigo

	// Verify workflow does NOT exist on standby
	_, descErr := standbyCluster.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: activeNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID3},
	})
	s.Require().Error(descErr, "Workflow should NOT replicate when replication is disabled")
}
