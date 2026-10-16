package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	computepb "go.temporal.io/api/compute/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	computeprovider "go.temporal.io/auto-scaled-workers/wci/workflow/compute_provider"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/wci"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
)

type WCIChasmMigrationSuite struct {
	parallelsuite.Suite[*WCIChasmMigrationSuite]
}

func TestWCIChasmMigrationSuite(t *testing.T) {
	parallelsuite.Run(t, &WCIChasmMigrationSuite{})
}

// invokeCounter counts test-invoke "invoke" actions for one build, from either backend.
type invokeCounter struct{ n atomic.Int64 }

func (c *invokeCounter) ObserveProviderInvoke(_ computeprovider.RequestContext, action string) {
	if action == "invoke" {
		c.n.Add(1)
	}
}

func wciMigrationComputeConfig() *computepb.ComputeConfig {
	return &computepb.ComputeConfig{ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
		"default": {
			Provider: &computepb.ComputeProvider{Type: string(iface.ComputeProviderTypeTestInvoke)},
			Scaler:   &computepb.ComputeScaler{Type: string(iface.ScalingAlgorithmNoSync)},
		},
	}}
}

func wciMigrationWorkflow(workflow.Context) (string, error) { return "done", nil }

// TestRoundTripUnderLoad creates a WCI on CHASM, migrates it to the workflow backend and back
// while backlogged task-adds keep arriving, and checks each phase: it keeps scaling up, exactly
// one backend serves it, and the spec survives.
func (s *WCIChasmMigrationSuite) TestRoundTripUnderLoad() {
	env := testcore.NewEnv(s.T(),
		testcore.WithWorkerService("WCI workflow backend and worker deployment workflows"),
		testcore.WithDynamicConfig(dynamicconfig.MatchingDeploymentWorkflowVersion, int(workerdeployment.VersionDataRevisionNumber)),
		testcore.WithDynamicConfig(dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS, 1000),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		testcore.WithDynamicConfig(wciclient.WorkerControllerEnabled, true),
		testcore.WithDynamicConfig(wciclient.WorkerControllerEnabledComputeProviders, []string{string(iface.ComputeProviderTypeTestInvoke)}),
		// One signal per backlogged task-add, so scale-ups do not wait on batching windows.
		testcore.WithDynamicConfig(wciclient.WorkerControllerMinSignalIntervalNoSyncMatchMilliseconds, 1),
		testcore.WithDynamicConfig(wciclient.WorkerControllerCHASMRoutingEnabled, true),
		testcore.WithDynamicConfig(wciclient.WorkerControllerCHASMCreationEnabled, true),
		testcore.WithDynamicConfig(wciclient.WorkerControllerCHASMCreationRolloutPercent, 100),
		// The workflow run started by the CHASM→workflow migration must be able to migrate back.
		testcore.WithDynamicConfig(wciclient.WorkerControllerInstanceWorkflowVersion, 5),
	)
	ctx := s.Context()
	sdk := env.SdkClient()
	ns := env.Namespace().String()
	deploymentName, buildID := "wcim-"+uuid.NewString()[:8], "b-"+uuid.NewString()[:8]
	taskQueue := "wcim-tq-" + deploymentName
	version := &deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: buildID}
	instanceID := wciclient.GenerateWorkerControllerInstanceWorkflowID(version)
	chasmClient := env.GetTestCluster().WCIClient()

	invokes := &invokeCounter{}
	s.T().Cleanup(computeprovider.SetComputeObserver(buildID, invokes))

	// --- Create: the creation gate puts the WCI on CHASM. ---
	_, err := sdk.WorkflowService().CreateWorkerDeployment(ctx, &workflowservice.CreateWorkerDeploymentRequest{
		Namespace: ns, DeploymentName: deploymentName, RequestId: uuid.NewString(),
	})
	s.NoError(err)
	_, err = sdk.WorkflowService().CreateWorkerDeploymentVersion(ctx, &workflowservice.CreateWorkerDeploymentVersionRequest{
		Namespace: ns, DeploymentVersion: version, ComputeConfig: wciMigrationComputeConfig(), RequestId: uuid.NewString(),
	})
	s.NoError(err)

	onCHASM := func() bool {
		resp, err := chasmClient.DescribeWorkerControllerInstance(ctx, &wcipb.DescribeWorkerControllerInstanceRequest{
			NamespaceId: env.NamespaceID().String(), InstanceId: instanceID,
		})
		return err == nil && resp.GetScalingGroupSpecs()["default"] != nil
	}
	workflowStatus := func() enumspb.WorkflowExecutionStatus {
		resp, err := sdk.DescribeWorkflowExecution(ctx, instanceID, "")
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
		}
		s.NoError(err)
		return resp.GetWorkflowExecutionInfo().GetStatus()
	}
	s.AwaitTruef(onCHASM, 30*time.Second, 200*time.Millisecond, "WCI created on CHASM")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, workflowStatus(), "no workflow-backed WCI")

	// Register the task queue against the version, then drop the poller so a backlog can form.
	w := worker.New(sdk, taskQueue, worker.Options{DeploymentOptions: worker.DeploymentOptions{
		UseVersioning:             true,
		Version:                   worker.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildID: buildID},
		DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	}})
	w.RegisterWorkflow(wciMigrationWorkflow)
	s.NoError(w.Start())
	s.AwaitTruef(func() bool {
		resp, err := sdk.WorkflowService().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{Namespace: ns, DeploymentVersion: version})
		return err == nil && len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()) > 0
	}, 60*time.Second, 500*time.Millisecond, "task queue registered against the version")
	w.Stop()

	// --- Continuous load: pinned workflows with no poller keep the backlog growing. ---
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-loadCtx.Done():
				return
			case <-ticker.C:
				_, _ = sdk.ExecuteWorkflow(loadCtx, sdkclient.StartWorkflowOptions{
					TaskQueue: taskQueue,
					ID:        "wcim-load-" + uuid.NewString(),
					VersioningOverride: &sdkclient.PinnedVersioningOverride{
						Version: worker.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildID: buildID},
					},
				}, wciMigrationWorkflow)
			}
		}
	}()
	scalesUp := func(phase string) {
		before := invokes.n.Load()
		s.AwaitTruef(func() bool { return invokes.n.Load() > before }, 60*time.Second, 200*time.Millisecond, "scale-up on %s", phase)
	}
	scalesUp("CHASM")

	// --- CHASM → workflow. ---
	_, err = chasmClient.MigrateToWorkflow(ctx, &wcipb.MigrateToWorkflowRequest{NamespaceId: env.NamespaceID().String(), InstanceId: instanceID})
	s.NoError(err)
	s.AwaitTruef(func() bool { return workflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING }, 30*time.Second, 200*time.Millisecond, "workflow started")
	s.AwaitTruef(func() bool { return !onCHASM() }, 30*time.Second, 200*time.Millisecond, "CHASM closed after the hand-off")
	_, err = chasmClient.RecordTaskAdd(ctx, &wcipb.RecordTaskAddRequest{
		NamespaceId: env.NamespaceID().String(), InstanceId: instanceID,
		Event: &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	})
	s.ErrorContains(err, wci.ErrClosed.Error(), "CHASM rejects work once migrated")
	scalesUp("workflow")

	// --- workflow → CHASM, triggered by an update at the next CaN boundary. ---
	env.OverrideDynamicConfig(wciclient.WorkerControllerCHASMMigrationEnabled, true)
	env.OverrideDynamicConfig(wciclient.WorkerControllerCHASMMigrationRolloutPercent, 100)
	_, err = sdk.WorkflowService().UpdateWorkerDeploymentVersionComputeConfig(ctx, &workflowservice.UpdateWorkerDeploymentVersionComputeConfigRequest{
		Namespace:         ns,
		DeploymentVersion: version,
		ComputeConfigScalingGroups: map[string]*computepb.ComputeConfigScalingGroupUpdate{
			"default": {ScalingGroup: wciMigrationComputeConfig().GetScalingGroups()["default"]},
		},
		RequestId: uuid.NewString(),
	})
	s.NoError(err)
	s.AwaitTruef(onCHASM, 60*time.Second, 200*time.Millisecond, "back on CHASM")
	s.AwaitTruef(func() bool { return workflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED }, 30*time.Second, 200*time.Millisecond, "workflow completed, not continued-as-new")
	scalesUp("CHASM after the round trip")

	// --- Spec preserved end to end. ---
	resp, err := sdk.WorkflowService().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{Namespace: ns, DeploymentVersion: version})
	s.NoError(err)
	group := resp.GetWorkerDeploymentVersionInfo().GetComputeConfig().GetScalingGroups()["default"]
	s.Require().NotNil(group)
	s.Equal(string(iface.ComputeProviderTypeTestInvoke), group.GetProvider().GetType())
}
