package main

import (
	"context"
	"log"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	computepb "go.temporal.io/api/compute/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/service/worker/workerdeployment/replaytester"
)

// computeProviderType is the WCI compute provider used for the compute-config part of this
// driver.
const computeProviderType = "test-invoke"

// computeScalerType specifies the scaler type for the compute configuration
const computeScalerType = "no-sync"

// newScalingGroup builds a scaling group backed by the local test provider.
func newScalingGroup(note string, taskTypes ...enumspb.TaskQueueType) *computepb.ComputeConfigScalingGroup {
	return &computepb.ComputeConfigScalingGroup{
		TaskQueueTypes: taskTypes,
		Provider: &computepb.ComputeProvider{
			Type:    computeProviderType,
			Details: encodeProviderDetails(map[string]any{"note": note}),
		},
		Scaler: &computepb.ComputeScaler{
			Type: computeScalerType,
		},
	}
}

func main() {
	// The client and worker are heavyweight objects that should be created once per process.
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	identity := "test-identity"
	deploymentName := "foo"
	build1 := "1.0"
	// build2 is never polled by a worker; it exists only to contain a compute config.
	build2 := "2.0"
	v1 := worker.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildID:        build1,
	}
	w1 := worker.New(c, "hello-world", worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   v1,
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		},
	})

	w2 := worker.New(c, "hello-world-2", worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   v1,
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		},
	})

	w1.RegisterWorkflowWithOptions(replaytester.HelloWorld, workflow.RegisterOptions{
		Name:               "HelloWorld",
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	w2.RegisterWorkflowWithOptions(replaytester.HelloWorld, workflow.RegisterOptions{
		Name:               "HelloWorld",
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	err = w1.Start()
	if err != nil {
		log.Fatalf("Unable to start worker: %v", err)
	}

	err = w2.Start()
	if err != nil {
		log.Fatalf("Unable to start worker: %v", err)
	}

	// Wait for the workers to register the version.
	time.Sleep(5 * time.Second)

	// Testing various worker-deployment API's
	deploymentClient := c.WorkerDeploymentClient()
	dHandle := deploymentClient.GetHandle(deploymentName)

	// Set Client as the ManagerIdentity so that those validators are exercised
	_, err = c.WorkflowService().SetWorkerDeploymentManager(context.Background(), &workflowservice.SetWorkerDeploymentManagerRequest{
		Namespace:          client.DefaultNamespace,
		DeploymentName:     deploymentName,
		NewManagerIdentity: &workflowservice.SetWorkerDeploymentManagerRequest_Self{Self: true},
		Identity:           identity,
	})
	if err != nil {
		log.Fatalf("Unable to set deployment manager: %v", err)
	}

	// Update version metadata
	_, err = dHandle.UpdateVersionMetadata(context.Background(), client.WorkerDeploymentUpdateVersionMetadataOptions{
		Version: v1,
		MetadataUpdate: client.WorkerDeploymentMetadataUpdate{
			UpsertEntries: map[string]any{
				"key": "value",
			},
		},
	})
	if err != nil {
		log.Fatalf("Unable to update version metadata: %v", err)
	}

	// Set ramping version to 1.0
	_, err = dHandle.SetRampingVersion(context.Background(), client.WorkerDeploymentSetRampingVersionOptions{
		BuildID:    build1,
		Percentage: 1,
		Identity:   identity,
	})
	if err != nil {
		log.Fatalf("Unable to set ramping version: %v", err)
	}
	verifyDeployment(dHandle, "", build1, 1, client.WorkerDeploymentVersionDrainageStatusUnspecified)

	// Unset the ramping version
	_, err = dHandle.SetRampingVersion(context.Background(), client.WorkerDeploymentSetRampingVersionOptions{
		BuildID:    "",
		Percentage: 0,
		Identity:   identity,
	})
	if err != nil {
		log.Fatalf("Unable to set ramping version to zero: %v", err)
	}
	verifyDeployment(dHandle, "", "", 0, client.WorkerDeploymentVersionDrainageStatusDraining)

	// Set current version to 1.0
	_, err = dHandle.SetCurrentVersion(context.Background(), client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID:                 build1,
		IgnoreMissingTaskQueues: true,
		Identity:                identity,
	})
	if err != nil {
		log.Fatalf("Unable to set current version: %v", err)
	}
	verifyDeployment(dHandle, build1, "", 0, client.WorkerDeploymentVersionDrainageStatusUnspecified)

	// Ramp the "__unversioned__" version
	_, err = dHandle.SetRampingVersion(context.Background(), client.WorkerDeploymentSetRampingVersionOptions{
		BuildID:                 "",
		Percentage:              20,
		IgnoreMissingTaskQueues: true,
		Identity:                identity,
	})
	if err != nil {
		log.Fatalf("Unable to set ramping version: %v", err)
	}
	verifyDeployment(dHandle, build1, "", 20, client.WorkerDeploymentVersionDrainageStatusUnspecified)

	// Set current version to "__unversioned__"
	_, err = dHandle.SetCurrentVersion(context.Background(), client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID:                 "",
		IgnoreMissingTaskQueues: true,
		Identity:                identity,
	})
	if err != nil {
		log.Fatalf("Unable to set current version: %v", err)
	}
	verifyDeployment(dHandle, "", "", 0, client.WorkerDeploymentVersionDrainageStatusDraining)

	// Simulating a scenario when a drained version is reactivated and then re-deactivated.

	// Waiting for the version 1.0 to become drained.
	time.Sleep(8 * time.Second)
	// Make sure 1.0 is drained.
	verifyDeployment(dHandle, "", "", 0, client.WorkerDeploymentVersionDrainageStatusDrained)

	// Rollback a drained version 1.0, so that it is the current version for this deployment
	_, err = dHandle.SetCurrentVersion(context.Background(), client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID:                 build1,
		IgnoreMissingTaskQueues: true,
		Identity:                identity,
	})
	if err != nil {
		log.Fatalf("Unable to set current version: %v", err)
	}

	// Set current version to "__unversioned__" again so that version 1.0 can start draining. This replicates the
	// scenario where a rolled back version is now draining.
	_, err = dHandle.SetCurrentVersion(context.Background(), client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID:                 "",
		IgnoreMissingTaskQueues: true,
		Identity:                identity,
	})
	if err != nil {
		log.Fatalf("Unable to set current version: %v", err)
	}

	// Waiting for the version 1.0 to become drained.
	time.Sleep(8 * time.Second)
	// Make sure 1.0 is drained.
	verifyDeployment(dHandle, "", "", 0, client.WorkerDeploymentVersionDrainageStatusDrained)

	// Exercise the compute config (Worker Controller Instance) paths. Requires
	// `workercontroller.enabled: true` in config/dynamicconfig/development-sql.yaml.
	exerciseComputeConfig(c, deploymentName, build2, identity)

	// Stopping both workers
	w1.Stop()
	w2.Stop()

	// clearing out the poller history to delete the version
	time.Sleep(2 * time.Second)

	// Delete the deployment version
	_, err = dHandle.DeleteVersion(context.Background(), client.WorkerDeploymentDeleteVersionOptions{
		BuildID:      build1,
		SkipDrainage: true,
		Identity:     identity,
	})
	if err != nil {
		log.Fatalf("Unable to delete version: %v", err)
	}

	// Delete the compute-config version. This also tears down its Worker Controller
	// Instance from inside the version workflow.
	_, err = dHandle.DeleteVersion(context.Background(), client.WorkerDeploymentDeleteVersionOptions{
		BuildID:      build2,
		SkipDrainage: true,
		Identity:     identity,
	})
	if err != nil {
		log.Fatalf("Unable to delete version %s: %v", build2, err)
	}

	// Delete the deployment
	_, err = deploymentClient.Delete(context.Background(), client.WorkerDeploymentDeleteOptions{
		Name:     deploymentName,
		Identity: identity,
	})
	if err != nil {
		log.Fatalf("Unable to delete deployment: %v", err)
	}

}

// exerciseComputeConfig drives the compute config paths of the version and deployment
// workflows so the captured histories cover them.
//
// In the version workflow this covers:
//   - starting with a non-nil VersionLocalState.ComputeConfig, which satisfies the
//     `ComputeConfig != nil` guard.
//   - the SignalSyncValidationStatus handler, which sets ComputeStatus and then signals the
//     deployment workflow via syncSummary.
//   - handleUpdateVersionComputeConfig, which also calls syncSummary.
//   - ValidateComputeConfig, both as a dry-run of proposed changes and as a re-validation of
//     the stored spec.
//   - the DescribeWorkerControllerInstanceStatus pull inside syncVersionDataToComputeStatus
//     returning a non-nil ProviderValidation
//
// In the deployment workflow this covers handleCreateWorkerDeploymentVersion's WCI creation
// and the SyncVersionSummary signals arriving from the version workflow.
//
// After regenerating, confirm the new version-workflow history still contains a
// "sync-compute-status-to-deployment" Version marker followed by a sync-version-summary signal;
// if it does not, that fixture does not cover the pull path.
//
//nolint:revive
func exerciseComputeConfig(c client.Client, deploymentName, buildID, identity string) {
	version := &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	}

	// Create the version with a compute config. This creates the WCI first, then starts the
	// version workflow with the resulting ComputeConfigSummary in its initial state.
	_, err := c.WorkflowService().CreateWorkerDeploymentVersion(context.Background(), &workflowservice.CreateWorkerDeploymentVersionRequest{
		Namespace:         client.DefaultNamespace,
		DeploymentVersion: version,
		ComputeConfig: &computepb.ComputeConfig{
			ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
				"group1": newScalingGroup("replaytester-group1", enumspb.TASK_QUEUE_TYPE_WORKFLOW),
			},
		},
		Identity:  identity,
		RequestId: "replaytester-create-version-" + buildID,
	})
	if err != nil {
		log.Fatalf("Unable to create version %s with a compute config (is workercontroller.enabled set?): %v", buildID, err)
	}

	// Dry-run validation of a proposed scaling group: exercises ValidateComputeConfig without
	// mutating the stored spec. Does not set ValidationStatus.
	_, err = c.WorkflowService().ValidateWorkerDeploymentVersionComputeConfig(context.Background(), &workflowservice.ValidateWorkerDeploymentVersionComputeConfigRequest{
		Namespace:         client.DefaultNamespace,
		DeploymentVersion: version,
		ComputeConfigScalingGroups: map[string]*computepb.ComputeConfigScalingGroupUpdate{
			"dry-run-group": {
				ScalingGroup: newScalingGroup("replaytester-dry-run", enumspb.TASK_QUEUE_TYPE_ACTIVITY),
			},
		},
		Identity: identity,
	})
	if err != nil {
		log.Fatalf("Unable to dry-run validate compute config for version %s: %v", buildID, err)
	}

	// Re-validate the stored spec by sending no changes. This is the path that sets
	// ValidationStatus on the WCI and signals the version workflow, which sets ComputeStatus and
	// calls syncSummary.
	_, err = c.WorkflowService().ValidateWorkerDeploymentVersionComputeConfig(context.Background(), &workflowservice.ValidateWorkerDeploymentVersionComputeConfigRequest{
		Namespace:         client.DefaultNamespace,
		DeploymentVersion: version,
		Identity:          identity,
	})
	if err != nil {
		log.Fatalf("Unable to re-validate the stored compute config for version %s "+
			"(does the pinned temporal-auto-scaled-workers include PR #99?): %v", buildID, err)
	}

	// Wait for the validation signal to land, so the captured history contains the ComputeStatus
	// update and the syncSummary it triggers.
	waitForComputeStatus(c, version)

	// Add a second scaling group, exercising handleUpdateVersionComputeConfig.
	_, err = c.WorkflowService().UpdateWorkerDeploymentVersionComputeConfig(context.Background(), &workflowservice.UpdateWorkerDeploymentVersionComputeConfigRequest{
		Namespace:         client.DefaultNamespace,
		DeploymentVersion: version,
		ComputeConfigScalingGroups: map[string]*computepb.ComputeConfigScalingGroupUpdate{
			"group2": {
				ScalingGroup: newScalingGroup("replaytester-group2", enumspb.TASK_QUEUE_TYPE_ACTIVITY),
			},
		},
		Identity:  identity,
		RequestId: "replaytester-update-compute-config-" + buildID,
	})
	if err != nil {
		log.Fatalf("Unable to update compute config for version %s: %v", buildID, err)
	}

	// Remove the group we just added, exercising the removal branch of the same handler.
	_, err = c.WorkflowService().UpdateWorkerDeploymentVersionComputeConfig(context.Background(), &workflowservice.UpdateWorkerDeploymentVersionComputeConfigRequest{
		Namespace:                        client.DefaultNamespace,
		DeploymentVersion:                version,
		RemoveComputeConfigScalingGroups: []string{"group2"},
		Identity:                         identity,
		RequestId:                        "replaytester-remove-compute-config-" + buildID,
	})
	if err != nil {
		log.Fatalf("Unable to remove compute config scaling group for version %s: %v", buildID, err)
	}
}

// waitForComputeStatus polls DescribeWorkerDeployment until the version's ComputeStatus shows
// up in the deployment workflow's version summary.
//
//nolint:revive
func waitForComputeStatus(c client.Client, version *deploymentpb.WorkerDeploymentVersion) {
	log.Printf("Waiting for compute status to propagate to the deployment workflow for version %s...", version.GetBuildId())
	for range 40 {
		resp, err := c.WorkflowService().DescribeWorkerDeployment(context.Background(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      client.DefaultNamespace,
			DeploymentName: version.GetDeploymentName(),
		})
		if err != nil {
			log.Fatalf("Unable to describe deployment %s: %v", version.GetDeploymentName(), err)
		}
		for _, summary := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			if summary.GetDeploymentVersion().GetBuildId() != version.GetBuildId() {
				continue
			}
			if summary.GetComputeStatus().GetProviderValidation() != nil {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("Timed out waiting for compute status to propagate to the deployment workflow for version %s", version.GetBuildId())
}

// encodeProviderDetails encodes provider-specific config the way the server decodes it
//
//nolint:revive
func encodeProviderDetails(details map[string]any) *commonpb.Payload {
	p, err := converter.GetDefaultDataConverter().ToPayload(details)
	if err != nil {
		log.Fatalf("Unable to encode compute provider details: %v", err)
	}
	return p
}

//nolint:revive
func verifyDeployment(dHandle client.WorkerDeploymentHandle,
	expectedCurrentVersionBuildId string,
	expectedRampingVersionBuildId string,
	expectedRampPercentage float32,
	expectedDrainageStatus client.WorkerDeploymentVersionDrainageStatus,
) {
	describeResponse, err := dHandle.Describe(context.Background(), client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		log.Fatalf("Unable to describe deployment: %v", err)
	}
	if cv := describeResponse.Info.RoutingConfig.CurrentVersion; cv != nil {
		if cv.BuildID != expectedCurrentVersionBuildId {
			log.Fatalf("Current version build id is %s not %s", cv.BuildID, expectedCurrentVersionBuildId)
		}
	} else {
		if expectedCurrentVersionBuildId != "" {
			log.Fatalf("Current version is empty, expected build id %s", expectedCurrentVersionBuildId)
		}
	}

	if rv := describeResponse.Info.RoutingConfig.RampingVersion; rv != nil {
		if rv.BuildID != expectedRampingVersionBuildId {
			log.Fatalf("Ramping version build id is %s not %s", rv.BuildID, expectedRampingVersionBuildId)
		}
	} else {
		if expectedRampingVersionBuildId != "" {
			log.Fatalf("Ramping version is empty, expected build id %s", expectedRampingVersionBuildId)
		}
	}

	if rp := describeResponse.Info.RoutingConfig.RampingVersionPercentage; rp != expectedRampPercentage {
		log.Fatalf("Ramping percent is %v, expected %v", rp, expectedRampPercentage)
	}

	if ds := describeResponse.Info.VersionSummaries[0].DrainageStatus; ds != expectedDrainageStatus {
		log.Fatalf("Drainage status is %v, not %v", drainageStatusString(ds), drainageStatusString(expectedDrainageStatus))
	}
}

func drainageStatusString(ds client.WorkerDeploymentVersionDrainageStatus) string {
	switch ds {
	case client.WorkerDeploymentVersionDrainageStatusDrained:
		return "Drained"
	case client.WorkerDeploymentVersionDrainageStatusDraining:
		return "Draining"
	case client.WorkerDeploymentVersionDrainageStatusUnspecified:
		return "Unspecified"
	}
	return "Unknown"
}
