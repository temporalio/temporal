package main

import (
	"context"
	"log"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/service/worker/workerdeployment/replaytester"
)

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
			UpsertEntries: map[string]interface{}{
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

	// Delete the deployment
	_, err = deploymentClient.Delete(context.Background(), client.WorkerDeploymentDeleteOptions{
		Name:     deploymentName,
		Identity: identity,
	})
	if err != nil {
		log.Fatalf("Unable to delete deployment: %v", err)
	}

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
