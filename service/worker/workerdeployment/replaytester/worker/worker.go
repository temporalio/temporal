package main

import (
	"context"
	"log"
	"time"

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

	deploymentName := "foo"
	w1 := worker.New(c, "hello-world", worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   deploymentName + ".1.0",
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		},
	})

	w2 := worker.New(c, "hello-world-2", worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   deploymentName + ".1.0",
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
		log.Fatalln("Unable to start worker", err)
	}

	err = w2.Start()
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}

	// Wait for the workers to register the version.
	time.Sleep(5 * time.Second)

	// Testing various worker-deployment API's
	deploymentClient := c.WorkerDeploymentClient()
	dHandle := deploymentClient.GetHandle(deploymentName)

	// Update version metadata
	_, err = dHandle.UpdateVersionMetadata(context.Background(), client.WorkerDeploymentUpdateVersionMetadataOptions{
		Version: deploymentName + ".1.0",
		MetadataUpdate: client.WorkerDeploymentMetadataUpdate{
			UpsertEntries: map[string]interface{}{
				"key": "value",
			},
		},
	})
	if err != nil {
		log.Fatalln("Unable to update version metadata", err)
	}

	// Set ramping version to 1.0
	_, err = dHandle.SetRampingVersion(context.Background(), client.WorkerDeploymentSetRampingVersionOptions{
		Version: deploymentName + ".1.0",
	})
	if err != nil {
		log.Fatalln("Unable to set ramping version", err)
	}
	verifyDeployment(dHandle, "__unversioned__", deploymentName+".1.0", client.WorkerDeploymentVersionDrainageStatusUnspecified)

	// Unset the ramping version
	_, err = dHandle.SetRampingVersion(context.Background(), client.WorkerDeploymentSetRampingVersionOptions{
		Version: "",
	})
	if err != nil {
		log.Fatalln("Unable to unset ramping version", err)
	}
	verifyDeployment(dHandle, "__unversioned__", "", client.WorkerDeploymentVersionDrainageStatusDraining)

	// Set current version to 1.0
	_, err = dHandle.SetCurrentVersion(context.Background(), client.WorkerDeploymentSetCurrentVersionOptions{
		Version:                 deploymentName + ".1.0",
		IgnoreMissingTaskQueues: true,
	})
	if err != nil {
		log.Fatalln("Unable to set current version", err)
	}
	verifyDeployment(dHandle, deploymentName+".1.0", "", client.WorkerDeploymentVersionDrainageStatusUnspecified)

	// Ramp the "__unversioned__" version
	_, err = dHandle.SetRampingVersion(context.Background(), client.WorkerDeploymentSetRampingVersionOptions{
		Version:                 "__unversioned__",
		Percentage:              20,
		IgnoreMissingTaskQueues: true,
	})
	if err != nil {
		log.Fatalln("Unable to set ramping version", err)
	}
	verifyDeployment(dHandle, deploymentName+".1.0", "__unversioned__", client.WorkerDeploymentVersionDrainageStatusUnspecified)

	// Set current version to "__unversioned__"
	_, err = dHandle.SetCurrentVersion(context.Background(), client.WorkerDeploymentSetCurrentVersionOptions{
		Version:                 "__unversioned__",
		IgnoreMissingTaskQueues: true,
	})
	if err != nil {
		log.Fatalln("Unable to set current version", err)
	}
	verifyDeployment(dHandle, "__unversioned__", "", client.WorkerDeploymentVersionDrainageStatusDraining)

	// Stopping both workers
	w1.Stop()
	w2.Stop()

	// clearing out the poller history to delete the version
	time.Sleep(2 * time.Second)

	// Delete the deployment version
	_, err = dHandle.DeleteVersion(context.Background(), client.WorkerDeploymentDeleteVersionOptions{
		Version:      deploymentName + ".1.0",
		SkipDrainage: true,
	})
	if err != nil {
		log.Fatalln("Unable to delete version", err)
	}

	// Delete the deployment
	_, err = deploymentClient.Delete(context.Background(), client.WorkerDeploymentDeleteOptions{
		Name: deploymentName,
	})
	if err != nil {
		log.Fatalln("Unable to delete deployment", err)
	}

}

//nolint:revive
func verifyDeployment(dHandle client.WorkerDeploymentHandle,
	expectedCurrentVersion string,
	expectedRampingVersion string,
	expectedDrainageStatus client.WorkerDeploymentVersionDrainageStatus,
) {
	describeResponse, err := dHandle.Describe(context.Background(), client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		log.Fatalln("Unable to describe deployment", err)
	}
	if describeResponse.Info.RoutingConfig.CurrentVersion != expectedCurrentVersion {
		log.Fatalln("Current version is not ", expectedCurrentVersion)
	}

	if describeResponse.Info.RoutingConfig.RampingVersion != expectedRampingVersion {
		log.Fatalln("Ramping version is not ", expectedRampingVersion)
	}

	if describeResponse.Info.VersionSummaries[0].DrainageStatus != expectedDrainageStatus {
		log.Fatalln("Drainage status is not ", expectedDrainageStatus)
	}
}
