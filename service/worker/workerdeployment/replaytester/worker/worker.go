package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	replaytester "go.temporal.io/server/service/worker/workerdeployment/replaytester"
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

	w1.RegisterWorkflowWithOptions(replaytester.HelloWorld, workflow.RegisterOptions{
		Name:               "HelloWorld",
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	err = w1.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
