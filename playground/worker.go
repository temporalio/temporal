package playground

import (
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func startWorker(c client.Client) func() {
	ready := make(chan bool)
	stop := make(chan bool)
	stopped := make(chan bool)

	go func() {
		w := worker.New(c, taskQueue, worker.Options{})
		w.RegisterWorkflow(Workflow)

		defer func() {
			fmt.Println("[[ WORKER STOPPING ]]")
			w.Stop()
			stopped <- true
		}()

		err := w.Start()
		if err != nil {
			panic(err)
		}

		ready <- true
		<-stop
	}()

	<-ready
	fmt.Println("[[ WORKER STARTED ]]")

	return func() {
		stop <- true
		<-stopped
	}
}
