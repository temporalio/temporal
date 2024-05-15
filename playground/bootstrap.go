package playground

import (
	"go.temporal.io/sdk/client"
)

var WORKFLOW_ID = "MY-WORKFLOW"

func Bootstrap() {
	stopServer := startServer()

	c, err := client.Dial(client.Options{})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	done := starter(c)

	stopWorker := startWorker(c)

	<-done

	stopWorker()
	stopServer()
}
