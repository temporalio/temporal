package handler

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const HandlerTaskQueue = "handler-task-queue"

// NewWorker returns a new worker, registered with the handler namespace's
// workflows and Nexus operations.
func NewWorker(c client.Client) (worker.Worker, error) {
	w := worker.New(c, HandlerTaskQueue, worker.Options{})

	w.RegisterWorkflow(addOperationWorkflow)

	nexusSvc := nexus.NewService(NexusServiceName)
	err := nexusSvc.Register(addOperation)
	if err != nil {
		return nil, fmt.Errorf("registering Nexus service: %w", err)
	}
	w.RegisterNexusService(nexusSvc)

	return w, nil
}
