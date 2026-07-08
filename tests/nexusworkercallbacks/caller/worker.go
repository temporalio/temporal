package caller

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const CallerTaskQueue = "caller-task-queue"

func NewWorker(c client.Client) (worker.Worker, error) {
	w := worker.New(c, CallerTaskQueue, worker.Options{})

	nexusSvc := nexus.NewService(NexusCompletionServiceName)
	err := nexusSvc.Register(completionHandler)
	if err != nil {
		return nil, fmt.Errorf("registering Nexus service: %w", err)
	}
	w.RegisterNexusService(nexusSvc)

	return w, nil
}
