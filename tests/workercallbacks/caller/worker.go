package caller

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const callerTaskQueue = "caller-task-queue"

// NewWorker returns a new worker, registered with the handler namespace's
// workflows and Nexus operations.
func NewWorker(c client.Client) (worker.Worker, error) {
	w := worker.New(c, callerTaskQueue, worker.Options{})

	// TODO(chrsmith): Wrap this in a faux-SDK call. But really, we are just
	// registering an Activity handler for the SAA that are created to track
	// worker callbacks.
	w.RegisterActivityWithOptions(
		onNexusOpCompleteActivity,
		activity.RegisterOptions{
			Name: "onNexusOpCompleteActivity",
		})

	return w, nil
}
