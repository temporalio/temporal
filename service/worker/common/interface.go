//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package common

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/namespace"
)

type (
	// WorkerComponent represents a type of work needed for worker role
	WorkerComponent interface {
		// RegisterWorkflow registers Workflow types provided by this worker component.
		// Local Activities should also be registered here because they are executed on the workflow worker.
		RegisterWorkflow(registry sdkworker.Registry)
		// DedicatedWorkflowWorkerOptions returns a DedicatedWorkerOptions for this worker component.
		// Return nil to use default worker instance.
		DedicatedWorkflowWorkerOptions() *DedicatedWorkerOptions
		// RegisterActivities registers remote Activity types provided by this worker component.
		// Local Activities should be registered in RegisterWorkflow
		RegisterActivities(registry sdkworker.Registry)
		// DedicatedActivityWorkerOptions returns a DedicatedWorkerOptions for this worker component.
		// Return nil to use default worker instance.
		DedicatedActivityWorkerOptions() *DedicatedWorkerOptions
	}

	DedicatedWorkerOptions struct {
		// TaskQueue is optional
		TaskQueue string
		// How many worker nodes should run a worker per namespace
		NumWorkers int
		// Other worker options
		Options sdkworker.Options
	}

	// PerNSWorkerComponent represents a per-namespace worker needed for worker role
	PerNSWorkerComponent interface {
		// Register registers Workflow and Activity types provided by this worker component.
		// The namespace that this worker is running in is also provided.
		// If Register returns a function, that function will be called when the worker is
		// stopped. This can be used to clean up any state.
		Register(sdkworker.Registry, *namespace.Namespace, RegistrationDetails) func()
		// DedicatedWorkerOptions returns a PerNSDedicatedWorkerOptions for this worker component.
		DedicatedWorkerOptions(*namespace.Namespace) *PerNSDedicatedWorkerOptions
	}

	PerNSDedicatedWorkerOptions struct {
		// Set this to false to disable this worker for this namespace
		Enabled bool
	}

	RegistrationDetails struct {
		// TotalWorkers is the number of requested per-namespace workers for this namespace.
		TotalWorkers int
		// Multiplicity is the number of those workers that this particular sdkworker.Worker
		// represents. It may be more than one if the requested number is more than the total
		// number of worker nodes or if consistent hashing decided to place more than one on
		// the same node.
		Multiplicity int
	}
)
