package dummy

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/namespace"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type workerComponent struct{}

type fxResult struct {
	fx.Out
	Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
}

var Module = fx.Options(fx.Provide(NewResult))

func NewResult() fxResult {
	return fxResult{
		Component: &workerComponent{},
	}
}

func (c *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, _ workercommon.RegistrationDetails) func() {
	registry.RegisterWorkflowWithOptions(DummyWorkflow, workflow.RegisterOptions{Name: DummyWFTypeName})
	return nil
}

func (c *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}
}
