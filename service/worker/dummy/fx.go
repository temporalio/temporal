package dummy

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type workerComponent struct {
	enabledForNs dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

type fxResult struct {
	fx.Out
	Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
}

var Module = fx.Options(fx.Provide(NewResult))

func NewResult(dc *dynamicconfig.Collection) fxResult {
	return fxResult{
		Component: &workerComponent{
			enabledForNs: dynamicconfig.EnableDummyWorkflow.Get(dc),
		},
	}
}

func (c *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, _ workercommon.RegistrationDetails) func() {
	registry.RegisterWorkflowWithOptions(DummyWorkflow, workflow.RegisterOptions{Name: DummyWFTypeName})
	return nil
}

func (c *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: c.enabledForNs(ns.Name().String()),
	}
}
