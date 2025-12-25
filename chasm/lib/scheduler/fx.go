package scheduler

import (
	"go.temporal.io/server/chasm"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

var FrontendModule = fx.Module(
	"chasm.lib.scheduler-frontend",
	fx.Invoke(func(registry *chasm.Registry) {
		registry.Register(&ComponentOnlyLibrary{})
	}),
)
var Module = fx.Module(
	"chasm.lib.scheduler",
	fx.Provide(ConfigProvider),
	fx.Provide(legacyscheduler.NewSpecBuilder),
	fx.Provide(NewSpecProcessor),
	fx.Provide(func(impl *SpecProcessorImpl) SpecProcessor { return impl }),
	fx.Provide(newHandler),
	fx.Provide(NewSchedulerIdleTaskHandler),
	fx.Provide(NewSchedulerCallbacksTaskHandler),
	fx.Provide(NewGeneratorTaskHandler),
	fx.Provide(NewInvokerExecuteTaskHandler),
	fx.Provide(NewInvokerProcessBufferTaskHandler),
	fx.Provide(NewBackfillerTaskHandler),
	fx.Provide(NewSchedulerMigrateToWorkflowTaskHandler),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)
