package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

var Module = fx.Module(
	"chasm.lib.scheduler",
	fx.Provide(ConfigProvider),
	fx.Provide(func(dc *dynamicconfig.Collection) *legacyscheduler.SpecBuilder {
		return legacyscheduler.NewSpecBuilder(
			dynamicconfig.SchedulerSpecWarnIterations.Get(dc),
			dynamicconfig.SchedulerSpecMaxIterations.Get(dc),
		)
	}),
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
