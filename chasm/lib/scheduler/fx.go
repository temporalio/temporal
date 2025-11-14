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

var Module = fx.Module(
	"chasm.lib.scheduler",
	fx.Provide(ConfigProvider),
	fx.Provide(legacyscheduler.NewSpecBuilder),
	fx.Provide(NewSpecProcessor),
	fx.Provide(func(impl *SpecProcessorImpl) SpecProcessor { return impl }),
	fx.Provide(newHandler),
	fx.Provide(NewSchedulerIdleTaskExecutor),
	fx.Provide(NewGeneratorTaskExecutor),
	fx.Provide(NewInvokerExecuteTaskExecutor),
	fx.Provide(NewInvokerProcessBufferTaskExecutor),
	fx.Provide(NewBackfillerTaskExecutor),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)
