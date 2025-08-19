package scheduler

import (
	"go.temporal.io/server/chasm"
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
	fx.Provide(NewGeneratorTaskExecutor),
	fx.Provide(NewInvokerExecuteTaskExecutor),
	fx.Provide(NewInvokerProcessBufferTaskExecutor),
	fx.Provide(NewBackfillerTaskExecutor),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)
