package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	generatorTaskExecutor *GeneratorTaskExecutor,
	invokerExecuteTaskExecutor *InvokerExecuteTaskExecutor,
	invokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor,
	backfillerTaskExecutor *BackfillerTaskExecutor,
) error {
	Library.SetGeneratorTaskExecutor(generatorTaskExecutor)
	Library.SetInvokerExecuteTaskExecutor(invokerExecuteTaskExecutor)
	Library.SetInvokerProcessBufferTaskExecutor(invokerProcessBufferTaskExecutor)
	Library.SetBackfillerTaskExecutor(backfillerTaskExecutor)

	return registry.Register(Library)
}

var Module = fx.Module(
	"chasm.lib.scheduler",
	fx.Provide(NewGeneratorTaskExecutor),
	fx.Provide(NewInvokerExecuteTaskExecutor),
	fx.Provide(NewInvokerProcessBufferTaskExecutor),
	fx.Provide(NewBackfillerTaskExecutor),
	fx.Invoke(Register),
)
