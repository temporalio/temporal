package scheduler

import "go.temporal.io/server/chasm"

type (
	library struct {
		chasm.UnimplementedLibrary

		schedulerIdleTaskExecutor        *SchedulerIdleTaskExecutor
		generatorTaskExecutor            *GeneratorTaskExecutor
		invokerExecuteTaskExecutor       *InvokerExecuteTaskExecutor
		invokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor
		backfillerTaskExecutor           *BackfillerTaskExecutor
	}
)

var Library = &library{}

func (l *library) SetSchedulerIdleTaskExecutor(executor *SchedulerIdleTaskExecutor) {
	l.schedulerIdleTaskExecutor = executor
}

func (l *library) SetGeneratorTaskExecutor(executor *GeneratorTaskExecutor) {
	l.generatorTaskExecutor = executor
}

func (l *library) SetInvokerExecuteTaskExecutor(executor *InvokerExecuteTaskExecutor) {
	l.invokerExecuteTaskExecutor = executor
}

func (l *library) SetInvokerProcessBufferTaskExecutor(executor *InvokerProcessBufferTaskExecutor) {
	l.invokerProcessBufferTaskExecutor = executor
}

func (l *library) SetBackfillerTaskExecutor(executor *BackfillerTaskExecutor) {
	l.backfillerTaskExecutor = executor
}

func (l *library) Name() string {
	return "scheduler"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler]("scheduler"),
		chasm.NewRegistrableComponent[*Generator]("generator"),
		chasm.NewRegistrableComponent[*Invoker]("invoker"),
		chasm.NewRegistrableComponent[*Backfiller]("backfiller"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"idle",
			l.schedulerIdleTaskExecutor,
			l.schedulerIdleTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"generate",
			l.generatorTaskExecutor,
			l.generatorTaskExecutor,
		),
		chasm.NewRegistrableSideEffectTask(
			"execute",
			l.invokerExecuteTaskExecutor,
			l.invokerExecuteTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"processBuffer",
			l.invokerProcessBufferTaskExecutor,
			l.invokerProcessBufferTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"backfill",
			l.backfillerTaskExecutor,
			l.backfillerTaskExecutor,
		),
	}
}
