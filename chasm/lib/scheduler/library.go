package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

type (
	Library struct {
		fx.In

		chasm.UnimplementedLibrary

		SchedulerIdleTaskExecutor        *SchedulerIdleTaskExecutor
		GeneratorTaskExecutor            *GeneratorTaskExecutor
		InvokerExecuteTaskExecutor       *InvokerExecuteTaskExecutor
		InvokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor
		BackfillerTaskExecutor           *BackfillerTaskExecutor
	}
)

func NewLibrary(
	SchedulerIdleTaskExecutor *SchedulerIdleTaskExecutor,
	GeneratorTaskExecutor *GeneratorTaskExecutor,
	InvokerExecuteTaskExecutor *InvokerExecuteTaskExecutor,
	InvokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor,
	BackfillerTaskExecutor *BackfillerTaskExecutor,
) *Library {
	return &Library{
		SchedulerIdleTaskExecutor:        SchedulerIdleTaskExecutor,
		GeneratorTaskExecutor:            GeneratorTaskExecutor,
		InvokerExecuteTaskExecutor:       InvokerExecuteTaskExecutor,
		InvokerProcessBufferTaskExecutor: InvokerProcessBufferTaskExecutor,
		BackfillerTaskExecutor:           BackfillerTaskExecutor,
	}
}

func (l *Library) Name() string {
	return "scheduler"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler]("scheduler"),
		chasm.NewRegistrableComponent[*Generator]("generator"),
		chasm.NewRegistrableComponent[*Invoker]("invoker"),
		chasm.NewRegistrableComponent[*Backfiller]("backfiller"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"idle",
			l.SchedulerIdleTaskExecutor,
			l.SchedulerIdleTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"generate",
			l.GeneratorTaskExecutor,
			l.GeneratorTaskExecutor,
		),
		chasm.NewRegistrableSideEffectTask(
			"execute",
			l.InvokerExecuteTaskExecutor,
			l.InvokerExecuteTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"processBuffer",
			l.InvokerProcessBufferTaskExecutor,
			l.InvokerProcessBufferTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"backfill",
			l.BackfillerTaskExecutor,
			l.BackfillerTaskExecutor,
		),
	}
}
