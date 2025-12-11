package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/grpc"
)

type (
	Library struct {
		chasm.UnimplementedLibrary

		handler *handler

		SchedulerIdleTaskExecutor        *SchedulerIdleTaskExecutor
		GeneratorTaskExecutor            *GeneratorTaskExecutor
		InvokerExecuteTaskExecutor       *InvokerExecuteTaskExecutor
		InvokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor
		BackfillerTaskExecutor           *BackfillerTaskExecutor
	}
)

func NewLibrary(
	handler *handler,
	SchedulerIdleTaskExecutor *SchedulerIdleTaskExecutor,
	GeneratorTaskExecutor *GeneratorTaskExecutor,
	InvokerExecuteTaskExecutor *InvokerExecuteTaskExecutor,
	InvokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor,
	BackfillerTaskExecutor *BackfillerTaskExecutor,
) *Library {
	return &Library{
		handler:                          handler,
		SchedulerIdleTaskExecutor:        SchedulerIdleTaskExecutor,
		GeneratorTaskExecutor:            GeneratorTaskExecutor,
		InvokerExecuteTaskExecutor:       InvokerExecuteTaskExecutor,
		InvokerProcessBufferTaskExecutor: InvokerProcessBufferTaskExecutor,
		BackfillerTaskExecutor:           BackfillerTaskExecutor,
	}
}

func (l *Library) Name() string {
	return chasm.SchedulerLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler](
			chasm.SchedulerComponentName,
			chasm.WithBusinessIDAlias("ScheduleId"),
		),
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

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&schedulerpb.SchedulerService_ServiceDesc, l.handler)
}
