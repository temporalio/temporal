package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/grpc"
)

type ComponentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *ComponentOnlyLibrary) Name() string {
	return chasm.SchedulerLibraryName
}

func (l *ComponentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler](
			chasm.SchedulerComponentName,
			chasm.WithBusinessIDAlias("ScheduleId"),
			chasm.WithSearchAttributes(executionStatusSearchAttribute),
		),
		chasm.NewRegistrableComponent[*Generator]("generator"),
		chasm.NewRegistrableComponent[*Invoker]("invoker"),
		chasm.NewRegistrableComponent[*Backfiller]("backfiller"),
	}
}

type Library struct {
	ComponentOnlyLibrary

	handler *handler

	SchedulerIdleTaskExecutor        *SchedulerIdleTaskExecutor
	GeneratorTaskExecutor            *GeneratorTaskExecutor
	InvokerExecuteTaskExecutor       *InvokerExecuteTaskExecutor
	InvokerProcessBufferTaskExecutor *InvokerProcessBufferTaskExecutor
	BackfillerTaskExecutor           *BackfillerTaskExecutor
}

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

func NewLibrary(
	handler *handler,
	SchedulerIdleTaskHandler *SchedulerIdleTaskHandler,
	SchedulerCallbacksTaskHandler *SchedulerCallbacksTaskHandler,
	GeneratorTaskHandler *GeneratorTaskHandler,
	InvokerExecuteTaskHandler *InvokerExecuteTaskHandler,
	InvokerProcessBufferTaskHandler *InvokerProcessBufferTaskHandler,
	BackfillerTaskHandler *BackfillerTaskHandler,
	MigrateToWorkflowTaskHandler *SchedulerMigrateToWorkflowTaskHandler,
) *Library {
	return &Library{
		handler:                         handler,
		SchedulerIdleTaskHandler:        SchedulerIdleTaskHandler,
		SchedulerCallbacksTaskHandler:   SchedulerCallbacksTaskHandler,
		GeneratorTaskHandler:            GeneratorTaskHandler,
		InvokerExecuteTaskHandler:       InvokerExecuteTaskHandler,
		InvokerProcessBufferTaskHandler: InvokerProcessBufferTaskHandler,
		BackfillerTaskHandler:           BackfillerTaskHandler,
		MigrateToWorkflowTaskHandler:    MigrateToWorkflowTaskHandler,
	}
}


func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"idle",
			l.SchedulerIdleTaskHandler,
		),
		chasm.NewRegistrableSideEffectTask(
			"callbacks",
			l.SchedulerCallbacksTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"generate",
			l.GeneratorTaskHandler,
		),
		chasm.NewRegistrableSideEffectTask(
			"execute",
			l.InvokerExecuteTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"processBuffer",
			l.InvokerProcessBufferTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"backfill",
			l.BackfillerTaskHandler,
		),
		chasm.NewRegistrableSideEffectTask(
			"migrateToWorkflow",
			l.MigrateToWorkflowTaskHandler,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&schedulerpb.SchedulerService_ServiceDesc, l.handler)
}
