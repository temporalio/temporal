package wci

import (
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/grpc"
)

const (
	LibraryName       = "wci"
	InstanceComponent = "instance"
)

// Archetype identifies WCI executions, e.g. for visibility queries.
var Archetype = chasm.Archetype(chasm.FullyQualifiedName(LibraryName, InstanceComponent))

type Library struct {
	chasm.UnimplementedLibrary

	handler *handler

	FetchSnapshotTaskHandler         *FetchSnapshotTaskHandler
	DeferredDecisionTaskHandler      *DeferredDecisionTaskHandler
	ResizeTaskHandler                *ResizeTaskHandler
	InvokeTaskHandler                *InvokeTaskHandler
	ValidateSpecTaskHandler          *ValidateSpecTaskHandler
	SignalVersionWorkflowTaskHandler *SignalVersionWorkflowTaskHandler
	MigrateToWorkflowTaskHandler     *MigrateToWorkflowTaskHandler
}

// NewNilLibrary has no handlers; non-history services and offline readers (tdbg) use it to
// resolve and decode WCI components and tasks.
func NewNilLibrary() *Library {
	return &Library{}
}

func NewLibrary(
	handler *handler,
	fetchSnapshot *FetchSnapshotTaskHandler,
	deferredDecision *DeferredDecisionTaskHandler,
	resize *ResizeTaskHandler,
	invoke *InvokeTaskHandler,
	validateSpec *ValidateSpecTaskHandler,
	signalVersionWorkflow *SignalVersionWorkflowTaskHandler,
	migrateToWorkflow *MigrateToWorkflowTaskHandler,
) *Library {
	return &Library{
		handler:                          handler,
		FetchSnapshotTaskHandler:         fetchSnapshot,
		DeferredDecisionTaskHandler:      deferredDecision,
		ResizeTaskHandler:                resize,
		InvokeTaskHandler:                invoke,
		ValidateSpecTaskHandler:          validateSpec,
		SignalVersionWorkflowTaskHandler: signalVersionWorkflow,
		MigrateToWorkflowTaskHandler:     migrateToWorkflow,
	}
}

func (l *Library) Name() string {
	return LibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*WorkerControllerInstance](InstanceComponent, chasm.WithBusinessIDAlias("WorkerControllerInstanceId")),
		chasm.NewRegistrableComponent[*ScalingGroup]("scaling_group"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("metricsPollTimer", &MetricsPollTimerTaskHandler{},
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace)),
		chasm.NewRegistrablePureTask("periodicValidationTimer", &PeriodicValidationTimerTaskHandler{},
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace)),
		chasm.NewRegistrablePureTask("scalingGroupBackoff", &ScalingGroupBackoffTaskHandler{}),
		chasm.NewRegistrableSideEffectTask("fetchSnapshot", l.FetchSnapshotTaskHandler,
			chasm.WithSingletonTask(chasm.SingletonTaskModeIgnore)),
		chasm.NewRegistrableSideEffectTask("deferredDecision", l.DeferredDecisionTaskHandler),
		chasm.NewRegistrableSideEffectTask("validateSpec", l.ValidateSpecTaskHandler,
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace)),
		chasm.NewRegistrableSideEffectTask("signalVersionWorkflow", l.SignalVersionWorkflowTaskHandler,
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace)),
		chasm.NewRegistrableSideEffectTask("resize", l.ResizeTaskHandler,
			chasm.WithTaskGroup(computeTaskGroup)),
		chasm.NewRegistrableSideEffectTask("invoke", l.InvokeTaskHandler,
			chasm.WithTaskGroup(computeTaskGroup), chasm.WithSingletonTask(chasm.SingletonTaskModeIgnore)),
		chasm.NewRegistrableSideEffectTask("migrateToWorkflow", l.MigrateToWorkflowTaskHandler,
			chasm.WithSingletonTask(chasm.SingletonTaskModeIgnore)),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&wcipb.WorkerControllerInstanceService_ServiceDesc, l.handler)
}
