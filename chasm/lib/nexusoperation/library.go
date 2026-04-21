package nexusoperation

import (
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

const TaskGroupName = "nexusoperation"

type ctxKeyOperationContextType struct{}

var ctxKeyOperationContext = ctxKeyOperationContextType{}

// operationContext holds dependencies injected into the chasm.Context for use by Operation methods.
type operationContext struct {
	namespaceRegistry namespace.Registry
	metricTagConfig   dynamicconfig.TypedPropertyFn[NexusMetricTagConfig]
}

// componentOnlyLibrary registers just the components without task executors or gRPC handlers.
// Used in the frontend to enable component ref serialization.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
	namespaceRegistry namespace.Registry
	metricTagConfig   dynamicconfig.TypedPropertyFn[NexusMetricTagConfig]
}

func newComponentOnlyLibrary(namespaceRegistry namespace.Registry, dc *dynamicconfig.Collection) *componentOnlyLibrary {
	return &componentOnlyLibrary{
		namespaceRegistry: namespaceRegistry,
		metricTagConfig:   MetricTagConfiguration.Get(dc),
	}
}

func (l *componentOnlyLibrary) Name() string {
	return "nexusoperation"
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Operation](
			"operation",
			chasm.WithSearchAttributes(
				EndpointSearchAttribute,
				ServiceSearchAttribute,
				OperationSearchAttribute,
				StatusSearchAttribute,
			),
			chasm.WithBusinessIDAlias("OperationId"),
			chasm.WithContextValues(map[any]any{
				ctxKeyOperationContext: &operationContext{
					namespaceRegistry: l.namespaceRegistry,
					metricTagConfig:   l.metricTagConfig,
				},
			}),
		),
		chasm.NewRegistrableComponent[*Cancellation]("cancellation"),
	}
}

type Library struct {
	componentOnlyLibrary

	handler *handler

	operationBackoffTaskHandler                *operationBackoffTaskHandler
	operationInvocationTaskHandler             *operationInvocationTaskHandler
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler
	operationStartToCloseTimeoutTaskHandler    *operationStartToCloseTimeoutTaskHandler

	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler
	cancellationBackoffTaskHandler    *cancellationBackoffTaskHandler
}

func newLibrary(
	handler *handler,
	operationBackoffTaskHandler *operationBackoffTaskHandler,
	operationInvocationTaskHandler *operationInvocationTaskHandler,
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler,
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler,
	operationStartToCloseTimeoutTaskHandler *operationStartToCloseTimeoutTaskHandler,
	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler,
	cancellationBackoffTaskHandler *cancellationBackoffTaskHandler,
	namespaceRegistry namespace.Registry,
	dc *dynamicconfig.Collection,
) *Library {
	return &Library{
		componentOnlyLibrary:                       *newComponentOnlyLibrary(namespaceRegistry, dc),
		handler:                                    handler,
		operationBackoffTaskHandler:                operationBackoffTaskHandler,
		operationInvocationTaskHandler:             operationInvocationTaskHandler,
		operationScheduleToCloseTimeoutTaskHandler: operationScheduleToCloseTimeoutTaskHandler,
		operationScheduleToStartTimeoutTaskHandler: operationScheduleToStartTimeoutTaskHandler,
		operationStartToCloseTimeoutTaskHandler:    operationStartToCloseTimeoutTaskHandler,
		cancellationInvocationTaskHandler:          cancellationInvocationTaskHandler,
		cancellationBackoffTaskHandler:             cancellationBackoffTaskHandler,
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invocation",
			l.operationInvocationTaskHandler,
			chasm.WithTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("invocationBackoff", l.operationBackoffTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToStartTimeout", l.operationScheduleToStartTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("startToCloseTimeout", l.operationStartToCloseTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToCloseTimeout", l.operationScheduleToCloseTimeoutTaskHandler),
		chasm.NewRegistrableSideEffectTask(
			"cancellation",
			l.cancellationInvocationTaskHandler,
			chasm.WithTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("cancellationBackoff", l.cancellationBackoffTaskHandler),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&nexusoperationpb.NexusOperationService_ServiceDesc, l.handler)
}
