package nexusoperation

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

const (
	libraryName   = "nexusoperation"
	componentName = "operation"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type operationContextKeyType struct{}

// OperationContextKey is the context key for OperationContext, registered as a CHASM component
// context value. Exported for use in tests that need to set up MockContext.
var OperationContextKey = operationContextKeyType{}

// DestinationBlockedFn reports whether the outbound queue is blocking an endpoint.
type DestinationBlockedFn func(namespaceID string, destination string) bool

// OperationContext holds dependencies injected into the chasm.Context for use by Operation methods.
type OperationContext struct {
	DestinationBlocked DestinationBlockedFn
	MetricTagConfig    dynamicconfig.TypedPropertyFn[NexusMetricTagConfig]
}

// componentOnlyLibrary registers just the components without task executors or gRPC handlers.
// Used in the frontend to enable component ref serialization.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
	destinationBlocked DestinationBlockedFn
	metricTagConfig    dynamicconfig.TypedPropertyFn[NexusMetricTagConfig]
}

func newComponentOnlyLibrary(dc *dynamicconfig.Collection) *componentOnlyLibrary {
	return &componentOnlyLibrary{
		metricTagConfig: MetricTagConfiguration.Get(dc),
	}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Operation](
			componentName,
			chasm.WithExecutionType(enumspb.EXECUTION_TYPE_NEXUS_OPERATION),
			chasm.WithSearchAttributes(
				EndpointSearchAttribute,
				ServiceSearchAttribute,
				OperationSearchAttribute,
				RequestIDSearchAttribute,
				StatusSearchAttribute,
			),
			chasm.WithBusinessIDAlias("OperationId"),
			chasm.WithContextValues(map[any]any{
				OperationContextKey: &OperationContext{
					MetricTagConfig:    l.metricTagConfig,
					DestinationBlocked: l.destinationBlocked,
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

type libraryParams struct {
	fx.In

	Handler                                    *handler
	OperationBackoffTaskHandler                *operationBackoffTaskHandler
	OperationInvocationTaskHandler             *operationInvocationTaskHandler
	OperationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler
	OperationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler
	OperationStartToCloseTimeoutTaskHandler    *operationStartToCloseTimeoutTaskHandler
	CancellationInvocationTaskHandler          *cancellationInvocationTaskHandler
	CancellationBackoffTaskHandler             *cancellationBackoffTaskHandler
	DynamicConfig                              *dynamicconfig.Collection
	DestinationBlocked                         DestinationBlockedFn `optional:"true"`
}

func newLibrary(params libraryParams) *Library {
	return &Library{
		componentOnlyLibrary: componentOnlyLibrary{
			metricTagConfig:    MetricTagConfiguration.Get(params.DynamicConfig),
			destinationBlocked: params.DestinationBlocked,
		},
		handler:                                    params.Handler,
		operationBackoffTaskHandler:                params.OperationBackoffTaskHandler,
		operationInvocationTaskHandler:             params.OperationInvocationTaskHandler,
		operationScheduleToCloseTimeoutTaskHandler: params.OperationScheduleToCloseTimeoutTaskHandler,
		operationScheduleToStartTimeoutTaskHandler: params.OperationScheduleToStartTimeoutTaskHandler,
		operationStartToCloseTimeoutTaskHandler:    params.OperationStartToCloseTimeoutTaskHandler,
		cancellationInvocationTaskHandler:          params.CancellationInvocationTaskHandler,
		cancellationBackoffTaskHandler:             params.CancellationBackoffTaskHandler,
	}
}

// NewNilLibrary returns a Library with nil handlers, for decoding contexts such as tdbg where
// no task execution happens.
func NewNilLibrary() *Library { return &Library{} }

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
