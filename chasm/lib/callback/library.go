package callback

import (
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

// Slimmed down library, only defining the Callback, CallbackExecution components
// but not any of their implementation details.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary(config *Config, namespaceRegistry namespace.Registry) *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return chasm.CallbackLibraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Callback](
			chasm.CallbackComponentName,
			chasm.WithDetached(),
		),
		chasm.NewRegistrableComponent[*CallbackExecution](
			chasm.CallbackExecutionComponentName,
			chasm.WithBusinessIDAlias("CallbackId"),
			chasm.WithSearchAttributes(executionStatusSearchAttribute),
		),
	}
}

// Defines the complete CHASM library for the callback-related components.
type library struct {
	componentOnlyLibrary

	config                            *Config
	InvocationTaskHandler             *invocationTaskHandler
	BackoffTaskHandler                *backoffTaskHandler
	ScheduleToCloseTimeoutTaskHandler *ScheduleToCloseTimeoutTaskHandler
	callbackExecutionHandler          *callbackExecutionHandler
}

func newLibrary(
	InvocationTaskHandler *invocationTaskHandler,
	BackoffTaskHandler *backoffTaskHandler,
	ScheduleToCloseTimeoutTaskHandler *ScheduleToCloseTimeoutTaskHandler,
	callbackExecutionHandler *callbackExecutionHandler,
) *library {
	return &library{
		InvocationTaskHandler:             InvocationTaskHandler,
		BackoffTaskHandler:                BackoffTaskHandler,
		ScheduleToCloseTimeoutTaskHandler: ScheduleToCloseTimeoutTaskHandler,
		callbackExecutionHandler:          callbackExecutionHandler,
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invoke",
			l.InvocationTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"backoff",
			l.BackoffTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToCloseTimer",
			l.ScheduleToCloseTimeoutTaskHandler,
		),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	callbackspb.RegisterCallbackExecutionServiceServer(server, l.callbackExecutionHandler)
}
