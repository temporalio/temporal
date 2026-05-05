package callback

import (
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

// componentOnlyLibrary only containing the component definitions, but no implementation details.
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
			chasm.WithBusinessIDAlias("CallbackId"),
			chasm.WithSearchAttributes(executionStatusSearchAttribute),
		),
	}
}

type library struct {
	componentOnlyLibrary

	config                                      *Config
	InvocationTaskHandler                       *invocationTaskHandler
	BackoffTaskHandler                          *backoffTaskHandler
	CompletionScheduleToCloseTimeoutTaskHandler *CompletionScheduleToCloseTimeoutTaskHandler
	callbackSvcHandler                          *callbackHandler
}

func newLibrary(
	InvocationTaskHandler *invocationTaskHandler,
	BackoffTaskHandler *backoffTaskHandler,
	CompletionScheduleToCloseTimeoutTaskHandler *CompletionScheduleToCloseTimeoutTaskHandler,
	callbackSvcHandler *callbackHandler,
) *library {
	return &library{
		InvocationTaskHandler:                       InvocationTaskHandler,
		BackoffTaskHandler:                          BackoffTaskHandler,
		CompletionScheduleToCloseTimeoutTaskHandler: CompletionScheduleToCloseTimeoutTaskHandler,
		callbackSvcHandler:                          callbackSvcHandler,
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
			"completionScheduleToCloseTimer",
			l.CompletionScheduleToCloseTimeoutTaskHandler,
		),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	callbackspb.RegisterCallbackServiceServer(server, l.callbackSvcHandler)
}
