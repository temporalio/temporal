package callback

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

// invocationTaskType is the task type of the side effect task that delivers a callback.
const invocationTaskType = "invoke"

// InvocationTaskGroup is the outbound queue task group that callback invocation tasks are
// scheduled under. The queue's per-destination rate limiters and circuit breakers are keyed by it.
var InvocationTaskGroup = chasm.FullyQualifiedName(chasm.CallbackLibraryName, invocationTaskType)

// DestinationBlockedFn reports whether the outbound queue is currently holding back callback
// deliveries to the given destination, i.e. whether its circuit breaker is open.
type DestinationBlockedFn func(namespaceID string, destination string) bool

type ctxKeyCallbackContextType struct{}

var ctxKeyCallbackContext = ctxKeyCallbackContextType{}

// callbackContext holds the dependencies injected into the chasm.Context for use by Callback
// methods.
type callbackContext struct {
	destinationBlocked DestinationBlockedFn
}

// callbackContextFromChasm extracts the callbackContext from a chasm.Context. Returns nil in
// processes that registered the library without it, such as tdbg.
func callbackContextFromChasm(ctx chasm.Context) *callbackContext {
	cbCtx, _ := ctx.Value(ctxKeyCallbackContext).(*callbackContext)
	return cbCtx
}

type (
	Library struct {
		chasm.UnimplementedLibrary

		InvocationTaskHandler *invocationTaskHandler
		BackoffTaskHandler    *backoffTaskHandler

		destinationBlocked DestinationBlockedFn
	}
)

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

type libraryParams struct {
	fx.In

	InvocationTaskHandler *invocationTaskHandler
	BackoffTaskHandler    *backoffTaskHandler
	// Only the history service runs the outbound queue, so only it can report whether a
	// destination is blocked. Elsewhere callbacks are simply never reported as blocked.
	DestinationBlocked DestinationBlockedFn `optional:"true"`
}

func newLibrary(params libraryParams) *Library {
	return &Library{
		InvocationTaskHandler: params.InvocationTaskHandler,
		BackoffTaskHandler:    params.BackoffTaskHandler,
		destinationBlocked:    params.DestinationBlocked,
	}
}

func (l *Library) Name() string {
	return chasm.CallbackLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Callback](
			chasm.CallbackComponentName,
			chasm.WithDetached(),
			chasm.WithContextValues(map[any]any{
				ctxKeyCallbackContext: &callbackContext{
					destinationBlocked: l.destinationBlocked,
				},
			}),
		),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			invocationTaskType,
			l.InvocationTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"backoff",
			l.BackoffTaskHandler,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
}
