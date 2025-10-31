package callback

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

// httpCallerProviderProvider adapts the HSM callbacks HTTPCallerProvider to the CHASM callback HTTPCallerProvider.
// Since both types have the same signature, we just need to wrap the function.
func httpCallerProviderProvider(hsmProvider callbacks.HTTPCallerProvider) HTTPCallerProvider {
	return func(dest queues.NamespaceIDAndDestination) HTTPCaller {
		hsmCaller := hsmProvider(dest)
		// Convert callbacks.HTTPCaller to callback.HTTPCaller
		return HTTPCaller(hsmCaller)
	}
}

var Module = fx.Module(
	"chasm.lib.callback",
	fx.Provide(configProvider),
	fx.Provide(httpCallerProviderProvider),
	fx.Provide(newInvocationTaskExecutor),
	fx.Provide(newBackoffTaskExecutor),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)
