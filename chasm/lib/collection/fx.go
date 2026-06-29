package collection

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"collection-history",
	fx.Provide(newHandler),
	fx.Provide(newLibrary),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"collection-frontend",
	fx.Provide(NewComponentOnlyLibrary),
	fx.Invoke(func(l *componentOnlyLibrary, registry *chasm.Registry) error {
		// Frontend registers the component only, to serialize ComponentRefs; it needs no task or
		// gRPC handlers.
		return registry.Register(l)
	}),
)
