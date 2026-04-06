package workspace

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

// HistoryModule provides workspace CHASM component for history services.
var HistoryModule = fx.Module(
	"workspace-history",
	fx.Provide(ConfigProvider),
	fx.Provide(func(config *Config, logger log.Logger) *Handler {
		return newHandler(config, logger)
	}),
	fx.Provide(func(h *Handler) *library {
		return newLibrary(h)
	}),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

// FrontendModule provides workspace CHASM component for frontend services (component-only).
var FrontendModule = fx.Module(
	"workspace-frontend",
	fx.Provide(ConfigProvider),
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(newComponentOnlyLibrary())
	}),
)
