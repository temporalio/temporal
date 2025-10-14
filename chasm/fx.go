package chasm

import "go.uber.org/fx"

var Module = fx.Module(
	"chasm",
	fx.Provide(NewRegistry),
	fx.Invoke(func(registry *Registry) error {
		return registry.Register(&CoreLibrary{})
	}),
)
