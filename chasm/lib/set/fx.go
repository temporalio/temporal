package set

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func register(registry *chasm.Registry) error {
	return registry.Register(NewNilLibrary())
}

var Module = fx.Module(
	"chasm.lib.set",
	fx.Invoke(register),
)
