package chasm

import (
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/fx"
)

type registryInitializerParams struct {
	fx.In

	Registry  *Registry
	TestHooks testhooks.TestHooks `optional:"true"`
}

var Module = fx.Module(
	"chasm",
	fx.Provide(NewRegistry),
	fx.Invoke(func(params registryInitializerParams) error {
		if err := params.Registry.Register(&CoreLibrary{}); err != nil {
			return err
		}
		if hook, ok := testhooks.Get(params.TestHooks, RegistryInitializer, testhooks.GlobalScope); ok {
			return hook(params.Registry)
		}
		return nil
	}),
)
