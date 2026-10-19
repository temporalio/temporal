package faultinjection

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/faultinjection"
)

// newStoreFaultInjector supplies store/method targeting to the shared engine.
func newStoreFaultInjector(store config.DataStoreName, cfg *config.FaultInjectionDataStoreConfig, injector config.FaultInjector) (faultinjection.Generator[any, struct{}], error) {
	var testHookGenerator faultinjection.Generator[any, struct{}]
	if injector != nil {
		testHookGenerator = faultinjection.Callbacks[any, struct{}]{
			Request: func(_ context.Context, method string, request any) *faultinjection.Outcome[struct{}] {
				if err := injector(config.FaultInjectionTarget{Store: store, Method: method, Request: request}); err != nil {
					return &faultinjection.Outcome[struct{}]{Error: err}
				}
				return nil
			},
		}
	}
	methods := make(map[string]faultinjection.Generator[any, struct{}], len(cfg.Methods))
	for method, settings := range cfg.Methods {
		generator, err := faultinjection.NewConfiguredGenerator[any, struct{}](settings, faultinjection.Config{}, func(name string, rate float64) *faultinjection.Outcome[struct{}] {
			return newFault(name, rate, method)
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", store, method, err)
		}
		if generator != nil {
			methods[method] = generator
		}
	}
	var configuredGenerator faultinjection.Generator[any, struct{}]
	if len(methods) > 0 {
		configuredGenerator = faultinjection.Callbacks[any, struct{}]{
			Request: func(ctx context.Context, method string, request any) *faultinjection.Outcome[struct{}] {
				if generator := methods[method]; generator != nil {
					return generator.GenerateRequest(ctx, method, request)
				}
				return nil
			},
		}
	}
	return faultinjection.WithTestHooks(configuredGenerator, testHookGenerator), nil
}
