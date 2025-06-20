package ringpop

import (
	"go.temporal.io/server/common/membership"
	"go.uber.org/fx"
)

// MembershipModule provides membership objects given the types in factoryParams.
var MembershipModule = fx.Provide(
	provideFactory,
	provideMembership,
	provideHostInfoProvider,
)

func provideFactory(lc fx.Lifecycle, params factoryParams) (*factory, error) {
	f, err := newFactory(params)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.StopHook(f.closeTChannel))
	return f, nil
}

func provideMembership(lc fx.Lifecycle, f *factory) membership.Monitor {
	m := f.getMonitor()
	lc.Append(fx.StopHook(m.Stop))
	return m
}

func provideHostInfoProvider(lc fx.Lifecycle, f *factory) (membership.HostInfoProvider, error) {
	return f.getHostInfoProvider()
}
