package static

import (
	"fmt"

	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
)

func MembershipModule(
	hostsByService map[primitives.ServiceName]Hosts,
) fx.Option {
	return fx.Options(
		fx.Provide(func() membership.Monitor {
			return newStaticMonitor(hostsByService)
		}),
		fx.Provide(func(serviceName primitives.ServiceName) membership.HostInfoProvider {
			hosts := hostsByService[serviceName]
			if len(hosts.All) == 0 {
				panic(fmt.Sprintf("hosts for %v service are missing in static hosts", serviceName))
			}
			if len(hosts.Self) == 0 {
				panic(fmt.Sprintf("self host for %v service is missing in static hosts", serviceName))
			}

			for _, serviceHost := range hosts.All {
				if serviceHost == hosts.Self {
					hostInfo := membership.NewHostInfoFromAddress(serviceHost)
					return membership.NewHostInfoProvider(hostInfo)
				}
			}
			panic(fmt.Sprintf("self host %v for %v service is defined, but missing from the list of all static hosts", hosts.Self, serviceName))
		}),
	)
}
