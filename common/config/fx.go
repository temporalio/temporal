package config

import (
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
)

// ServicePortMap contains the gRPC ports for our services.
type ServicePortMap map[primitives.ServiceName]int

var Module = fx.Provide(
	provideRPCConfig,
	provideMembershipConfig,
	provideServicePortMap,
)

func provideRPCConfig(cfg *Config, svcName primitives.ServiceName) *RPC {
	c := cfg.Services[string(svcName)].RPC

	return &c
}

func provideMembershipConfig(cfg *Config) *Membership {
	return &cfg.Global.Membership
}

func provideServicePortMap(cfg *Config) ServicePortMap {
	servicePortMap := make(ServicePortMap)
	for sn, sc := range cfg.Services {
		servicePortMap[primitives.ServiceName(sn)] = sc.RPC.GRPCPort
	}

	return servicePortMap
}
