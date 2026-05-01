package nexusoperations

import (
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.nexusoperations",
	fx.Provide(ConfigProvider),
	fx.Invoke(RegisterStateMachines),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterEventDefinitions),
	fx.Invoke(RegisterExecutor),
	// Bridge CHASM ClientProvider to HSM ClientProvider type.
	fx.Provide(func(cp chasmnexus.ClientProvider) ClientProvider {
		return ClientProvider(cp)
	}),
)

const NexusCallbackSourceHeader = "Nexus-Callback-Source"
