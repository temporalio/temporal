package frontend

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustoken"
	"go.temporal.io/server/components/nexusoperations"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.nexusoperations.frontend",
	fx.Provide(ConfigProvider),
	fx.Provide(nexustoken.NewCallbackTokenGenerator),
	fx.Provide(newCompletionHandler),
)

func ConfigProvider(coll *dynamicconfig.Collection) *Config {
	return &Config{
		PayloadSizeLimit:              dynamicconfig.BlobSizeLimitError.Get(coll),
		ForwardingEnabledForNamespace: dynamicconfig.EnableNamespaceNotActiveAutoForwarding.Get(coll),
		MaxOperationTokenLength:       nexusoperations.MaxOperationTokenLength.Get(coll),
	}
}
