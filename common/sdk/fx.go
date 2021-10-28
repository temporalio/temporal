package sdk

import (
	"go.temporal.io/server/common/rpc/encryption"
)

func ClientFactoryProvider(
	so *
	tlsConfigProvider encryption.TLSConfigProvider,
) ClientFactory {
	return newClientFactory(
		"",
		tlsConfigProvider.GetFrontendClientConfig(),

		)
}
