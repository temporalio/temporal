package sdk

import (
	"crypto/tls"
	"fmt"

	"github.com/uber-go/tally/v4"
	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
)

type (
	ClientFactory interface {
		NewClient(namespaceName string, logger log.Logger) (sdkclient.Client, error)
		NewSystemClient(logger log.Logger) (sdkclient.Client, error)
	}

	clientFactory struct {
		hostPort  string
		tlsConfig *tls.Config
		scope     tally.Scope
	}
)

func newClientFactory(hostPort string, tlsConfig *tls.Config, scope tally.Scope) *clientFactory {
	return &clientFactory{
		hostPort:  hostPort,
		tlsConfig: tlsConfig,
		scope:     scope,
	}
}

func (f *clientFactory) NewClient(namespaceName string, logger log.Logger) (sdkclient.Client, error) {
	sdkClient, err := sdkclient.NewClient(sdkclient.Options{
		HostPort:     f.hostPort,
		Namespace:    namespaceName,
		MetricsScope: f.scope,
		Logger:       log.NewSdkLogger(logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS:                f.tlsConfig,
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create SDK client: %w", err)
	}

	return sdkClient, nil
}

func (f *clientFactory) NewSystemClient(logger log.Logger) (sdkclient.Client, error) {
	return f.NewClient(common.SystemLocalNamespace, logger)
}
