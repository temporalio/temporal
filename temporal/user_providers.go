// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package temporal

import (
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	tlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	esclient "go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/encryption"
)

type (
	ServiceNamesList []string
	NamespaceLogger  tlog.Logger
	ESHttpClient     *http.Client
	MetricsReporters struct {
		serverReporter metrics.Reporter
		sdkReporter    metrics.Reporter
	}
	ServerInterruptCh <-chan interface{}
)

func DefaultConfigProvider(c *cli.Context) (*config.Config, error) {
	env := c.String("env")
	zone := c.String("zone")
	configDir := path.Join(c.String("root"), c.String("config"))
	cfg, err := config.LoadConfig(env, configDir, zone)
	if err != nil {
		return nil, fmt.Errorf("Unable to load configuration: %v.", err)
	}

	return cfg, err
}

func DefaultLogger(cfg *config.Config) tlog.Logger {
	return tlog.NewZapLogger(tlog.BuildZapLogger(cfg.Log))
}

func DefaultServiceNameListProvider(log tlog.Logger, c *cli.Context) ServiceNamesList {
	services := c.StringSlice("service")
	if c.IsSet("Services") {
		log.Warn("WARNING: --Services flag is deprecated. Specify multiply --service flags instead.")
		services = strings.Split(c.String("Services"), ",")
	}
	return services
}

func DefaultDynamicConfigClientProvider(cfg *config.Config, logger tlog.Logger) dynamicconfig.Client {
	dynamicConfigClient, err := dynamicconfig.NewFileBasedClient(&cfg.DynamicConfigClient, logger, InterruptCh())
	if err != nil {
		logger.Info(
			"Unable to create file based dynamic config client, use no-op config client instead.",
			tag.Error(err),
		)
		dynamicConfigClient = dynamicconfig.NewNoopClient()
	}
	return dynamicConfigClient
}

func DefaultDynamicConfigCollectionProvider(client dynamicconfig.Client, logger tlog.Logger) *dynamicconfig.Collection {
	return dynamicconfig.NewCollection(client, logger)
}

func DefaultAuthorizerProvider(cfg *config.Config) (authorization.Authorizer, error) {
	authorizer, err := authorization.GetAuthorizerFromConfig(&cfg.Global.Authorization)
	return authorizer, err
}

func DefaultClaimMapper(cfg *config.Config, logger tlog.Logger) (authorization.ClaimMapper, error) {
	claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
	return claimMapper, err
}

func DefaultDatastoreFactory() persistenceClient.AbstractDataStoreFactory {
	return nil
}

func DefaultMetricsReportersProvider(cfg *config.Config, logger tlog.Logger) (*MetricsReporters, error) {
	result := &MetricsReporters{}
	if cfg.Global.Metrics == nil {
		return result, nil
	}

	var err error
	result.serverReporter, result.sdkReporter, err = cfg.Global.Metrics.InitMetricReporters(logger, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func DefaultTLSConfigProvider(
	cfg *config.Config,
	logger tlog.Logger,
	metricReporters *MetricsReporters,
) (encryption.TLSConfigProvider, error) {
	scope, err := extractTallyScopeForSDK(metricReporters.sdkReporter)
	if err != nil {
		return nil, err
	}
	tlsConfigProvider, err := encryption.NewTLSConfigProviderFromConfig(
		cfg.Global.TLS, scope, logger, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("TLS provider initialization error: %w", err)
	}
	return tlsConfigProvider, nil
}

func DefaultAudienceGetterProvider() authorization.JWTAudienceMapper {
	return nil
}

func DefaultPersistenceServiceResolverProvider() resolver.ServiceResolver {
	return resolver.NewNoopResolver()
}

func DefaultElasticSearchHttpClientProvider(esConfig *config.Elasticsearch) (ESHttpClient, error) {
	if esConfig == nil {
		return nil, nil
	}

	elasticseachHttpClient, err := esclient.NewAwsHttpClient(esConfig.AWSRequestSigning)
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
	}
	return elasticseachHttpClient, nil
}

func DefaultInterruptChProvider() ServerInterruptCh {
	return InterruptCh()
}
