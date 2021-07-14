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
	"errors"
	"fmt"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	esclient "go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
	"go.uber.org/dig"
)

type VisibilityWritingMode string

func AdvancedVisibilityWritingModeProvider(cfg *config.Config, dc *dynamicconfig.Collection) (
	VisibilityWritingMode,
	error,
) {
	advancedVisibilityWritingMode := dc.GetStringProperty(
		dynamicconfig.AdvancedVisibilityWritingMode,
		common.GetDefaultAdvancedVisibilityWritingMode(cfg.Persistence.IsAdvancedVisibilityConfigExist()),
	)()
	return VisibilityWritingMode(advancedVisibilityWritingMode), nil
}

// todomigryz: verify if this needs custom type
func AdvancedVisibilityStoreProvider(cfg *config.Config, visibilityWritingMode VisibilityWritingMode) (
	config.DataStore,
	error,
) {
	if visibilityWritingMode == common.AdvancedVisibilityWritingModeOff {
		return config.DataStore{}, nil
	}

	advancedVisibilityStore, ok := cfg.Persistence.DataStores[cfg.Persistence.AdvancedVisibilityStore]
	if !ok {
		return config.DataStore{}, fmt.Errorf(
			"unable to find advanced visibility store in config for %q key",
			cfg.Persistence.AdvancedVisibilityStore,
		)
	}

	return advancedVisibilityStore, nil
}

func ESConfigProvider(advancedVisibilityStoreConfig config.DataStore) *config.Elasticsearch {
	return advancedVisibilityStoreConfig.ElasticSearch
}

func ESClientProvider(
	cfg *config.Config,
	logger log.Logger,
	esHttpClient ESHttpClient,
	persistenceServiceResolver resolver.ServiceResolver,
	esConfig *config.Elasticsearch,
	advancedVisibilityWritingMode VisibilityWritingMode,
) (esclient.Client, error) {

	// todomigryz: this should be in start
	err := verifyPersistenceCompatibleVersion(
		cfg.Persistence,
		persistenceServiceResolver,
		advancedVisibilityWritingMode != common.AdvancedVisibilityWritingModeOn,
	)
	if err != nil {
		return nil, err
	}

	if advancedVisibilityWritingMode == common.AdvancedVisibilityWritingModeOff {
		return nil, nil
	}

	indexName := esConfig.GetVisibilityIndex()
	if len(indexName) == 0 {
		return nil, errors.New("visibility index in missing in Elasticsearch config")
	}

	esClient, err := esclient.NewClient(esConfig, esHttpClient, logger)
	if err != nil {
		return nil, fmt.Errorf("unable to create Elasticsearch client: %w", err)
	}

	return esClient, nil
}

// Populates parameters for a service
func makeBootstrapParams(
	cfg *config.Config,
	svcName string,
	logger log.Logger,
	nsLogger NamespaceLogger,
	authorizer authorization.Authorizer,
	claimMapper authorization.ClaimMapper,
	dynamicConfigClient dynamicconfig.Client,
	dynamicConfigCollection *dynamicconfig.Collection,
	customDatastoreFactory persistenceClient.AbstractDataStoreFactory,
	metricReporters *MetricsReporters,
	tlsConfigProvider encryption.TLSConfigProvider,
	audienceGetter authorization.JWTAudienceMapper,
	persistenceServiceResolver resolver.ServiceResolver,
	esConfig *config.Elasticsearch,
	esClient esclient.Client,
) (*resource.BootstrapParams, error) {
	params := &resource.BootstrapParams{
		Name:                     svcName,
		Logger:                   logger,
		NamespaceLogger:          nsLogger,
		PersistenceConfig:        cfg.Persistence,
		DynamicConfigClient:      dynamicConfigClient,
		ClusterMetadataConfig:    cfg.ClusterMetadata,
		DCRedirectionPolicy:      cfg.DCRedirectionPolicy,
		AbstractDatastoreFactory: customDatastoreFactory,
		ESConfig:                 esConfig,
		ESClient:                 esClient,
	}

	svcCfg := cfg.Services[svcName]
	rpcFactory := rpc.NewFactory(&svcCfg.RPC, svcName, logger, tlsConfigProvider)
	params.RPCFactory = rpcFactory

	// Ringpop uses a different port to register handlers, this map is needed to resolve
	// Services to correct addresses used by clients through ServiceResolver lookup API
	servicePortMap := make(map[string]int)
	for sn, sc := range cfg.Services {
		servicePortMap[sn] = sc.RPC.GRPCPort
	}

	params.MembershipFactoryInitializer =
		func(persistenceBean persistenceClient.Bean, logger log.Logger) (resource.MembershipMonitorFactory, error) {
			return ringpop.NewRingpopFactory(
				&cfg.Global.Membership,
				rpcFactory.GetRingpopChannel(),
				svcName,
				servicePortMap,
				logger,
				persistenceBean.GetClusterMetadataManager(),
			)
		}

	serverReporter := metricReporters.serverReporter
	sdkReporter := metricReporters.sdkReporter
	// todomigryz: remove support of configuring metrics reporter per-service. Sync with Samar.
	// todo: Replace this hack with actually using sdkReporter, Client or Scope.
	if serverReporter == nil {
		var err error
		serverReporter, sdkReporter, err = svcCfg.Metrics.InitMetricReporters(logger, nil)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to initialize per-service metric client. "+
					"This is deprecated behavior used as fallback, please use global metric config. Error: %w", err,
			)
		}
		params.ServerMetricsReporter = serverReporter
		params.SDKMetricsReporter = sdkReporter
	}

	globalTallyScope, err := extractTallyScopeForSDK(sdkReporter)
	if err != nil {
		return nil, err
	}
	params.MetricsScope = globalTallyScope

	serviceIdx := metrics.GetMetricsServiceIdx(svcName, logger)

	metricsClient, err := serverReporter.NewClient(logger, serviceIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize metrics client: %w", err)
	}

	params.MetricsClient = metricsClient

	options, err := tlsConfigProvider.GetFrontendClientConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load frontend TLS configuration: %w", err)
	}

	params.SdkClient, err = sdkclient.NewClient(
		sdkclient.Options{
			HostPort:     cfg.PublicClient.HostPort,
			Namespace:    common.SystemLocalNamespace,
			MetricsScope: globalTallyScope,
			Logger:       log.NewSdkLogger(logger),
			ConnectionOptions: sdkclient.ConnectionOptions{
				TLS:                options,
				DisableHealthCheck: true,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create public client: %w", err)
	}

	params.ArchivalMetadata = archiver.NewArchivalMetadata(
		dynamicConfigCollection,
		cfg.Archival.History.State,
		cfg.Archival.History.EnableRead,
		cfg.Archival.Visibility.State,
		cfg.Archival.Visibility.EnableRead,
		&cfg.NamespaceDefaults.Archival,
	)

	params.ArchiverProvider = provider.NewArchiverProvider(
		cfg.Archival.History.Provider,
		cfg.Archival.Visibility.Provider,
	)
	params.PersistenceConfig.TransactionSizeLimit = dynamicConfigCollection.GetIntProperty(
		dynamicconfig.TransactionSizeLimit,
		common.DefaultTransactionSizeLimit,
	)

	params.Authorizer = authorizer
	params.ClaimMapper = claimMapper
	params.AudienceGetter = audienceGetter
	params.PersistenceServiceResolver = persistenceServiceResolver

	return params, nil
}

type ServicesProviderDeps struct {
	dig.In

	Cfg                        *config.Config
	Services                   ServiceNamesList
	Logger                     log.Logger
	NamespaceLogger            NamespaceLogger
	Authorizer                 authorization.Authorizer
	ClaimMapper                authorization.ClaimMapper
	DynamicConfigClient        dynamicconfig.Client
	DynamicConfigCollection    *dynamicconfig.Collection
	CustomDatastoreFactory     persistenceClient.AbstractDataStoreFactory `optional:"true"`
	MetricReporters            *MetricsReporters
	TlsConfigProvider          encryption.TLSConfigProvider
	AudienceGetter             authorization.JWTAudienceMapper
	PersistenceServiceResolver resolver.ServiceResolver
	EsConfig                   *config.Elasticsearch
	EsClient                   esclient.Client
}

type ServicesMap map[string]common.Daemon
func ServicesProvider(
	deps ServicesProviderDeps,
) (ServicesMap, error) {
	result := make(map[string]common.Daemon)
	for _, svcName := range deps.Services {
		var err error
		var svc common.Daemon
		switch svcName {
		// todo: All Services should follow this path or better be split into separate providers
		case primitives.MatchingService:
			svc, err = matching.InitializeMatchingService(
				matching.ServiceName(svcName),
				deps.Logger,
				deps.DynamicConfigClient,
				deps.MetricReporters.serverReporter,
				deps.MetricReporters.sdkReporter,
				deps.Cfg.Services[svcName],
				deps.Cfg.ClusterMetadata,
				deps.TlsConfigProvider,
				deps.Cfg.Services,
				&deps.Cfg.Global.Membership,
				&deps.Cfg.Persistence,
				deps.PersistenceServiceResolver,
				deps.CustomDatastoreFactory,
			)
			result[svcName] = svc
			continue
		default:
		}

		params, err := makeBootstrapParams(
			deps.Cfg,
			svcName,
			deps.Logger,
			deps.NamespaceLogger,
			deps.Authorizer,
			deps.ClaimMapper,
			deps.DynamicConfigClient,
			deps.DynamicConfigCollection,
			deps.CustomDatastoreFactory,
			deps.MetricReporters,
			deps.TlsConfigProvider,
			deps.AudienceGetter,
			deps.PersistenceServiceResolver,
			deps.EsConfig,
			deps.EsClient,
		)

		if err != nil {
			return nil, err
		}

		switch svcName {
		case primitives.FrontendService:
			svc, err = frontend.NewService(params)
		case primitives.HistoryService:
			svc, err = history.NewService(params)
		case primitives.WorkerService:
			svc, err = worker.NewService(params)
		default:
			return nil, fmt.Errorf("uknown service %q", svcName)
		}

		result[svcName] = svc
	}
	return result, nil
}

func ServerProvider(
	logger log.Logger,
	cfg *config.Config,
	requiredServices ServiceNamesList, // todomigryz: might use specific type name here, or wrap in provider
	services ServicesMap,
	persistenceServiceResolver resolver.ServiceResolver,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
	dynamicConfigClient dynamicconfig.Client,
	dc *dynamicconfig.Collection,
	interruptCh ServerInterruptCh,
) (*Server, error) {
	// todomigryz: ultimately, we don't need serveropts here.
	s, err := NewServer(
		logger,
		cfg,
		requiredServices,
		services,
		persistenceServiceResolver,
		customDataStoreFactory,
		dynamicConfigClient,
		dc,
		interruptCh,
	)
	return s, err
}
