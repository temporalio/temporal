// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"go.uber.org/fx"

	"github.com/pborman/uuid"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/resource"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/pprof"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
)

type (
	ServiceStopFn func()

	ServicesGroupOut struct {
		fx.Out

		Services *ServicesMetadata `group:"services"`
	}

	ServicesGroupIn struct {
		fx.In
		Services []*ServicesMetadata `group:"services"`
	}

	ServicesMetadata struct {
		App           *fx.App // Added for info. ServiceStopFn is enough.
		ServiceName   string
		ServiceStopFn ServiceStopFn
	}

	ServerFx struct {
		app *fx.App
	}

	serverOptionsProvider struct {
		fx.Out
		ServerOptions *serverOptions
		StopChan      chan interface{}

		Config      *config.Config
		PProfConfig *config.PProf
		LogConfig   log.Config

		ServiceNames    resource.ServiceNames
		NamespaceLogger resource.NamespaceLogger

		ServiceResolver        resolver.ServiceResolver
		CustomDataStoreFactory persistenceClient.AbstractDataStoreFactory

		SearchAttributesMapper searchattribute.Mapper
		CustomInterceptors     []grpc.UnaryServerInterceptor
		Authorizer             authorization.Authorizer
		ClaimMapper            authorization.ClaimMapper
		AudienceGetter         authorization.JWTAudienceMapper

		// below are things that could be over write by server options or may have default if not supplied by serverOptions.
		Logger                  log.Logger
		ClientFactoryProvider   client.FactoryProvider
		ServerReporter          resource.ServerReporter
		MetricsClient           metrics.Client
		DynamicConfigClient     dynamicconfig.Client
		DynamicConfigCollection *dynamicconfig.Collection
		TLSConfigProvider       encryption.TLSConfigProvider
		EsConfig                *esclient.Config
		EsClient                esclient.Client
	}
)

func NewServerFx(opts ...ServerOption) *ServerFx {
	app := fx.New(
		pprof.Module,
		ServerFxImplModule,
		fx.Supply(opts),
		fx.Provide(ServerOptionsProvider),

		fx.Provide(PersistenceFactoryProvider),
		fx.Provide(HistoryServiceProvider),
		fx.Provide(MatchingServiceProvider),
		fx.Provide(FrontendServiceProvider),
		fx.Provide(WorkerServiceProvider),

		fx.Provide(ApplyClusterMetadataConfigProvider),
		fx.Invoke(ServerLifetimeHooks),
		fx.NopLogger,
	)
	s := &ServerFx{
		app,
	}
	return s
}

func ServerOptionsProvider(opts []ServerOption) (serverOptionsProvider, error) {
	so := newServerOptions(opts)

	err := so.loadAndValidate()
	if err != nil {
		return serverOptionsProvider{}, err
	}

	err = verifyPersistenceCompatibleVersion(so.config.Persistence, so.persistenceServiceResolver)
	if err != nil {
		return serverOptionsProvider{}, err
	}

	err = ringpop.ValidateRingpopConfig(&so.config.Global.Membership)
	if err != nil {
		return serverOptionsProvider{}, fmt.Errorf("ringpop config validation error: %w", err)
	}

	stopChan := make(chan interface{})

	// Logger
	logger := so.logger
	if logger == nil {
		logger = log.NewZapLogger(log.BuildZapLogger(so.config.Log))
	}

	// ClientFactoryProvider
	clientFactoryProvider := so.clientFactoryProvider
	if clientFactoryProvider == nil {
		clientFactoryProvider = client.NewFactoryProvider()
	}

	// ServerReporter
	serverReporter := so.metricsReporter
	if serverReporter == nil {
		if so.config.Global.Metrics == nil {
			logger.Warn("no metrics config provided, using noop reporter")
			serverReporter = metrics.NoopReporter
		} else {
			serverReporter, err = metrics.InitMetricsReporter(logger, so.config.Global.Metrics)
			if err != nil {
				return serverOptionsProvider{}, err
			}
		}
	}

	// MetricsClient
	var metricsClient metrics.Client
	metricsClient, err = serverReporter.NewClient(logger, metrics.Server)
	if err != nil {
		return serverOptionsProvider{}, err
	}

	// DynamicConfigClient
	dcClient := so.dynamicConfigClient
	if dcClient == nil {
		dcConfig := so.config.DynamicConfigClient
		if dcConfig != nil {
			dcClient, err = dynamicconfig.NewFileBasedClient(dcConfig, logger, stopChan)
			if err != nil {
				return serverOptionsProvider{}, fmt.Errorf("unable to create dynamic config client: %w", err)
			}
		} else {
			// noop client
			logger.Info("Dynamic config client is not configured. Using default values.")
			dcClient = dynamicconfig.NewNoopClient()
		}
	}

	// TLSConfigProvider
	tlsConfigProvider := so.tlsConfigProvider
	if tlsConfigProvider == nil {
		tlsConfigProvider, err = encryption.NewTLSConfigProviderFromConfig(so.config.Global.TLS, metricsClient.Scope(metrics.ServerTlsScope), logger, nil)
		if err != nil {
			return serverOptionsProvider{}, err
		}
	}

	// EsConfig / EsClient
	var esConfig *esclient.Config
	var esClient esclient.Client
	if so.config.Persistence.AdvancedVisibilityConfigExist() {
		advancedVisibilityStore, ok := so.config.Persistence.DataStores[so.config.Persistence.AdvancedVisibilityStore]
		if !ok {
			return serverOptionsProvider{}, fmt.Errorf("persistence config: advanced visibility datastore %q: missing config", so.config.Persistence.AdvancedVisibilityStore)
		}

		esHttpClient := so.elasticsearchHttpClient
		if esHttpClient == nil {
			var err error
			esHttpClient, err = esclient.NewAwsHttpClient(advancedVisibilityStore.Elasticsearch.AWSRequestSigning)
			if err != nil {
				return serverOptionsProvider{}, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
			}
		}

		esConfig = advancedVisibilityStore.Elasticsearch

		esClient, err = esclient.NewClient(esConfig, esHttpClient, logger)
		if err != nil {
			return serverOptionsProvider{}, fmt.Errorf("unable to create Elasticsearch client: %w", err)
		}
	}

	return serverOptionsProvider{
		ServerOptions: so,
		StopChan:      stopChan,

		Config:      so.config,
		PProfConfig: &so.config.Global.PProf,
		LogConfig:   so.config.Log,

		ServiceNames:    so.serviceNames,
		NamespaceLogger: so.namespaceLogger,

		ServiceResolver:        so.persistenceServiceResolver,
		CustomDataStoreFactory: so.customDataStoreFactory,

		SearchAttributesMapper: so.searchAttributesMapper,
		CustomInterceptors:     so.customInterceptors,
		Authorizer:             so.authorizer,
		ClaimMapper:            so.claimMapper,
		AudienceGetter:         so.audienceGetter,

		Logger:                  logger,
		ClientFactoryProvider:   clientFactoryProvider,
		ServerReporter:          serverReporter,
		MetricsClient:           metricsClient,
		DynamicConfigClient:     dcClient,
		DynamicConfigCollection: dynamicconfig.NewCollection(dcClient, logger),
		TLSConfigProvider:       tlsConfigProvider,
		EsConfig:                esConfig,
		EsClient:                esClient,
	}, nil
}

func (s ServerFx) Start() error {
	return s.app.Start(context.Background())
}

func (s ServerFx) Stop() {
	s.app.Stop(context.Background())
}

func StopService(logger log.Logger, app *fx.App, svcName string, stopChan chan struct{}) {
	stopCtx, cancelFunc := context.WithTimeout(context.Background(), serviceStopTimeout)
	err := app.Stop(stopCtx)
	cancelFunc()
	if err != nil {
		logger.Error("Failed to stop service", tag.Service(svcName), tag.Error(err))
	}

	// verify "Start" goroutine returned
	select {
	case <-stopChan:
	case <-time.After(time.Minute):
		logger.Error("Timed out (1 minute) waiting for service to stop.", tag.Service(svcName))
	}
}

type (
	ServiceProviderParamsCommon struct {
		fx.In

		Cfg                        *config.Config
		ServiceNames               resource.ServiceNames
		Logger                     log.Logger
		NamespaceLogger            resource.NamespaceLogger
		DynamicConfigClient        dynamicconfig.Client
		ServerReporter             resource.ServerReporter
		EsConfig                   *esclient.Config
		EsClient                   esclient.Client
		TlsConfigProvider          encryption.TLSConfigProvider
		PersistenceConfig          config.Persistence
		ClusterMetadata            *cluster.Config
		ClientFactoryProvider      client.FactoryProvider
		AudienceGetter             authorization.JWTAudienceMapper
		PersistenceServiceResolver resolver.ServiceResolver
		PersistenceFactoryProvider persistenceClient.FactoryProviderFn
		SearchAttributesMapper     searchattribute.Mapper
		CustomInterceptors         []grpc.UnaryServerInterceptor
		Authorizer                 authorization.Authorizer
		ClaimMapper                authorization.ClaimMapper
		DataStoreFactory           persistenceClient.AbstractDataStoreFactory
	}
)

func HistoryServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.HistoryService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(fx.NopLogger),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})

	app := fx.New(
		fx.Supply(
			stopChan,
			params.EsConfig,
			params.PersistenceConfig,
			params.ClusterMetadata,
			params.Cfg,
		),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return params.DataStoreFactory }),
		fx.Provide(func() client.FactoryProvider { return params.ClientFactoryProvider }),
		fx.Provide(func() authorization.JWTAudienceMapper { return params.AudienceGetter }),
		fx.Provide(func() resolver.ServiceResolver { return params.PersistenceServiceResolver }),
		fx.Provide(func() searchattribute.Mapper { return params.SearchAttributesMapper }),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return params.CustomInterceptors }),
		fx.Provide(func() authorization.Authorizer { return params.Authorizer }),
		fx.Provide(func() authorization.ClaimMapper { return params.ClaimMapper }),
		fx.Provide(func() encryption.TLSConfigProvider { return params.TlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return params.DynamicConfigClient }),
		fx.Provide(func() resource.ServiceName { return resource.ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() resource.ServerReporter { return params.ServerReporter }),
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(params.PersistenceFactoryProvider),
		fx.Provide(workflow.NewTaskGeneratorProvider),
		resource.DefaultOptions,
		history.QueueProcessorModule,
		history.Module,
		fx.NopLogger,
	)

	stopFn := func() { StopService(params.Logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

func MatchingServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.MatchingService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(fx.NopLogger),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			params.EsConfig,
			params.PersistenceConfig,
			params.ClusterMetadata,
			params.Cfg,
		),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return params.DataStoreFactory }),
		fx.Provide(func() client.FactoryProvider { return params.ClientFactoryProvider }),
		fx.Provide(func() authorization.JWTAudienceMapper { return params.AudienceGetter }),
		fx.Provide(func() resolver.ServiceResolver { return params.PersistenceServiceResolver }),
		fx.Provide(func() searchattribute.Mapper { return params.SearchAttributesMapper }),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return params.CustomInterceptors }),
		fx.Provide(func() authorization.Authorizer { return params.Authorizer }),
		fx.Provide(func() authorization.ClaimMapper { return params.ClaimMapper }),
		fx.Provide(func() encryption.TLSConfigProvider { return params.TlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return params.DynamicConfigClient }),
		fx.Provide(func() resource.ServiceName { return resource.ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() resource.ServerReporter { return params.ServerReporter }),
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(params.PersistenceFactoryProvider),
		resource.DefaultOptions,
		matching.Module,
		fx.NopLogger,
	)

	stopFn := func() { StopService(params.Logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

func FrontendServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.FrontendService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(fx.NopLogger),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			params.EsConfig,
			params.PersistenceConfig,
			params.ClusterMetadata,
			params.Cfg,
		),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return params.DataStoreFactory }),
		fx.Provide(func() client.FactoryProvider { return params.ClientFactoryProvider }),
		fx.Provide(func() authorization.JWTAudienceMapper { return params.AudienceGetter }),
		fx.Provide(func() resolver.ServiceResolver { return params.PersistenceServiceResolver }),
		fx.Provide(func() searchattribute.Mapper { return params.SearchAttributesMapper }),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return params.CustomInterceptors }),
		fx.Provide(func() authorization.Authorizer { return params.Authorizer }),
		fx.Provide(func() authorization.ClaimMapper { return params.ClaimMapper }),
		fx.Provide(func() encryption.TLSConfigProvider { return params.TlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return params.DynamicConfigClient }),
		fx.Provide(func() resource.ServiceName { return resource.ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() resource.ServerReporter { return params.ServerReporter }),
		fx.Provide(func() resource.NamespaceLogger { return params.NamespaceLogger }),
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(params.PersistenceFactoryProvider),
		resource.DefaultOptions,
		frontend.Module,
		fx.NopLogger,
	)

	stopFn := func() { StopService(params.Logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

func WorkerServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.WorkerService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(fx.NopLogger),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			params.EsConfig,
			params.PersistenceConfig,
			params.ClusterMetadata,
			params.Cfg,
		),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return params.DataStoreFactory }),
		fx.Provide(func() client.FactoryProvider { return params.ClientFactoryProvider }),
		fx.Provide(func() authorization.JWTAudienceMapper { return params.AudienceGetter }),
		fx.Provide(func() resolver.ServiceResolver { return params.PersistenceServiceResolver }),
		fx.Provide(func() searchattribute.Mapper { return params.SearchAttributesMapper }),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return params.CustomInterceptors }),
		fx.Provide(func() authorization.Authorizer { return params.Authorizer }),
		fx.Provide(func() authorization.ClaimMapper { return params.ClaimMapper }),
		fx.Provide(func() encryption.TLSConfigProvider { return params.TlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return params.DynamicConfigClient }),
		fx.Provide(func() resource.ServiceName { return resource.ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() resource.ServerReporter { return params.ServerReporter }),
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(params.PersistenceFactoryProvider),
		resource.DefaultOptions,
		worker.Module,
		fx.NopLogger,
	)

	stopFn := func() { StopService(params.Logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

// ApplyClusterMetadataConfigProvider performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from downstream calls.
// TODO: move this to cluster.fx
func ApplyClusterMetadataConfigProvider(
	logger log.Logger,
	config *config.Config,
	persistenceServiceResolver resolver.ServiceResolver,
	persistenceFactoryProvider persistenceClient.FactoryProviderFn,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
) (*cluster.Config, config.Persistence, error) {
	ctx := context.TODO()
	logger = log.With(logger, tag.ComponentMetadataInitializer)

	clusterName := persistenceClient.ClusterName(config.ClusterMetadata.CurrentClusterName)
	dataStoreFactory, _ := persistenceClient.DataStoreFactoryProvider(
		clusterName,
		persistenceServiceResolver,
		&config.Persistence,
		customDataStoreFactory,
		logger,
		nil,
	)
	factory := persistenceFactoryProvider(persistenceClient.NewFactoryParams{
		DataStoreFactory:  dataStoreFactory,
		Cfg:               &config.Persistence,
		PersistenceMaxQPS: nil,
		ClusterName:       persistenceClient.ClusterName(config.ClusterMetadata.CurrentClusterName),
		MetricsClient:     nil,
		Logger:            logger,
	})
	defer factory.Close()

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		return config.ClusterMetadata, config.Persistence, fmt.Errorf("error initializing cluster metadata manager: %w", err)
	}
	defer clusterMetadataManager.Close()

	clusterData := config.ClusterMetadata
	for clusterName, clusterInfo := range clusterData.ClusterInformation {
		if clusterName != clusterData.CurrentClusterName {
			logger.Warn(
				"ClusterInformation in ClusterMetadata config is deprecated. "+
					"Please use TCTL admin tool to configure remote cluster connections",
				tag.Key("clusterInformation"),
				tag.ClusterName(clusterName),
				tag.IgnoredValue(clusterInfo))
			continue
		}

		// Only configure current cluster metadata from static config file
		clusterId := uuid.New()
		applied, err := clusterMetadataManager.SaveClusterMetadata(
			ctx,
			&persistence.SaveClusterMetadataRequest{
				ClusterMetadata: persistencespb.ClusterMetadata{
					HistoryShardCount:        config.Persistence.NumHistoryShards,
					ClusterName:              clusterName,
					ClusterId:                clusterId,
					ClusterAddress:           clusterInfo.RPCAddress,
					FailoverVersionIncrement: clusterData.FailoverVersionIncrement,
					InitialFailoverVersion:   clusterInfo.InitialFailoverVersion,
					IsGlobalNamespaceEnabled: clusterData.EnableGlobalNamespace,
					IsConnectionEnabled:      clusterInfo.Enabled,
				}})
		if err != nil {
			logger.Warn("Failed to save cluster metadata.", tag.Error(err), tag.ClusterName(clusterName))
		}
		if applied {
			logger.Info("Successfully saved cluster metadata.", tag.ClusterName(clusterName))
			continue
		}

		resp, err := clusterMetadataManager.GetClusterMetadata(ctx, &persistence.GetClusterMetadataRequest{
			ClusterName: clusterName,
		})
		if err != nil {
			return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while fetching cluster metadata: %w", err)
		}

		// Allow updating cluster metadata if global namespace is disabled
		if !resp.IsGlobalNamespaceEnabled && clusterData.EnableGlobalNamespace {
			currentMetadata := resp.ClusterMetadata
			currentMetadata.IsGlobalNamespaceEnabled = clusterData.EnableGlobalNamespace
			currentMetadata.InitialFailoverVersion = clusterInfo.InitialFailoverVersion
			currentMetadata.FailoverVersionIncrement = clusterData.FailoverVersionIncrement

			applied, err = clusterMetadataManager.SaveClusterMetadata(
				ctx,
				&persistence.SaveClusterMetadataRequest{
					ClusterMetadata: currentMetadata,
					Version:         resp.Version,
				})
			if !applied || err != nil {
				return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while updating cluster metadata: %w", err)
			}
		} else if resp.IsGlobalNamespaceEnabled != clusterData.EnableGlobalNamespace {
			logger.Warn(
				mismatchLogMessage,
				tag.Key("clusterMetadata.EnableGlobalNamespace"),
				tag.IgnoredValue(clusterData.EnableGlobalNamespace),
				tag.Value(resp.IsGlobalNamespaceEnabled))
			config.ClusterMetadata.EnableGlobalNamespace = resp.IsGlobalNamespaceEnabled
		}

		// Verify current cluster metadata
		var persistedShardCount = resp.HistoryShardCount
		if config.Persistence.NumHistoryShards != persistedShardCount {
			logger.Warn(
				mismatchLogMessage,
				tag.Key("persistence.numHistoryShards"),
				tag.IgnoredValue(config.Persistence.NumHistoryShards),
				tag.Value(persistedShardCount))
			config.Persistence.NumHistoryShards = persistedShardCount
		}
		if resp.FailoverVersionIncrement != clusterData.FailoverVersionIncrement {
			logger.Warn(
				mismatchLogMessage,
				tag.Key("clusterMetadata.FailoverVersionIncrement"),
				tag.IgnoredValue(clusterData.FailoverVersionIncrement),
				tag.Value(resp.FailoverVersionIncrement))
			config.ClusterMetadata.FailoverVersionIncrement = resp.FailoverVersionIncrement
		}
	}
	err = loadClusterInformationFromStore(ctx, config, clusterMetadataManager, logger)
	if err != nil {
		return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while loading metadata from cluster: %w", err)
	}
	return config.ClusterMetadata, config.Persistence, nil
}

func PersistenceFactoryProvider() persistenceClient.FactoryProviderFn {
	return persistenceClient.FactoryProvider
}

// TODO: move this to cluster.fx
func loadClusterInformationFromStore(ctx context.Context, config *config.Config, clusterMsg persistence.ClusterMetadataManager, logger log.Logger) error {
	iter := collection.NewPagingIterator(func(paginationToken []byte) ([]interface{}, []byte, error) {
		request := &persistence.ListClusterMetadataRequest{
			PageSize:      100,
			NextPageToken: nil,
		}
		resp, err := clusterMsg.ListClusterMetadata(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		var pageItem []interface{}
		for _, metadata := range resp.ClusterMetadata {
			pageItem = append(pageItem, metadata)
		}
		return pageItem, resp.NextPageToken, nil
	})

	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			return err
		}
		metadata := item.(*persistence.GetClusterMetadataResponse)
		newMetadata := cluster.ClusterInformation{
			Enabled:                metadata.IsConnectionEnabled,
			InitialFailoverVersion: metadata.InitialFailoverVersion,
			RPCAddress:             metadata.ClusterAddress,
		}
		if staticClusterMetadata, ok := config.ClusterMetadata.ClusterInformation[metadata.ClusterName]; ok && metadata.ClusterName != config.ClusterMetadata.CurrentClusterName {
			logger.Warn(
				"ClusterInformation in ClusterMetadata config is deprecated. Please use TCTL tool to configure remote cluster connections",
				tag.Key("clusterInformation"),
				tag.IgnoredValue(staticClusterMetadata),
				tag.Value(newMetadata))
		}
		config.ClusterMetadata.ClusterInformation[metadata.ClusterName] = newMetadata
	}
	return nil
}

func ServerLifetimeHooks(
	lc fx.Lifecycle,
	svr Server,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return svr.Start()
			},
			OnStop: func(ctx context.Context) error {
				svr.Stop()
				return nil
			},
		},
	)
}

func verifyPersistenceCompatibleVersion(config config.Persistence, persistenceServiceResolver resolver.ServiceResolver) error {
	// cassandra schema version validation
	if err := cassandra.VerifyCompatibleVersion(config, persistenceServiceResolver); err != nil {
		return fmt.Errorf("cassandra schema version compatibility check failed: %w", err)
	}
	// sql schema version validation
	if err := sql.VerifyCompatibleVersion(config, persistenceServiceResolver); err != nil {
		return fmt.Errorf("sql schema version compatibility check failed: %w", err)
	}
	return nil
}
