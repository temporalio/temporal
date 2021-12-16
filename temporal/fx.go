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

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.uber.org/fx"

	"github.com/pborman/uuid"

	"go.temporal.io/server/common/collection"

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
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
)

type (
	ServerReporter metrics.Reporter
	SdkReporter    metrics.Reporter

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
)

func NewServerFx(opts ...ServerOption) *ServerFx {
	app := fx.New(
		pprof.Module,
		ServerFxImplModule,
		fx.Supply(opts),
		fx.Provide(LoggerProvider),
		fx.Provide(StopChanProvider),
		fx.Provide(NamespaceLoggerProvider),
		fx.Provide(ServerOptionsProvider),
		fx.Provide(DcCollectionProvider),
		fx.Provide(DynamicConfigClientProvider),
		fx.Provide(MetricReportersProvider),
		fx.Provide(TlsConfigProviderProvider),
		fx.Provide(SoExpander),
		fx.Provide(ESConfigAndClientProvider),
		fx.Provide(HistoryServiceProvider),
		fx.Provide(MatchingServiceProvider),
		fx.Provide(FrontendServiceProvider),
		fx.Provide(WorkerServiceProvider),
		fx.Provide(ApplyClusterMetadataConfigProvider),
		fx.Provide(MetricsClientProvider),
		fx.Provide(ServiceNamesProvider),
		fx.Provide(AbstractDatastoreFactoryProvider),
		fx.Provide(ClientFactoryProvider),
		fx.Provide(SearchAttributeMapperProvider),
		fx.Provide(UnaryInterceptorsProvider),
		fx.Provide(AuthorizerProvider),
		fx.Provide(ClaimMapperProvider),
		fx.Provide(JWTAudienceMapperProvider),
		fx.Invoke(ServerLifetimeHooks),
		fx.NopLogger,
	)
	s := &ServerFx{
		app,
	}
	return s
}

func (s ServerFx) Start() error {
	return s.app.Start(context.Background())
}

func (s ServerFx) Stop() {
	s.app.Stop(context.Background())
}

func StopChanProvider() chan interface{} {
	return make(chan interface{})
}

func stopService(logger log.Logger, app *fx.App, svcName string, stopChan chan struct{}) {
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

func ESConfigAndClientProvider(so *serverOptions, logger log.Logger) (*esclient.Config, esclient.Client, error) {
	if !so.config.Persistence.AdvancedVisibilityConfigExist() {
		return nil, nil, nil
	}

	advancedVisibilityStore, ok := so.config.Persistence.DataStores[so.config.Persistence.AdvancedVisibilityStore]
	if !ok {
		return nil, nil, fmt.Errorf("persistence config: advanced visibility datastore %q: missing config", so.config.Persistence.AdvancedVisibilityStore)
	}

	if so.elasticsearchHttpClient == nil {
		var err error
		so.elasticsearchHttpClient, err = esclient.NewAwsHttpClient(advancedVisibilityStore.Elasticsearch.AWSRequestSigning)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
		}
	}

	esClient, err := esclient.NewClient(advancedVisibilityStore.Elasticsearch, so.elasticsearchHttpClient, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create Elasticsearch client: %w", err)
	}

	return advancedVisibilityStore.Elasticsearch, esClient, nil
}

func ServiceNamesProvider(so *serverOptions) ServiceNames {
	return so.serviceNames
}

func AbstractDatastoreFactoryProvider(so *serverOptions) persistenceClient.AbstractDataStoreFactory {
	return so.customDataStoreFactory
}

func ClientFactoryProvider(so *serverOptions) client.FactoryProvider {
	factoryProvider := so.clientFactoryProvider
	if factoryProvider == nil {
		factoryProvider = client.NewFactoryProvider()
	}
	return factoryProvider
}

func SearchAttributeMapperProvider(so *serverOptions) searchattribute.Mapper {
	return so.searchAttributesMapper
}

func UnaryInterceptorsProvider(so *serverOptions) []grpc.UnaryServerInterceptor {
	return so.customInterceptors
}

func AuthorizerProvider(so *serverOptions) authorization.Authorizer {
	return so.authorizer
}

func ClaimMapperProvider(so *serverOptions) authorization.ClaimMapper {
	return so.claimMapper
}

func JWTAudienceMapperProvider(so *serverOptions) authorization.JWTAudienceMapper {
	return so.audienceGetter
}

type (
	ServiceProviderParamsCommon struct {
		fx.In

		Cfg                        *config.Config
		ServiceNames               ServiceNames
		Logger                     log.Logger
		NamespaceLogger            NamespaceLogger
		DynamicConfigClient        dynamicconfig.Client
		ServerReporter             ServerReporter
		SdkReporter                SdkReporter
		EsConfig                   *esclient.Config
		EsClient                   esclient.Client
		TlsConfigProvider          encryption.TLSConfigProvider
		PersistenceConfig          config.Persistence
		ClusterMetadata            *cluster.Config
		ClientFactoryProvider      client.FactoryProvider
		AudienceGetter             authorization.JWTAudienceMapper
		PersistenceServiceResolver resolver.ServiceResolver
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
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() ServerReporter { return params.ServerReporter }),
		fx.Provide(func() SdkReporter { return params.SdkReporter }),
		fx.Provide(func() NamespaceLogger { return params.NamespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(newBootstrapParams),
		history.Module,
		fx.NopLogger,
	)

	stopFn := func() { stopService(params.Logger, app, serviceName, stopChan) }
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
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() ServerReporter { return params.ServerReporter }),
		fx.Provide(func() SdkReporter { return params.SdkReporter }),
		fx.Provide(func() NamespaceLogger { return params.NamespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(newBootstrapParams),
		matching.Module,
		fx.NopLogger,
	)

	stopFn := func() { stopService(params.Logger, app, serviceName, stopChan) }
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
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() ServerReporter { return params.ServerReporter }),
		fx.Provide(func() SdkReporter { return params.SdkReporter }),
		fx.Provide(func() NamespaceLogger { return params.NamespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(newBootstrapParams),
		frontend.Module,
		fx.NopLogger,
	)

	stopFn := func() { stopService(params.Logger, app, serviceName, stopChan) }
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
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return params.Logger }),
		fx.Provide(func() ServerReporter { return params.ServerReporter }),
		fx.Provide(func() SdkReporter { return params.SdkReporter }),
		fx.Provide(func() NamespaceLogger { return params.NamespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return params.EsClient }),
		fx.Provide(newBootstrapParams),
		worker.Module,
		fx.NopLogger,
	)

	stopFn := func() { stopService(params.Logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

// This is a place to expand SO
// Important note, persistence config and cluster metadata are later overriden via ApplyClusterMetadataConfigProvider.
func SoExpander(so *serverOptions) (
	*config.PProf,
	*config.Config,
	resolver.ServiceResolver,
) {
	return &so.config.Global.PProf, so.config, so.persistenceServiceResolver
}

func DynamicConfigClientProvider(so *serverOptions, logger log.Logger, stoppedCh chan interface{}) (dynamicconfig.Client, error) {
	var result dynamicconfig.Client
	var err error
	if so.dynamicConfigClient != nil {
		return so.dynamicConfigClient, nil
	}

	if so.config.DynamicConfigClient != nil {
		result, err = dynamicconfig.NewFileBasedClient(so.config.DynamicConfigClient, logger, stoppedCh)
		if err != nil {
			return nil, fmt.Errorf("unable to create dynamic config client: %w", err)
		}
		return result, nil
	}

	logger.Info("Dynamic config client is not configured. Using default values.")
	return dynamicconfig.NewNoopClient(), nil
}

func DcCollectionProvider(dynamicConfigClient dynamicconfig.Client, logger log.Logger) *dynamicconfig.Collection {
	return dynamicconfig.NewCollection(dynamicConfigClient, logger)
}

func ServerOptionsProvider(opts []ServerOption) (*serverOptions, error) {
	so := newServerOptions(opts)

	err := so.loadAndValidate()
	if err != nil {
		return nil, err
	}

	err = verifyPersistenceCompatibleVersion(so.config.Persistence, so.persistenceServiceResolver)
	if err != nil {
		return nil, err
	}

	err = ringpop.ValidateRingpopConfig(&so.config.Global.Membership)
	if err != nil {
		return nil, fmt.Errorf("ringpop config validation error: %w", err)
	}

	return so, nil
}

// ApplyClusterMetadataConfigProvider performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from downstream calls.
func ApplyClusterMetadataConfigProvider(
	logger log.Logger,
	config *config.Config,
	persistenceServiceResolver resolver.ServiceResolver,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
) (*cluster.Config, config.Persistence, error) {
	logger = log.With(logger, tag.ComponentMetadataInitializer)

	factory := persistenceClient.NewFactory(
		&config.Persistence,
		persistenceServiceResolver,
		nil,
		customDataStoreFactory,
		config.ClusterMetadata.CurrentClusterName,
		nil,
		logger,
	)
	defer factory.Close()

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		return config.ClusterMetadata, config.Persistence, fmt.Errorf("error initializing cluster metadata manager: %w", err)
	}
	defer clusterMetadataManager.Close()

	/**
	 * 1. Create cluster metadata info in both cluster_metadata and cluster_metadata_info tables.
	 * 2. For non current clusters, the initialization will be succeeded in the first time but fails in the following due to version conditional update.
	 * 3. For current cluster, 1) initialize in both tables (applied == true), 2) migrate data from cluster_metadata to cluster_metadata_info (applied == false)
	 */
	clusterData := config.ClusterMetadata
	for clusterName, clusterInfo := range clusterData.ClusterInformation {
		var clusterId = ""
		if clusterName == clusterData.CurrentClusterName {
			// Only set current cluster Id as we don't know the remote cluster Id.
			clusterId = uuid.New()
		}
		// Case 1: initialize cluster metadata config
		// We assume the existing remote cluster info is correct.
		applied, err := clusterMetadataManager.SaveClusterMetadata(
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

		resp, err := clusterMetadataManager.GetClusterMetadata(&persistence.GetClusterMetadataRequest{
			ClusterName: clusterName,
		})
		switch err.(type) {
		case nil:
			// Verify current cluster metadata
			if clusterName == clusterData.CurrentClusterName {
				var persistedShardCount = resp.HistoryShardCount
				if config.Persistence.NumHistoryShards != persistedShardCount {
					logger.Error(
						mismatchLogMessage,
						tag.Key("persistence.numHistoryShards"),
						tag.IgnoredValue(config.Persistence.NumHistoryShards),
						tag.Value(persistedShardCount))
					config.Persistence.NumHistoryShards = persistedShardCount
				}
				if resp.IsGlobalNamespaceEnabled != clusterData.EnableGlobalNamespace {
					logger.Error(
						mismatchLogMessage,
						tag.Key("clusterMetadata.EnableGlobalNamespace"),
						tag.IgnoredValue(clusterData.EnableGlobalNamespace),
						tag.Value(resp.IsGlobalNamespaceEnabled))
					config.ClusterMetadata.EnableGlobalNamespace = resp.IsGlobalNamespaceEnabled
				}
				if resp.FailoverVersionIncrement != clusterData.FailoverVersionIncrement {
					logger.Error(
						mismatchLogMessage,
						tag.Key("clusterMetadata.FailoverVersionIncrement"),
						tag.IgnoredValue(clusterData.FailoverVersionIncrement),
						tag.Value(resp.FailoverVersionIncrement))
					config.ClusterMetadata.FailoverVersionIncrement = resp.FailoverVersionIncrement
				}
			}
		case *serviceerror.NotFound:
			if clusterName == clusterData.CurrentClusterName {
				// Case 3: data exists in cluster metadata but not in cluster metadata info. Back fill data.
				oldClusterMetadata, err := clusterMetadataManager.GetClusterMetadataV1()
				if err != nil {
					return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while getting old cluster metadata: %w", err)
				}
				applied, err = clusterMetadataManager.SaveClusterMetadata(&persistence.SaveClusterMetadataRequest{
					ClusterMetadata: persistencespb.ClusterMetadata{
						HistoryShardCount:        oldClusterMetadata.HistoryShardCount,
						ClusterName:              oldClusterMetadata.ClusterName,
						ClusterId:                oldClusterMetadata.ClusterId,
						VersionInfo:              oldClusterMetadata.VersionInfo,
						IndexSearchAttributes:    oldClusterMetadata.IndexSearchAttributes,
						ClusterAddress:           clusterInfo.RPCAddress,
						FailoverVersionIncrement: clusterData.FailoverVersionIncrement,
						InitialFailoverVersion:   clusterInfo.InitialFailoverVersion,
						IsGlobalNamespaceEnabled: clusterData.EnableGlobalNamespace,
						IsConnectionEnabled:      clusterInfo.Enabled,
					}})
				if err != nil || !applied {
					return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while backfiling cluster metadata: %w", err)
				}
			} else {
				return config.ClusterMetadata,
					config.Persistence,
					fmt.Errorf("error while fetching metadata from cluster %s: %w", clusterName, err)
			}
		default:
			return config.ClusterMetadata,
				config.Persistence,
				fmt.Errorf("error while fetching metadata from cluster %s: %w", clusterName, err)
		}
	}
	err = loadClusterInformationFromStore(config, clusterMetadataManager, logger)
	if err != nil {
		return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while loading metadata from cluster: %w", err)
	}
	return config.ClusterMetadata, config.Persistence, nil
}

func loadClusterInformationFromStore(config *config.Config, clusterMsg persistence.ClusterMetadataManager, logger log.Logger) error {
	iter := collection.NewPagingIterator(func(paginationToken []byte) ([]interface{}, []byte, error) {
		request := &persistence.ListClusterMetadataRequest{
			PageSize:      100,
			NextPageToken: nil,
		}
		resp, err := clusterMsg.ListClusterMetadata(request)
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
		if staticMetadata, ok := config.ClusterMetadata.ClusterInformation[metadata.ClusterName]; ok {
			if staticMetadata.Enabled != metadata.IsConnectionEnabled {
				logger.Error(
					mismatchLogMessage,
					tag.Key("clusterInformation.Enabled"),
					tag.IgnoredValue(staticMetadata),
					tag.Value(newMetadata))
			}
			if staticMetadata.RPCAddress != metadata.ClusterAddress {
				logger.Error(
					mismatchLogMessage,
					tag.Key("clusterInformation.RPCAddress"),
					tag.IgnoredValue(staticMetadata),
					tag.Value(newMetadata))
			}
			if staticMetadata.InitialFailoverVersion != metadata.InitialFailoverVersion {
				logger.Error(
					mismatchLogMessage,
					tag.Key("clusterInformation.InitialFailoverVersion"),
					tag.IgnoredValue(staticMetadata),
					tag.Value(newMetadata))
			}
		} else {
			logger.Error(
				mismatchLogMessage,
				tag.Key("clusterInformation"),
				tag.IgnoredValue(config.ClusterMetadata.ClusterInformation),
				tag.Value(newMetadata))
		}
		config.ClusterMetadata.ClusterInformation[metadata.ClusterName] = newMetadata
	}
	return nil
}

func LoggerProvider(so *serverOptions) log.Logger {
	logger := so.logger
	if logger == nil {
		logger = log.NewZapLogger(log.BuildZapLogger(so.config.Log))
	}
	return logger
}

func NamespaceLoggerProvider(so *serverOptions) NamespaceLogger {
	return so.namespaceLogger
}

func MetricReportersProvider(so *serverOptions, logger log.Logger) (ServerReporter, SdkReporter, error) {
	var serverReporter ServerReporter
	var sdkReporter SdkReporter
	if so.config.Global.Metrics != nil {
		var err error
		serverReporter, sdkReporter, err = so.config.Global.Metrics.InitMetricReporters(logger, so.metricsReporter)
		if err != nil {
			return nil, nil, err
		}
	}
	return serverReporter, sdkReporter, nil
}

func MetricsClientProvider(logger log.Logger, serverReporter ServerReporter) (metrics.Client, error) {
	// todo: remove after deprecating per-service metrics config.
	if serverReporter == nil {
		return metrics.NewNoopMetricsClient(), nil
	}
	return serverReporter.NewClient(logger, metrics.Server)
}

func TlsConfigProviderProvider(
	logger log.Logger,
	so *serverOptions,
	metricsClient metrics.Client,
) (encryption.TLSConfigProvider, error) {
	if so.tlsConfigProvider != nil {
		return so.tlsConfigProvider, nil
	}

	return encryption.NewTLSConfigProviderFromConfig(so.config.Global.TLS, metricsClient.Scope(metrics.ServerTlsScope), logger, nil)
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
