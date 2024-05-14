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
	"errors"
	"fmt"
	"os"

	"github.com/pborman/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	otelresource "go.opentelemetry.io/otel/sdk/resource"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership/ringpop"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/visibility"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/pprof"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
)

var (
	clusterMetadataInitErr           = errors.New("failed to initialize current cluster metadata")
	missingCurrentClusterMetadataErr = errors.New("missing current cluster metadata under clusterMetadata.ClusterInformation")
	missingServiceInStaticHosts      = errors.New("hosts are missing in static hosts for service: ")
)

type (
	ServicesGroupOut struct {
		fx.Out
		Services *ServicesMetadata `group:"services"`
	}

	ServicesGroupIn struct {
		fx.In
		Services []*ServicesMetadata `group:"services"`
	}

	ServicesMetadata struct {
		app         *fx.App
		serviceName primitives.ServiceName
		logger      log.Logger
	}

	ServerFx struct {
		app                        *fx.App
		startupSynchronizationMode synchronizationModeParams
		logger                     log.Logger
	}

	serverOptionsProvider struct {
		fx.Out
		ServerOptions              *serverOptions
		StopChan                   chan interface{}
		StartupSynchronizationMode synchronizationModeParams

		Config      *config.Config
		PProfConfig *config.PProf
		LogConfig   log.Config

		ServiceNames    resource.ServiceNames
		NamespaceLogger resource.NamespaceLogger

		ServiceResolver        resolver.ServiceResolver
		CustomDataStoreFactory persistenceClient.AbstractDataStoreFactory
		CustomVisibilityStore  visibility.VisibilityStoreFactory

		SearchAttributesMapper     searchattribute.Mapper
		CustomFrontendInterceptors []grpc.UnaryServerInterceptor
		Authorizer                 authorization.Authorizer
		ClaimMapper                authorization.ClaimMapper
		AudienceGetter             authorization.JWTAudienceMapper
		ServiceHosts               map[primitives.ServiceName]static.Hosts

		// below are things that could be over write by server options or may have default if not supplied by serverOptions.
		Logger                log.Logger
		ClientFactoryProvider client.FactoryProvider
		DynamicConfigClient   dynamicconfig.Client
		TLSConfigProvider     encryption.TLSConfigProvider
		EsConfig              *esclient.Config
		EsClient              esclient.Client
		MetricsHandler        metrics.Handler
	}
)

var (
	TopLevelModule = fx.Options(
		fx.Provide(
			NewServerFxImpl,
			ServerOptionsProvider,
			dynamicconfig.NewCollection,
			resource.ArchivalMetadataProvider,
			TaskCategoryRegistryProvider,
			PersistenceFactoryProvider,
			HistoryServiceProvider,
			MatchingServiceProvider,
			FrontendServiceProvider,
			InternalFrontendServiceProvider,
			WorkerServiceProvider,
			ApplyClusterMetadataConfigProvider,
		),
		pprof.Module,
		TraceExportModule,
		FxLogAdapter,
		fx.Invoke(ServerLifetimeHooks),
	)
)

func NewServerFx(topLevelModule fx.Option, opts ...ServerOption) (*ServerFx, error) {
	var s ServerFx
	s.app = fx.New(
		topLevelModule,
		fx.Supply(opts),
		fx.Populate(&s.startupSynchronizationMode),
		fx.Populate(&s.logger),
	)
	if err := s.app.Err(); err != nil {
		return nil, err
	}
	return &s, nil
}

func ServerOptionsProvider(opts []ServerOption) (serverOptionsProvider, error) {
	so := newServerOptions(opts)

	err := so.loadAndValidate()
	if err != nil {
		return serverOptionsProvider{}, err
	}

	persistenceConfig := so.config.Persistence
	err = verifyPersistenceCompatibleVersion(persistenceConfig, so.persistenceServiceResolver)
	if err != nil {
		return serverOptionsProvider{}, err
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

	// MetricsHandler
	metricHandler := so.metricHandler
	if metricHandler == nil {
		metricHandler, err = metrics.MetricsHandlerFromConfig(logger, so.config.Global.Metrics)
		if err != nil {
			return serverOptionsProvider{}, fmt.Errorf("unable to create metrics handler: %w", err)
		}
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
		tlsConfigProvider, err = encryption.NewTLSConfigProviderFromConfig(so.config.Global.TLS, metricHandler, logger, nil)
		if err != nil {
			return serverOptionsProvider{}, err
		}
	}

	// EsConfig / EsClient
	var esConfig *esclient.Config
	var esClient esclient.Client

	if persistenceConfig.VisibilityConfigExist() &&
		persistenceConfig.DataStores[persistenceConfig.VisibilityStore].Elasticsearch != nil {
		esConfig = persistenceConfig.DataStores[persistenceConfig.VisibilityStore].Elasticsearch
	} else if persistenceConfig.SecondaryVisibilityConfigExist() &&
		persistenceConfig.DataStores[persistenceConfig.SecondaryVisibilityStore].Elasticsearch != nil {
		esConfig = persistenceConfig.DataStores[persistenceConfig.SecondaryVisibilityStore].Elasticsearch
	} else if persistenceConfig.AdvancedVisibilityConfigExist() {
		esConfig = persistenceConfig.DataStores[persistenceConfig.AdvancedVisibilityStore].Elasticsearch
	}

	if esConfig != nil {
		esHttpClient := so.elasticsearchHttpClient
		if esHttpClient == nil {
			var err error
			esHttpClient, err = esclient.NewAwsHttpClient(esConfig.AWSRequestSigning)
			if err != nil {
				return serverOptionsProvider{}, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
			}
		}

		esClient, err = esclient.NewClient(esConfig, esHttpClient, logger)
		if err != nil {
			return serverOptionsProvider{}, fmt.Errorf("unable to create Elasticsearch client (URL = %v, username = %q): %w",
				esConfig.URL.Redacted(), esConfig.Username, err)
		}
	}

	// check that when static hosts are defined, they are defined for all required hosts
	if len(so.hostsByService) > 0 {
		for _, service := range DefaultServices {
			hosts := so.hostsByService[primitives.ServiceName(service)]
			if len(hosts.All) == 0 {
				return serverOptionsProvider{}, fmt.Errorf("%w: %v", missingServiceInStaticHosts, service)
			}
		}
	}

	return serverOptionsProvider{
		ServerOptions:              so,
		StopChan:                   stopChan,
		StartupSynchronizationMode: so.startupSynchronizationMode,

		Config:      so.config,
		PProfConfig: &so.config.Global.PProf,
		LogConfig:   so.config.Log,

		ServiceNames:    so.serviceNames,
		ServiceHosts:    so.hostsByService,
		NamespaceLogger: so.namespaceLogger,

		ServiceResolver:        so.persistenceServiceResolver,
		CustomDataStoreFactory: so.customDataStoreFactory,
		CustomVisibilityStore:  so.customVisibilityStoreFactory,

		SearchAttributesMapper:     so.searchAttributesMapper,
		CustomFrontendInterceptors: so.customFrontendInterceptors,
		Authorizer:                 so.authorizer,
		ClaimMapper:                so.claimMapper,
		AudienceGetter:             so.audienceGetter,

		Logger:                logger,
		ClientFactoryProvider: clientFactoryProvider,
		DynamicConfigClient:   dcClient,
		TLSConfigProvider:     tlsConfigProvider,
		EsConfig:              esConfig,
		EsClient:              esClient,
		MetricsHandler:        metricHandler,
	}, nil
}

// Start temporal server.
// This function should be called only once, Server doesn't support multiple restarts.
func (s *ServerFx) Start() error {
	err := s.app.Start(context.Background())
	if err != nil {
		return err
	}

	if s.startupSynchronizationMode.blockingStart {
		// If s.so.interruptCh is nil this will wait forever.
		interruptSignal := <-s.startupSynchronizationMode.interruptCh
		s.logger.Info("Received interrupt signal, stopping the server.", tag.Value(interruptSignal))
		return s.Stop()
	}

	return nil
}

// Stop stops the server.
func (s *ServerFx) Stop() error {
	return s.app.Stop(context.Background())
}

func (svc *ServicesMetadata) Stop(ctx context.Context) {
	stopCtx, cancelFunc := context.WithTimeout(ctx, serviceStopTimeout)
	defer cancelFunc()
	err := svc.app.Stop(stopCtx)
	if err != nil {
		svc.logger.Error("Failed to stop service", tag.Service(svc.serviceName), tag.Error(err))
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
		MetricsHandler             metrics.Handler
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
		CustomFrontendInterceptors []grpc.UnaryServerInterceptor
		Authorizer                 authorization.Authorizer
		ClaimMapper                authorization.ClaimMapper
		DataStoreFactory           persistenceClient.AbstractDataStoreFactory
		VisibilityStoreFactory     visibility.VisibilityStoreFactory
		SpanExporters              []otelsdktrace.SpanExporter
		InstanceID                 resource.InstanceID                     `optional:"true"`
		StaticServiceHosts         map[primitives.ServiceName]static.Hosts `optional:"true"`
		TaskCategoryRegistry       tasks.TaskCategoryRegistry
	}
)

// GetCommonServiceOptions returns an fx.Option which combines all the common dependencies for a service. Why do we need
// this? We are propagating dependencies from one fx graph to another. This is not ideal, since we should instead either
// have one shared fx graph, or propagate the individual fx options. So, in the server graph, dependencies exist as fx
// providers, and, in the process of building the server graph, we build the individual service graphs. We realize the
// dependencies in the server graph implicitly into the ServiceProviderParamsCommon object. Then, we convert them back
// into fx providers here. Essentially, we want an `fx.In` object in the server graph, and an `fx.Out` object in the
// service graphs. This is a workaround to achieve something similar.
func (params ServiceProviderParamsCommon) GetCommonServiceOptions(serviceName primitives.ServiceName) fx.Option {
	membershipModule := ringpop.MembershipModule
	if len(params.StaticServiceHosts) > 0 {
		membershipModule = static.MembershipModule(params.StaticServiceHosts)
	}

	return fx.Options(
		fx.Supply(
			serviceName,
			params.EsConfig,
			params.PersistenceConfig,
			params.ClusterMetadata,
			params.Cfg,
			params.SpanExporters,
		),
		fx.Provide(
			resource.DefaultSnTaggedLoggerProvider,
			params.PersistenceFactoryProvider,
			func() persistenceClient.AbstractDataStoreFactory {
				return params.DataStoreFactory
			},
			func() visibility.VisibilityStoreFactory {
				return params.VisibilityStoreFactory
			},
			func() client.FactoryProvider {
				return params.ClientFactoryProvider
			},
			func() authorization.JWTAudienceMapper {
				return params.AudienceGetter
			},
			func() resolver.ServiceResolver {
				return params.PersistenceServiceResolver
			},
			func() searchattribute.Mapper {
				return params.SearchAttributesMapper
			},
			func() authorization.Authorizer {
				return params.Authorizer
			},
			func() authorization.ClaimMapper {
				return params.ClaimMapper
			},
			func() encryption.TLSConfigProvider {
				return params.TlsConfigProvider
			},
			func() dynamicconfig.Client {
				return params.DynamicConfigClient
			},
			func() log.Logger {
				return params.Logger
			},
			func() metrics.Handler {
				return params.MetricsHandler.WithTags(metrics.ServiceNameTag(serviceName))
			},
			func() esclient.Client {
				return params.EsClient
			},
			func() resource.NamespaceLogger {
				return params.NamespaceLogger
			},
			func() tasks.TaskCategoryRegistry {
				return params.TaskCategoryRegistry
			},
		),
		ServiceTracingModule,
		resource.DefaultOptions,
		membershipModule,
		FxLogAdapter,
	)
}

// TaskCategoryRegistryProvider provides an immutable tasks.TaskCategoryRegistry to the server, which is intended to be
// shared by each service. Why do we need to initialize this at the top-level? Because, even though the presence of the
// archival task category is only needed by the history service, which must conditionally start a queue processor for
// it, we also do validation on request task categories in the frontend service. As a result, we need to initialize the
// registry in the server graph, and then propagate it to the service graphs. Otherwise, it would be isolated to the
// history service's graph.
func TaskCategoryRegistryProvider(archivalMetadata archiver.ArchivalMetadata, dc *dynamicconfig.Collection) tasks.TaskCategoryRegistry {
	registry := tasks.NewDefaultTaskCategoryRegistry()
	if archivalMetadata.GetHistoryConfig().StaticClusterState() == archiver.ArchivalEnabled ||
		archivalMetadata.GetVisibilityConfig().StaticClusterState() == archiver.ArchivalEnabled {
		registry.AddCategory(tasks.CategoryArchival)
	}
	// Can't use history service configs.Config because this provider is applied to all services (see docstring for this
	// function for more info).
	if dynamicconfig.OutboundProcessorEnabled.Get(dc)() {
		registry.AddCategory(tasks.CategoryOutbound)
	}
	return registry
}

func NewService(app *fx.App, serviceName primitives.ServiceName, logger log.Logger) ServicesGroupOut {
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			app:         app,
			serviceName: serviceName,
			logger:      logger,
		},
	}
}

func HistoryServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.HistoryService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{}, nil
	}

	app := fx.New(
		fx.Provide(workflow.NewTaskGeneratorProvider),
		params.GetCommonServiceOptions(serviceName),
		history.QueueModule,
		history.Module,
		replication.Module,
	)

	return NewService(app, serviceName, params.Logger), app.Err()
}

func MatchingServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.MatchingService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{}, nil
	}

	app := fx.New(
		params.GetCommonServiceOptions(serviceName),
		matching.Module,
	)

	return NewService(app, serviceName, params.Logger), app.Err()
}

func FrontendServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	return genericFrontendServiceProvider(params, primitives.FrontendService)
}

func InternalFrontendServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	return genericFrontendServiceProvider(params, primitives.InternalFrontendService)
}

func genericFrontendServiceProvider(
	params ServiceProviderParamsCommon,
	serviceName primitives.ServiceName,
) (ServicesGroupOut, error) {
	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{}, nil
	}

	app := fx.New(
		params.GetCommonServiceOptions(serviceName),
		fx.Supply(params.CustomFrontendInterceptors),
		fx.Decorate(func() authorization.ClaimMapper {
			switch serviceName {
			case primitives.FrontendService:
				return params.ClaimMapper
			case primitives.InternalFrontendService:
				return authorization.NewNoopClaimMapper()
			default:
				panic("Unexpected frontend service name")
			}
		}),
		fx.Decorate(func() log.SnTaggedLogger {
			// Use "frontend" for logs even if serviceName is "internal-frontend", but add an
			// extra tag to differentiate.
			tags := []tag.Tag{tag.Service(primitives.FrontendService)}
			if serviceName == primitives.InternalFrontendService {
				tags = append(tags, tag.NewBoolTag("internal-frontend", true))
			}
			return log.With(params.Logger, tags...)
		}),
		frontend.Module,
	)

	return NewService(app, serviceName, params.Logger), app.Err()
}

func WorkerServiceProvider(
	params ServiceProviderParamsCommon,
) (ServicesGroupOut, error) {
	serviceName := primitives.WorkerService

	if _, ok := params.ServiceNames[serviceName]; !ok {
		params.Logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{}, nil
	}

	app := fx.New(
		params.GetCommonServiceOptions(serviceName),
		worker.Module,
	)

	return NewService(app, serviceName, params.Logger), app.Err()
}

// ApplyClusterMetadataConfigProvider performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from downstream calls.
// TODO: move this to cluster.fx
func ApplyClusterMetadataConfigProvider(
	logger log.Logger,
	svc *config.Config,
	persistenceServiceResolver resolver.ServiceResolver,
	persistenceFactoryProvider persistenceClient.FactoryProviderFn,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
	metricsHandler metrics.Handler,
) (*cluster.Config, config.Persistence, error) {
	ctx := context.TODO()
	logger = log.With(logger, tag.ComponentMetadataInitializer)
	metricsHandler = metricsHandler.WithTags(metrics.ServiceNameTag(primitives.ServerService))
	clusterName := persistenceClient.ClusterName(svc.ClusterMetadata.CurrentClusterName)
	dataStoreFactory := persistenceClient.DataStoreFactoryProvider(
		clusterName,
		persistenceServiceResolver,
		&svc.Persistence,
		customDataStoreFactory,
		logger,
		metricsHandler,
	)
	factory := persistenceFactoryProvider(persistenceClient.NewFactoryParams{
		DataStoreFactory:           dataStoreFactory,
		Cfg:                        &svc.Persistence,
		PersistenceMaxQPS:          nil,
		PersistenceNamespaceMaxQPS: nil,
		ClusterName:                persistenceClient.ClusterName(svc.ClusterMetadata.CurrentClusterName),
		MetricsHandler:             metricsHandler,
		Logger:                     logger,
	})
	defer factory.Close()

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		return svc.ClusterMetadata, svc.Persistence, fmt.Errorf("error initializing cluster metadata manager: %w", err)
	}
	defer clusterMetadataManager.Close()

	initialIndexSearchAttributes := make(map[string]*persistencespb.IndexSearchAttributes)
	if ds := svc.Persistence.GetVisibilityStoreConfig(); ds.SQL != nil {
		initialIndexSearchAttributes[ds.GetIndexName()] = searchattribute.GetSqlDbIndexSearchAttributes()
	}
	if ds := svc.Persistence.GetSecondaryVisibilityStoreConfig(); ds.SQL != nil {
		initialIndexSearchAttributes[ds.GetIndexName()] = searchattribute.GetSqlDbIndexSearchAttributes()
	}

	clusterMetadata := svc.ClusterMetadata
	if len(clusterMetadata.ClusterInformation) > 1 {
		logger.Warn(
			"All remote cluster settings under ClusterMetadata.ClusterInformation config will be ignored. "+
				"Please use TCTL admin tool to configure remote cluster settings",
			tag.Key("clusterInformation"))
	}
	if _, ok := clusterMetadata.ClusterInformation[clusterMetadata.CurrentClusterName]; !ok {
		logger.Error("Current cluster setting is missing under clusterMetadata.ClusterInformation",
			tag.ClusterName(clusterMetadata.CurrentClusterName))
		return svc.ClusterMetadata, svc.Persistence, missingCurrentClusterMetadataErr
	}
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)
	resp, err := clusterMetadataManager.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: clusterMetadata.CurrentClusterName},
	)
	switch err.(type) {
	case nil:
		// Update current record
		if updateErr := updateCurrentClusterMetadataRecord(
			ctx,
			clusterMetadataManager,
			svc,
			initialIndexSearchAttributes,
			resp,
		); updateErr != nil {
			return svc.ClusterMetadata, svc.Persistence, updateErr
		}
		// Ignore invalid cluster metadata
		overwriteCurrentClusterMetadataWithDBRecord(
			svc,
			resp,
			logger,
		)
	case *serviceerror.NotFound:
		// Initialize current cluster record
		if initErr := initCurrentClusterMetadataRecord(
			ctx,
			clusterMetadataManager,
			svc,
			initialIndexSearchAttributes,
			logger,
		); initErr != nil {
			return svc.ClusterMetadata, svc.Persistence, initErr
		}
	default:
		return svc.ClusterMetadata, svc.Persistence, fmt.Errorf("error while fetching cluster metadata: %w", err)
	}

	clusterLoader := NewClusterMetadataLoader(clusterMetadataManager, logger)
	err = clusterLoader.LoadAndMergeWithStaticConfig(ctx, svc)
	if err != nil {
		return svc.ClusterMetadata, svc.Persistence, fmt.Errorf("error while loading metadata from cluster: %w", err)
	}
	return svc.ClusterMetadata, svc.Persistence, nil
}

func initCurrentClusterMetadataRecord(
	ctx context.Context,
	clusterMetadataManager persistence.ClusterMetadataManager,
	svc *config.Config,
	initialIndexSearchAttributes map[string]*persistencespb.IndexSearchAttributes,
	logger log.Logger,
) error {
	var clusterId string
	currentClusterName := svc.ClusterMetadata.CurrentClusterName
	currentClusterInfo := svc.ClusterMetadata.ClusterInformation[currentClusterName]
	if uuid.Parse(currentClusterInfo.ClusterID) == nil {
		if currentClusterInfo.ClusterID != "" {
			logger.Warn("Cluster Id in Cluster Metadata config is not a valid uuid. Generating a new Cluster Id")
		}
		clusterId = uuid.New()
	} else {
		clusterId = currentClusterInfo.ClusterID
	}

	applied, err := clusterMetadataManager.SaveClusterMetadata(
		ctx,
		&persistence.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				HistoryShardCount:        svc.Persistence.NumHistoryShards,
				ClusterName:              currentClusterName,
				ClusterId:                clusterId,
				ClusterAddress:           currentClusterInfo.RPCAddress,
				HttpAddress:              currentClusterInfo.HTTPAddress,
				FailoverVersionIncrement: svc.ClusterMetadata.FailoverVersionIncrement,
				InitialFailoverVersion:   currentClusterInfo.InitialFailoverVersion,
				IsGlobalNamespaceEnabled: svc.ClusterMetadata.EnableGlobalNamespace,
				IsConnectionEnabled:      currentClusterInfo.Enabled,
				UseClusterIdMembership:   true, // Enable this for new cluster after 1.19. This is to prevent two clusters join into one ring.
				IndexSearchAttributes:    initialIndexSearchAttributes,
				Tags:                     svc.ClusterMetadata.Tags,
			},
		})
	if err != nil {
		logger.Warn("Failed to save cluster metadata.", tag.Error(err), tag.ClusterName(currentClusterName))
		return err
	}
	if !applied {
		logger.Error("Failed to apply cluster metadata.", tag.ClusterName(currentClusterName))
		return clusterMetadataInitErr
	}
	return nil
}

func updateCurrentClusterMetadataRecord(
	ctx context.Context,
	clusterMetadataManager persistence.ClusterMetadataManager,
	svc *config.Config,
	initialIndexSearchAttributes map[string]*persistencespb.IndexSearchAttributes,
	currentClusterDBRecord *persistence.GetClusterMetadataResponse,
) error {
	updateDBRecord := false
	currentClusterMetadata := svc.ClusterMetadata
	currentClusterName := currentClusterMetadata.CurrentClusterName
	currentCLusterInfo := currentClusterMetadata.ClusterInformation[currentClusterName]
	// Allow updating cluster metadata if global namespace is disabled
	if !currentClusterDBRecord.IsGlobalNamespaceEnabled && currentClusterMetadata.EnableGlobalNamespace {
		currentClusterDBRecord.IsGlobalNamespaceEnabled = currentClusterMetadata.EnableGlobalNamespace
		currentClusterDBRecord.InitialFailoverVersion = currentCLusterInfo.InitialFailoverVersion
		currentClusterDBRecord.FailoverVersionIncrement = currentClusterMetadata.FailoverVersionIncrement
		updateDBRecord = true
	}
	if currentClusterDBRecord.ClusterAddress != currentCLusterInfo.RPCAddress {
		currentClusterDBRecord.ClusterAddress = currentCLusterInfo.RPCAddress
		updateDBRecord = true
	}
	if currentClusterDBRecord.HttpAddress != currentCLusterInfo.HTTPAddress {
		currentClusterDBRecord.HttpAddress = currentCLusterInfo.HTTPAddress
		updateDBRecord = true
	}
	if !maps.Equal(currentClusterDBRecord.Tags, svc.ClusterMetadata.Tags) {
		currentClusterDBRecord.Tags = svc.ClusterMetadata.Tags
		updateDBRecord = true
	}

	if len(initialIndexSearchAttributes) > 0 {
		if currentClusterDBRecord.IndexSearchAttributes == nil {
			currentClusterDBRecord.IndexSearchAttributes = initialIndexSearchAttributes
			updateDBRecord = true
		} else {
			for indexName, initialValue := range initialIndexSearchAttributes {
				if _, ok := currentClusterDBRecord.IndexSearchAttributes[indexName]; !ok {
					currentClusterDBRecord.IndexSearchAttributes[indexName] = initialValue
					updateDBRecord = true
				}
			}
		}
	}

	if !updateDBRecord {
		return nil
	}

	applied, err := clusterMetadataManager.SaveClusterMetadata(
		ctx,
		&persistence.SaveClusterMetadataRequest{
			ClusterMetadata: currentClusterDBRecord.ClusterMetadata,
			Version:         currentClusterDBRecord.Version,
		})
	if !applied || err != nil {
		return fmt.Errorf("error while updating cluster metadata: %w", err)
	}
	return nil
}

func overwriteCurrentClusterMetadataWithDBRecord(
	svc *config.Config,
	currentClusterDBRecord *persistence.GetClusterMetadataResponse,
	logger log.Logger,
) {
	clusterMetadata := svc.ClusterMetadata
	if currentClusterDBRecord.IsGlobalNamespaceEnabled && !clusterMetadata.EnableGlobalNamespace {
		logger.Warn(
			mismatchLogMessage,
			tag.Key("clusterMetadata.EnableGlobalNamespace"),
			tag.IgnoredValue(clusterMetadata.EnableGlobalNamespace),
			tag.Value(currentClusterDBRecord.IsGlobalNamespaceEnabled))
		svc.ClusterMetadata.EnableGlobalNamespace = currentClusterDBRecord.IsGlobalNamespaceEnabled
	}
	persistedShardCount := currentClusterDBRecord.HistoryShardCount
	if svc.Persistence.NumHistoryShards != persistedShardCount {
		logger.Warn(
			mismatchLogMessage,
			tag.Key("persistence.numHistoryShards"),
			tag.IgnoredValue(svc.Persistence.NumHistoryShards),
			tag.Value(persistedShardCount))
		svc.Persistence.NumHistoryShards = persistedShardCount
	}
	if currentClusterDBRecord.FailoverVersionIncrement != clusterMetadata.FailoverVersionIncrement {
		logger.Warn(
			mismatchLogMessage,
			tag.Key("clusterMetadata.FailoverVersionIncrement"),
			tag.IgnoredValue(clusterMetadata.FailoverVersionIncrement),
			tag.Value(currentClusterDBRecord.FailoverVersionIncrement))
		svc.ClusterMetadata.FailoverVersionIncrement = currentClusterDBRecord.FailoverVersionIncrement
	}
}

func PersistenceFactoryProvider() persistenceClient.FactoryProviderFn {
	return persistenceClient.FactoryProvider
}

func ServerLifetimeHooks(
	lc fx.Lifecycle,
	svr *ServerImpl,
) {
	lc.Append(fx.StartStopHook(svr.Start, svr.Stop))
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

// TraceExportModule holds process-global telemetry fx state defining the set of
// OTEL trace/span exporters used by tracing instrumentation. The following
// types can be overriden/augmented with fx.Replace/fx.Decorate:
//
// - []go.opentelemetry.io/otel/sdk/trace.SpanExporter
var TraceExportModule = fx.Options(
	fx.Invoke(func(log log.Logger) {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(
			func(err error) {
				log.Warn("OTEL error", tag.Error(err), tag.ServiceErrorType(err))
			}),
		)
	}),

	fx.Provide(func(lc fx.Lifecycle, c *config.Config) ([]otelsdktrace.SpanExporter, error) {
		exportersByType, err := c.ExporterConfig.SpanExporters()
		if err != nil {
			return nil, err
		}

		exportersByTypeFromEnv, err := telemetry.SpanExportersFromEnv(os.LookupEnv)
		if err != nil {
			return nil, err
		}

		// config-defined exporters override env-defined exporters with the same type
		maps.Copy(exportersByType, exportersByTypeFromEnv)

		exporters := maps.Values(exportersByType)
		lc.Append(fx.Hook{
			OnStart: startAll(exporters),
			OnStop:  shutdownAll(exporters),
		})
		return exporters, nil
	}),
)

// ServiceTracingModule holds per-service (i.e. frontend/history/matching/worker) fx
// state. The following types can be overriden with fx.Replace/fx.Decorate:
//
//   - []go.opentelemetry.io/otel/sdk/trace.BatchSpanProcessorOption
//     default: empty slice
//   - []go.opentelemetry.io/otel/sdk/trace.SpanProcessor
//     default: wrap each otelsdktrace.SpanExporter with otelsdktrace.NewBatchSpanProcessor
//   - *go.opentelemetry.io/otel/sdk/resource.Resource
//     default: resource.Default() augmented with the supplied serviceName
//   - []go.opentelemetry.io/otel/sdk/trace.TracerProviderOption
//     default: the provided resource.Resource and each of the otelsdktrace.SpanExporter
//   - go.opentelemetry.io/otel/trace.TracerProvider
//     default: otelsdktrace.NewTracerProvider with each of the otelsdktrace.TracerProviderOption
//   - go.opentelemetry.io/otel/ppropagation.TextMapPropagator
//     default: propagation.TraceContext{}
//   - telemetry.ServerTraceInterceptor
//   - telemetry.ClientTraceInterceptor
var ServiceTracingModule = fx.Options(
	fx.Supply([]otelsdktrace.BatchSpanProcessorOption{}),
	fx.Provide(
		fx.Annotate(
			func(exps []otelsdktrace.SpanExporter, opts []otelsdktrace.BatchSpanProcessorOption) []otelsdktrace.SpanProcessor {
				sps := make([]otelsdktrace.SpanProcessor, 0, len(exps))
				for _, exp := range exps {
					sps = append(sps, otelsdktrace.NewBatchSpanProcessor(exp, opts...))
				}
				return sps
			},
			fx.ParamTags(`optional:"true"`, ``),
		),
	),
	fx.Provide(
		fx.Annotate(
			func(rsn primitives.ServiceName, rsi resource.InstanceID) (*otelresource.Resource, error) {
				attrs := []attribute.KeyValue{
					semconv.ServiceNameKey.String(telemetry.ResourceServiceName(rsn, os.LookupEnv)),
					semconv.ServiceVersionKey.String(headers.ServerVersion),
				}
				if rsi != "" {
					attrs = append(attrs, semconv.ServiceInstanceIDKey.String(string(rsi)))
				}

				return otelresource.New(context.Background(),
					otelresource.WithProcess(),
					otelresource.WithOS(),
					otelresource.WithHost(),
					otelresource.WithContainer(),
					otelresource.WithAttributes(attrs...),
				)
			},
			fx.ParamTags(``, `optional:"true"`),
		),
	),
	fx.Provide(
		func(r *otelresource.Resource, sps []otelsdktrace.SpanProcessor) []otelsdktrace.TracerProviderOption {
			opts := make([]otelsdktrace.TracerProviderOption, 0, len(sps)+1)
			opts = append(opts, otelsdktrace.WithResource(r))
			for _, sp := range sps {
				opts = append(opts, otelsdktrace.WithSpanProcessor(sp))
			}
			return opts
		},
	),
	fx.Provide(func(lc fx.Lifecycle, opts []otelsdktrace.TracerProviderOption) trace.TracerProvider {
		tp := otelsdktrace.NewTracerProvider(opts...)
		lc.Append(fx.Hook{OnStop: func(ctx context.Context) error {
			return tp.Shutdown(ctx)
		}})
		return tp
	}),
	// Haven't had use for baggage propagation yet
	fx.Provide(func() propagation.TextMapPropagator { return propagation.TraceContext{} }),
	fx.Provide(telemetry.NewServerTraceInterceptor),
	fx.Provide(telemetry.NewClientTraceInterceptor),
)

func startAll(exporters []otelsdktrace.SpanExporter) func(ctx context.Context) error {
	type starter interface{ Start(context.Context) error }
	return func(ctx context.Context) error {
		for _, e := range exporters {
			if starter, ok := e.(starter); ok {
				err := starter.Start(ctx)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func shutdownAll(exporters []otelsdktrace.SpanExporter) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for _, e := range exporters {
			err := e.Shutdown(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

var FxLogAdapter = fx.WithLogger(func(logger log.Logger) fxevent.Logger {
	return &fxLogAdapter{logger: logger}
})

type fxLogAdapter struct {
	logger log.Logger
}

func (l *fxLogAdapter) LogEvent(e fxevent.Event) {
	switch e := e.(type) {
	case *fxevent.OnStartExecuting:
		l.logger.Debug("OnStart hook executing",
			tag.ComponentFX,
			tag.NewStringTag("callee", e.FunctionName),
			tag.NewStringTag("caller", e.CallerName),
		)
	case *fxevent.OnStartExecuted:
		if e.Err != nil {
			l.logger.Error("OnStart hook failed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.Error(e.Err),
			)
		} else {
			l.logger.Debug("OnStart hook executed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.NewStringTag("runtime", e.Runtime.String()),
			)
		}
	case *fxevent.OnStopExecuting:
		l.logger.Debug("OnStop hook executing",
			tag.ComponentFX,
			tag.NewStringTag("callee", e.FunctionName),
			tag.NewStringTag("caller", e.CallerName),
		)
	case *fxevent.OnStopExecuted:
		if e.Err != nil {
			l.logger.Error("OnStop hook failed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.Error(e.Err),
			)
		} else {
			l.logger.Debug("OnStop hook executed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.NewStringTag("runtime", e.Runtime.String()),
			)
		}
	case *fxevent.Supplied:
		if e.Err != nil {
			l.logger.Error("supplied",
				tag.ComponentFX,
				tag.NewStringTag("type", e.TypeName),
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Provided:
		if e.Err != nil {
			l.logger.Error("error encountered while applying options",
				tag.ComponentFX,
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Replaced:
		if e.Err != nil {
			l.logger.Error("error encountered while replacing",
				tag.ComponentFX,
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Decorated:
		if e.Err != nil {
			l.logger.Error("error encountered while applying options",
				tag.ComponentFX,
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Run:
		if e.Err != nil {
			l.logger.Error("error returned",
				tag.ComponentFX,
				tag.NewStringTag("name", e.Name),
				tag.NewStringTag("kind", e.Kind),
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err),
			)
		}
	case *fxevent.Invoking:
		// Do not log stack as it will make logs hard to read.
		l.logger.Debug("invoking",
			tag.ComponentFX,
			tag.NewStringTag("function", e.FunctionName),
			tag.NewStringTag("module", e.ModuleName),
		)
	case *fxevent.Invoked:
		if e.Err != nil {
			l.logger.Error("invoke failed",
				tag.ComponentFX,
				tag.Error(e.Err),
				tag.NewStringTag("stack", e.Trace),
				tag.NewStringTag("function", e.FunctionName),
				tag.NewStringTag("module", e.ModuleName),
			)
		}
	case *fxevent.Stopping:
		l.logger.Info("received signal",
			tag.ComponentFX,
			tag.NewStringTag("signal", e.Signal.String()))
	case *fxevent.Stopped:
		if e.Err != nil {
			l.logger.Error("stop failed", tag.ComponentFX, tag.Error(e.Err))
		}
	case *fxevent.RollingBack:
		l.logger.Error("start failed, rolling back", tag.ComponentFX, tag.Error(e.StartErr))
	case *fxevent.RolledBack:
		if e.Err != nil {
			l.logger.Error("rollback failed", tag.ComponentFX, tag.Error(e.Err))
		}
	case *fxevent.Started:
		if e.Err != nil {
			l.logger.Error("start failed", tag.ComponentFX, tag.Error(e.Err))
		} else {
			l.logger.Debug("started", tag.ComponentFX)
		}
	case *fxevent.LoggerInitialized:
		if e.Err != nil {
			l.logger.Error("custom logger initialization failed", tag.ComponentFX, tag.Error(e.Err))
		} else {
			l.logger.Debug("initialized custom fxevent.Logger",
				tag.ComponentFX,
				tag.NewStringTag("function", e.ConstructorName))
		}
	default:
		l.logger.Warn("unknown fx log type, update fxLogAdapter",
			tag.ComponentFX,
			tag.ValueType(e),
		)
	}
}
