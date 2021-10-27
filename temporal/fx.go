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

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"

	"github.com/pborman/uuid"

	persistencespb "go.temporal.io/server/api/persistence/v1"
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
		fx.Invoke(ServerLifetimeHooks),
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

func HistoryServiceProvider(
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	so *serverOptions,
	dynamicConfigClient dynamicconfig.Client,
	serverReporter ServerReporter,
	sdkReporter SdkReporter,
	esConfig *esclient.Config,
	esClient esclient.Client,
	tlsConfigProvider encryption.TLSConfigProvider,
	persistenceConfig config.Persistence,
	clusterMetadata *config.ClusterMetadata,
) (ServicesGroupOut, error) {
	serviceName := primitives.HistoryService

	if _, ok := so.serviceNames[serviceName]; !ok {
		logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			so,
			esConfig,
			persistenceConfig,
			clusterMetadata,
		),
		fx.Provide(func() encryption.TLSConfigProvider { return tlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return dynamicConfigClient }),
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return logger }),
		fx.Provide(func() ServerReporter { return serverReporter }),
		fx.Provide(func() SdkReporter { return sdkReporter }),
		fx.Provide(func() NamespaceLogger { return namespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return esClient }),
		fx.Provide(newBootstrapParams),
		history.Module,
	)

	stopFn := func() { stopService(logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

func MatchingServiceProvider(
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	so *serverOptions,
	dynamicConfigClient dynamicconfig.Client,
	serverReporter ServerReporter,
	sdkReporter SdkReporter,
	esConfig *esclient.Config,
	esClient esclient.Client,
	tlsConfigProvider encryption.TLSConfigProvider,
	persistenceConfig config.Persistence,
	clusterMetadata *config.ClusterMetadata,
) (ServicesGroupOut, error) {
	serviceName := primitives.MatchingService

	if _, ok := so.serviceNames[serviceName]; !ok {
		logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			so,
			esConfig,
			persistenceConfig,
			clusterMetadata,
		),
		fx.Provide(func() encryption.TLSConfigProvider { return tlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return dynamicConfigClient }),
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return logger }),
		fx.Provide(func() ServerReporter { return serverReporter }),
		fx.Provide(func() SdkReporter { return sdkReporter }),
		fx.Provide(func() NamespaceLogger { return namespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return esClient }),
		fx.Provide(newBootstrapParams),
		matching.Module,
	)

	stopFn := func() { stopService(logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

func FrontendServiceProvider(
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	so *serverOptions,
	dynamicConfigClient dynamicconfig.Client,
	serverReporter ServerReporter,
	sdkReporter SdkReporter,
	esConfig *esclient.Config,
	esClient esclient.Client,
	tlsConfigProvider encryption.TLSConfigProvider,
	persistenceConfig config.Persistence,
	clusterMetadata *config.ClusterMetadata,
) (ServicesGroupOut, error) {
	serviceName := primitives.FrontendService

	if _, ok := so.serviceNames[serviceName]; !ok {
		logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			so,
			esConfig,
			persistenceConfig,
			clusterMetadata,
		),
		fx.Provide(func() encryption.TLSConfigProvider { return tlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return dynamicConfigClient }),
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return logger }),
		fx.Provide(func() ServerReporter { return serverReporter }),
		fx.Provide(func() SdkReporter { return sdkReporter }),
		fx.Provide(func() NamespaceLogger { return namespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return esClient }),
		fx.Provide(newBootstrapParams),
		frontend.Module,
	)

	stopFn := func() { stopService(logger, app, serviceName, stopChan) }
	return ServicesGroupOut{
		Services: &ServicesMetadata{
			App:           app,
			ServiceName:   serviceName,
			ServiceStopFn: stopFn,
		},
	}, app.Err()
}

func WorkerServiceProvider(
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	so *serverOptions,
	dynamicConfigClient dynamicconfig.Client,
	serverReporter ServerReporter,
	sdkReporter SdkReporter,
	esConfig *esclient.Config,
	esClient esclient.Client,
	tlsConfigProvider encryption.TLSConfigProvider,
	persistenceConfig config.Persistence,
	clusterMetadata *config.ClusterMetadata,
) (ServicesGroupOut, error) {
	serviceName := primitives.WorkerService

	if _, ok := so.serviceNames[serviceName]; !ok {
		logger.Info("Service is not requested, skipping initialization.", tag.Service(serviceName))
		return ServicesGroupOut{
			Services: &ServicesMetadata{
				App:           fx.New(),
				ServiceName:   serviceName,
				ServiceStopFn: func() {},
			},
		}, nil
	}

	stopChan := make(chan struct{})
	app := fx.New(
		fx.Supply(
			stopChan,
			so,
			esConfig,
			persistenceConfig,
			clusterMetadata,
		),
		fx.Provide(func() encryption.TLSConfigProvider { return tlsConfigProvider }),
		fx.Provide(func() dynamicconfig.Client { return dynamicConfigClient }),
		fx.Provide(func() ServiceName { return ServiceName(serviceName) }),
		fx.Provide(func() log.Logger { return logger }),
		fx.Provide(func() ServerReporter { return serverReporter }),
		fx.Provide(func() SdkReporter { return sdkReporter }),
		fx.Provide(func() NamespaceLogger { return namespaceLogger }), // resolves untyped nil error
		fx.Provide(func() esclient.Client { return esClient }),
		fx.Provide(newBootstrapParams),
		worker.Module,
	)

	stopFn := func() { stopService(logger, app, serviceName, stopChan) }
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
	persistenceClient.AbstractDataStoreFactory,
) {
	return &so.config.Global.PProf, so.config, so.persistenceServiceResolver, so.customDataStoreFactory
}

func DynamicConfigClientProvider(so *serverOptions, logger log.Logger, stoppedCh chan interface{}) dynamicconfig.Client {
	var result dynamicconfig.Client
	var err error
	if so.dynamicConfigClient != nil {
		return so.dynamicConfigClient
	}

	if so.config.DynamicConfigClient != nil {
		result, err = dynamicconfig.NewFileBasedClient(so.config.DynamicConfigClient, logger, stoppedCh)
		if err != nil {
			// TODO: uncomment the next line and remove next 3 lines in 1.14.
			// return fmt.Errorf("unable to create dynamic config client: %w", err)
			logger.Error("Unable to read dynamic config file. Continue with default settings but the ERROR MUST BE FIXED before the next upgrade", tag.Error(err))
			result = dynamicconfig.NewNoopClient()
		}
		return result
	}

	logger.Info("Dynamic config client is not configured. Using default values.")
	return dynamicconfig.NewNoopClient()
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
) (*config.ClusterMetadata, config.Persistence, error) {
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

	applied, err := clusterMetadataManager.SaveClusterMetadata(
		&persistence.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				HistoryShardCount: config.Persistence.NumHistoryShards,
				ClusterName:       config.ClusterMetadata.CurrentClusterName,
				ClusterId:         uuid.New(),
			}})
	if err != nil {
		logger.Warn("Failed to save cluster metadata.", tag.Error(err))
	}
	if applied {
		logger.Info("Successfully saved cluster metadata.")
		return config.ClusterMetadata, config.Persistence, nil
	}

	resp, err := clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		return config.ClusterMetadata, config.Persistence, fmt.Errorf("error while fetching cluster metadata: %w", err)
	}

	if config.ClusterMetadata.CurrentClusterName != resp.ClusterName {
		logger.Error(
			mismatchLogMessage,
			tag.Key("clusterMetadata.currentClusterName"),
			tag.IgnoredValue(config.ClusterMetadata.CurrentClusterName),
			tag.Value(resp.ClusterName))

		config.ClusterMetadata.CurrentClusterName = resp.ClusterName
	}

	var persistedShardCount = resp.HistoryShardCount
	if config.Persistence.NumHistoryShards != persistedShardCount {
		logger.Error(
			mismatchLogMessage,
			tag.Key("persistence.numHistoryShards"),
			tag.IgnoredValue(config.Persistence.NumHistoryShards),
			tag.Value(persistedShardCount))
		config.Persistence.NumHistoryShards = persistedShardCount
	}

	return config.ClusterMetadata, config.Persistence, nil
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

func MetricReportersProvider(so *serverOptions, logger log.Logger) (ServerReporter, SdkReporter, tally.Scope, error) {
	var serverReporter ServerReporter
	var sdkReporter SdkReporter
	var globalMetricsScope tally.Scope
	if so.config.Global.Metrics != nil {
		var err error
		serverReporter, sdkReporter, err = so.config.Global.Metrics.InitMetricReporters(logger, so.metricsReporter)
		if err != nil {
			return nil, nil, nil, err
		}
		globalMetricsScope, err = extractTallyScopeForSDK(sdkReporter)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return serverReporter, sdkReporter, globalMetricsScope, nil
}

func TlsConfigProviderProvider(
	logger log.Logger,
	so *serverOptions,
	globalMetricsScope tally.Scope,
) (encryption.TLSConfigProvider, error) {
	if so.tlsConfigProvider != nil {
		return so.tlsConfigProvider, nil
	}

	return encryption.NewTLSConfigProviderFromConfig(so.config.Global.TLS, globalMetricsScope, logger, nil)
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
