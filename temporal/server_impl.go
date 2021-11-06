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
	"sync"

	"github.com/uber-go/tally/v4"
	sdkclient "go.temporal.io/sdk/client"
	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
)

type (
	NamespaceLogger log.Logger
	ServiceName     string

	// ServerImpl is temporal server.
	ServerImpl struct {
		so                 *serverOptions
		servicesMetadata   []*ServicesMetadata
		stoppedCh          chan interface{}
		logger             log.Logger
		namespaceLogger    NamespaceLogger
		serverReporter     metrics.Reporter
		sdkReporter        metrics.Reporter
		globalMetricsScope tally.Scope

		dynamicConfigClient dynamicconfig.Client
		dcCollection        *dynamicconfig.Collection

		persistenceConfig config.Persistence
		clusterMetadata   *cluster.Config
	}
)

var ServerFxImplModule = fx.Options(
	fx.Provide(NewServerFxImpl),
	fx.Provide(func(src *ServerImpl) Server { return src }),
)

// NewServer returns a new instance of server that serves one or many services.
func NewServerFxImpl(
	opts *serverOptions,
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	stoppedCh chan interface{},
	dynamicConfigClient dynamicconfig.Client,
	dcCollection *dynamicconfig.Collection,
	serverReporter ServerReporter,
	sdkReporter SdkReporter,
	globalMetricsScope tally.Scope,
	servicesGroup ServicesGroupIn,
	persistenceConfig config.Persistence,
	clusterMetadata *cluster.Config,
) *ServerImpl {
	s := &ServerImpl{
		so:                  opts,
		servicesMetadata:    servicesGroup.Services,
		stoppedCh:           stoppedCh,
		logger:              logger,
		namespaceLogger:     namespaceLogger,
		serverReporter:      serverReporter,
		sdkReporter:         sdkReporter,
		globalMetricsScope:  globalMetricsScope,
		dynamicConfigClient: dynamicConfigClient,
		dcCollection:        dcCollection,
		persistenceConfig:   persistenceConfig,
		clusterMetadata:     clusterMetadata,
	}
	return s
}

// Start temporal server.
// This function should be called only once, Server doesn't support multiple restarts.
func (s *ServerImpl) Start() error {
	s.logger.Info("Starting server for services", tag.Value(s.so.serviceNames))
	s.logger.Debug(s.so.config.String())

	var err error

	err = initSystemNamespaces(
		&s.persistenceConfig,
		s.clusterMetadata.CurrentClusterName,
		s.so.persistenceServiceResolver,
		s.logger,
		s.so.customDataStoreFactory)
	if err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	for _, svcMeta := range s.servicesMetadata {
		timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), serviceStartTimeout)
		svcMeta.App.Start(timeoutCtx)
		cancelFunc()
	}

	if s.so.blockingStart {
		// If s.so.interruptCh is nil this will wait forever.
		interruptSignal := <-s.so.interruptCh
		s.logger.Info("Received interrupt signal, stopping the server.", tag.Value(interruptSignal))
		s.Stop()
	}

	return nil
}

// Stop stops the server.
func (s *ServerImpl) Stop() {
	var wg sync.WaitGroup
	wg.Add(len(s.servicesMetadata))
	close(s.stoppedCh)

	for _, svcMeta := range s.servicesMetadata {
		go func(svc *ServicesMetadata) {
			svc.ServiceStopFn()
			wg.Done()
		}(svcMeta)
	}

	wg.Wait()

	if s.sdkReporter != nil {
		s.sdkReporter.Stop(s.logger)
	}

	if s.serverReporter != nil {
		s.serverReporter.Stop(s.logger)
	}
}

// Populates parameters for a service
func newBootstrapParams(
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	so *serverOptions,
	serviceName ServiceName,
	dc *dynamicconfig.Collection,
	serverReporter ServerReporter,
	sdkReporter SdkReporter,
	tlsConfigProvider encryption.TLSConfigProvider,
	persistenceConfig config.Persistence,
	clusterMetadata *cluster.Config,
) (*resource.BootstrapParams, error) {
	svcName := string(serviceName)
	params := &resource.BootstrapParams{
		Name:                     svcName,
		NamespaceLogger:          namespaceLogger,
		PersistenceConfig:        persistenceConfig,
		ClusterMetadataConfig:    clusterMetadata,
		DCRedirectionPolicy:      so.config.DCRedirectionPolicy,
		AbstractDatastoreFactory: so.customDataStoreFactory,
		ClientFactoryProvider:    so.clientFactoryProvider,
	}

	svcCfg := so.config.Services[svcName]
	rpcFactory := rpc.NewFactory(&svcCfg.RPC, svcName, logger, tlsConfigProvider, dc)
	params.RPCFactory = rpcFactory

	// Ringpop uses a different port to register handlers, this map is needed to resolve
	// services to correct addresses used by clients through ServiceResolver lookup API
	servicePortMap := make(map[string]int)
	for sn, sc := range so.config.Services {
		servicePortMap[sn] = sc.RPC.GRPCPort
	}

	params.MembershipFactoryInitializer =
		func(persistenceBean persistenceClient.Bean, logger log.Logger) (resource.MembershipMonitorFactory, error) {
			return ringpop.NewRingpopFactory(
				&so.config.Global.Membership,
				rpcFactory.GetRingpopChannel(),
				svcName,
				servicePortMap,
				logger,
				persistenceBean.GetClusterMetadataManager(),
			)
		}

	// todo: Replace this hack with actually using sdkReporter, Client or Scope.
	if serverReporter == nil {
		var err error
		serverReporter, sdkReporter, err = svcCfg.Metrics.InitMetricReporters(logger, nil)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to initialize per-service metric client. "+
					"This is deprecated behavior used as fallback, please use global metric config. Error: %w", err)
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

	params.SdkClient, err = sdkclient.NewClient(sdkclient.Options{
		HostPort:     so.config.PublicClient.HostPort,
		Namespace:    common.SystemLocalNamespace,
		MetricsScope: globalTallyScope,
		Logger:       log.NewSdkLogger(logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS:                options,
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create public client: %w", err)
	}

	params.ArchivalMetadata = archiver.NewArchivalMetadata(
		dc,
		so.config.Archival.History.State,
		so.config.Archival.History.EnableRead,
		so.config.Archival.Visibility.State,
		so.config.Archival.Visibility.EnableRead,
		&so.config.NamespaceDefaults.Archival,
	)

	params.ArchiverProvider = provider.NewArchiverProvider(so.config.Archival.History.Provider, so.config.Archival.Visibility.Provider)
	params.PersistenceConfig.TransactionSizeLimit = dc.GetIntProperty(dynamicconfig.TransactionSizeLimit, common.DefaultTransactionSizeLimit)

	if so.authorizer != nil {
		params.Authorizer = so.authorizer
	} else {
		params.Authorizer = authorization.NewNoopAuthorizer()
	}
	if so.claimMapper != nil {
		params.ClaimMapper = so.claimMapper
	} else {
		params.ClaimMapper = authorization.NewNoopClaimMapper()
	}
	params.AudienceGetter = so.audienceGetter

	params.PersistenceServiceResolver = so.persistenceServiceResolver
	params.SearchAttributesMapper = so.searchAttributesMapper

	return params, nil
}

func extractTallyScopeForSDK(sdkReporter metrics.Reporter) (tally.Scope, error) {
	if sdkTallyReporter, ok := sdkReporter.(*metrics.TallyReporter); ok {
		return sdkTallyReporter.GetScope(), nil
	} else {
		return nil, fmt.Errorf(
			"Sdk reporter is not of Tally type. Unfortunately, SDK only supports Tally for now. "+
				"Please specify prometheusSDK in metrics config with framework type %s.", metrics.FrameworkTally,
		)
	}
}

func initSystemNamespaces(
	cfg *config.Persistence,
	currentClusterName string,
	persistenceServiceResolver resolver.ServiceResolver,
	logger log.Logger,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
) error {
	factory := persistenceClient.NewFactory(
		cfg,
		persistenceServiceResolver,
		nil,
		customDataStoreFactory,
		currentClusterName,
		nil,
		logger,
	)
	defer factory.Close()

	metadataManager, err := factory.NewMetadataManager()
	if err != nil {
		return fmt.Errorf("unable to initialize metadata manager: %w", err)
	}
	defer metadataManager.Close()
	if err = metadataManager.InitializeSystemNamespaces(currentClusterName); err != nil {
		return fmt.Errorf("unable to register system namespace: %w", err)
	}
	return nil
}
