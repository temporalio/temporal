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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	sdkclient "go.temporal.io/sdk/client"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/pprof"
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
)

const (
	mismatchLogMessage = "Supplied configuration key/value mismatches persisted cluster metadata. Continuing with the persisted value as this value cannot be changed once initialized."
)

type (
	// Server is temporal server.
	Server struct {
		so                *serverOptions
		services          map[string]common.Daemon
		serviceStoppedChs map[string]chan struct{}
		stoppedCh         chan struct{}
		logger            log.Logger
	}
)

// Services is the list of all valid temporal services
var (
	Services = []string{
		primitives.FrontendService,
		primitives.HistoryService,
		primitives.MatchingService,
		primitives.WorkerService,
	}
)

// NewServer returns a new instance of server that serves one or many services.
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		so:                newServerOptions(opts),
		services:          make(map[string]common.Daemon),
		serviceStoppedChs: make(map[string]chan struct{}),
	}
	return s
}

// Start temporal server.
func (s *Server) Start() error {
	err := s.so.loadAndValidate()
	if err != nil {
		return err
	}

	s.stoppedCh = make(chan struct{})

	s.logger = s.so.logger
	if s.logger == nil {
		s.logger = log.NewZapLogger(log.BuildZapLogger(s.so.config.Log))
	}

	s.logger.Info("Starting server for services", tag.Value(s.so.serviceNames))
	s.logger.Debug(s.so.config.String())

	if s.so.persistenceServiceResolver == nil {
		s.so.persistenceServiceResolver = resolver.NewNoopResolver()
	}

	err = verifyPersistenceCompatibleVersion(s.so.config.Persistence, s.so.persistenceServiceResolver)

	if err = pprof.NewInitializer(&s.so.config.Global.PProf, s.logger).Start(); err != nil {
		return fmt.Errorf("unable to start PProf: %w", err)
	}

	err = ringpop.ValidateRingpopConfig(&s.so.config.Global.Membership)
	if err != nil {
		return fmt.Errorf("ringpop config validation error: %w", err)
	}

	if s.so.dynamicConfigClient == nil {
		s.so.dynamicConfigClient, err = dynamicconfig.NewFileBasedClient(&s.so.config.DynamicConfigClient, s.logger, s.stoppedCh)
		if err != nil {
			s.logger.Info("Error creating file based dynamic config client, use no-op config client instead.", tag.Error(err))
			s.so.dynamicConfigClient = dynamicconfig.NewNoopClient()
		}
	}
	dc := dynamicconfig.NewCollection(s.so.dynamicConfigClient, s.logger)

	err = updateClusterMetadataConfig(s.so.config, s.so.persistenceServiceResolver, s.logger)
	if err != nil {
		return fmt.Errorf("unable to initialize cluster metadata: %w", err)
	}

	err = initSystemNamespaces(&s.so.config.Persistence, s.so.config.ClusterMetadata.CurrentClusterName, s.so.persistenceServiceResolver, s.logger)
	if err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	var globalMetricsScope tally.Scope
	if s.so.metricsReporter != nil {
		globalMetricsScope = s.so.config.Global.Metrics.NewCustomReporterScope(s.logger, s.so.metricsReporter)
	} else if s.so.config.Global.Metrics != nil {
		globalMetricsScope = s.so.config.Global.Metrics.NewScope(s.logger)
	}

	if s.so.tlsConfigProvider == nil {
		s.so.tlsConfigProvider, err = encryption.NewTLSConfigProviderFromConfig(s.so.config.Global.TLS, globalMetricsScope, nil)
		if err != nil {
			return fmt.Errorf("TLS provider initialization error: %w", err)
		}
	}

	for _, svcName := range s.so.serviceNames {
		params, err := s.newBootstrapParams(svcName, dc, globalMetricsScope)
		if err != nil {
			return err
		}

		var svc common.Daemon
		switch svcName {
		case primitives.FrontendService:
			svc, err = frontend.NewService(params)
		case primitives.HistoryService:
			svc, err = history.NewService(params)
		case primitives.MatchingService:
			svc, err = matching.NewService(params)
		case primitives.WorkerService:
			svc, err = worker.NewService(params)
		default:
			return fmt.Errorf("uknown service %q", svcName)
		}
		if err != nil {
			return fmt.Errorf("unable to start service %q: %w", svcName, err)
		}

		s.services[svcName] = svc
		s.serviceStoppedChs[svcName] = make(chan struct{})

		go func(svc common.Daemon, svcStoppedCh chan<- struct{}) {
			// Start is blocked until Stop() is called.
			svc.Start()
			close(svcStoppedCh)
		}(svc, s.serviceStoppedChs[svcName])

	}

	if s.so.blockingStart {
		// If s.so.interruptCh is nil this will wait forever.
		interruptSignal := <-s.so.interruptCh
		s.logger.Info("Received interrupt signal, stopping the server.", tag.Value(interruptSignal))
		s.Stop()
	}

	return nil
}

// Stops the server.
func (s *Server) Stop() {
	var wg sync.WaitGroup
	wg.Add(len(s.services))
	close(s.stoppedCh)

	for svcName, svc := range s.services {
		go func(svc common.Daemon, svcName string, svcStoppedCh <-chan struct{}) {
			svc.Stop()
			select {
			case <-svcStoppedCh:
			case <-time.After(time.Minute):
				s.logger.Error("Timed out (1 minute) waiting for service to stop.", tag.Service(svcName))
			}
			wg.Done()
		}(svc, svcName, s.serviceStoppedChs[svcName])
	}
	wg.Wait()
}

// Populates parameters for a service
func (s *Server) newBootstrapParams(
	svcName string,
	dc *dynamicconfig.Collection,
	metricsScope tally.Scope,
) (*resource.BootstrapParams, error) {

	params := resource.BootstrapParams{}
	params.Name = svcName
	params.Logger = s.logger
	params.PersistenceConfig = s.so.config.Persistence
	params.DynamicConfigClient = s.so.dynamicConfigClient
	params.ClusterMetadataConfig = s.so.config.ClusterMetadata

	svcCfg := s.so.config.Services[svcName]
	rpcFactory := rpc.NewFactory(&svcCfg.RPC, svcName, s.logger, s.so.tlsConfigProvider)
	params.RPCFactory = rpcFactory

	// Ringpop uses a different port to register handlers, this map is needed to resolve
	// services to correct addresses used by clients through ServiceResolver lookup API
	servicePortMap := make(map[string]int)
	for svcName, svcCfg := range s.so.config.Services {
		servicePortMap[svcName] = svcCfg.RPC.GRPCPort
	}

	params.MembershipFactoryInitializer =
		func(persistenceBean persistenceClient.Bean, logger log.Logger) (resource.MembershipMonitorFactory, error) {
			return ringpop.NewRingpopFactory(
				&s.so.config.Global.Membership,
				rpcFactory.GetRingpopChannel(),
				svcName,
				servicePortMap,
				logger,
				persistenceBean.GetClusterMetadataManager(),
			)
		}

	params.DCRedirectionPolicy = s.so.config.DCRedirectionPolicy
	if metricsScope == nil {
		metricsScope = svcCfg.Metrics.NewScope(s.logger)
	}
	params.MetricsScope = metricsScope
	metricsClient := metrics.NewClient(metricsScope, metrics.GetMetricsServiceIdx(svcName, s.logger))
	params.MetricsClient = metricsClient

	options, err := s.so.tlsConfigProvider.GetFrontendClientConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load frontend TLS configuration: %w", err)
	}

	params.PublicClient, err = sdkclient.NewClient(sdkclient.Options{
		HostPort:     s.so.config.PublicClient.HostPort,
		Namespace:    common.SystemLocalNamespace,
		MetricsScope: metricsScope,
		Logger:       log.NewSdkLogger(s.logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS:                options,
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create public client: %w", err)
	}

	advancedVisMode := dc.GetStringProperty(
		dynamicconfig.AdvancedVisibilityWritingMode,
		common.GetDefaultAdvancedVisibilityWritingMode(s.so.config.Persistence.IsAdvancedVisibilityConfigExist()),
	)()

	if advancedVisMode != common.AdvancedVisibilityWritingModeOff {
		// verify config of advanced visibility store
		advancedVisStoreKey := s.so.config.Persistence.AdvancedVisibilityStore
		advancedVisStore, ok := s.so.config.Persistence.DataStores[advancedVisStoreKey]
		if !ok {
			return nil, fmt.Errorf("unable to find advanced visibility store in config for %q key", advancedVisStoreKey)
		}

		if s.so.elasticseachHttpClient == nil {
			s.so.elasticseachHttpClient, err = client.NewAwsHttpClient(advancedVisStore.ElasticSearch.AWSRequestSigning)
			if err != nil {
				return nil, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
			}
		}

		esClient, err := client.NewClient(advancedVisStore.ElasticSearch, s.so.elasticseachHttpClient, s.logger)
		if err != nil {
			return nil, fmt.Errorf("unable to create Elasticsearch client: %w", err)
		}
		params.ESConfig = advancedVisStore.ElasticSearch
		params.ESClient = esClient

		// verify index name
		indexName := advancedVisStore.ElasticSearch.GetVisibilityIndex()
		if len(indexName) == 0 {
			return nil, errors.New("visibility index in missing in Elasticsearch config")
		}
	}

	params.ArchivalMetadata = archiver.NewArchivalMetadata(
		dc,
		s.so.config.Archival.History.State,
		s.so.config.Archival.History.EnableRead,
		s.so.config.Archival.Visibility.State,
		s.so.config.Archival.Visibility.EnableRead,
		&s.so.config.NamespaceDefaults.Archival,
	)

	params.ArchiverProvider = provider.NewArchiverProvider(s.so.config.Archival.History.Provider, s.so.config.Archival.Visibility.Provider)
	params.PersistenceConfig.TransactionSizeLimit = dc.GetIntProperty(dynamicconfig.TransactionSizeLimit, common.DefaultTransactionSizeLimit)

	if s.so.authorizer != nil {
		params.Authorizer = s.so.authorizer
	} else {
		params.Authorizer = authorization.NewNoopAuthorizer()
	}
	if s.so.claimMapper != nil {
		params.ClaimMapper = s.so.claimMapper
	} else {
		params.ClaimMapper = authorization.NewNoopClaimMapper()
	}

	params.PersistenceServiceResolver = s.so.persistenceServiceResolver

	return &params, nil
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

// updateClusterMetadataConfig performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from downstream calls.
func updateClusterMetadataConfig(cfg *config.Config, persistenceServiceResolver resolver.ServiceResolver, logger log.Logger) error {
	logger = log.With(logger, tag.ComponentMetadataInitializer)

	factory := persistenceClient.NewFactory(
		&cfg.Persistence,
		persistenceServiceResolver,
		nil,
		nil,
		cfg.ClusterMetadata.CurrentClusterName,
		nil,
		logger,
	)

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		return fmt.Errorf("error initializing cluster metadata manager: %w", err)
	}
	defer clusterMetadataManager.Close()

	applied, err := clusterMetadataManager.SaveClusterMetadata(
		&persistence.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				HistoryShardCount: cfg.Persistence.NumHistoryShards,
				ClusterName:       cfg.ClusterMetadata.CurrentClusterName,
				ClusterId:         uuid.New(),
			}})
	if err != nil {
		logger.Warn("Failed to save cluster metadata.", tag.Error(err))
	}
	if applied {
		logger.Info("Successfully saved cluster metadata.")
		return nil
	}

	resp, err := clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		return fmt.Errorf("error while fetching cluster metadata: %w", err)
	}
	if cfg.ClusterMetadata.CurrentClusterName != resp.ClusterName {
		logger.Error(
			mismatchLogMessage,
			tag.Key("clusterMetadata.currentClusterName"),
			tag.IgnoredValue(cfg.ClusterMetadata.CurrentClusterName),
			tag.Value(resp.ClusterName))
		cfg.ClusterMetadata.CurrentClusterName = resp.ClusterName
	}

	var persistedShardCount = resp.HistoryShardCount
	if cfg.Persistence.NumHistoryShards != persistedShardCount {
		logger.Error(
			mismatchLogMessage,
			tag.Key("persistence.numHistoryShards"),
			tag.IgnoredValue(cfg.Persistence.NumHistoryShards),
			tag.Value(persistedShardCount))
		cfg.Persistence.NumHistoryShards = persistedShardCount
	}

	return nil
}

func initSystemNamespaces(cfg *config.Persistence, currentClusterName string, persistenceServiceResolver resolver.ServiceResolver, logger log.Logger) error {
	factory := persistenceClient.NewFactory(
		cfg,
		persistenceServiceResolver,
		nil,
		nil,
		currentClusterName,
		nil,
		logger,
	)

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
