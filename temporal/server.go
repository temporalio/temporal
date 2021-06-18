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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	esclient "go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/pprof"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
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
		stoppedCh         chan interface{}
		logger            log.Logger
		serverReporter    metrics.Reporter
		sdkReporter       metrics.Reporter
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

	s.stoppedCh = make(chan interface{})

	s.logger = s.so.logger
	if s.logger == nil {
		s.logger = log.NewZapLogger(log.BuildZapLogger(s.so.config.Log))
	}

	s.logger.Info("Starting server for services", tag.Value(s.so.serviceNames))
	s.logger.Debug(s.so.config.String())

	if s.so.persistenceServiceResolver == nil {
		s.so.persistenceServiceResolver = resolver.NewNoopResolver()
	}

	if s.so.dynamicConfigClient == nil {
		s.so.dynamicConfigClient, err = dynamicconfig.NewFileBasedClient(&s.so.config.DynamicConfigClient, s.logger, s.stoppedCh)
		if err != nil {
			s.logger.Info("Error creating file based dynamic config client, use no-op config client instead.", tag.Error(err))
			s.so.dynamicConfigClient = dynamicconfig.NewNoopClient()
		}
	}
	dc := dynamicconfig.NewCollection(s.so.dynamicConfigClient, s.logger)

	advancedVisibilityWritingMode := dc.GetStringProperty(dynamicconfig.AdvancedVisibilityWritingMode, common.GetDefaultAdvancedVisibilityWritingMode(s.so.config.Persistence.IsAdvancedVisibilityConfigExist()))()

	err = verifyPersistenceCompatibleVersion(s.so.config.Persistence, s.so.persistenceServiceResolver, advancedVisibilityWritingMode != common.AdvancedVisibilityWritingModeOn)
	if err != nil {
		return err
	}

	if err = pprof.NewInitializer(&s.so.config.Global.PProf, s.logger).Start(); err != nil {
		return fmt.Errorf("unable to start PProf: %w", err)
	}

	err = ringpop.ValidateRingpopConfig(&s.so.config.Global.Membership)
	if err != nil {
		return fmt.Errorf("ringpop config validation error: %w", err)
	}

	err = updateClusterMetadataConfig(s.so.config, s.so.persistenceServiceResolver, s.logger)
	if err != nil {
		return fmt.Errorf("unable to initialize cluster metadata: %w", err)
	}

	// TODO: remove this call after 1.10 release
	copyCustomSearchAttributesFromDynamicConfigToClusterMetadata(s.so.config, s.so.persistenceServiceResolver, s.logger, dc)

	err = initSystemNamespaces(&s.so.config.Persistence, s.so.config.ClusterMetadata.CurrentClusterName, s.so.persistenceServiceResolver, s.logger)
	if err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	// todo: Replace this with Client or Scope implementation.
	var globalMetricsScope tally.Scope = nil

	s.serverReporter = nil
	s.sdkReporter = nil
	if s.so.config.Global.Metrics != nil {
		s.serverReporter, s.sdkReporter, err = s.so.config.Global.Metrics.InitMetricReporters(s.logger, s.so.metricsReporter)
		if err != nil {
			return err
		}
		globalMetricsScope, err = s.extractTallyScopeForSDK(s.sdkReporter)
		if err != nil {
			return err
		}
	}

	if s.so.tlsConfigProvider == nil {
		s.so.tlsConfigProvider, err = encryption.NewTLSConfigProviderFromConfig(
			s.so.config.Global.TLS, globalMetricsScope, s.logger, nil)
		if err != nil {
			return fmt.Errorf("TLS provider initialization error: %w", err)
		}
	}

	esConfig, esClient, err := s.getESConfigClient(advancedVisibilityWritingMode)
	if err != nil {
		return err
	}

	for _, svcName := range s.so.serviceNames {
		params, err := s.newBootstrapParams(svcName, dc, s.serverReporter, s.sdkReporter, esConfig, esClient)
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

	s.sdkReporter.Stop(s.logger)
	s.serverReporter.Stop(s.logger)
}

// Populates parameters for a service
func (s *Server) newBootstrapParams(
	svcName string,
	dc *dynamicconfig.Collection,
	serverReporter metrics.Reporter,
	sdkReporter metrics.Reporter,
	esConfig *config.Elasticsearch,
	esClient esclient.Client,
) (*resource.BootstrapParams, error) {

	params := &resource.BootstrapParams{
		Name:                  svcName,
		Logger:                s.logger,
		PersistenceConfig:     s.so.config.Persistence,
		DynamicConfigClient:   s.so.dynamicConfigClient,
		ClusterMetadataConfig: s.so.config.ClusterMetadata,
		DCRedirectionPolicy:   s.so.config.DCRedirectionPolicy,
		ESConfig:              esConfig,
		ESClient:              esClient,
	}

	svcCfg := s.so.config.Services[svcName]
	rpcFactory := rpc.NewFactory(&svcCfg.RPC, svcName, s.logger, s.so.tlsConfigProvider)
	params.RPCFactory = rpcFactory

	// Ringpop uses a different port to register handlers, this map is needed to resolve
	// services to correct addresses used by clients through ServiceResolver lookup API
	servicePortMap := make(map[string]int)
	for sn, sc := range s.so.config.Services {
		servicePortMap[sn] = sc.RPC.GRPCPort
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

	// todo: Replace this hack with actually using sdkReporter, Client or Scope.
	if serverReporter == nil {
		var err error
		serverReporter, sdkReporter, err = svcCfg.Metrics.InitMetricReporters(s.logger, nil)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to initialize per-service metric client. "+
					"This is deprecated behavior used as fallback, please use global metric config. Error: %w", err)
		}
		params.ServerMetricsReporter = serverReporter
		params.SDKMetricsReporter = sdkReporter
	}

	globalTallyScope, err := s.extractTallyScopeForSDK(sdkReporter)
	if err != nil {
		return nil, err
	}
	params.MetricsScope = globalTallyScope

	serviceIdx := metrics.GetMetricsServiceIdx(svcName, s.logger)
	metricsClient, err := serverReporter.NewClient(s.logger, serviceIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize metrics client: %w", err)
	}

	params.MetricsClient = metricsClient

	options, err := s.so.tlsConfigProvider.GetFrontendClientConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load frontend TLS configuration: %w", err)
	}

	params.SdkClient, err = sdkclient.NewClient(sdkclient.Options{
		HostPort:     s.so.config.PublicClient.HostPort,
		Namespace:    common.SystemLocalNamespace,
		MetricsScope: globalTallyScope,
		Logger:       log.NewSdkLogger(s.logger),
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
	params.AudienceGetter = s.so.audienceGetter

	params.PersistenceServiceResolver = s.so.persistenceServiceResolver

	return params, nil
}

func (s *Server) getESConfigClient(advancedVisibilityWritingMode string) (*config.Elasticsearch, esclient.Client, error) {
	if advancedVisibilityWritingMode == common.AdvancedVisibilityWritingModeOff {
		return nil, nil, nil
	}

	advancedVisibilityStore, ok := s.so.config.Persistence.DataStores[s.so.config.Persistence.AdvancedVisibilityStore]
	if !ok {
		return nil, nil, fmt.Errorf("unable to find advanced visibility store in config for %q key", s.so.config.Persistence.AdvancedVisibilityStore)
	}

	indexName := advancedVisibilityStore.ElasticSearch.GetVisibilityIndex()
	if len(indexName) == 0 {
		return nil, nil, errors.New("visibility index in missing in Elasticsearch config")
	}

	if s.so.elasticseachHttpClient == nil {
		var err error
		s.so.elasticseachHttpClient, err = esclient.NewAwsHttpClient(advancedVisibilityStore.ElasticSearch.AWSRequestSigning)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
		}
	}

	esClient, err := esclient.NewClient(advancedVisibilityStore.ElasticSearch, s.so.elasticseachHttpClient, s.logger)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create Elasticsearch client: %w", err)
	}

	return advancedVisibilityStore.ElasticSearch, esClient, nil
}

func verifyPersistenceCompatibleVersion(config config.Persistence, persistenceServiceResolver resolver.ServiceResolver, checkVisibility bool) error {
	// cassandra schema version validation
	if err := cassandra.VerifyCompatibleVersion(config, persistenceServiceResolver, checkVisibility); err != nil {
		return fmt.Errorf("cassandra schema version compatibility check failed: %w", err)
	}
	// sql schema version validation
	if err := sql.VerifyCompatibleVersion(config, persistenceServiceResolver, checkVisibility); err != nil {
		return fmt.Errorf("sql schema version compatibility check failed: %w", err)
	}
	return nil
}

// TODO: remove this func after 1.10 release
func copyCustomSearchAttributesFromDynamicConfigToClusterMetadata(
	cfg *config.Config,
	persistenceServiceResolver resolver.ServiceResolver,
	logger log.Logger,
	dc *dynamicconfig.Collection,
) {

	var visibilityIndex string
	if cfg.Persistence.IsAdvancedVisibilityConfigExist() {
		advancedVisibilityDataStore, ok := cfg.Persistence.DataStores[cfg.Persistence.AdvancedVisibilityStore]
		if ok {
			visibilityIndex = advancedVisibilityDataStore.ElasticSearch.GetVisibilityIndex()
		}
	}

	if visibilityIndex == "" {
		logger.Debug("Advanced visibility Elasticsearch index is not configured. Search attributes migration will use empty string as index name.")
	}

	defaultTypeMap := map[string]interface{}{}

	dcSearchAttributes, err := searchattribute.BuildTypeMap(dc.GetMapProperty(dynamicconfig.ValidSearchAttributes, defaultTypeMap))
	if err != nil {
		logger.Error("Unable to read search attributes from dynamic config. Search attributes migration is cancelled.", tag.Error(err))
		return
	}
	dcCustomSearchAttributes := searchattribute.FilterCustomOnly(dcSearchAttributes)
	if len(dcCustomSearchAttributes) == 0 {
		logger.Debug("Custom search attributes are not defined in dynamic config. Search attributes migration is cancelled.", tag.Error(err))
		return
	}

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
		logger.Error("Unable to initialize cluster metadata manager. Search attributes migration is cancelled.", tag.Error(err))
		return
	}
	defer clusterMetadataManager.Close()

	saManager := persistence.NewSearchAttributesManager(clock.NewRealTimeSource(), clusterMetadataManager)

	existingSearchAttributes, err := saManager.GetSearchAttributes(visibilityIndex, true)
	if err != nil {
		logger.Error("Unable to read current search attributes from cluster metadata. Search attributes migration is cancelled.", tag.Error(err), tag.ESIndex(visibilityIndex))
		return
	}

	if len(existingSearchAttributes.Custom()) != 0 {
		logger.Debug("Search attributes already exist in cluster metadata. Search attributes migration is cancelled.", tag.Error(err))
		return
	}

	err = saManager.SaveSearchAttributes(visibilityIndex, dcCustomSearchAttributes)
	if err != nil {
		logger.Error("Unable to save search attributes to cluster metadata. Search attributes migration is cancelled.", tag.Error(err), tag.ESIndex(visibilityIndex))
		return
	}

	logger.Info("Search attributes are successfully saved from dynamic config to cluster metadata.", tag.Value(dcCustomSearchAttributes), tag.ESIndex(visibilityIndex))
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

func (s *Server) extractTallyScopeForSDK(sdkReporter metrics.Reporter) (tally.Scope, error) {
	if sdkTallyReporter, ok := sdkReporter.(*metrics.TallyReporter); ok {
		return sdkTallyReporter.GetScope(), nil
	} else {
		return nil, fmt.Errorf(
			"Sdk reporter is not of Tally type. Unfortunately, SDK only supports Tally for now. "+
				"Please specify prometheusSDK in metrics config with framework type %s.", metrics.FrameworkTally,
		)
	}
}
