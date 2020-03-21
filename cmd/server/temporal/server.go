// Copyright (c) 2017 Uber Technologies, Inc.
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
	"log"
	"time"

	sdkclient "go.temporal.io/temporal/client"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/authorization"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/elasticsearch"
	l "github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/config/ringpop"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/service/frontend"
	"github.com/temporalio/temporal/service/history"
	"github.com/temporalio/temporal/service/matching"
	"github.com/temporalio/temporal/service/worker"
)

type (
	server struct {
		name   string
		cfg    *config.Config
		doneC  chan struct{}
		daemon common.Daemon
	}
)

// newServer returns a new instance of a daemon
// that represents a cadence service
func newServer(service string, cfg *config.Config) common.Daemon {
	return &server{
		cfg:   cfg,
		name:  service,
		doneC: make(chan struct{}),
	}
}

// Start starts the server
func (s *server) Start() {
	if _, ok := s.cfg.Services[s.name]; !ok {
		log.Fatalf("`%v` service missing config", s)
	}
	s.daemon = s.startService()
}

// Stop stops the server
func (s *server) Stop() {

	if s.daemon == nil {
		return
	}

	select {
	case <-s.doneC:
	default:
		s.daemon.Stop()
		select {
		case <-s.doneC:
		case <-time.After(time.Minute):
			log.Printf("timed out waiting for server %v to exit\n", s.name)
		}
	}
}

// startService starts a service with the given name and config
func (s *server) startService() common.Daemon {
	var err error

	params := resource.BootstrapParams{}
	params.Name = s.name
	params.Logger = loggerimpl.NewLogger(s.cfg.Log.NewZapLogger())
	params.PersistenceConfig = s.cfg.Persistence

	params.DynamicConfig, err = dynamicconfig.NewFileBasedClient(&s.cfg.DynamicConfigClient, params.Logger.WithTags(tag.Service(params.Name)), s.doneC)
	if err != nil {
		log.Printf("error creating file based dynamic config client, use no-op config client instead. error: %v", err)
		params.DynamicConfig = dynamicconfig.NewNopClient()
	}
	dc := dynamicconfig.NewCollection(params.DynamicConfig, params.Logger)

	svcCfg := s.cfg.Services[s.name]
	params.MetricScope = svcCfg.Metrics.NewScope(params.Logger)
	params.RPCFactory = svcCfg.RPC.NewFactory(params.Name, params.Logger)

	// Ringpop uses a different port to register handlers, this map is needed to resolve
	// services to correct addresses used by clients through ServiceResolver lookup API
	servicePortMap := make(map[string]int)
	for roleName, svcCfg := range s.cfg.Services {
		serviceName := roleName
		servicePortMap[serviceName] = svcCfg.RPC.GRPCPort
	}

	params.MembershipFactoryInitializer =
		func(persistenceBean persistenceClient.Bean, logger l.Logger) (resource.MembershipMonitorFactory, error) {
			return ringpop.NewRingpopFactory(
				&s.cfg.Server.Ringpop,
				params.RPCFactory.GetRingpopChannel(),
				params.Name,
				servicePortMap,
				logger,
				persistenceBean.GetClusterMetadataManager(),
			)
		}

	params.DCRedirectionPolicy = s.cfg.DCRedirectionPolicy

	params.MetricsClient = metrics.NewClient(params.MetricScope, metrics.GetMetricsServiceIdx(params.Name, params.Logger))

	clusterMetadata := s.cfg.ClusterMetadata

	// This call performs a config check against the configured persistence store for immutable cluster metadata.
	// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
	// This is to keep this check hidden from independent downstream daemons and keep this in a single place.
	immutableClusterMetadataInitialization(params.Logger, dc, &params.PersistenceConfig, &params.AbstractDatastoreFactory, &params.MetricsClient, clusterMetadata)

	params.ClusterMetadata = cluster.NewMetadata(
		params.Logger,
		dc.GetBoolProperty(dynamicconfig.EnableGlobalDomain, clusterMetadata.EnableGlobalDomain),
		clusterMetadata.FailoverVersionIncrement,
		clusterMetadata.MasterClusterName,
		clusterMetadata.CurrentClusterName,
		clusterMetadata.ClusterInformation,
		clusterMetadata.ReplicationConsumer,
	)

	if s.cfg.PublicClient.HostPort == "" {
		log.Fatalf("need to provide an endpoint config for PublicClient")
	} else {
		var err error
		params.PublicClient, err = sdkclient.NewClient(sdkclient.Options{
			HostPort:     s.cfg.PublicClient.HostPort,
			DomainName:   common.SystemLocalDomainName,
			MetricsScope: params.MetricScope,
		})
		if err != nil {
			log.Fatalf("failed to create public client: %v", err)
		}
	}

	advancedVisMode := dc.GetStringProperty(
		dynamicconfig.AdvancedVisibilityWritingMode,
		common.GetDefaultAdvancedVisibilityWritingMode(params.PersistenceConfig.IsAdvancedVisibilityConfigExist()),
	)()
	isAdvancedVisEnabled := advancedVisMode != common.AdvancedVisibilityWritingModeOff
	if params.ClusterMetadata.IsGlobalDomainEnabled() {
		params.MessagingClient = messaging.NewKafkaClient(&s.cfg.Kafka, params.MetricsClient, zap.NewNop(), params.Logger, params.MetricScope, true, isAdvancedVisEnabled)
	} else if isAdvancedVisEnabled {
		params.MessagingClient = messaging.NewKafkaClient(&s.cfg.Kafka, params.MetricsClient, zap.NewNop(), params.Logger, params.MetricScope, false, isAdvancedVisEnabled)
	} else {
		params.MessagingClient = nil
	}

	if isAdvancedVisEnabled {
		// verify config of advanced visibility store
		advancedVisStoreKey := s.cfg.Persistence.AdvancedVisibilityStore
		advancedVisStore, ok := s.cfg.Persistence.DataStores[advancedVisStoreKey]
		if !ok {
			log.Fatalf("not able to find advanced visibility store in config: %v", advancedVisStoreKey)
		}

		params.ESConfig = advancedVisStore.ElasticSearch
		esClient, err := elasticsearch.NewClient(params.ESConfig)
		if err != nil {
			log.Fatalf("error creating elastic search client: %v", err)
		}
		params.ESClient = esClient

		// verify index name
		indexName, ok := params.ESConfig.Indices[common.VisibilityAppName]
		if !ok || len(indexName) == 0 {
			log.Fatalf("elastic search config missing visibility index")
		}
	}

	params.ArchivalMetadata = archiver.NewArchivalMetadata(
		dc,
		s.cfg.Archival.History.Status,
		s.cfg.Archival.History.EnableRead,
		s.cfg.Archival.Visibility.Status,
		s.cfg.Archival.Visibility.EnableRead,
		&s.cfg.DomainDefaults.Archival,
	)

	params.ArchiverProvider = provider.NewArchiverProvider(s.cfg.Archival.History.Provider, s.cfg.Archival.Visibility.Provider)

	params.PersistenceConfig.TransactionSizeLimit = dc.GetIntProperty(dynamicconfig.TransactionSizeLimit, common.DefaultTransactionSizeLimit)

	params.Authorizer = authorization.NewNopAuthorizer()

	params.Logger.Info("Starting service " + s.name)

	var daemon common.Daemon

	switch s.name {
	case primitives.FrontendService:
		daemon, err = frontend.NewService(&params)
	case primitives.HistoryService:
		daemon, err = history.NewService(&params)
	case primitives.MatchingService:
		daemon, err = matching.NewService(&params)
	case primitives.WorkerService:
		daemon, err = worker.NewService(&params)
	}
	if err != nil {
		params.Logger.Fatal("Fail to start "+s.name+" service ", tag.Error(err))
	}

	go execute(daemon, s.doneC)

	return daemon
}

func immutableClusterMetadataInitialization(
	logger l.Logger,
	dc *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
	abstractDatastoreFactory *persistenceClient.AbstractDataStoreFactory,
	metricsClient *metrics.Client,
	clusterMetadata *config.ClusterMetadata) {

	logger = logger.WithTags(tag.ComponentMetadataInitializer)
	clusterMetadataManager, err := persistenceClient.NewFactory(
		persistenceConfig,
		dc.GetIntProperty(dynamicconfig.HistoryPersistenceMaxQPS, 3000),
		*abstractDatastoreFactory,
		clusterMetadata.CurrentClusterName,
		*metricsClient,
		logger,
	).NewClusterMetadataManager()

	if err != nil {
		log.Fatalf("Error initializing cluster metadata manager: %v", err)
	}

	defer clusterMetadataManager.Close()

	resp, err := clusterMetadataManager.InitializeImmutableClusterMetadata(
		&persistence.InitializeImmutableClusterMetadataRequest{
			ImmutableClusterMetadata: persistenceblobs.ImmutableClusterMetadata{
				HistoryShardCount: int32(persistenceConfig.NumHistoryShards),
				ClusterName:       clusterMetadata.CurrentClusterName,
			}})

	if err != nil {
		log.Fatalf("Error while fetching or persisting immutable cluster metadata: %v", err)
	}

	if resp.RequestApplied {
		logger.Info("Successfully applied immutable cluster metadata.")
	} else {
		if clusterMetadata.CurrentClusterName != resp.PersistedImmutableData.ClusterName {
			logImmutableMismatch(logger,
				"ClusterMetadata.CurrentClusterName",
				clusterMetadata.CurrentClusterName,
				resp.PersistedImmutableData.ClusterName)

			clusterMetadata.CurrentClusterName = resp.PersistedImmutableData.ClusterName
		}

		var persistedShardCount = int(resp.PersistedImmutableData.HistoryShardCount)
		if persistenceConfig.NumHistoryShards != persistedShardCount {
			logImmutableMismatch(logger,
				"Persistence.NumHistoryShards",
				persistenceConfig.NumHistoryShards,
				persistedShardCount)

			persistenceConfig.NumHistoryShards = persistedShardCount
		}
	}
}

func logImmutableMismatch(l l.Logger, key string, ignored interface{}, value interface{}) {
	l.Error(
		"Supplied configuration key/value mismatches persisted ImmutableClusterMetadata."+
			"Continuing with the persisted value as this value cannot be changed once initialized.",
		tag.Key(key),
		tag.IgnoredValue(ignored),
		tag.Value(value))
}

// execute runs the daemon in a separate go routine
func execute(d common.Daemon, doneC chan struct{}) {
	d.Start()
	close(doneC)
}
