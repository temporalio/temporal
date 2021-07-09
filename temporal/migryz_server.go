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
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/pprof"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/searchattribute"
)

const (
	mismatchLogMessage = "Supplied configuration key/value mismatches persisted cluster metadata. Continuing with the persisted value as this value cannot be changed once initialized."
)

type (
	// Server is temporal server.
	Server struct {
		// so                *serverOptions
		dc                         *dynamicconfig.Collection
		dynamicConfigClient        dynamicconfig.Client
		config                     *config.Config
		serviceNames               []string
		services                   map[string]common.Daemon
		serviceStoppedChs          map[string]chan struct{}
		stoppedCh                  chan interface{}
		logger                     log.Logger
		namespaceLogger            log.Logger
		serverReporter             metrics.Reporter
		sdkReporter                metrics.Reporter
		persistenceServiceResolver resolver.ServiceResolver
		customDataStoreFactory     persistenceClient.AbstractDataStoreFactory
		interruptCh                <-chan interface{}
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
func NewServer(
	logger log.Logger,
	cfg *config.Config,
	requiredServices []string, // todomigryz: might use specific type name here, or wrap in provider
	services map[string]common.Daemon,
	persistenceServiceResolver resolver.ServiceResolver,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
	dynamicConfigClient dynamicconfig.Client,
	dc *dynamicconfig.Collection,
	interruptCh <-chan interface{},
) (*Server, error) {
	err := cfg.Validate(requiredServices)
	if err != nil {
		return nil, err
	}

	s := &Server{
		logger:                     logger,
		config:                     cfg,
		serviceNames:               requiredServices,
		services:                   services,
		serviceStoppedChs:          make(map[string]chan struct{}),
		persistenceServiceResolver: persistenceServiceResolver,
		customDataStoreFactory:     customDataStoreFactory,
		dc:                         dc,
		dynamicConfigClient:        dynamicConfigClient,
		interruptCh:                interruptCh,
	}

	return s, nil
}

// todomigryz: decompose this in revers order. Figure out top level items we need first.
// Start temporal server.
func (s *Server) Start() error {

	// todomigryz: move declaration closer to the usage
	var err error

	s.stoppedCh = make(chan interface{})

	s.logger.Info("Starting server for services", tag.Value(s.serviceNames))
	s.logger.Debug(s.config.String())

	// todomigryz: potentially can be moved to DI as well
	if err = pprof.NewInitializer(&s.config.Global.PProf, s.logger).Start(); err != nil {
		return fmt.Errorf("unable to start PProf: %w", err)
	}

	err = ringpop.ValidateRingpopConfig(&s.config.Global.Membership)
	if err != nil {
		return fmt.Errorf("ringpop config validation error: %w", err)
	}

	// todomigryz: this code updates config, this should not be happening.
	err = updateClusterMetadataConfig(s.config, s.persistenceServiceResolver, s.logger, s.customDataStoreFactory)
	if err != nil {
		return fmt.Errorf("unable to initialize cluster metadata: %w", err)
	}

	// TODO: remove this call after 1.10 release
	copyCustomSearchAttributesFromDynamicConfigToClusterMetadata(
		s.config,
		s.persistenceServiceResolver,
		s.logger,
		s.dc,
		s.customDataStoreFactory,
	)

	err = initSystemNamespaces(
		&s.config.Persistence,
		s.config.ClusterMetadata.CurrentClusterName,
		s.persistenceServiceResolver,
		s.logger,
		s.customDataStoreFactory,
	)
	if err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	// todomigryz: metrics might still be required somewhere in server code

	for svcName, svc := range s.services {
		s.serviceStoppedChs[svcName] = make(chan struct{})

		go func(svc common.Daemon, svcStoppedCh chan<- struct{}) {
			// Start is blocked until Stop() is called.
			svc.Start()
			close(svcStoppedCh)
		}(svc, s.serviceStoppedChs[svcName])
	}

	// todomigryz: this is not set yet. Add to DI
	if s.interruptCh != nil {
		// If s.so.interruptCh is nil this will wait forever.
		interruptSignal := <-s.interruptCh
		s.logger.Info("Received interrupt signal, stopping the server.", tag.Value(interruptSignal))
		s.Stop()
	}

	return nil
}

// Stop stops the server.
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

	if s.sdkReporter != nil {
		s.sdkReporter.Stop(s.logger)
	}

	if s.serverReporter != nil {
		s.serverReporter.Stop(s.logger)
	}
}

func verifyPersistenceCompatibleVersion(
	config config.Persistence,
	persistenceServiceResolver resolver.ServiceResolver,
	checkVisibility bool,
) error {
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
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
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

	dcSearchAttributes, err := searchattribute.BuildTypeMap(
		dc.GetMapProperty(
			dynamicconfig.ValidSearchAttributes,
			defaultTypeMap,
		),
	)
	if err != nil {
		logger.Error(
			"Unable to read search attributes from dynamic config. Search attributes migration is cancelled.",
			tag.Error(err),
		)
		return
	}
	dcCustomSearchAttributes := searchattribute.FilterCustomOnly(dcSearchAttributes)
	if len(dcCustomSearchAttributes) == 0 {
		logger.Debug(
			"Custom search attributes are not defined in dynamic config. Search attributes migration is cancelled.",
			tag.Error(err),
		)
		return
	}

	factory := persistenceClient.NewFactory(
		&cfg.Persistence,
		persistenceServiceResolver,
		nil,
		customDataStoreFactory,
		cfg.ClusterMetadata.CurrentClusterName,
		nil,
		logger,
	)

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		logger.Error(
			"Unable to initialize cluster metadata manager. Search attributes migration is cancelled.",
			tag.Error(err),
		)
		return
	}
	defer clusterMetadataManager.Close()

	saManager := persistence.NewSearchAttributesManager(clock.NewRealTimeSource(), clusterMetadataManager)

	existingSearchAttributes, err := saManager.GetSearchAttributes(visibilityIndex, true)
	if err != nil {
		logger.Error(
			"Unable to read current search attributes from cluster metadata. Search attributes migration is cancelled.",
			tag.Error(err),
			tag.ESIndex(visibilityIndex),
		)
		return
	}

	if len(existingSearchAttributes.Custom()) != 0 {
		logger.Debug(
			"Search attributes already exist in cluster metadata. Search attributes migration is cancelled.",
			tag.Error(err),
		)
		return
	}

	err = saManager.SaveSearchAttributes(visibilityIndex, dcCustomSearchAttributes)
	if err != nil {
		logger.Error(
			"Unable to save search attributes to cluster metadata. Search attributes migration is cancelled.",
			tag.Error(err),
			tag.ESIndex(visibilityIndex),
		)
		return
	}

	logger.Info(
		"Search attributes are successfully saved from dynamic config to cluster metadata.",
		tag.Value(dcCustomSearchAttributes),
		tag.ESIndex(visibilityIndex),
	)
}

// updateClusterMetadataConfig performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from downstream calls.
func updateClusterMetadataConfig(
	cfg *config.Config,
	persistenceServiceResolver resolver.ServiceResolver,
	logger log.Logger,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
) error {
	logger = log.With(logger, tag.ComponentMetadataInitializer)

	factory := persistenceClient.NewFactory(
		&cfg.Persistence,
		persistenceServiceResolver,
		nil,
		customDataStoreFactory,
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
			},
		},
	)
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
			tag.Value(resp.ClusterName),
		)
		cfg.ClusterMetadata.CurrentClusterName = resp.ClusterName
	}

	var persistedShardCount = resp.HistoryShardCount
	if cfg.Persistence.NumHistoryShards != persistedShardCount {
		logger.Error(
			mismatchLogMessage,
			tag.Key("persistence.numHistoryShards"),
			tag.IgnoredValue(cfg.Persistence.NumHistoryShards),
			tag.Value(persistedShardCount),
		)
		cfg.Persistence.NumHistoryShards = persistedShardCount
	}

	return nil
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
