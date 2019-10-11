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

package frontend

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	espersistence "github.com/uber/cadence/common/persistence/elasticsearch"
	persistencefactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-frontend service
type Config struct {
	NumHistoryShards                int
	PersistenceMaxQPS               dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize           dynamicconfig.IntPropertyFnWithDomainFilter
	EnableVisibilitySampling        dynamicconfig.BoolPropertyFn
	EnableReadFromClosedExecutionV2 dynamicconfig.BoolPropertyFn
	VisibilityListMaxQPS            dynamicconfig.IntPropertyFnWithDomainFilter
	EnableReadVisibilityFromES      dynamicconfig.BoolPropertyFnWithDomainFilter
	ESVisibilityListMaxQPS          dynamicconfig.IntPropertyFnWithDomainFilter
	ESIndexMaxResultWindow          dynamicconfig.IntPropertyFn
	HistoryMaxPageSize              dynamicconfig.IntPropertyFnWithDomainFilter
	RPS                             dynamicconfig.IntPropertyFn
	DomainRPS                       dynamicconfig.IntPropertyFnWithDomainFilter
	MaxIDLengthLimit                dynamicconfig.IntPropertyFn
	EnableClientVersionCheck        dynamicconfig.BoolPropertyFn
	MinRetentionDays                dynamicconfig.IntPropertyFn

	// Persistence settings
	HistoryMgrNumConns dynamicconfig.IntPropertyFn

	MaxBadBinaries dynamicconfig.IntPropertyFnWithDomainFilter

	// security protection settings
	EnableAdminProtection         dynamicconfig.BoolPropertyFn
	AdminOperationToken           dynamicconfig.StringPropertyFn
	DisableListVisibilityByFilter dynamicconfig.BoolPropertyFnWithDomainFilter

	// size limit system protection
	BlobSizeLimitError dynamicconfig.IntPropertyFnWithDomainFilter
	BlobSizeLimitWarn  dynamicconfig.IntPropertyFnWithDomainFilter

	ThrottledLogRPS dynamicconfig.IntPropertyFn

	// Domain specific config
	EnableDomainNotActiveAutoForwarding dynamicconfig.BoolPropertyFnWithDomainFilter

	// ValidSearchAttributes is legal indexed keys that can be used in list APIs
	ValidSearchAttributes             dynamicconfig.MapPropertyFn
	SearchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithDomainFilter
	SearchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithDomainFilter
	SearchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithDomainFilter
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numHistoryShards int, enableReadFromES bool) *Config {
	return &Config{
		NumHistoryShards:                    numHistoryShards,
		PersistenceMaxQPS:                   dc.GetIntProperty(dynamicconfig.FrontendPersistenceMaxQPS, 2000),
		VisibilityMaxPageSize:               dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		EnableVisibilitySampling:            dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		EnableReadFromClosedExecutionV2:     dc.GetBoolProperty(dynamicconfig.EnableReadFromClosedExecutionV2, false),
		VisibilityListMaxQPS:                dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendVisibilityListMaxQPS, 1),
		EnableReadVisibilityFromES:          dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableReadVisibilityFromES, enableReadFromES),
		ESVisibilityListMaxQPS:              dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendESVisibilityListMaxQPS, 3),
		ESIndexMaxResultWindow:              dc.GetIntProperty(dynamicconfig.FrontendESIndexMaxResultWindow, 10000),
		HistoryMaxPageSize:                  dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                                 dc.GetIntProperty(dynamicconfig.FrontendRPS, 1200),
		DomainRPS:                           dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendDomainRPS, 1200),
		MaxIDLengthLimit:                    dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		HistoryMgrNumConns:                  dc.GetIntProperty(dynamicconfig.FrontendHistoryMgrNumConns, 10),
		MaxBadBinaries:                      dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendMaxBadBinaries, domain.MaxBadBinaries),
		EnableAdminProtection:               dc.GetBoolProperty(dynamicconfig.EnableAdminProtection, false),
		AdminOperationToken:                 dc.GetStringProperty(dynamicconfig.AdminOperationToken, common.DefaultAdminOperationToken),
		DisableListVisibilityByFilter:       dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.DisableListVisibilityByFilter, false),
		BlobSizeLimitError:                  dc.GetIntPropertyFilteredByDomain(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:                   dc.GetIntPropertyFilteredByDomain(dynamicconfig.BlobSizeLimitWarn, 256*1024),
		ThrottledLogRPS:                     dc.GetIntProperty(dynamicconfig.FrontendThrottledLogRPS, 20),
		EnableDomainNotActiveAutoForwarding: dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableDomainNotActiveAutoForwarding, false),
		EnableClientVersionCheck:            dc.GetBoolProperty(dynamicconfig.EnableClientVersionCheck, false),
		ValidSearchAttributes:               dc.GetMapProperty(dynamicconfig.ValidSearchAttributes, definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit:   dc.GetIntPropertyFilteredByDomain(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:    dc.GetIntPropertyFilteredByDomain(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:      dc.GetIntPropertyFilteredByDomain(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		MinRetentionDays:                    dc.GetIntProperty(dynamicconfig.MinRetentionDays, domain.MinRetentionDays),
	}
}

// Service represents the cadence-frontend service
type Service struct {
	stopC  chan struct{}
	config *Config
	params *service.BootstrapParams
}

// NewService builds a new cadence-frontend service
func NewService(params *service.BootstrapParams) common.Daemon {
	isAdvancedVisExistInConfig := len(params.PersistenceConfig.AdvancedVisibilityStore) != 0
	config := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger), params.PersistenceConfig.NumHistoryShards, isAdvancedVisExistInConfig)
	params.ThrottledLogger = loggerimpl.NewThrottledLogger(params.Logger, config.ThrottledLogRPS)
	params.UpdateLoggerWithServiceName(common.FrontendServiceName)
	return &Service{
		params: params,
		config: config,
		stopC:  make(chan struct{}),
	}
}

// Start starts the service
func (s *Service) Start() {

	var params = s.params
	var log = params.Logger

	log.Info("starting", tag.Service(common.FrontendServiceName))

	base := service.New(params)

	pConfig := params.PersistenceConfig
	pConfig.HistoryMaxConns = s.config.HistoryMgrNumConns()
	pConfig.SetMaxQPS(pConfig.DefaultStore, s.config.PersistenceMaxQPS())
	pConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityListMaxQPS:            s.config.VisibilityListMaxQPS,
		EnableSampling:                  s.config.EnableVisibilitySampling,
		EnableReadFromClosedExecutionV2: s.config.EnableReadFromClosedExecutionV2,
	}
	pFactory := persistencefactory.New(&pConfig, params.ClusterMetadata.GetCurrentClusterName(), base.GetMetricsClient(), log)

	metadata, err := pFactory.NewMetadataManager()
	if err != nil {
		log.Fatal("failed to create metadata manager", tag.Error(err))
	}

	visibilityFromDB, err := pFactory.NewVisibilityManager()
	if err != nil {
		log.Fatal("failed to create visibility manager", tag.Error(err))
	}

	var visibilityFromES persistence.VisibilityManager
	if params.ESConfig != nil {
		visibilityIndexName := params.ESConfig.Indices[common.VisibilityAppName]
		visibilityConfigForES := &config.VisibilityConfig{
			MaxQPS:                 s.config.PersistenceMaxQPS,
			VisibilityListMaxQPS:   s.config.ESVisibilityListMaxQPS,
			ESIndexMaxResultWindow: s.config.ESIndexMaxResultWindow,
			ValidSearchAttributes:  s.config.ValidSearchAttributes,
		}
		visibilityFromES = espersistence.NewESVisibilityManager(visibilityIndexName, params.ESClient, visibilityConfigForES,
			nil, base.GetMetricsClient(), log)
	}
	visibility := persistence.NewVisibilityManagerWrapper(
		visibilityFromDB,
		visibilityFromES,
		s.config.EnableReadVisibilityFromES,
		dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeOff), // frontend visibility never write
	)

	historyV2, err := pFactory.NewHistoryV2Manager()
	if err != nil {
		log.Fatal("Creating historyV2 manager persistence failed", tag.Error(err))
	}

	domainCache := cache.NewDomainCache(metadata, base.GetClusterMetadata(), base.GetMetricsClient(), base.GetLogger())

	historyArchiverBootstrapContainer := &archiver.HistoryBootstrapContainer{
		HistoryV2Manager: historyV2,
		Logger:           base.GetLogger(),
		MetricsClient:    base.GetMetricsClient(),
		ClusterMetadata:  base.GetClusterMetadata(),
		DomainCache:      domainCache,
	}
	visibilityArchiverBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          base.GetLogger(),
		MetricsClient:   base.GetMetricsClient(),
		ClusterMetadata: base.GetClusterMetadata(),
		DomainCache:     domainCache,
	}
	err = params.ArchiverProvider.RegisterBootstrapContainer(common.FrontendServiceName, historyArchiverBootstrapContainer, visibilityArchiverBootstrapContainer)
	if err != nil {
		log.Fatal("Failed to register archiver bootstrap container", tag.Error(err))
	}

	var replicationMessageSink messaging.Producer
	var domainReplicationQueue persistence.DomainReplicationQueue
	clusterMetadata := base.GetClusterMetadata()
	if clusterMetadata.IsGlobalDomainEnabled() {
		consumerConfig := clusterMetadata.GetReplicationConsumerConfig()
		if consumerConfig != nil && consumerConfig.Type == config.ReplicationConsumerTypeRPC {
			domainReplicationQueue, err = pFactory.NewDomainReplicationQueue()
			if err != nil {
				log.Fatal("Failed to create domain replication queue", tag.Error(err))
			}
			replicationMessageSink = domainReplicationQueue
		} else {
			replicationMessageSink, err = base.GetMessagingClient().NewProducerWithClusterName(
				base.GetClusterMetadata().GetCurrentClusterName())
			if err != nil {
				log.Fatal("Creating replicationMessageSink producer failed", tag.Error(err))
			}
		}
	} else {
		replicationMessageSink = &mocks.KafkaProducer{}
	}

	wfHandler := NewWorkflowHandler(
		base,
		s.config,
		metadata,
		historyV2,
		visibility,
		replicationMessageSink,
		domainReplicationQueue,
		domainCache)
	dcRedirectionHandler := NewDCRedirectionHandler(wfHandler, params.DCRedirectionPolicy)
	dcRedirectionHandler.RegisterHandler()

	adminHandler := NewAdminHandler(base, pConfig.NumHistoryShards, domainCache, historyV2, s.params)
	adminHandler.RegisterHandler()

	// must start base service first
	base.Start()
	err = dcRedirectionHandler.Start()
	if err != nil {
		log.Fatal("DC redirection handler failed to start", tag.Error(err))
	}
	err = adminHandler.Start()
	if err != nil {
		log.Fatal("Admin handler failed to start", tag.Error(err))
	}

	// base (service is not started in frontend or admin handler) in case of race condition in yarpc registration function

	log.Info("started", tag.Service(common.FrontendServiceName))

	<-s.stopC

	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Info("stopped", tag.Service(common.FrontendServiceName))
}
