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

package history

import (
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	espersistence "github.com/uber/cadence/common/persistence/elasticsearch"
	persistencefactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-history service
type Config struct {
	NumberOfShards int

	EnableNDC                       dynamicconfig.BoolPropertyFnWithDomainFilter
	RPS                             dynamicconfig.IntPropertyFn
	MaxIDLengthLimit                dynamicconfig.IntPropertyFn
	PersistenceMaxQPS               dynamicconfig.IntPropertyFn
	EnableVisibilitySampling        dynamicconfig.BoolPropertyFn
	EnableReadFromClosedExecutionV2 dynamicconfig.BoolPropertyFn
	VisibilityOpenMaxQPS            dynamicconfig.IntPropertyFnWithDomainFilter
	VisibilityClosedMaxQPS          dynamicconfig.IntPropertyFnWithDomainFilter
	AdvancedVisibilityWritingMode   dynamicconfig.StringPropertyFn
	EmitShardDiffLog                dynamicconfig.BoolPropertyFn
	MaxAutoResetPoints              dynamicconfig.IntPropertyFnWithDomainFilter
	ThrottledLogRPS                 dynamicconfig.IntPropertyFn

	// HistoryCache settings
	// Change of these configs require shard restart
	HistoryCacheInitialSize dynamicconfig.IntPropertyFn
	HistoryCacheMaxSize     dynamicconfig.IntPropertyFn
	HistoryCacheTTL         dynamicconfig.DurationPropertyFn

	// EventsCache settings
	// Change of these configs require shard restart
	EventsCacheInitialSize dynamicconfig.IntPropertyFn
	EventsCacheMaxSize     dynamicconfig.IntPropertyFn
	EventsCacheTTL         dynamicconfig.DurationPropertyFn

	// ShardController settings
	RangeSizeBits        uint
	AcquireShardInterval dynamicconfig.DurationPropertyFn

	// the artificial delay added to standby cluster's view of active cluster's time
	StandbyClusterDelay dynamicconfig.DurationPropertyFn

	// TimerQueueProcessor settings
	TimerTaskBatchSize                               dynamicconfig.IntPropertyFn
	TimerTaskWorkerCount                             dynamicconfig.IntPropertyFn
	TimerTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	TimerProcessorGetFailureRetryCount               dynamicconfig.IntPropertyFn
	TimerProcessorCompleteTimerFailureRetryCount     dynamicconfig.IntPropertyFn
	TimerProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	TimerProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TimerProcessorCompleteTimerInterval              dynamicconfig.DurationPropertyFn
	TimerProcessorFailoverMaxPollRPS                 dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	TimerProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	TimerProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	TimerProcessorMaxTimeShift                       dynamicconfig.DurationPropertyFn
	TimerProcessorHistoryArchivalSizeLimit           dynamicconfig.IntPropertyFn
	TimerProcessorArchivalTimeLimit                  dynamicconfig.DurationPropertyFn

	// TransferQueueProcessor settings
	TransferTaskBatchSize                               dynamicconfig.IntPropertyFn
	TransferTaskWorkerCount                             dynamicconfig.IntPropertyFn
	TransferTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	TransferProcessorCompleteTransferFailureRetryCount  dynamicconfig.IntPropertyFn
	TransferProcessorFailoverMaxPollRPS                 dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	TransferProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	TransferProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	TransferProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	TransferProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	TransferProcessorCompleteTransferInterval           dynamicconfig.DurationPropertyFn
	TransferProcessorVisibilityArchivalTimeLimit        dynamicconfig.DurationPropertyFn

	// ReplicatorQueueProcessor settings
	ReplicatorTaskBatchSize                               dynamicconfig.IntPropertyFn
	ReplicatorTaskWorkerCount                             dynamicconfig.IntPropertyFn
	ReplicatorTaskMaxRetryCount                           dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxPollRPS                         dynamicconfig.IntPropertyFn
	ReplicatorProcessorMaxPollInterval                    dynamicconfig.DurationPropertyFn
	ReplicatorProcessorMaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
	ReplicatorProcessorUpdateAckInterval                  dynamicconfig.DurationPropertyFn
	ReplicatorProcessorUpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
	ReplicatorProcessorFetchTasksBatchSize                dynamicconfig.IntPropertyFn

	// Persistence settings
	ExecutionMgrNumConns dynamicconfig.IntPropertyFn
	HistoryMgrNumConns   dynamicconfig.IntPropertyFn

	// System Limits
	MaximumBufferedEventsBatch dynamicconfig.IntPropertyFn
	MaximumSignalsPerExecution dynamicconfig.IntPropertyFnWithDomainFilter

	// ShardUpdateMinInterval the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval dynamicconfig.DurationPropertyFn
	// ShardSyncMinInterval the minimal time interval which the shard info should be sync to remote
	ShardSyncMinInterval dynamicconfig.DurationPropertyFn

	// Time to hold a poll request before returning an empty response
	// right now only used by GetMutableState
	LongPollExpirationInterval dynamicconfig.DurationPropertyFnWithDomainFilter

	// encoding the history events
	EventEncodingType dynamicconfig.StringPropertyFnWithDomainFilter
	// whether or not using ParentClosePolicy
	EnableParentClosePolicy dynamicconfig.BoolPropertyFnWithDomainFilter
	// whether or not enable system workers for processing parent close policy task
	EnableParentClosePolicyWorker dynamicconfig.BoolPropertyFn
	// parent close policy will be processed by sys workers(if enabled) if
	// the number of children greater than or equal to this threshold
	ParentClosePolicyThreshold dynamicconfig.IntPropertyFnWithDomainFilter
	// total number of parentClosePolicy system workflows
	NumParentClosePolicySystemWorkflows dynamicconfig.IntPropertyFn

	// Archival settings
	NumArchiveSystemWorkflows dynamicconfig.IntPropertyFn
	ArchiveRequestRPS         dynamicconfig.IntPropertyFn

	// Size limit related settings
	BlobSizeLimitError     dynamicconfig.IntPropertyFnWithDomainFilter
	BlobSizeLimitWarn      dynamicconfig.IntPropertyFnWithDomainFilter
	HistorySizeLimitError  dynamicconfig.IntPropertyFnWithDomainFilter
	HistorySizeLimitWarn   dynamicconfig.IntPropertyFnWithDomainFilter
	HistoryCountLimitError dynamicconfig.IntPropertyFnWithDomainFilter
	HistoryCountLimitWarn  dynamicconfig.IntPropertyFnWithDomainFilter

	// ValidSearchAttributes is legal indexed keys that can be used in list APIs
	ValidSearchAttributes             dynamicconfig.MapPropertyFn
	SearchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithDomainFilter
	SearchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithDomainFilter
	SearchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithDomainFilter

	// Decision settings
	// StickyTTL is to expire a sticky tasklist if no update more than this duration
	// TODO https://github.com/uber/cadence/issues/2357
	StickyTTL dynamicconfig.DurationPropertyFnWithDomainFilter
	// DecisionHeartbeatTimeout is to timeout behavior of: RespondDecisionTaskComplete with ForceCreateNewDecisionTask == true without any decisions
	// So that decision will be scheduled to another worker(by clear stickyness)
	DecisionHeartbeatTimeout dynamicconfig.DurationPropertyFnWithDomainFilter
	// MaxDecisionStartToCloseSeconds is the StartToCloseSeconds for decision
	MaxDecisionStartToCloseSeconds dynamicconfig.IntPropertyFnWithDomainFilter
}

const (
	defaultHistoryMaxAutoResetPoints = 20
)

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numberOfShards int, storeType string, isAdvancedVisConfigExist bool) *Config {
	cfg := &Config{
		NumberOfShards:                               numberOfShards,
		EnableNDC:                                    dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableNDC, false),
		RPS:                                          dc.GetIntProperty(dynamicconfig.HistoryRPS, 3000),
		MaxIDLengthLimit:                             dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		PersistenceMaxQPS:                            dc.GetIntProperty(dynamicconfig.HistoryPersistenceMaxQPS, 9000),
		EnableVisibilitySampling:                     dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		EnableReadFromClosedExecutionV2:              dc.GetBoolProperty(dynamicconfig.EnableReadFromClosedExecutionV2, false),
		VisibilityOpenMaxQPS:                         dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistoryVisibilityOpenMaxQPS, 300),
		VisibilityClosedMaxQPS:                       dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistoryVisibilityClosedMaxQPS, 300),
		MaxAutoResetPoints:                           dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistoryMaxAutoResetPoints, defaultHistoryMaxAutoResetPoints),
		MaxDecisionStartToCloseSeconds:               dc.GetIntPropertyFilteredByDomain(dynamicconfig.MaxDecisionStartToCloseSeconds, 240),
		AdvancedVisibilityWritingMode:                dc.GetStringProperty(dynamicconfig.AdvancedVisibilityWritingMode, common.GetDefaultAdvancedVisibilityWritingMode(isAdvancedVisConfigExist)),
		EmitShardDiffLog:                             dc.GetBoolProperty(dynamicconfig.EmitShardDiffLog, false),
		HistoryCacheInitialSize:                      dc.GetIntProperty(dynamicconfig.HistoryCacheInitialSize, 128),
		HistoryCacheMaxSize:                          dc.GetIntProperty(dynamicconfig.HistoryCacheMaxSize, 512),
		HistoryCacheTTL:                              dc.GetDurationProperty(dynamicconfig.HistoryCacheTTL, time.Hour),
		EventsCacheInitialSize:                       dc.GetIntProperty(dynamicconfig.EventsCacheInitialSize, 128),
		EventsCacheMaxSize:                           dc.GetIntProperty(dynamicconfig.EventsCacheMaxSize, 512),
		EventsCacheTTL:                               dc.GetDurationProperty(dynamicconfig.EventsCacheTTL, time.Hour),
		RangeSizeBits:                                20, // 20 bits for sequencer, 2^20 sequence number for any range
		AcquireShardInterval:                         dc.GetDurationProperty(dynamicconfig.AcquireShardInterval, time.Minute),
		StandbyClusterDelay:                          dc.GetDurationProperty(dynamicconfig.AcquireShardInterval, 5*time.Minute),
		TimerTaskBatchSize:                           dc.GetIntProperty(dynamicconfig.TimerTaskBatchSize, 100),
		TimerTaskWorkerCount:                         dc.GetIntProperty(dynamicconfig.TimerTaskWorkerCount, 10),
		TimerTaskMaxRetryCount:                       dc.GetIntProperty(dynamicconfig.TimerTaskMaxRetryCount, 100),
		TimerProcessorGetFailureRetryCount:           dc.GetIntProperty(dynamicconfig.TimerProcessorGetFailureRetryCount, 5),
		TimerProcessorCompleteTimerFailureRetryCount: dc.GetIntProperty(dynamicconfig.TimerProcessorCompleteTimerFailureRetryCount, 10),
		TimerProcessorUpdateAckInterval:              dc.GetDurationProperty(dynamicconfig.TimerProcessorUpdateAckInterval, 30*time.Second),
		TimerProcessorUpdateAckIntervalJitterCoefficient:      dc.GetFloat64Property(dynamicconfig.TimerProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TimerProcessorCompleteTimerInterval:                   dc.GetDurationProperty(dynamicconfig.TimerProcessorCompleteTimerInterval, 60*time.Second),
		TimerProcessorFailoverMaxPollRPS:                      dc.GetIntProperty(dynamicconfig.TimerProcessorFailoverMaxPollRPS, 1),
		TimerProcessorMaxPollRPS:                              dc.GetIntProperty(dynamicconfig.TimerProcessorMaxPollRPS, 20),
		TimerProcessorMaxPollInterval:                         dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxPollInterval, 5*time.Minute),
		TimerProcessorMaxPollIntervalJitterCoefficient:        dc.GetFloat64Property(dynamicconfig.TimerProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TimerProcessorMaxTimeShift:                            dc.GetDurationProperty(dynamicconfig.TimerProcessorMaxTimeShift, 1*time.Second),
		TimerProcessorHistoryArchivalSizeLimit:                dc.GetIntProperty(dynamicconfig.TimerProcessorHistoryArchivalSizeLimit, 500*1024),
		TimerProcessorArchivalTimeLimit:                       dc.GetDurationProperty(dynamicconfig.TimerProcessorArchivalTimeLimit, 1*time.Second),
		TransferTaskBatchSize:                                 dc.GetIntProperty(dynamicconfig.TransferTaskBatchSize, 100),
		TransferProcessorFailoverMaxPollRPS:                   dc.GetIntProperty(dynamicconfig.TransferProcessorFailoverMaxPollRPS, 1),
		TransferProcessorMaxPollRPS:                           dc.GetIntProperty(dynamicconfig.TransferProcessorMaxPollRPS, 20),
		TransferTaskWorkerCount:                               dc.GetIntProperty(dynamicconfig.TransferTaskWorkerCount, 10),
		TransferTaskMaxRetryCount:                             dc.GetIntProperty(dynamicconfig.TransferTaskMaxRetryCount, 100),
		TransferProcessorCompleteTransferFailureRetryCount:    dc.GetIntProperty(dynamicconfig.TransferProcessorCompleteTransferFailureRetryCount, 10),
		TransferProcessorMaxPollInterval:                      dc.GetDurationProperty(dynamicconfig.TransferProcessorMaxPollInterval, 1*time.Minute),
		TransferProcessorMaxPollIntervalJitterCoefficient:     dc.GetFloat64Property(dynamicconfig.TransferProcessorMaxPollIntervalJitterCoefficient, 0.15),
		TransferProcessorUpdateAckInterval:                    dc.GetDurationProperty(dynamicconfig.TransferProcessorUpdateAckInterval, 30*time.Second),
		TransferProcessorUpdateAckIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.TransferProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		TransferProcessorCompleteTransferInterval:             dc.GetDurationProperty(dynamicconfig.TransferProcessorCompleteTransferInterval, 60*time.Second),
		TransferProcessorVisibilityArchivalTimeLimit:          dc.GetDurationProperty(dynamicconfig.TransferProcessorVisibilityArchivalTimeLimit, 1*time.Second),
		ReplicatorTaskBatchSize:                               dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 100),
		ReplicatorTaskWorkerCount:                             dc.GetIntProperty(dynamicconfig.ReplicatorTaskWorkerCount, 10),
		ReplicatorTaskMaxRetryCount:                           dc.GetIntProperty(dynamicconfig.ReplicatorTaskMaxRetryCount, 100),
		ReplicatorProcessorMaxPollRPS:                         dc.GetIntProperty(dynamicconfig.ReplicatorProcessorMaxPollRPS, 20),
		ReplicatorProcessorMaxPollInterval:                    dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorMaxPollInterval, 1*time.Minute),
		ReplicatorProcessorMaxPollIntervalJitterCoefficient:   dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorMaxPollIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorUpdateAckInterval:                  dc.GetDurationProperty(dynamicconfig.ReplicatorProcessorUpdateAckInterval, 5*time.Second),
		ReplicatorProcessorUpdateAckIntervalJitterCoefficient: dc.GetFloat64Property(dynamicconfig.ReplicatorProcessorUpdateAckIntervalJitterCoefficient, 0.15),
		ReplicatorProcessorFetchTasksBatchSize:                dc.GetIntProperty(dynamicconfig.ReplicatorTaskBatchSize, 25),
		ExecutionMgrNumConns:                                  dc.GetIntProperty(dynamicconfig.ExecutionMgrNumConns, 50),
		HistoryMgrNumConns:                                    dc.GetIntProperty(dynamicconfig.HistoryMgrNumConns, 50),
		MaximumBufferedEventsBatch:                            dc.GetIntProperty(dynamicconfig.MaximumBufferedEventsBatch, 100),
		MaximumSignalsPerExecution:                            dc.GetIntPropertyFilteredByDomain(dynamicconfig.MaximumSignalsPerExecution, 0),
		ShardUpdateMinInterval:                                dc.GetDurationProperty(dynamicconfig.ShardUpdateMinInterval, 5*time.Minute),
		ShardSyncMinInterval:                                  dc.GetDurationProperty(dynamicconfig.ShardSyncMinInterval, 5*time.Minute),

		// history client: client/history/client.go set the client timeout 30s
		LongPollExpirationInterval:          dc.GetDurationPropertyFilteredByDomain(dynamicconfig.HistoryLongPollExpirationInterval, time.Second*20),
		EventEncodingType:                   dc.GetStringPropertyFnWithDomainFilter(dynamicconfig.DefaultEventEncoding, string(common.EncodingTypeThriftRW)),
		EnableParentClosePolicy:             dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableParentClosePolicy, true),
		NumParentClosePolicySystemWorkflows: dc.GetIntProperty(dynamicconfig.NumParentClosePolicySystemWorkflows, 10),
		EnableParentClosePolicyWorker:       dc.GetBoolProperty(dynamicconfig.EnableParentClosePolicyWorker, true),
		ParentClosePolicyThreshold:          dc.GetIntPropertyFilteredByDomain(dynamicconfig.ParentClosePolicyThreshold, 10),

		NumArchiveSystemWorkflows: dc.GetIntProperty(dynamicconfig.NumArchiveSystemWorkflows, 1000),
		ArchiveRequestRPS:         dc.GetIntProperty(dynamicconfig.ArchiveRequestRPS, 300), // should be much smaller than frontend RPS

		BlobSizeLimitError:     dc.GetIntPropertyFilteredByDomain(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:      dc.GetIntPropertyFilteredByDomain(dynamicconfig.BlobSizeLimitWarn, 512*1024),
		HistorySizeLimitError:  dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistorySizeLimitError, 200*1024*1024),
		HistorySizeLimitWarn:   dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistorySizeLimitWarn, 50*1024*1024),
		HistoryCountLimitError: dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistoryCountLimitError, 200*1024),
		HistoryCountLimitWarn:  dc.GetIntPropertyFilteredByDomain(dynamicconfig.HistoryCountLimitWarn, 50*1024),

		ThrottledLogRPS: dc.GetIntProperty(dynamicconfig.HistoryThrottledLogRPS, 4),

		ValidSearchAttributes:             dc.GetMapProperty(dynamicconfig.ValidSearchAttributes, definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit: dc.GetIntPropertyFilteredByDomain(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:  dc.GetIntPropertyFilteredByDomain(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:    dc.GetIntPropertyFilteredByDomain(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		StickyTTL:                         dc.GetDurationPropertyFilteredByDomain(dynamicconfig.StickyTTL, time.Hour*24*365),
		DecisionHeartbeatTimeout:          dc.GetDurationPropertyFilteredByDomain(dynamicconfig.DecisionHeartbeatTimeout, time.Minute*30),
	}

	return cfg
}

// GetShardID return the corresponding shard ID for a given workflow ID
func (config *Config) GetShardID(workflowID string) int {
	return common.WorkflowIDToHistoryShard(workflowID, config.NumberOfShards)
}

// Service represents the cadence-history service
type Service struct {
	stopC         chan struct{}
	params        *service.BootstrapParams
	config        *Config
	metricsClient metrics.Client
}

// NewService builds a new cadence-history service
func NewService(params *service.BootstrapParams) common.Daemon {
	config := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger),
		params.PersistenceConfig.NumHistoryShards,
		params.PersistenceConfig.DefaultStoreType(),
		params.PersistenceConfig.IsAdvancedVisibilityConfigExist())
	params.ThrottledLogger = loggerimpl.NewThrottledLogger(params.Logger, config.ThrottledLogRPS)
	params.UpdateLoggerWithServiceName(common.HistoryServiceName)
	return &Service{
		params: params,
		stopC:  make(chan struct{}),
		config: config,
	}
}

// Start starts the service
func (s *Service) Start() {

	var params = s.params
	var log = params.Logger

	log.Info("elastic search config", tag.ESConfig(params.ESConfig))
	log.Info("starting", tag.Service(common.HistoryServiceName))

	base := service.New(params)

	s.metricsClient = base.GetMetricsClient()

	pConfig := params.PersistenceConfig
	pConfig.HistoryMaxConns = s.config.HistoryMgrNumConns()
	pConfig.SetMaxQPS(pConfig.DefaultStore, s.config.PersistenceMaxQPS())
	pConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityOpenMaxQPS:            s.config.VisibilityOpenMaxQPS,
		VisibilityClosedMaxQPS:          s.config.VisibilityClosedMaxQPS,
		EnableSampling:                  s.config.EnableVisibilitySampling,
		EnableReadFromClosedExecutionV2: s.config.EnableReadFromClosedExecutionV2,
	}
	pFactory := persistencefactory.New(&pConfig, params.ClusterMetadata.GetCurrentClusterName(), s.metricsClient, log)

	shardMgr, err := pFactory.NewShardManager()
	if err != nil {
		log.Fatal("failed to create shard manager", tag.Error(err))
	}

	metadata, err := pFactory.NewMetadataManager()
	if err != nil {
		log.Fatal("failed to create metadata manager", tag.Error(err))
	}

	visibility, err := pFactory.NewVisibilityManager()
	if err != nil {
		log.Fatal("failed to create visibility manager", tag.Error(err))
	}

	var esVisibility persistence.VisibilityManager
	if params.ESConfig != nil {
		visibilityProducer, err := s.params.MessagingClient.NewProducer(common.VisibilityAppName)
		if err != nil {
			log.Fatal("Creating visibility producer failed", tag.Error(err))
		}
		esVisibility = espersistence.NewESVisibilityManager("", nil, nil, visibilityProducer,
			s.metricsClient, log)
	}
	visibility = persistence.NewVisibilityManagerWrapper(
		visibility,
		esVisibility,
		dynamicconfig.GetBoolPropertyFnFilteredByDomain(false), // history visibility never read
		s.config.AdvancedVisibilityWritingMode,
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
	visibilityArchvierBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          base.GetLogger(),
		MetricsClient:   base.GetMetricsClient(),
		ClusterMetadata: base.GetClusterMetadata(),
		DomainCache:     domainCache,
	}
	err = params.ArchiverProvider.RegisterBootstrapContainer(common.HistoryServiceName, historyArchiverBootstrapContainer, visibilityArchvierBootstrapContainer)
	if err != nil {
		log.Fatal("Failed to register archiver bootstrap container", tag.Error(err))
	}

	handler := NewHandler(base, s.config, shardMgr, metadata, visibility, historyV2, pFactory, domainCache, params.PublicClient)
	handler.RegisterHandler()

	// must start base service first
	base.Start()
	err = handler.Start()
	if err != nil {
		log.Fatal("History handler failed to start", tag.Error(err))
	}

	log.Info("started", tag.Service(common.HistoryServiceName))

	<-s.stopC
	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Info("stopped", tag.Service(common.HistoryServiceName))
}
