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
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-history service
type Config struct {
	NumberOfShards int

	// HistoryCache settings
	HistoryCacheInitialSize int
	HistoryCacheMaxSize     int
	HistoryCacheTTL         time.Duration

	// ShardController settings
	RangeSizeBits        uint
	AcquireShardInterval time.Duration

	// Timeout settings
	DefaultScheduleToStartActivityTimeoutInSecs int32
	DefaultScheduleToCloseActivityTimeoutInSecs int32
	DefaultStartToCloseActivityTimeoutInSecs    int32

	// TimerQueueProcessor settings
	TimerTaskBatchSize                           int
	TimerTaskWorkerCount                         int
	TimerTaskMaxRetryCount                       int
	TimerProcessorGetFailureRetryCount           int
	TimerProcessorCompleteTimerFailureRetryCount int
	TimerProcessorUpdateShardTaskCount           int
	TimerProcessorUpdateAckInterval              time.Duration
	TimerProcessorCompleteTimerInterval          time.Duration
	TimerProcessorMaxPollInterval                time.Duration
	TimerProcessorStandbyTaskDelay               time.Duration

	// TransferQueueProcessor settings
	TransferTaskBatchSize                              int
	TransferProcessorMaxPollRPS                        int
	TransferTaskWorkerCount                            int
	TransferTaskMaxRetryCount                          int
	TransferProcessorCompleteTransferFailureRetryCount int
	TransferProcessorUpdateShardTaskCount              int
	TransferProcessorMaxPollInterval                   time.Duration
	TransferProcessorUpdateAckInterval                 time.Duration
	TransferProcessorCompleteTransferInterval          time.Duration
	TransferProcessorStandbyTaskDelay                  time.Duration

	// ReplicatorQueueProcessor settings
	ReplicatorTaskBatchSize                 int
	ReplicatorTaskWorkerCount               int
	ReplicatorTaskMaxRetryCount             int
	ReplicatorProcessorMaxPollRPS           int
	ReplicatorProcessorUpdateShardTaskCount int
	ReplicatorProcessorMaxPollInterval      time.Duration
	ReplicatorProcessorUpdateAckInterval    time.Duration

	// Persistence settings
	ExecutionMgrNumConns int
	HistoryMgrNumConns   int

	// System Limits
	MaximumBufferedEventsBatch int

	// ShardUpdateMinInterval the minimal time interval which the shard info can be updated
	ShardUpdateMinInterval time.Duration

	// Time to hold a poll request before returning an empty response
	// right now only used by GetMutableState
	LongPollExpirationInterval dynamicconfig.DurationPropertyFn
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numberOfShards int) *Config {
	return &Config{
		NumberOfShards:                                     numberOfShards,
		HistoryCacheInitialSize:                            128,
		HistoryCacheMaxSize:                                512,
		HistoryCacheTTL:                                    time.Hour,
		RangeSizeBits:                                      20, // 20 bits for sequencer, 2^20 sequence number for any range
		AcquireShardInterval:                               time.Minute,
		DefaultScheduleToStartActivityTimeoutInSecs:        10,
		DefaultScheduleToCloseActivityTimeoutInSecs:        10,
		DefaultStartToCloseActivityTimeoutInSecs:           10,
		TimerTaskBatchSize:                                 100,
		TimerTaskWorkerCount:                               30,
		TimerTaskMaxRetryCount:                             5,
		TimerProcessorGetFailureRetryCount:                 5,
		TimerProcessorCompleteTimerFailureRetryCount:       10,
		TimerProcessorUpdateShardTaskCount:                 100,
		TimerProcessorUpdateAckInterval:                    1 * time.Minute,
		TimerProcessorCompleteTimerInterval:                1 * time.Second,
		TimerProcessorMaxPollInterval:                      60 * time.Second,
		TimerProcessorStandbyTaskDelay:                     0 * time.Minute,
		TransferTaskBatchSize:                              10,
		TransferProcessorMaxPollRPS:                        100,
		TransferTaskWorkerCount:                            10,
		TransferTaskMaxRetryCount:                          100,
		TransferProcessorCompleteTransferFailureRetryCount: 10,
		TransferProcessorUpdateShardTaskCount:              100,
		TransferProcessorMaxPollInterval:                   60 * time.Second,
		TransferProcessorUpdateAckInterval:                 1 * time.Minute,
		TransferProcessorCompleteTransferInterval:          1 * time.Second,
		TransferProcessorStandbyTaskDelay:                  0 * time.Minute,
		ReplicatorTaskBatchSize:                            10,
		ReplicatorTaskWorkerCount:                          10,
		ReplicatorTaskMaxRetryCount:                        100,
		ReplicatorProcessorMaxPollRPS:                      100,
		ReplicatorProcessorUpdateShardTaskCount:            100,
		ReplicatorProcessorMaxPollInterval:                 60 * time.Second,
		ReplicatorProcessorUpdateAckInterval:               1 * time.Minute,
		ExecutionMgrNumConns:                               100,
		HistoryMgrNumConns:                                 100,
		MaximumBufferedEventsBatch:                         100,
		ShardUpdateMinInterval:                             60 * time.Second,
		// history client: client/history/client.go set the client timeout 30s
		LongPollExpirationInterval: dc.GetDurationProperty(
			dynamicconfig.HistoryLongPollExpirationInterval, time.Second*20,
		),
	}
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
	return &Service{
		params: params,
		stopC:  make(chan struct{}),
		config: NewConfig(
			dynamicconfig.NewCollection(params.DynamicConfig, params.Logger),
			params.CassandraConfig.NumHistoryShards,
		),
	}
}

// Start starts the service
func (s *Service) Start() {

	var p = s.params
	var log = p.Logger

	log.Infof("%v starting", common.HistoryServiceName)

	base := service.New(p)

	s.metricsClient = base.GetMetricsClient()

	shardMgr, err := persistence.NewCassandraShardPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		p.ClusterMetadata.GetCurrentClusterName(),
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create shard manager: %v", err)
	}
	shardMgr = persistence.NewShardPersistenceClient(shardMgr, base.GetMetricsClient(), log)

	// Hack to create shards for bootstrap purposes
	// TODO: properly pre-create all shards before deployment.
	for shardID := 0; shardID < p.CassandraConfig.NumHistoryShards; shardID++ {
		err := shardMgr.CreateShard(&persistence.CreateShardRequest{
			ShardInfo: &persistence.ShardInfo{
				ShardID:          shardID,
				RangeID:          0,
				TransferAckLevel: 0,
			}})

		if err != nil {
			if _, ok := err.(*persistence.ShardAlreadyExistError); !ok {
				log.Fatalf("failed to create shard for ShardId: %v, with error: %v", shardID, err)
			}
		}
	}

	metadata, err := persistence.NewMetadataManagerProxy(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		p.ClusterMetadata.GetCurrentClusterName(),
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create metadata manager: %v", err)
	}
	metadata = persistence.NewMetadataPersistenceClient(metadata, base.GetMetricsClient(), log)

	visibility, err := persistence.NewCassandraVisibilityPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.VisibilityKeyspace,
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create visiblity manager: %v", err)
	}
	visibility = persistence.NewVisibilityPersistenceClient(visibility, base.GetMetricsClient(), log)

	history, err := persistence.NewCassandraHistoryPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		s.config.HistoryMgrNumConns,
		p.Logger)

	if err != nil {
		log.Fatalf("Creating Cassandra history manager persistence failed: %v", err)
	}
	history = persistence.NewHistoryPersistenceClient(history, base.GetMetricsClient(), log)

	execMgrFactory, err := persistence.NewCassandraPersistenceClientFactory(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		s.config.ExecutionMgrNumConns,
		p.Logger,
		s.metricsClient,
	)
	if err != nil {
		log.Fatalf("Creating Cassandra execution manager persistence factory failed: %v", err)
	}

	handler := NewHandler(base,
		s.config,
		shardMgr,
		metadata,
		visibility,
		history,
		execMgrFactory)

	handler.Start()

	log.Infof("%v started", common.HistoryServiceName)

	<-s.stopC
	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Infof("%v stopped", common.HistoryServiceName)
}
