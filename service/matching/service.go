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

package matching

import (
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-matching service
type Config struct {
	PersistenceMaxQPS dynamicconfig.FloatPropertyFn
	EnableSyncMatch   dynamicconfig.BoolPropertyFnWithTaskListInfoFilters
	RPS               dynamicconfig.IntPropertyFn

	// taskListManager configuration
	RangeSize                 int64
	GetTasksBatchSize         dynamicconfig.IntPropertyFnWithTaskListInfoFilters
	UpdateAckInterval         dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
	IdleTasklistCheckInterval dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
	MaxTasklistIdleTime       dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
	// Time to hold a poll request before returning an empty response if there are no tasks
	LongPollExpirationInterval dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
	MinTaskThrottlingBurstSize dynamicconfig.IntPropertyFnWithTaskListInfoFilters

	// taskWriter configuration
	OutstandingTaskAppendsThreshold dynamicconfig.IntPropertyFnWithTaskListInfoFilters
	MaxTaskBatchSize                dynamicconfig.IntPropertyFnWithTaskListInfoFilters
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		PersistenceMaxQPS:               dc.GetFloat64Property(dynamicconfig.MatchingPersistenceMaxQPS, 3000),
		EnableSyncMatch:                 dc.GetBoolPropertyFilteredByTaskListInfo(dynamicconfig.MatchingEnableSyncMatch, true),
		RPS:                             dc.GetIntProperty(dynamicconfig.MatchingRPS, 1200),
		RangeSize:                       100000,
		GetTasksBatchSize:               dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingGetTasksBatchSize, 1000),
		UpdateAckInterval:               dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingUpdateAckInterval, 1*time.Minute),
		IdleTasklistCheckInterval:       dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingIdleTasklistCheckInterval, 5*time.Minute),
		MaxTasklistIdleTime:             dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MaxTasklistIdleTime, 5*time.Minute),
		LongPollExpirationInterval:      dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingLongPollExpirationInterval, time.Minute),
		MinTaskThrottlingBurstSize:      dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMinTaskThrottlingBurstSize, 1),
		OutstandingTaskAppendsThreshold: dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingOutstandingTaskAppendsThreshold, 250),
		MaxTaskBatchSize:                dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMaxTaskBatchSize, 100),
	}
}

// Service represents the cadence-matching service
type Service struct {
	stopC  chan struct{}
	params *service.BootstrapParams
	config *Config
}

// NewService builds a new cadence-matching service
func NewService(params *service.BootstrapParams) common.Daemon {
	return &Service{
		params: params,
		config: NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger)),
		stopC:  make(chan struct{}),
	}
}

// Start starts the service
func (s *Service) Start() {

	var p = s.params
	var log = p.Logger

	log.Infof("%v starting", common.MatchingServiceName)

	base := service.New(p)

	persistenceMaxQPS := int(s.config.PersistenceMaxQPS())
	persistenceRateLimiter := common.NewTokenBucket(persistenceMaxQPS, common.NewRealTimeSource())

	taskPersistence, err := persistence.NewCassandraTaskPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		log)
	if err != nil {
		log.Fatalf("failed to create task persistence: %v", err)
	}
	taskPersistence = persistence.NewTaskPersistenceRateLimitedClient(taskPersistence, persistenceRateLimiter, log)
	taskPersistence = persistence.NewTaskPersistenceMetricsClient(taskPersistence, base.GetMetricsClient(), log)

	metadata, err := persistence.NewMetadataManagerProxy(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		p.ClusterMetadata.GetCurrentClusterName(),
		log)

	if err != nil {
		log.Fatalf("failed to create metadata manager: %v", err)
	}
	metadata = persistence.NewMetadataPersistenceRateLimitedClient(metadata, persistenceRateLimiter, log)
	metadata = persistence.NewMetadataPersistenceMetricsClient(metadata, base.GetMetricsClient(), log)

	handler := NewHandler(base, s.config, taskPersistence, metadata)
	handler.Start()

	log.Infof("%v started", common.MatchingServiceName)
	<-s.stopC
	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Infof("%v stopped", common.MatchingServiceName)
}
