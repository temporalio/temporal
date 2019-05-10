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
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	persistencefactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-matching service
type Config struct {
	PersistenceMaxQPS dynamicconfig.IntPropertyFn
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
	MaxTaskDeleteBatchSize     dynamicconfig.IntPropertyFnWithTaskListInfoFilters

	// taskWriter configuration
	OutstandingTaskAppendsThreshold dynamicconfig.IntPropertyFnWithTaskListInfoFilters
	MaxTaskBatchSize                dynamicconfig.IntPropertyFnWithTaskListInfoFilters

	ThrottledLogRPS dynamicconfig.IntPropertyFn
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		PersistenceMaxQPS:               dc.GetIntProperty(dynamicconfig.MatchingPersistenceMaxQPS, 3000),
		EnableSyncMatch:                 dc.GetBoolPropertyFilteredByTaskListInfo(dynamicconfig.MatchingEnableSyncMatch, true),
		RPS:                             dc.GetIntProperty(dynamicconfig.MatchingRPS, 1200),
		RangeSize:                       100000,
		GetTasksBatchSize:               dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingGetTasksBatchSize, 1000),
		UpdateAckInterval:               dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingUpdateAckInterval, 1*time.Minute),
		IdleTasklistCheckInterval:       dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingIdleTasklistCheckInterval, 5*time.Minute),
		MaxTasklistIdleTime:             dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MaxTasklistIdleTime, 5*time.Minute),
		LongPollExpirationInterval:      dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingLongPollExpirationInterval, time.Minute),
		MinTaskThrottlingBurstSize:      dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMinTaskThrottlingBurstSize, 1),
		MaxTaskDeleteBatchSize:          dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMaxTaskDeleteBatchSize, 100),
		OutstandingTaskAppendsThreshold: dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingOutstandingTaskAppendsThreshold, 250),
		MaxTaskBatchSize:                dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMaxTaskBatchSize, 100),
		ThrottledLogRPS:                 dc.GetIntProperty(dynamicconfig.MatchingThrottledLogRPS, 20),
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
	config := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger))
	params.ThrottledLogger = loggerimpl.NewThrottledLogger(params.Logger, config.ThrottledLogRPS)
	params.UpdateLoggerWithServiceName(common.MatchingServiceName)
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

	log.Info("starting", tag.Service(common.MatchingServiceName))

	base := service.New(params)

	pConfig := params.PersistenceConfig
	pConfig.SetMaxQPS(pConfig.DefaultStore, s.config.PersistenceMaxQPS())
	pFactory := persistencefactory.New(&pConfig, params.ClusterMetadata.GetCurrentClusterName(), base.GetMetricsClient(), log)

	taskPersistence, err := pFactory.NewTaskManager()
	if err != nil {
		log.Fatal("failed to create task persistence", tag.Error(err))
	}

	metadata, err := pFactory.NewMetadataManager(persistencefactory.MetadataV1V2)
	if err != nil {
		log.Fatal("failed to create metadata manager", tag.Error(err))
	}

	handler := NewHandler(base, s.config, taskPersistence, metadata)
	handler.RegisterHandler()

	// must start base service first
	base.Start()
	err = handler.Start()
	if err != nil {
		log.Fatal("Matching handler failed to start", tag.Error(err))
	}

	log.Info("started", tag.Service(common.MatchingServiceName))
	<-s.stopC
	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Info("stopped", tag.Service(common.MatchingServiceName))
}
