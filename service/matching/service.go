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
)

// Config represents configuration for cadence-matching service
type Config struct {
	EnableSyncMatch bool
	// Time to hold a poll request before returning an empty response if there are no tasks
	LongPollExpirationInterval time.Duration

	// taskListManager configuration
	RangeSize                  int64
	GetTasksBatchSize          int
	UpdateAckInterval          time.Duration
	MinTaskThrottlingBurstSize int

	// taskWriter configuration
	OutstandingTaskAppendsThreshold int
	MaxTaskBatchSize                int
}

// NewConfig returns new service config with default values
func NewConfig() *Config {
	return &Config{
		EnableSyncMatch:                 true,
		LongPollExpirationInterval:      time.Minute,
		RangeSize:                       100000,
		GetTasksBatchSize:               1000,
		UpdateAckInterval:               10 * time.Second,
		OutstandingTaskAppendsThreshold: 250,
		MaxTaskBatchSize:                100,
		MinTaskThrottlingBurstSize:      10000,
	}
}

// Service represents the cadence-matching service
type Service struct {
	stopC  chan struct{}
	params *service.BootstrapParams
	config *Config
}

// NewService builds a new cadence-matching service
func NewService(params *service.BootstrapParams, config *Config) common.Daemon {
	return &Service{
		params: params,
		config: config,
		stopC:  make(chan struct{}),
	}
}

// Start starts the service
func (s *Service) Start() {

	var p = s.params
	var log = p.Logger

	log.Infof("%v starting", common.MatchingServiceName)

	base := service.New(p)

	taskPersistence, err := persistence.NewCassandraTaskPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		base.GetLogger())

	if err != nil {
		log.Fatalf("failed to create task persistence: %v", err)
	}

	taskPersistence = persistence.NewTaskPersistenceClient(taskPersistence, base.GetMetricsClient())

	handler := NewHandler(base, s.config, taskPersistence)
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
