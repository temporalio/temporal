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

package executions

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/scanner/executor"
)

const (
	executorPoolSize         = 4
	executorPollInterval     = time.Minute
	executorMaxDeferredTasks = 10000
)

type (
	// Scavenger is the type that holds the state for executions scavenger daemon
	Scavenger struct {
		status           int32
		numHistoryShards int32

		executionManager persistence.ExecutionManager
		executor         executor.Executor
		rateLimiter      quotas.RateLimiter
		metrics          metrics.Client
		logger           log.Logger

		stopC  chan struct{}
		stopWG sync.WaitGroup
	}
)

// NewScavenger returns an instance of executions scavenger daemon
// The Scavenger can be started by calling the Start() method on the
// returned object. Calling the Start() method will result in one
// complete iteration over all of the open workflow executions in the system. For
// each executions, will attempt to validate the workflow execution and emit metrics/logs on validation failures.
//
// The scavenger will retry on all persistence errors infinitely and will only stop under
// two conditions
//  - either all executions are processed successfully (or)
//  - Stop() method is called to stop the scavenger
func NewScavenger(
	numHistoryShards int32,
	executionManager persistence.ExecutionManager,
	metricsClient metrics.Client,
	logger log.Logger,
) *Scavenger {
	return &Scavenger{
		numHistoryShards: numHistoryShards,
		executionManager: executionManager,
		executor: executor.NewFixedSizePoolExecutor(
			executorPoolSize,
			executorMaxDeferredTasks,
			metricsClient,
			metrics.ExecutionsScavengerScope,
		),
		rateLimiter: quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(rateOverall) },
		),
		metrics: metricsClient,
		logger:  logger,

		stopC: make(chan struct{}),
	}
}

// Start starts the scavenger
func (s *Scavenger) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	s.logger.Info("Executions scavenger starting")
	s.stopWG.Add(1)
	s.executor.Start()
	go s.run()
	s.metrics.IncCounter(metrics.ExecutionsScavengerScope, metrics.StartedCount)
	s.logger.Info("Executions scavenger started")
}

// Stop stops the scavenger
func (s *Scavenger) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	s.metrics.IncCounter(metrics.ExecutionsScavengerScope, metrics.StoppedCount)
	s.logger.Info("Executions scavenger stopping")
	close(s.stopC)
	s.executor.Stop()
	s.stopWG.Wait()
	s.logger.Info("Executions scavenger stopped")
}

// Alive returns true if the scavenger is still running
func (s *Scavenger) Alive() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStarted
}

// run does a single run over all executions and validates them
func (s *Scavenger) run() {
	defer func() {
		go s.Stop()
		s.stopWG.Done()
	}()

	for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
		submitted := s.executor.Submit(newTask(
			shardID,
			s.executionManager,
			s.metrics,
			s.logger,
			s,
			quotas.NewMultiRateLimiter([]quotas.RateLimiter{
				quotas.NewDefaultOutgoingRateLimiter(
					func() float64 { return float64(ratePerShard) },
				),
				s.rateLimiter,
			}),
		))
		if !submitted {
			s.logger.Error("unable to submit task to executor", tag.ShardID(shardID))
		}
	}

	s.awaitExecutor()
}

func (s *Scavenger) awaitExecutor() {
	// gauge value persists, so we want to reset it to 0
	defer s.metrics.UpdateGauge(metrics.ExecutionsScavengerScope, metrics.ExecutionsOutstandingCount, float64(0))

	outstanding := s.executor.TaskCount()
	for outstanding > 0 {
		select {
		case <-time.After(executorPollInterval):
			outstanding = s.executor.TaskCount()
			s.metrics.UpdateGauge(metrics.ExecutionsScavengerScope, metrics.ExecutionsOutstandingCount, float64(outstanding))
		case <-s.stopC:
			return
		}
	}
}
