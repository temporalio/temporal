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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/scanner/executor"
)

type (
	// Scavenger is the type that holds the state for executions scavenger daemon
	Scavenger struct {
		frontendClient        frontend.Client // used to query visibility
		historyDB             persistence.HistoryManager
		numHistoryShards      int32
		executor              executor.Executor
		metrics               metrics.Client
		logger                log.Logger
		status                int32
		stopC                 chan struct{}
		stopWG                sync.WaitGroup
		shardValidatorFactory shardValidatorBuilder
	}

	shardValidatorBuilder func(rateLimiter quotas.RateLimiter) shardValidator

	// executorTask is a runnable task that adheres to the executor.Task interface
	// for the scavenger, each of this task processes a single workflow mutableState
	executorTask struct {
		shardID     int32
		scvg        *Scavenger
		rateLimiter quotas.RateLimiter
	}

	shardValidationReportExecutionsScanned struct {
		TotalExecutionsCount     int64
		CorruptedExecutionsCount int64
	}
)

// executionValidatorResultNoCorruption is and instance of result symbolizing no failure found
var executionValidatorResultNoCorruption = executionValidatorResult{isValid: true}

var (
	executorPoolSize         = 16
	executionsPageSize       = 1000
	executorPollInterval     = time.Minute
	executorMaxDeferredTasks = 10000
	targetRPS                = 50
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
	frontendClient frontend.Client,
	historyDB persistence.HistoryManager,
	metricsClient metrics.Client,
	logger log.Logger,
	execMgrFactory persistence.ExecutionManagerFactory,
	numHistoryShards int32,
) *Scavenger {
	stopC := make(chan struct{})
	taskExecutor := executor.NewFixedSizePoolExecutor(
		executorPoolSize, executorMaxDeferredTasks, metricsClient, metrics.ExecutionsScavengerScope)
	shardValidatorBuilder := func(limiter quotas.RateLimiter) shardValidator {
		return createShardValidator(
			execMgrFactory, limiter, executionsPageSize,
			historyDB, []executionValidator{newActivityIDValidator()})
	}
	return &Scavenger{
		frontendClient:        frontendClient,
		historyDB:             historyDB,
		metrics:               metricsClient,
		logger:                logger,
		stopC:                 stopC,
		executor:              taskExecutor,
		numHistoryShards:      numHistoryShards,
		shardValidatorFactory: shardValidatorBuilder,
	}
}

// Start starts the scavenger
func (s *Scavenger) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
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
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
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

	rateLimiter := getRateLimiter(targetRPS)
	if rateLimiter == nil {
		panic(
			fmt.Sprintf(
				"ExecutionsScavenger failed to initialize rate limiter with target qps %d. Aborting scan.",
				targetRPS,
			),
		)
	}

	for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
		if !s.executor.Submit(s.newTask(shardID, rateLimiter)) {
			return
		}
	}

	s.awaitExecutor()
}

func (s *Scavenger) awaitExecutor() {
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

// newTask returns a new instance of an executable task which will process a single mutableState
func (s *Scavenger) newTask(shardID int32, rateLimiter quotas.RateLimiter) executor.Task {
	return &executorTask{
		shardID:     shardID,
		rateLimiter: rateLimiter,
		scvg:        s,
	}
}

// Run runs the task
func (t *executorTask) Run() executor.TaskStatus {
	return t.scvg.validateShardHandler(t.shardID, t.rateLimiter)
}

func (s *Scavenger) validateShardHandler(shardID int32, rateLimiter quotas.RateLimiter) executor.TaskStatus {
	validationResult := s.shardValidatorFactory(rateLimiter).validate(shardID)
	dumpShardValidationReport(validationResult, s.logger, s.metrics)
	return executor.TaskStatusDone
}

func dumpShardValidationReport(report *shardValidationResult, logger log.Logger, metricClient metrics.Client) {
	metricsScope := metricClient.Scope(metrics.ExecutionsScavengerScope)
	metricsScope.AddCounter(metrics.ScavengerDBRequestsCount, report.TotalDBRequests)
	metricsScope.UpdateGauge(
		metrics.ExecutionsScavengerExecutionsCount,
		float64(report.ScanStats.TotalExecutionsCount))
	metricsScope.UpdateGauge(
		metrics.ExecutionsScavengerCorruptedExecutionsCount,
		float64(report.ScanStats.CorruptedExecutionsCount))

	if !report.IsFailure {
		return
	}
	if report.Error != nil {
		failureScope := metricsScope.Tagged(
			metrics.ValidatorTag("scavenger"),
			metrics.FailureTag("validation_failure"),
		)
		failureScope.IncCounter(metrics.ScavengerValidationFailuresCount)
		logger.Warn(
			"Validation failed for execution.",
			tag.ShardID(report.ShardID),
			tag.Error(report.Error))
	}
	for _, executionFailure := range report.ExecutionFailures {
		for validator, validatorFailure := range executionFailure.Failures {
			failureScope := metricsScope.Tagged(
				metrics.ValidatorTag(validator),
				metrics.FailureTag(validatorFailure.failureReasonTag),
			)
			failureScope.IncCounter(metrics.ScavengerValidationFailuresCount)
		}
		logger.Warn(
			"Validation failed for execution.",
			tag.ShardID(report.ShardID),
			tag.WorkflowNamespace(executionFailure.Namespace),
			tag.WorkflowID(executionFailure.WorkflowID),
			tag.WorkflowRunID(executionFailure.RunID),
			tag.WorkflowBranchID(executionFailure.BranchID),
			tag.WorkflowTreeID(executionFailure.TreeID),
			tag.Value(executionFailure.Failures))
	}
}
