// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/client/frontend"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executor"
)

type ()

type (
	// Scavenger is the type that holds the state for executions scavenger daemon
	Scavenger struct {
		params ScannerWorkflowParams
		// TODO: currently frontend client does not support scan visibility records without domain filter (need to chat with Bowei to understand if querying without domain filter is an issue)? Maybe we go directly to elasticSearch?
		frontendClient frontend.Client // used to query visibility
		historyDB      p.HistoryManager
		executor       executor.Executor
		metrics        metrics.Client
		logger         log.Logger
		stats          stats
		status         int32
		stopC          chan struct{}
		stopWG         sync.WaitGroup
	}

	// ScannerWorkflowParams are the parameters passed to the executions scanner workflow
	ScannerWorkflowParams struct {
		VisibilityQuery string // optionally can be provided to limit the scope of the scan

		// add other fields here such as: bool generateReport, string outputLocation, bool runInDryMode etc...
	}

	executionKey struct {
		domainID   string
		workflowID string
		runID      string
	}

	stats struct {
		// TODO: include stats here that should be tracked throughout execution of scavenger
	}

	// executorTask is a runnable task that adheres to the executor.Task interface
	// for the scavenger, each of this task processes a single workflow execution
	executorTask struct {
		executionKey
		scvg *Scavenger
	}
)

var (
	executionsBatchSize      = 32   // maximum number of executions we process concurrently
	executionsPageSize       = 1000 // page size of executions read from visibility manager
	executorPollInterval     = time.Minute
	executorMaxDeferredTasks = 10000
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
	params ScannerWorkflowParams,
	frontendClient frontend.Client,
	historyDB p.HistoryManager,
	metricsClient metrics.Client,
	logger log.Logger,
) *Scavenger {
	stopC := make(chan struct{})
	taskExecutor := executor.NewFixedSizePoolExecutor(
		executionsBatchSize, executorMaxDeferredTasks, metricsClient, metrics.ExecutionsScavengerScope)
	return &Scavenger{
		params:         params,
		frontendClient: frontendClient,
		historyDB:      historyDB,
		metrics:        metricsClient,
		logger:         logger,
		stopC:          stopC,
		executor:       taskExecutor,
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
	// TODO: implement this
	// 1. read from visibility records from frontend.Client
	// 2. create executionTasks
	// 3. pass them off to the executor to run
	// 4. wait until the executor is done
	// 5. emit metrics on the run of scavenger
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

func (s *Scavenger) emitStats() {
	// TODO: implement this, this will emit metrics after a full run of executor scavenger is finished
}

// newTask returns a new instance of an executable task which will process a single execution
func (s *Scavenger) newTask(domainID, workflowID, runID string) executor.Task {
	return &executorTask{
		executionKey: executionKey{
			domainID:   domainID,
			workflowID: workflowID,
			runID:      runID,
		},
		scvg: s,
	}
}

// Run runs the task
func (t *executorTask) Run() executor.TaskStatus {
	return t.scvg.validateHandler(&t.executionKey)
}
