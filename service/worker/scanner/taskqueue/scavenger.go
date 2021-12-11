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

package taskqueue

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/scanner/executor"
)

type (
	// Scavenger is the type that holds the state for task queue scavenger daemon
	Scavenger struct {
		db       p.TaskManager
		executor executor.Executor
		metrics  metrics.Client
		logger   log.Logger
		stats    stats
		status   int32
		stopC    chan struct{}
		stopWG   sync.WaitGroup
	}

	taskQueueState struct {
		rangeID     int64
		lastUpdated *time.Time
	}

	stats struct {
		taskqueue struct {
			nProcessed int64
			nDeleted   int64
		}
		task struct {
			nProcessed int64
			nDeleted   int64
		}
	}

	// executorTask is a runnable task that adheres to the executor.Task interface
	// for the scavenger, each of this task processes a single task queue
	executorTask struct {
		p.TaskQueueKey
		taskQueueState
		scvg *Scavenger
	}
)

var (
	taskQueueBatchSize       = 32             // maximum number of task queue we process concurrently
	taskBatchSize            = 16             // number of tasks we read from persistence in one call
	maxTasksPerJob           = 256            // maximum number of tasks we process for a executorTask queue as part of a single job
	taskQueueGracePeriod     = 48 * time.Hour // amount of time a executorTask queue has to be idle before it becomes a candidate for deletion
	executorPollInterval     = time.Minute
	executorMaxDeferredTasks = 10000
)

// NewScavenger returns an instance of executorTask queue scavenger daemon
// The Scavenger can be started by calling the Start() method on the
// returned object. Calling the Start() method will result in one
// complete iteration over all of the task queues in the system. For
// each task queue, the scavenger will attempt
//  - deletion of expired tasks in the task queues
//  - deletion of task queue itself, if there are no tasks and the task queue hasn't been updated for a grace period
//
// The scavenger will retry on all persistence errors infinitely and will only stop under
// two conditions
//  - either all task queues are processed successfully (or)
//  - Stop() method is called to stop the scavenger
func NewScavenger(db p.TaskManager, metricsClient metrics.Client, logger log.Logger) *Scavenger {
	stopC := make(chan struct{})
	taskExecutor := executor.NewFixedSizePoolExecutor(
		taskQueueBatchSize, executorMaxDeferredTasks, metricsClient, metrics.TaskQueueScavengerScope)
	return &Scavenger{
		db:       db,
		metrics:  metricsClient,
		logger:   logger,
		stopC:    stopC,
		executor: taskExecutor,
	}
}

// Start starts the scavenger
func (s *Scavenger) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	s.logger.Info("Taskqueue scavenger starting")
	s.stopWG.Add(1)
	s.executor.Start()
	go s.run()
	s.metrics.IncCounter(metrics.TaskQueueScavengerScope, metrics.StartedCount)
	s.logger.Info("Taskqueue scavenger started")
}

// Stop stops the scavenger
func (s *Scavenger) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	s.metrics.IncCounter(metrics.TaskQueueScavengerScope, metrics.StoppedCount)
	s.logger.Info("Taskqueue scavenger stopping")
	close(s.stopC)
	s.executor.Stop()
	s.stopWG.Wait()
	s.logger.Info("Taskqueue scavenger stopped")
}

// Alive returns true if the scavenger is still running
func (s *Scavenger) Alive() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStarted
}

// run does a single run over all executorTask queues
func (s *Scavenger) run() {
	defer func() {
		s.emitStats()
		go s.Stop()
		s.stopWG.Done()
	}()

	var pageToken []byte
	for {
		resp, err := s.listTaskQueue(taskQueueBatchSize, pageToken)
		if err != nil {
			s.logger.Error("listTaskQueue error", tag.Error(err))
			return
		}

		for _, item := range resp.Items {
			atomic.AddInt64(&s.stats.taskqueue.nProcessed, 1)
			if !s.executor.Submit(s.newTask(item)) {
				return
			}
		}

		pageToken = resp.NextPageToken
		if pageToken == nil {
			break
		}
	}

	s.awaitExecutor()
}

// process is a callback function that gets invoked from within the executor.Run() method
func (s *Scavenger) process(key *p.TaskQueueKey, state *taskQueueState) executor.TaskStatus {
	return s.deleteHandler(key, state)
}

func (s *Scavenger) awaitExecutor() {
	outstanding := s.executor.TaskCount()
	for outstanding > 0 {
		select {
		case <-time.After(executorPollInterval):
			outstanding = s.executor.TaskCount()
			s.metrics.UpdateGauge(metrics.TaskQueueScavengerScope, metrics.TaskQueueOutstandingCount, float64(outstanding))
		case <-s.stopC:
			return
		}
	}
}

func (s *Scavenger) emitStats() {
	s.metrics.UpdateGauge(metrics.TaskQueueScavengerScope, metrics.TaskProcessedCount, float64(s.stats.task.nProcessed))
	s.metrics.UpdateGauge(metrics.TaskQueueScavengerScope, metrics.TaskDeletedCount, float64(s.stats.task.nDeleted))
	s.metrics.UpdateGauge(metrics.TaskQueueScavengerScope, metrics.TaskQueueProcessedCount, float64(s.stats.taskqueue.nProcessed))
	s.metrics.UpdateGauge(metrics.TaskQueueScavengerScope, metrics.TaskQueueDeletedCount, float64(s.stats.taskqueue.nDeleted))
}

// newTask returns a new instance of an executable task which will process a single task queue
func (s *Scavenger) newTask(info *p.PersistedTaskQueueInfo) executor.Task {
	return &executorTask{
		TaskQueueKey: p.TaskQueueKey{
			NamespaceID:   info.Data.GetNamespaceId(),
			TaskQueueName: info.Data.Name,
			TaskQueueType: info.Data.TaskType,
		},
		taskQueueState: taskQueueState{
			rangeID:     info.RangeID,
			lastUpdated: info.Data.LastUpdateTime,
		},
		scvg: s,
	}
}

// Run runs the task
func (t *executorTask) Run() executor.TaskStatus {
	return t.scvg.process(&t.TaskQueueKey, &t.taskQueueState)
}
