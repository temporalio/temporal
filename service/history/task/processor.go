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

package task

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

type (
	schedulerOptions struct {
		schedulerType        task.SchedulerType
		fifoSchedulerOptions *task.FIFOTaskSchedulerOptions
		wrrSchedulerOptions  *task.WeightedRoundRobinTaskSchedulerOptions
	}

	processorImpl struct {
		sync.RWMutex

		priorityAssigner PriorityAssigner
		hostScheduler    task.Scheduler
		shardSchedulers  map[shard.Context]task.Scheduler

		status        int32
		options       *schedulerOptions
		shardOptions  *schedulerOptions
		logger        log.Logger
		metricsClient metrics.Client
	}
)

var (
	errTaskProcessorNotRunning = errors.New("queue task processor is not running")
)

// NewProcessor creates a new task processor
func NewProcessor(
	priorityAssigner PriorityAssigner,
	config *config.Config,
	logger log.Logger,
	metricsClient metrics.Client,
) (Processor, error) {
	options, err := newSchedulerOptions(
		config.TaskSchedulerType(),
		config.TaskSchedulerQueueSize(),
		config.TaskSchedulerWorkerCount(),
		config.TaskSchedulerDispatcherCount(),
		config.TaskSchedulerRoundRobinWeights,
	)
	if err != nil {
		return nil, err
	}

	shardWorkerCount := config.TaskSchedulerShardWorkerCount()
	var shardOptions *schedulerOptions
	if shardWorkerCount > 0 {
		shardOptions, err = newSchedulerOptions(
			config.TaskSchedulerType(),
			config.TaskSchedulerShardQueueSize(),
			shardWorkerCount,
			1,
			config.TaskSchedulerRoundRobinWeights,
		)
		if err != nil {
			return nil, err
		}
	}

	scheduler, err := createTaskScheduler(options, logger, metricsClient)
	if err != nil {
		return nil, err
	}

	return &processorImpl{
		priorityAssigner: priorityAssigner,
		hostScheduler:    scheduler,
		shardSchedulers:  make(map[shard.Context]task.Scheduler),
		status:           common.DaemonStatusInitialized,
		options:          options,
		shardOptions:     shardOptions,
		logger:           logger,
		metricsClient:    metricsClient,
	}, nil
}

func (p *processorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.hostScheduler.Start()

	p.logger.Info("Queue task processor started.")
}

func (p *processorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.hostScheduler.Stop()

	p.Lock()
	defer p.Unlock()

	for shard, scheduler := range p.shardSchedulers {
		delete(p.shardSchedulers, shard)
		scheduler.Stop()
	}

	p.logger.Info("Queue task processor stopped.")
}

func (p *processorImpl) StopShardProcessor(
	shard shard.Context,
) {
	p.Lock()
	scheduler, ok := p.shardSchedulers[shard]
	if !ok {
		p.Unlock()
		return
	}

	delete(p.shardSchedulers, shard)
	p.Unlock()

	// don't hold the lock while stopping the scheduler
	scheduler.Stop()
}

func (p *processorImpl) Submit(
	task Task,
) error {
	if err := p.priorityAssigner.Assign(task); err != nil {
		return err
	}

	submitted, err := p.hostScheduler.TrySubmit(task)
	if err != nil {
		return err
	}

	if submitted {
		return nil
	}

	shardScheduler, err := p.getOrCreateShardTaskScheduler(task.GetShard())
	if err != nil {
		return err
	}

	if shardScheduler != nil {
		return shardScheduler.Submit(task)
	}

	// if shard level scheduler is disabled
	return p.hostScheduler.Submit(task)
}

func (p *processorImpl) TrySubmit(
	task Task,
) (bool, error) {
	if err := p.priorityAssigner.Assign(task); err != nil {
		return false, err
	}

	submitted, err := p.hostScheduler.TrySubmit(task)
	if err != nil {
		return false, err
	}

	if submitted {
		return true, nil
	}

	shardScheduler, err := p.getOrCreateShardTaskScheduler(task.GetShard())
	if err != nil {
		return false, err
	}

	if shardScheduler != nil {
		return shardScheduler.TrySubmit(task)
	}

	// if shard level scheduler is disabled
	return false, nil
}

func (p *processorImpl) getOrCreateShardTaskScheduler(
	shard shard.Context,
) (task.Scheduler, error) {
	if p.shardOptions == nil {
		return nil, nil
	}

	p.RLock()
	if scheduler, ok := p.shardSchedulers[shard]; ok {
		p.RUnlock()
		return scheduler, nil
	}
	p.RUnlock()

	p.Lock()
	if scheduler, ok := p.shardSchedulers[shard]; ok {
		p.Unlock()
		return scheduler, nil
	}

	if !p.isRunning() {
		p.Unlock()
		return nil, errTaskProcessorNotRunning
	}

	scheduler, err := createTaskScheduler(p.shardOptions, p.logger, p.metricsClient)
	if err != nil {
		p.Unlock()
		return nil, err
	}

	p.shardSchedulers[shard] = scheduler
	p.Unlock()

	// don't hold the lock while starting the scheduler
	scheduler.Start()
	return scheduler, nil
}

func (p *processorImpl) isRunning() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStarted
}

func newSchedulerOptions(
	schedulerType int,
	queueSize int,
	workerCount int,
	dispatcherCount int,
	weights dynamicconfig.MapPropertyFn,
) (*schedulerOptions, error) {
	options := &schedulerOptions{
		schedulerType: task.SchedulerType(schedulerType),
	}
	switch task.SchedulerType(schedulerType) {
	case task.SchedulerTypeFIFO:
		options.fifoSchedulerOptions = &task.FIFOTaskSchedulerOptions{
			QueueSize:       queueSize,
			WorkerCount:     workerCount,
			DispatcherCount: dispatcherCount,
			RetryPolicy:     common.CreateTaskProcessingRetryPolicy(),
		}
	case task.SchedulerTypeWRR:
		options.wrrSchedulerOptions = &task.WeightedRoundRobinTaskSchedulerOptions{
			Weights:         weights,
			QueueSize:       queueSize,
			WorkerCount:     workerCount,
			DispatcherCount: dispatcherCount,
			RetryPolicy:     common.CreateTaskProcessingRetryPolicy(),
		}
	default:
		return nil, fmt.Errorf("unknown task scheduler type: %v", schedulerType)
	}
	return options, nil
}

func createTaskScheduler(
	options *schedulerOptions,
	logger log.Logger,
	metricsClient metrics.Client,
) (task.Scheduler, error) {
	var scheduler task.Scheduler
	var err error
	switch options.schedulerType {
	case task.SchedulerTypeFIFO:
		scheduler = task.NewFIFOTaskScheduler(
			logger,
			metricsClient,
			options.fifoSchedulerOptions,
		)
	case task.SchedulerTypeWRR:
		scheduler, err = task.NewWeightedRoundRobinTaskScheduler(
			logger,
			metricsClient,
			options.wrrSchedulerOptions,
		)
	default:
		// the scheduler type has already been verified when initializing the processor
		panic(fmt.Sprintf("Unknown task scheduler type, %v", options.schedulerType))
	}

	return scheduler, err
}
