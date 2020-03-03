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

package history

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/task"
)

type (
	queueTaskProcessorOptions struct {
		schedulerType        task.SchedulerType
		fifoSchedulerOptions *task.FIFOTaskSchedulerOptions
		wRRSchedulerOptions  *task.WeightedRoundRobinTaskSchedulerOptions
	}

	queueTaskProcessorImpl struct {
		sync.RWMutex

		priorityAssigner taskPriorityAssigner
		schedulers       map[int]task.Scheduler

		status        int32
		options       *queueTaskProcessorOptions
		logger        log.Logger
		metricsClient metrics.Client

		// TODO: add a host level task scheduler
	}
)

var (
	errUnknownTaskSchedulerType         = errors.New("unknown task scheduler type")
	errTaskSchedulerOptionsNotSpecified = errors.New("task scheduler option is not specified")
	errTaskProcessorNotRunning          = errors.New("queue task processor is not running")
)

func newQueueTaskProcessor(
	priorityAssigner taskPriorityAssigner,
	options *queueTaskProcessorOptions,
	logger log.Logger,
	metricsClient metrics.Client,
) (queueTaskProcessor, error) {
	switch options.schedulerType {
	case task.SchedulerTypeFIFO:
		if options.fifoSchedulerOptions == nil {
			return nil, errTaskSchedulerOptionsNotSpecified
		}
	case task.SchedulerTypeWRR:
		if options.wRRSchedulerOptions == nil {
			return nil, errTaskSchedulerOptionsNotSpecified
		}
	default:
		return nil, errUnknownTaskSchedulerType
	}

	return &queueTaskProcessorImpl{
		priorityAssigner: priorityAssigner,
		schedulers:       make(map[int]task.Scheduler),
		status:           common.DaemonStatusInitialized,
		options:          options,
		logger:           logger,
		metricsClient:    metricsClient,
	}, nil
}

func (p *queueTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("Queue task processor started.")
}

func (p *queueTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.Lock()
	defer p.Unlock()

	for shardID, scheduler := range p.schedulers {
		delete(p.schedulers, shardID)
		scheduler.Stop()
	}

	p.logger.Info("Queue task processor stopped.")
}

func (p *queueTaskProcessorImpl) StopShardProcessor(
	shardID int,
) {
	p.Lock()
	scheduler, ok := p.schedulers[shardID]
	if !ok {
		p.Unlock()
		return
	}

	delete(p.schedulers, shardID)
	p.Unlock()

	// don't hold the lock while stopping the scheduler
	scheduler.Stop()
}

func (p *queueTaskProcessorImpl) Submit(
	task queueTask,
) error {
	scheduler, err := p.prepareSubmit(task)
	if err != nil {
		return err
	}
	return scheduler.Submit(task)
}

func (p *queueTaskProcessorImpl) TrySubmit(
	task queueTask,
) (bool, error) {
	scheduler, err := p.prepareSubmit(task)
	if err != nil {
		return false, err
	}
	return scheduler.TrySubmit(task)
}

func (p *queueTaskProcessorImpl) prepareSubmit(
	task queueTask,
) (task.Scheduler, error) {
	if err := p.priorityAssigner.Assign(task); err != nil {
		return nil, err
	}

	return p.getOrCreateTaskScheduler(task.GetShardID())
}

func (p *queueTaskProcessorImpl) getOrCreateTaskScheduler(
	shardID int,
) (task.Scheduler, error) {
	p.RLock()
	if scheduler, ok := p.schedulers[shardID]; ok {
		p.RUnlock()
		return scheduler, nil
	}
	p.RUnlock()

	p.Lock()
	if scheduler, ok := p.schedulers[shardID]; ok {
		p.Unlock()
		return scheduler, nil
	}

	if !p.isRunning() {
		p.Unlock()
		return nil, errTaskProcessorNotRunning
	}

	var scheduler task.Scheduler
	var err error
	switch p.options.schedulerType {
	case task.SchedulerTypeFIFO:
		scheduler = task.NewFIFOTaskScheduler(
			p.logger,
			p.metricsClient.Scope(metrics.TaskSchedulerScope),
			p.options.fifoSchedulerOptions,
		)
	case task.SchedulerTypeWRR:
		scheduler, err = task.NewWeightedRoundRobinTaskScheduler(
			p.logger,
			p.metricsClient.Scope(metrics.TaskSchedulerScope),
			p.options.wRRSchedulerOptions,
		)
	default:
		err = errUnknownTaskSchedulerType
	}

	if err != nil {
		p.Unlock()
		return nil, err
	}

	p.schedulers[shardID] = scheduler
	p.Unlock()

	// don't hold the lock while starting the scheduler
	scheduler.Start()
	return scheduler, nil
}

func (p *queueTaskProcessorImpl) isRunning() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStarted
}
