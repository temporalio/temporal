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

package queues

import (
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	hshard "go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

var _ Queue = (*immediateQueue)(nil)

type (
	immediateQueue struct {
		*queueBase

		notifyCh chan struct{}
	}
)

func NewImmediateQueue(
	shard hshard.Context,
	category tasks.Category,
	scheduler Scheduler,
	rescheduler Rescheduler,
	options *Options,
	hostRateLimiter quotas.RequestRateLimiter,
	grouper Grouper,
	logger log.Logger,
	metricsHandler metrics.Handler,
	factory ExecutableFactory,
) *immediateQueue {
	paginationFnProvider := func(r Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			ctx, cancel := newQueueIOContext()
			defer cancel()

			request := &persistence.GetHistoryTasksRequest{
				ShardID:             shard.GetShardID(),
				TaskCategory:        category,
				InclusiveMinTaskKey: r.InclusiveMin,
				ExclusiveMaxTaskKey: r.ExclusiveMax,
				BatchSize:           options.BatchSize(),
				NextPageToken:       paginationToken,
			}

			resp, err := shard.GetExecutionManager().GetHistoryTasks(ctx, request)
			if err != nil {
				return nil, nil, err
			}

			return resp.Tasks, resp.NextPageToken, nil
		}
	}

	return &immediateQueue{
		queueBase: newQueueBase(
			shard,
			category,
			paginationFnProvider,
			scheduler,
			rescheduler,
			factory,
			options,
			hostRateLimiter,
			NoopReaderCompletionFn,
			grouper,
			logger,
			metricsHandler,
		),

		notifyCh: make(chan struct{}, 1),
	}
}

func (p *immediateQueue) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting)
	defer p.logger.Info("", tag.LifeCycleStarted)

	p.queueBase.Start()

	p.shutdownWG.Add(1)
	go p.processEventLoop()

	p.notify()
}

func (p *immediateQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("", tag.LifeCycleStopping)
	defer p.logger.Info("", tag.LifeCycleStopped)

	close(p.shutdownCh)

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	p.queueBase.Stop()
}

func (p *immediateQueue) NotifyNewTasks(tasks []tasks.Task) {
	if len(tasks) == 0 {
		return
	}

	p.notify()
}

func (p *immediateQueue) processEventLoop() {
	defer p.shutdownWG.Done()

	pollTimer := time.NewTimer(backoff.Jitter(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		default:
		}

		select {
		case <-p.shutdownCh:
			return
		case <-p.notifyCh:
			p.processNewRange()
		case <-pollTimer.C:
			p.processPollTimer(pollTimer)
		case <-p.checkpointTimer.C:
			p.checkpoint()
		case alert := <-p.alertCh:
			p.handleAlert(alert)
		}
	}
}

func (p *immediateQueue) processPollTimer(pollTimer *time.Timer) {
	p.processNewRange()
	pollTimer.Reset(backoff.Jitter(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
}

func (p *immediateQueue) notify() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}
