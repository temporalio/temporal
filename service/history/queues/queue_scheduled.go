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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

var _ Queue = (*scheduledQueue)(nil)

type (
	scheduledQueue struct {
		*queueBase

		timerGate   timer.Gate
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

const (
	scheduledTaskPrecision = time.Millisecond
)

func NewScheduledQueue(
	shard shard.Context,
	category tasks.Category,
	scheduler Scheduler,
	priorityAssigner PriorityAssigner,
	executor Executor,
	options *Options,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *scheduledQueue {
	paginationFnProvider := func(r Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			ctx, cancel := newQueueIOContext()
			defer cancel()

			request := &persistence.GetHistoryTasksRequest{
				ShardID:             shard.GetShardID(),
				TaskCategory:        category,
				InclusiveMinTaskKey: tasks.NewKey(r.InclusiveMin.FireTime, 0),
				ExclusiveMaxTaskKey: tasks.NewKey(r.ExclusiveMax.FireTime.Add(scheduledTaskPrecision), 0),
				BatchSize:           options.BatchSize(),
				NextPageToken:       paginationToken,
			}

			resp, err := shard.GetExecutionManager().GetHistoryTasks(ctx, request)
			if err != nil {
				return nil, nil, err
			}

			// The rest of the code assumes task loaded is ordered by task key, which has precision of ns for time.
			// However for cassandra impl, the task returned is ordered by visibilitystamp column which only has
			// ms precision, which makes tasks out of order, even across multiple loads.
			// So truncate task key time also to ms precision to make them ordered.
			//
			// This however, moves task visibility time forward for 1ms and may cause timer tasks to be skipped
			// during processing. To compensate for that, add 1ms back when scheduling the task in reader.go.
			for _, task := range resp.Tasks {
				task.SetVisibilityTime(task.GetVisibilityTime().Truncate(scheduledTaskPrecision))
			}

			for len(resp.Tasks) > 0 && !r.ContainsKey(resp.Tasks[0].GetKey()) {
				resp.Tasks = resp.Tasks[1:]
			}

			for len(resp.Tasks) > 0 && !r.ContainsKey(resp.Tasks[len(resp.Tasks)-1].GetKey()) {
				resp.Tasks = resp.Tasks[:len(resp.Tasks)-1]
				resp.NextPageToken = nil
			}

			return resp.Tasks, resp.NextPageToken, nil
		}
	}

	return &scheduledQueue{
		queueBase: newQueueBase(
			shard,
			category,
			paginationFnProvider,
			scheduler,
			priorityAssigner,
			executor,
			options,
			rateLimiter,
			logger,
			metricsHandler,
		),

		timerGate:  timer.NewLocalGate(shard.GetTimeSource()),
		newTimerCh: make(chan struct{}, 1),
	}
}

func (p *scheduledQueue) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting)
	defer p.logger.Info("", tag.LifeCycleStarted)

	p.queueBase.Start()

	p.shutdownWG.Add(1)
	go p.processEventLoop()

	p.notify(time.Time{})
}

func (p *scheduledQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("", tag.LifeCycleStopping)
	defer p.logger.Info("", tag.LifeCycleStopped)

	close(p.shutdownCh)
	p.timerGate.Close()

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	p.queueBase.Stop()
}

func (p *scheduledQueue) NotifyNewTasks(_ string, tasks []tasks.Task) {
	if len(tasks) == 0 {
		return
	}

	newTime := tasks[0].GetVisibilityTime()
	for _, task := range tasks {
		ts := task.GetVisibilityTime()
		if ts.Before(newTime) {
			newTime = ts
		}
	}

	p.notify(newTime)
}

func (p *scheduledQueue) processEventLoop() {
	defer p.shutdownWG.Done()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-p.newTimerCh:
			p.metricsHandler.Counter(NewTimerNotifyCounter).Record(1)
			p.processNewTime()
		case <-p.timerGate.FireChan():
			p.processNewRange()
		case <-p.pollTimer.C:
			p.processPollTimer()
		case <-p.checkpointTimer.C:
			p.checkpoint()
		case alert := <-p.alertCh:
			p.handleAlert(alert)
		}
	}
}

func (p *scheduledQueue) notify(newTime time.Time) {
	p.newTimeLock.Lock()
	defer p.newTimeLock.Unlock()

	if !p.newTime.IsZero() && !newTime.Before(p.newTime) {
		return
	}

	p.newTime = newTime
	select {
	case p.newTimerCh <- struct{}{}:
	default:
	}
}

func (p *scheduledQueue) processNewTime() {
	p.newTimeLock.Lock()
	newTime := p.newTime
	p.newTime = time.Time{}
	p.newTimeLock.Unlock()

	p.timerGate.Update(newTime)
}

func (p *scheduledQueue) processNewRange() {
	p.queueBase.processNewRange()
	p.lookAheadTask()
}

func (p *scheduledQueue) processPollTimer() {
	p.queueBase.processPollTimer()
	p.lookAheadTask()
}

func (p *scheduledQueue) lookAheadTask() {
	lookAheadMinTime := p.nonReadableScope.Range.InclusiveMin.FireTime
	lookAheadMaxTime := lookAheadMinTime.Add(p.options.MaxPollInterval())

	ctx, cancel := newQueueIOContext()
	defer cancel()

	request := &persistence.GetHistoryTasksRequest{
		ShardID:             p.shard.GetShardID(),
		TaskCategory:        p.category,
		InclusiveMinTaskKey: tasks.NewKey(lookAheadMinTime, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(lookAheadMaxTime, 0),
		BatchSize:           1,
		NextPageToken:       nil,
	}
	response, err := p.shard.GetExecutionManager().GetHistoryTasks(ctx, request)
	if err != nil {
		// NOTE: the backoff is actually 2*TimerProcessorMaxTimeShift = 2s
		p.timerGate.Update(lookAheadMinTime)
		p.logger.Error("Failed to load look ahead task", tag.Error(err))
		return
	}

	if len(response.Tasks) == 1 {
		p.timerGate.Update(response.Tasks[0].GetKey().FireTime)
	}

	// no look ahead task, wait for max poll interval or new task notification
}
