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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination rescheduler_mock.go

package queues

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/timer"
)

const (
	rescheduleFailureBackoff = 3 * time.Second
)

type (
	// Rescheduler buffers task executables that are failed to process and
	// resubmit them to the task scheduler when the Reschedule method is called.
	Rescheduler interface {
		common.Daemon

		// Add task executable to the rescheduler.
		Add(task Executable, rescheduleTime time.Time)

		// Len returns the total number of task executables waiting to be rescheduled.
		Len() int
	}

	rescheduledExecuable struct {
		executable     Executable
		rescheduleTime time.Time
	}

	reschedulerImpl struct {
		scheduler      Scheduler
		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.MetricsHandler

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		timerGate timer.Gate

		sync.Mutex
		pq collection.Queue[rescheduledExecuable]
	}
)

func NewRescheduler(
	scheduler Scheduler,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *reschedulerImpl {
	return &reschedulerImpl{
		scheduler:      scheduler,
		timeSource:     timeSource,
		logger:         logger,
		metricsHandler: metricsHandler,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		timerGate: timer.NewLocalGate(timeSource),

		pq: collection.NewPriorityQueue((func(this rescheduledExecuable, that rescheduledExecuable) bool {
			return this.rescheduleTime.Before(that.rescheduleTime)
		})),
	}
}

func (r *reschedulerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	r.shutdownWG.Add(1)
	go r.rescheduleLoop()

	r.logger.Info("Task rescheduler started.", tag.LifeCycleStarted)
}

func (r *reschedulerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(r.shutdownCh)
	r.timerGate.Close()

	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("Task rescheduler timedout on shutdown.", tag.LifeCycleStopTimedout)
	}

	r.logger.Info("Task rescheduler stopped.", tag.LifeCycleStopped)
}

func (r *reschedulerImpl) Add(
	executable Executable,
	rescheduleTime time.Time,
) {
	r.Lock()
	r.pq.Add(rescheduledExecuable{
		executable:     executable,
		rescheduleTime: rescheduleTime,
	})
	r.Unlock()

	r.timerGate.Update(rescheduleTime)

	if r.isStopped() {
		r.drain()
	}
}

func (r *reschedulerImpl) Len() int {
	r.Lock()
	defer r.Unlock()

	return r.pq.Len()
}

func (r *reschedulerImpl) rescheduleLoop() {
	defer r.shutdownWG.Done()

	for {
		select {
		case <-r.shutdownCh:
			r.drain()
			return
		case <-r.timerGate.FireChan():
			r.reschedule()
		}
	}

}

func (r *reschedulerImpl) reschedule() {
	r.Lock()
	defer r.Unlock()

	r.metricsHandler.Histogram(TaskReschedulerPendingTasks, metrics.Dimensionless).Record(int64(r.pq.Len()))

	var failToSubmit []rescheduledExecuable
	for !r.pq.IsEmpty() {
		if r.timeSource.Now().Before(r.pq.Peek().rescheduleTime) {
			break
		}

		rescheduled := r.pq.Remove()
		executable := rescheduled.executable
		if executable.State() == ctasks.TaskStateCancelled {
			continue
		}

		submitted, err := r.scheduler.TrySubmit(executable)
		if err != nil {
			executable.Logger().Error("Failed to reschedule task", tag.Error(err))
		}

		if !submitted {
			rescheduled.rescheduleTime.Add(rescheduleFailureBackoff)
			failToSubmit = append(failToSubmit, rescheduled)
		}
	}

	for _, rescheduled := range failToSubmit {
		r.pq.Add(rescheduled)
	}

	if !r.pq.IsEmpty() {
		r.timerGate.Update(r.pq.Peek().rescheduleTime)
	}
}

func (r *reschedulerImpl) drain() {
	r.Lock()
	defer r.Unlock()

	for !r.pq.IsEmpty() {
		r.pq.Remove()
	}
}

func (r *reschedulerImpl) isStopped() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStopped
}
