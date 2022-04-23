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
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// Rescheduler buffers task executables that are failed to process and
	// resubmit them to the task scheduler when the Reschedule method is called.
	// TODO: remove this component when implementing multi-cursor queue processor.
	// Failed task executables can be tracke by task reader/queue range
	Rescheduler interface {
		// Add task executable to the rescheudler.
		// The backoff duration is just a hint for how long the executable
		// should be bufferred before rescheduling.
		Add(task Executable, backoff time.Duration)

		// Reschedule re-submit buffered executables to the scheduler and stops when
		// targetRescheduleSize number of executables are successfully submitted.
		// If targetRescheduleSize is 0, then there's no limit for the number of reschduled
		// executables.
		Reschedule(targetRescheduleSize int)

		// Len returns the total number of task executables waiting to be rescheduled.
		Len() int
	}

	rescheduledExecuable struct {
		executable     Executable
		rescheduleTime time.Time
	}

	reschedulerImpl struct {
		scheduler  Scheduler
		timeSource clock.TimeSource
		scope      metrics.Scope

		sync.Mutex
		pq collection.Queue[rescheduledExecuable]
	}
)

func NewRescheduler(
	scheduler Scheduler,
	timeSource clock.TimeSource,
	scope metrics.Scope,
) *reschedulerImpl {
	return &reschedulerImpl{
		scheduler:  scheduler,
		timeSource: timeSource,
		scope:      scope,
		pq: collection.NewPriorityQueue((func(this rescheduledExecuable, that rescheduledExecuable) bool {
			return this.rescheduleTime.Before(that.rescheduleTime)
		})),
	}
}

func (r *reschedulerImpl) Add(
	executable Executable,
	backoff time.Duration,
) {
	r.Lock()
	defer r.Unlock()

	r.pq.Add(rescheduledExecuable{
		executable:     executable,
		rescheduleTime: r.timeSource.Now().Add(backoff),
	})
}

func (r *reschedulerImpl) Reschedule(
	targetRescheduleSize int,
) {
	r.Lock()
	defer r.Unlock()

	r.scope.RecordDistribution(metrics.TaskReschedulerPendingTasks, r.pq.Len())

	if targetRescheduleSize == 0 {
		targetRescheduleSize = r.pq.Len()
	}

	var failToSubmit []rescheduledExecuable
	numRescheduled := 0
	for !r.pq.IsEmpty() && numRescheduled < targetRescheduleSize {
		if r.timeSource.Now().Before(r.pq.Peek().rescheduleTime) {
			break
		}

		rescheduled := r.pq.Remove()
		submitted, err := r.scheduler.TrySubmit(rescheduled.executable)
		if err != nil {
			rescheduled.executable.Logger().Error("Failed to reschedule task", tag.Error(err))
		}

		if !submitted {
			failToSubmit = append(failToSubmit, rescheduled)
		} else {
			numRescheduled++
		}
	}

	for _, rescheduled := range failToSubmit {
		r.pq.Add(rescheduled)
	}
}

func (r *reschedulerImpl) Len() int {
	r.Lock()
	defer r.Unlock()

	return r.pq.Len()
}
