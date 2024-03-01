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

package replication

import (
	"sync"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination batchable_task_mock.go

type (
	BatchableTask interface {
		TrackableExecutableTask
		// BatchWith task and return a new TrackableExecutableTask
		BatchWith(task BatchableTask) (TrackableExecutableTask, bool)
		CanBatch() bool
		// MarkUnbatchable will mark current task not batchable, so CanBatch() will return false
		MarkUnbatchable()
	}

	batchedTask struct {
		batchedTask     TrackableExecutableTask
		individualTasks []TrackableExecutableTask
		lock            sync.Mutex
		state           batchState
		// individualTaskHandler will be called when batched task was Nack, Reschedule, MarkPoisonPill
		individualTaskHandler func(task TrackableExecutableTask)
		logger                log.Logger
		metricsHandler        metrics.Handler
	}

	batchState int
)

const (
	batchStateOpen  = 0
	batchStateClose = 1
)

var _ TrackableExecutableTask = (*batchedTask)(nil)

func (w *batchedTask) QueueID() interface{} {
	return w.batchedTask.QueueID()
}

func (w *batchedTask) TaskID() int64 {
	return w.individualTasks[0].TaskID()
}

func (w *batchedTask) TaskCreationTime() time.Time {
	return w.individualTasks[0].TaskCreationTime()
}

func (w *batchedTask) MarkPoisonPill() error {
	if len(w.individualTasks) == 1 {
		return w.batchedTask.MarkPoisonPill()
	}
	w.handleIndividualTasks()
	return nil
}

func (w *batchedTask) Ack() {
	metrics.BatchableTaskBatchCount.With(w.metricsHandler).Record(
		float64(len(w.individualTasks)),
	)
	w.callIndividual(TrackableExecutableTask.Ack)
}

func (w *batchedTask) Execute() error {
	w.lock.Lock()
	w.state = batchStateClose
	w.lock.Unlock()
	return w.batchedTask.Execute()
}

func (w *batchedTask) HandleErr(err error) error {
	return w.batchedTask.HandleErr(err)
}

func (w *batchedTask) IsRetryableError(err error) bool {
	return w.batchedTask.IsRetryableError(err)
}

func (w *batchedTask) RetryPolicy() backoff.RetryPolicy {
	w.lock.Lock()
	defer w.lock.Unlock()
	if len(w.individualTasks) == 1 {
		return w.batchedTask.RetryPolicy()
	}
	return backoff.DisabledRetryPolicy
}

func (w *batchedTask) Abort() {
	w.callIndividual(TrackableExecutableTask.Abort)
}

func (w *batchedTask) Cancel() {
	w.callIndividual(TrackableExecutableTask.Cancel)
}

func (w *batchedTask) Nack(err error) {
	if len(w.individualTasks) == 1 {
		w.batchedTask.Nack(err)
	} else {
		w.logger.Warn("Failed to process batched replication task", tag.Error(err))
		w.handleIndividualTasks()
	}
}

func (w *batchedTask) Reschedule() {
	if len(w.individualTasks) == 1 {
		w.batchedTask.Reschedule()
	} else {
		w.handleIndividualTasks()
	}
}

func (w *batchedTask) State() ctasks.State {
	return w.batchedTask.State()
}

func (w *batchedTask) callIndividual(f func(task TrackableExecutableTask)) {
	for _, task := range w.individualTasks {
		f(task)
	}
}

func (w *batchedTask) handleIndividualTasks() {
	w.callIndividual(func(t TrackableExecutableTask) {
		batchableTask, isBatchable := t.(BatchableTask)
		if isBatchable {
			batchableTask.MarkUnbatchable()
		}
		w.individualTaskHandler(t)
	})
}

func (w *batchedTask) AddTask(incomingTask TrackableExecutableTask) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.state != batchStateOpen {
		return false
	}

	incomingBatchableTask, isIncomingTaskBatchable := incomingTask.(BatchableTask)
	currentBatchableTask, isCurrentTaskBatchable := w.batchedTask.(BatchableTask)

	if !isIncomingTaskBatchable || !isCurrentTaskBatchable || !incomingBatchableTask.CanBatch() || !currentBatchableTask.CanBatch() {
		return false
	}

	newBatchedTask, success := currentBatchableTask.BatchWith(incomingBatchableTask)
	if !success {
		return false
	}
	w.batchedTask = newBatchedTask
	w.individualTasks = append(w.individualTasks, incomingTask)
	return true
}
