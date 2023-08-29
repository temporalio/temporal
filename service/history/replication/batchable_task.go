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

type (
	BatchableTask interface {
		TrackableExecutableTask
		// BatchWith task and return a new BatchableTask
		BatchWith(task BatchableTask) (BatchableTask, error)
		CanBatch() bool
		// MarkUnbatchable will mark current task not batchable, so CanBatch() will return false
		MarkUnbatchable()
	}

	batchedTask struct {
		batchedTask     BatchableTask
		individualTasks []BatchableTask
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
	// TODO: emit metrics
	w.callIndividual(BatchableTask.Ack)
}

func (w *batchedTask) Execute() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.state = batchStateClose
	return w.batchedTask.Execute()
}

func (w *batchedTask) HandleErr(err error) error {
	return w.batchedTask.HandleErr(err)
}

func (w *batchedTask) IsRetryableError(err error) bool {
	return w.batchedTask.IsRetryableError(err)
}

func (w *batchedTask) RetryPolicy() backoff.RetryPolicy {
	return w.batchedTask.RetryPolicy()
}

func (w *batchedTask) Abort() {
	w.callIndividual(BatchableTask.Abort)
}

func (w *batchedTask) Cancel() {
	w.callIndividual(BatchableTask.Cancel)
}

func (w *batchedTask) Nack(err error) {
	if len(w.individualTasks) == 1 {
		w.batchedTask.Nack(err)
	} else {
		w.logger.Info("Failed to process batched replication task", tag.Error(err))
		w.handleIndividualTasks()
	}
}

func (w *batchedTask) Reschedule() {
	if len(w.individualTasks) == 1 {
		w.Reschedule()
	} else {
		w.handleIndividualTasks()
	}
}

func (w *batchedTask) State() ctasks.State {
	return w.batchedTask.State()
}

func (w *batchedTask) callIndividual(f func(task BatchableTask)) {
	for _, task := range w.individualTasks {
		f(task)
	}
}

func (w *batchedTask) handleIndividualTasks() {
	w.callIndividual(func(t BatchableTask) {
		t.MarkUnbatchable()
		w.individualTaskHandler(t)
	})
}

func (w *batchedTask) AddTask(task BatchableTask) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	// This is to make sure no task can be added into this batch after it starts executing
	if w.state != batchStateOpen {
		return false
	}
	newTask, err := w.batchedTask.BatchWith(task)
	if err != nil {
		w.logger.Info("Failed to batch task", tag.Error(err))
		return false
	}
	w.batchedTask = newTask
	w.individualTasks = append(w.individualTasks, task)
	return true
}
