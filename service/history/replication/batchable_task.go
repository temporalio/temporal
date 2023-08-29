package replication

import (
	"sync"
	"time"

	"go.temporal.io/server/common/backoff"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	BatchableTask interface {
		TrackableExecutableTask
		// BatchWith bt and return a new BatchableTask
		BatchWith(bt BatchableTask) (BatchableTask, error)
		CanBatch() bool
		// MarkUnbatchable will mark current task not batchable, so CanBatch() will return false
		MarkUnbatchable()
	}

	batchedTask struct {
		batchedTask       BatchableTask
		individualTasks   []BatchableTask
		lock              sync.Mutex
		state             batchState
		reSubmitScheduler ctasks.Scheduler[TrackableExecutableTask] // This scheduler is used to re-submit individual tasks if batch processing failed
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
	w.reSubmitIndividualTasks()
	return nil
}

func (w *batchedTask) Ack() {
	// TODO: emit metrics to count how many tasks are successfully batch executed
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
		// TODO: log the error
		w.reSubmitIndividualTasks()
	}
}

func (w *batchedTask) Reschedule() {
	if len(w.individualTasks) == 1 {
		w.Reschedule()
	} else {
		w.reSubmitIndividualTasks()
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

func (w *batchedTask) reSubmitIndividualTasks() {
	w.callIndividual(func(t BatchableTask) {
		t.MarkUnbatchable()
		w.reSubmitScheduler.Submit(t)
	})
}

func (w *batchedTask) addTask(task BatchableTask) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	// This is to make sure no task can be added into this batch after it starts executing
	if w.state != batchStateOpen {
		return false
	}
	newTask, err := w.batchedTask.BatchWith(task)
	if err != nil {
		return false
	}
	w.batchedTask = newTask
	w.individualTasks = append(w.individualTasks, task)
	return true
}
