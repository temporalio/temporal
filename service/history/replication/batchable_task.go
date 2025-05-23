package replication

import (
	"sync"
	"time"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination batchable_task_mock.go

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

func (w *batchedTask) SourceClusterName() string {
	return w.batchedTask.SourceClusterName()
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

func (w *batchedTask) ReplicationTask() *replicationspb.ReplicationTask {
	return w.batchedTask.ReplicationTask()
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
