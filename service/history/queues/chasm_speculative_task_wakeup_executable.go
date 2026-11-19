package queues

import (
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

var _ Executable = (*chasmSpeculativeTaskWakeupExecutable)(nil)

type (
	chasmSpeculativeTaskWakeupExecutable struct {
		Executable
		wakeupTask *tasks.ChasmSpeculativeTaskWakeupTask
	}
)

func newChasmSpeculativeTaskWakeupExecutable(
	executable Executable,
	wakeupTask *tasks.ChasmSpeculativeTaskWakeupTask,
) *chasmSpeculativeTaskWakeupExecutable {
	return &chasmSpeculativeTaskWakeupExecutable{
		Executable: executable,
		wakeupTask: wakeupTask,
	}
}

// IsRetryableError returns false: the tasks it runs live in the cached mutable state, which an
// error usually reloads from the database.
func (e *chasmSpeculativeTaskWakeupExecutable) IsRetryableError(_ error) bool {
	return false
}

func (e *chasmSpeculativeTaskWakeupExecutable) Abort() {
}

func (e *chasmSpeculativeTaskWakeupExecutable) Cancel() {
	e.wakeupTask.Cancel()
}

func (e *chasmSpeculativeTaskWakeupExecutable) Ack() {
}

func (e *chasmSpeculativeTaskWakeupExecutable) Nack(_ error) {
}

func (e *chasmSpeculativeTaskWakeupExecutable) Reschedule() {
}

func (e *chasmSpeculativeTaskWakeupExecutable) State() ctasks.State {
	return e.wakeupTask.State()
}
