package queues

import (
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

var _ Executable = (*chasmMemoryWakeupExecutable)(nil)

type (
	chasmMemoryWakeupExecutable struct {
		Executable
		wakeupTask *tasks.ChasmMemoryWakeupTask
	}
)

func newChasmMemoryWakeupExecutable(
	executable Executable,
	wakeupTask *tasks.ChasmMemoryWakeupTask,
) *chasmMemoryWakeupExecutable {
	return &chasmMemoryWakeupExecutable{
		Executable: executable,
		wakeupTask: wakeupTask,
	}
}

// IsRetryableError returns false: a wake-up acts on the cached mutable state, which an error
// usually clears.
func (e *chasmMemoryWakeupExecutable) IsRetryableError(_ error) bool {
	return false
}

func (e *chasmMemoryWakeupExecutable) Abort() {
}

func (e *chasmMemoryWakeupExecutable) Cancel() {
	e.wakeupTask.Cancel()
}

func (e *chasmMemoryWakeupExecutable) Ack() {
}

func (e *chasmMemoryWakeupExecutable) Nack(_ error) {
}

func (e *chasmMemoryWakeupExecutable) Reschedule() {
}

func (e *chasmMemoryWakeupExecutable) State() ctasks.State {
	return e.wakeupTask.State()
}
