package common

const (
	// used for background threads

	// DaemonStatusInitialized coroutine pool initialized
	DaemonStatusInitialized int32 = 0
	// DaemonStatusStarted coroutine pool started
	DaemonStatusStarted int32 = 1
	// DaemonStatusStopped coroutine pool stopped
	DaemonStatusStopped int32 = 2
)

type (
	// Daemon is the base interfaces implemented by
	// background tasks within Temporal
	Daemon interface {
		Start()
		Stop()
	}
)
