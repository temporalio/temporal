package common

type (
	// Daemon is the base interfaces implemented by
	// background tasks within cherami
	Daemon interface {
		Start()
		Stop()
	}
)
