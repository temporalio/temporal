package locks

type (
	// Locker is the interface for lock
	Locker interface {
		// Lock locks
		Lock()
		// Unlock unlocks
		Unlock()
	}
)
