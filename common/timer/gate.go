package timer

import (
	"time"
)

type (
	// Gate interface
	Gate interface {
		// FireCh return the channel which will be fired when time is up
		FireCh() <-chan struct{}
		// FireAfter check will the timer get fired after a certain time
		FireAfter(now time.Time) bool
		// Update the timer gate, return true if update is a success.
		// Success means timer is idle or timer is set with a sooner time to fire
		Update(nextTime time.Time) bool
		// Close shutdown the timer
		Close()
	}
)
