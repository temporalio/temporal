package circuitbreaker

import (
	"strings"
	"sync"
)

type (
	// FailureRecorder is implemented by circuit breakers that report the failures that open them. Callers record
	// each outcome before completing the request, so the failure that opens the breaker is included.
	FailureRecorder interface {
		RecordFailure(message string)
		RecordSuccess()
	}

	// RecentFailures holds the current run of consecutive failures of a circuit breaker. It keeps the most recent
	// maxFailures messages, each truncated to maxMessageLength bytes, and is emptied by the next success, so it
	// only holds memory while its breaker is failing.
	RecentFailures struct {
		maxFailures      int
		maxMessageLength int

		mu       sync.Mutex
		messages []string // oldest first
		count    int      // failures in the current run, including those no longer kept
	}
)

var _ FailureRecorder = (*RecentFailures)(nil)

func NewRecentFailures(maxFailures, maxMessageLength int) *RecentFailures {
	return &RecentFailures{
		maxFailures:      maxFailures,
		maxMessageLength: maxMessageLength,
	}
}

func (f *RecentFailures) RecordFailure(message string) {
	if len(message) > f.maxMessageLength {
		message = strings.ToValidUTF8(message[:f.maxMessageLength], "")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	if len(f.messages) == f.maxFailures {
		f.messages = append(f.messages[:0], f.messages[1:]...)
	}
	f.messages = append(f.messages, message)
}

func (f *RecentFailures) RecordSuccess() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = nil
	f.count = 0
}

// Take returns the kept failure messages, oldest first, and the length of the run they belong to, and empties the
// record.
func (f *RecentFailures) Take() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	messages, count := f.messages, f.count
	f.messages = nil
	f.count = 0
	return messages, count
}
