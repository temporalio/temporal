package util

import (
	"sync"
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/backoff"
)

const (
	retryPersistenceOperationInitialInterval = 50 * time.Millisecond
	// TODO: Just minimizing for demo from 10 * time.second to time.second
	retryPersistenceOperationMaxInterval        = time.Second
	retryPersistenceOperationExpirationInterval = 10 * time.Second
)

// MergeDictoRight copies the contents of src to dest
func MergeDictoRight(src map[string]string, dest map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
}

// MergeDicts creates a union of the two dicts
func MergeDicts(dic1 map[string]string, dic2 map[string]string) (resultDict map[string]string) {
	resultDict = make(map[string]string)
	MergeDictoRight(dic1, resultDict)
	MergeDictoRight(dic2, resultDict)
	return
}

// AwaitWaitGroup calls Wait on the given wait
// Returns true if the Wait() call succeeded before the timeout
// Returns false if the Wait() did not return before the timeout
func AwaitWaitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {

	doneC := make(chan struct{})

	go func() {
		wg.Wait()
		close(doneC)
	}()

	select {
	case <-doneC:
		return true
	case <-time.After(timeout):
		return false
	}
}

// AddSecondsToBaseTime - Gets the UnixNano with given duration and base time.
func AddSecondsToBaseTime(baseTimeInNanoSec int64, durationInSeconds int64) int64 {
	timeOut := time.Duration(durationInSeconds) * time.Second
	return time.Unix(0, baseTimeInNanoSec).Add(timeOut).UnixNano()
}

// CreatePersistanceRetryPolicy creates a retry policy for persistence layer operations
func CreatePersistanceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryPersistenceOperationInitialInterval)
	policy.SetMaximumInterval(retryPersistenceOperationMaxInterval)
	policy.SetExpirationInterval(retryPersistenceOperationExpirationInterval)

	return policy
}

// IsPersistenceTransientError checks if the error is a transient persistence error
func IsPersistenceTransientError(err error) bool {
	switch err.(type) {
	case *workflow.InternalServiceError:
		return true
	}

	return false
}
