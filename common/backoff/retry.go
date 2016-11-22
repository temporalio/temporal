package backoff

import "time"

type (
	// Operation to retry
	Operation func() error

	// IsRetryable handler can be used to exclude certain errors during retry
	IsRetryable func(error) bool
)

// Retry function can be used to wrap any call with retry logic using the passed in policy
func Retry(operation Operation, policy RetryPolicy, isRetryable IsRetryable) error {
	var err error
	var next time.Duration

	r := NewRetrier(policy, SystemClock)
	for {
		// operation completed successfully.  No need to retry.
		if err = operation(); err == nil {
			return nil
		}

		if next = r.NextBackOff(); next == done {
			return err
		}

		// Check if the error is retryable
		if isRetryable != nil && !isRetryable(err) {
			return err
		}

		time.Sleep(next)
	}
}

// IgnoreErrors can be used as IsRetryable handler for Retry function to exclude certain errors from the retry list
func IgnoreErrors(errorsToExclude []error) func(error) bool {
	return func(err error) bool {
		for _, errorToExclude := range errorsToExclude {
			if err == errorToExclude {
				return false
			}
		}

		return true
	}
}
