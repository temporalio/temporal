package common

import "time"

// DefaultRetrySettings indicates what the "default" retry settings
// are if it is not specified on an Activity or for any unset fields
// if a policy is explicitly set on a workflow
type DefaultRetrySettings struct {
	InitialInterval            time.Duration
	MaximumIntervalCoefficient float64
	BackoffCoefficient         float64
	MaximumAttempts            int32
}
