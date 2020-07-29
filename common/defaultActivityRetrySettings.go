package common

// DefaultActivityRetrySettings indicates what the "default" activity retry settings
// are of it is not specified on an Activity
type DefaultActivityRetrySettings struct {
	InitialRetryIntervalInSeconds   int32
	MaximumRetryIntervalCoefficient float64
	ExponentialBackoffCoefficient   float64
	MaximumAttempts                 int32
}
