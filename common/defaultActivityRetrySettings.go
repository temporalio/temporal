package common

// DefaultActivityRetrySettings indicates what the "default" activity retry settings
// are if it is not specified on an Activity
type DefaultActivityRetrySettings struct {
	InitialIntervalInSeconds   int32
	MaximumIntervalCoefficient float64
	BackoffCoefficient         float64
	MaximumAttempts            int32
}
