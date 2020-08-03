package common

// DefaultRetrySettings indicates what the "default" retry settings
// are if it is not specified on an Activity or for any unset fields
// if a policy is explicitly set on a Child Workflow
type DefaultRetrySettings struct {
	InitialIntervalInSeconds   int32
	MaximumIntervalCoefficient float64
	BackoffCoefficient         float64
	MaximumAttempts            int32
}
