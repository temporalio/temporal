package retrypolicy

import (
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// TimeoutFailureTypePrefix is the prefix for timeout failure types
	// used in retry policy
	// the actual failure type will be prefix + enums.TimeoutType.String()
	// e.g. "TemporalTimeout:StartToClose" or "TemporalTimeout:Heartbeat"
	TimeoutFailureTypePrefix = "TemporalTimeout:"
)

// DefaultRetrySettings indicates what the "default" retry settings
// are if it is not specified on an Activity or for any unset fields
// if a policy is explicitly set on a workflow
type DefaultRetrySettings struct {
	InitialInterval            time.Duration `mapstructure:"InitialIntervalInSeconds"`
	MaximumIntervalCoefficient float64
	BackoffCoefficient         float64
	MaximumAttempts            int32
}

var DefaultDefaultRetrySettings = DefaultRetrySettings{
	InitialInterval:            time.Second,
	MaximumIntervalCoefficient: 100.0,
	BackoffCoefficient:         2.0,
	MaximumAttempts:            0,
}

// EnsureDefaults ensures the policy subfields, if not explicitly set, are set to the specified defaults
func EnsureDefaults(originalPolicy *commonpb.RetryPolicy, defaultSettings DefaultRetrySettings) {
	if originalPolicy.GetMaximumAttempts() == 0 {
		originalPolicy.MaximumAttempts = defaultSettings.MaximumAttempts
	}

	if timestamp.DurationValue(originalPolicy.GetInitialInterval()) == 0 {
		originalPolicy.InitialInterval = durationpb.New(defaultSettings.InitialInterval)
	}

	if timestamp.DurationValue(originalPolicy.GetMaximumInterval()) == 0 {
		originalPolicy.MaximumInterval = durationpb.New(time.Duration(defaultSettings.MaximumIntervalCoefficient) * timestamp.DurationValue(originalPolicy.GetInitialInterval()))
	}

	if originalPolicy.GetBackoffCoefficient() == 0 {
		originalPolicy.BackoffCoefficient = defaultSettings.BackoffCoefficient
	}
}

// Validate validates a retry policy
func Validate(policy *commonpb.RetryPolicy) error {
	if policy == nil {
		// nil policy is valid which means no retry
		return nil
	}

	if policy.GetMaximumAttempts() == 1 {
		// One maximum attempt effectively disable retries. Validating the
		// rest of the arguments is pointless
		return nil
	}
	if err := timestamp.ValidateAndCapProtoDuration(policy.GetInitialInterval()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid InitialInterval set on retry policy: %v", err)
	}
	if policy.GetBackoffCoefficient() < 1 {
		return serviceerror.NewInvalidArgument("BackoffCoefficient cannot be less than 1 on retry policy.")
	}
	if err := timestamp.ValidateAndCapProtoDuration(policy.GetMaximumInterval()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid MaximumInterval set on retry policy: %v", err)
	}
	if timestamp.DurationValue(policy.GetMaximumInterval()) > 0 && timestamp.DurationValue(policy.GetMaximumInterval()) < timestamp.DurationValue(policy.GetInitialInterval()) {
		return serviceerror.NewInvalidArgument("MaximumInterval cannot be less than InitialInterval on retry policy.")
	}
	if policy.GetMaximumAttempts() < 0 {
		return serviceerror.NewInvalidArgument("MaximumAttempts cannot be negative on retry policy.")
	}

	for _, nrt := range policy.NonRetryableErrorTypes {
		if strings.HasPrefix(nrt, TimeoutFailureTypePrefix) {
			timeoutTypeValue := nrt[len(TimeoutFailureTypePrefix):]
			timeoutType, err := enumspb.TimeoutTypeFromString(timeoutTypeValue)
			if err != nil || enumspb.TimeoutType(timeoutType) == enumspb.TIMEOUT_TYPE_UNSPECIFIED {
				return serviceerror.NewInvalidArgumentf("Invalid timeout type value: %v.", timeoutTypeValue)
			}
		}
	}

	return nil
}
