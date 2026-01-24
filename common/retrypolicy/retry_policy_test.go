package retrypolicy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestEnsureRetryPolicyDefaults(t *testing.T) {
	defaultRetrySettings := DefaultRetrySettings{
		InitialInterval:            time.Second,
		MaximumIntervalCoefficient: 100,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            120,
	}

	defaultRetryPolicy := commonpb.RetryPolicy_builder{
		InitialInterval:    durationpb.New(1 * time.Second),
		MaximumInterval:    durationpb.New(100 * time.Second),
		BackoffCoefficient: 2.0,
		MaximumAttempts:    120,
	}.Build()

	testCases := []struct {
		name  string
		input *commonpb.RetryPolicy
		want  *commonpb.RetryPolicy
	}{
		{
			name:  "default fields are set ",
			input: &commonpb.RetryPolicy{},
			want:  defaultRetryPolicy,
		},
		{
			name: "non-default InitialIntervalInSeconds is not set",
			input: commonpb.RetryPolicy_builder{
				InitialInterval: durationpb.New(2 * time.Second),
			}.Build(),
			want: commonpb.RetryPolicy_builder{
				InitialInterval:    durationpb.New(2 * time.Second),
				MaximumInterval:    durationpb.New(200 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    120,
			}.Build(),
		},
		{
			name: "non-default MaximumIntervalInSeconds is not set",
			input: commonpb.RetryPolicy_builder{
				MaximumInterval: durationpb.New(1000 * time.Second),
			}.Build(),
			want: commonpb.RetryPolicy_builder{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(1000 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    120,
			}.Build(),
		},
		{
			name: "non-default BackoffCoefficient is not set",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 1.5,
			}.Build(),
			want: commonpb.RetryPolicy_builder{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 1.5,
				MaximumAttempts:    120,
			}.Build(),
		},
		{
			name: "non-default Maximum attempts is not set",
			input: commonpb.RetryPolicy_builder{
				MaximumAttempts: 49,
			}.Build(),
			want: commonpb.RetryPolicy_builder{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    49,
			}.Build(),
		},
		{
			name: "non-retryable errors are set",
			input: commonpb.RetryPolicy_builder{
				NonRetryableErrorTypes: []string{"testFailureType"},
			}.Build(),
			want: commonpb.RetryPolicy_builder{
				InitialInterval:        durationpb.New(1 * time.Second),
				MaximumInterval:        durationpb.New(100 * time.Second),
				BackoffCoefficient:     2.0,
				MaximumAttempts:        120,
				NonRetryableErrorTypes: []string{"testFailureType"},
			}.Build(),
		},
		{
			name:  "empty policy has non-zero defaults",
			input: &commonpb.RetryPolicy{},
			want: commonpb.RetryPolicy_builder{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    120,
			}.Build(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			EnsureDefaults(tt.input, defaultRetrySettings)
			assert.Equal(t, tt.want, tt.input)
		})
	}
}

func TestValidateRetryPolicy(t *testing.T) {
	testCases := []struct {
		name          string
		input         *commonpb.RetryPolicy
		wantErr       bool
		wantErrString string
	}{
		{
			name:          "nil policy is okay",
			input:         nil,
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "maxAttempts is 1, coefficient < 1",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 0.5,
				MaximumAttempts:    1,
			}.Build(),
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "initial interval negative",
			input: commonpb.RetryPolicy_builder{
				InitialInterval: durationpb.New(-22 * time.Second),
			}.Build(),
			wantErr:       true,
			wantErrString: "invalid InitialInterval set on retry policy: negative duration",
		},
		{
			name: "coefficient < 1",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 0.8,
			}.Build(),
			wantErr:       true,
			wantErrString: "BackoffCoefficient cannot be less than 1 on retry policy.",
		},
		{
			name: "maximum interval in seconds is negative",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 2.0,
				MaximumInterval:    durationpb.New(-2 * time.Second),
			}.Build(),
			wantErr:       true,
			wantErrString: "invalid MaximumInterval set on retry policy: negative duration",
		},
		{
			name: "maximum interval in less than initial interval",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 2.0,
				MaximumInterval:    durationpb.New(5 * time.Second),
				InitialInterval:    durationpb.New(10 * time.Second),
			}.Build(),
			wantErr:       true,
			wantErrString: "MaximumInterval cannot be less than InitialInterval on retry policy.",
		},
		{
			name: "maximum attempts negative",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 2.0,
				MaximumAttempts:    -3,
			}.Build(),
			wantErr:       true,
			wantErrString: "MaximumAttempts cannot be negative on retry policy.",
		},
		{
			name: "timeout nonretryable error - valid type",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
				},
			}.Build(),
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "timeout nonretryable error - unspecified type",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_UNSPECIFIED.String(),
				},
			}.Build(),
			wantErr:       true,
			wantErrString: "Invalid timeout type value: Unspecified.",
		},
		{
			name: "timeout nonretryable error - unknown type",
			input: commonpb.RetryPolicy_builder{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + "unknown",
				},
			}.Build(),
			wantErr:       true,
			wantErrString: "Invalid timeout type value: unknown.",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.input)
			if tt.wantErr {
				assert.NotNil(t, err, "expected error - did not get one")
				assert.Equal(t, err.Error(), tt.wantErrString, "unexpected error message")
			} else {
				assert.Nil(t, err, "unexpected error")
			}
		})
	}
}
