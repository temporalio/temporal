package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/common/primitives/timestamp"
)

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
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 0.5,
				MaximumAttempts:    1,
			},
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "initial interval negative",
			input: &commonpb.RetryPolicy{
				InitialInterval: timestamp.DurationPtr(-22 * time.Second),
			},
			wantErr:       true,
			wantErrString: "InitialInterval cannot be negative on retry policy.",
		},
		{
			name: "coefficient < 1",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 0.8,
			},
			wantErr:       true,
			wantErrString: "BackoffCoefficient cannot be less than 1 on retry policy.",
		},
		{
			name: "maximum interval in seconds is negative",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumInterval:    timestamp.DurationPtr(-2 * time.Second),
			},
			wantErr:       true,
			wantErrString: "MaximumInterval cannot be negative on retry policy.",
		},
		{
			name: "maximum interval in less than initial interval",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumInterval:    timestamp.DurationPtr(5 * time.Second),
				InitialInterval:    timestamp.DurationPtr(10 * time.Second),
			},
			wantErr:       true,
			wantErrString: "MaximumInterval cannot be less than InitialInterval on retry policy.",
		},
		{
			name: "maximum attempts negative",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumAttempts:    -3,
			},
			wantErr:       true,
			wantErrString: "MaximumAttempts cannot be negative on retry policy.",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRetryPolicy(tt.input)
			if tt.wantErr {
				assert.NotNil(t, err, "expected error - did not get one")
				assert.Equal(t, err.Error(), tt.wantErrString, "unexpected error message")
			} else {
				assert.Nil(t, err, "unexpected error")
			}
		})
	}
}

func TestEnsureRetryPolicyDefaults(t *testing.T) {
	defaultRetrySettings := DefaultRetrySettings{
		InitialIntervalInSeconds:   1,
		MaximumIntervalCoefficient: 100,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            120,
	}

	defaultRetryPolicy := &commonpb.RetryPolicy{
		InitialInterval:    timestamp.DurationPtr(1 * time.Second),
		MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
		BackoffCoefficient: 2.0,
		MaximumAttempts:    120,
	}

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
			input: &commonpb.RetryPolicy{
				InitialInterval: timestamp.DurationPtr(2 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(2 * time.Second),
				MaximumInterval:    timestamp.DurationPtr(200 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    120,
			},
		},
		{
			name: "non-default MaximumIntervalInSeconds is not set",
			input: &commonpb.RetryPolicy{
				MaximumInterval: timestamp.DurationPtr(1000 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(1 * time.Second),
				MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    120,
			},
		},
		{
			name: "non-default BackoffCoefficient is not set",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1.5,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(1 * time.Second),
				MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
				BackoffCoefficient: 1.5,
				MaximumAttempts:    120,
			},
		},
		{
			name: "non-default Maximum attempts is not set",
			input: &commonpb.RetryPolicy{
				MaximumAttempts: 49,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(1 * time.Second),
				MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    49,
			},
		},
		{
			name: "non-retryable errors are set",
			input: &commonpb.RetryPolicy{
				NonRetryableErrorTypes: []string{"testFailureType"},
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:        timestamp.DurationPtr(1 * time.Second),
				MaximumInterval:        timestamp.DurationPtr(100 * time.Second),
				BackoffCoefficient:     2.0,
				MaximumAttempts:        120,
				NonRetryableErrorTypes: []string{"testFailureType"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			EnsureRetryPolicyDefaults(tt.input, defaultRetrySettings)
			assert.Equal(t, tt.want, tt.input)
		})
	}
}

func Test_FromConfigToRetryPolicy(t *testing.T) {
	options := map[string]interface{}{
		"InitialIntervalInSeconds":   2,
		"MaximumIntervalCoefficient": 100.0,
		"BackoffCoefficient":         4.0,
		"MaximumAttempts":            5,
	}

	defaultSettings := FromConfigToDefaultRetrySettings(options)
	assert.Equal(t, int32(2), defaultSettings.InitialIntervalInSeconds)
	assert.Equal(t, 100.0, defaultSettings.MaximumIntervalCoefficient)
	assert.Equal(t, 4.0, defaultSettings.BackoffCoefficient)
	assert.Equal(t, int32(5), defaultSettings.MaximumAttempts)
}
