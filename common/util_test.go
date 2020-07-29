package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
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
				InitialIntervalInSeconds: -22,
			},
			wantErr:       true,
			wantErrString: "InitialIntervalInSeconds cannot be negative on retry policy.",
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
				BackoffCoefficient:       2.0,
				MaximumIntervalInSeconds: -2,
			},
			wantErr:       true,
			wantErrString: "MaximumIntervalInSeconds cannot be negative on retry policy.",
		},
		{
			name: "maximum interval in less than initial interval in seconds",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient:       2.0,
				MaximumIntervalInSeconds: 5,
				InitialIntervalInSeconds: 10,
			},
			wantErr:       true,
			wantErrString: "MaximumIntervalInSeconds cannot be less than InitialIntervalInSeconds on retry policy.",
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
	defaultRetryPolicy := &commonpb.RetryPolicy{
		InitialIntervalInSeconds: 1,
		MaximumIntervalInSeconds: 100,
		BackoffCoefficient:       2,
		MaximumAttempts:          120,
	}

	testCases := []struct {
		name  string
		input *commonpb.RetryPolicy
		want  *commonpb.RetryPolicy
	}{
		{
			name:  "nil policy is okay",
			input: nil,
			want:  defaultRetryPolicy,
		},
		{
			name:  "default fields are set ",
			input: &commonpb.RetryPolicy{},
			want:  defaultRetryPolicy,
		},
		{
			name: "non-default InitialIntervalInSeconds is not set",
			input: &commonpb.RetryPolicy{
				InitialIntervalInSeconds: 2,
			},
			want: &commonpb.RetryPolicy{
				InitialIntervalInSeconds: 2,
				MaximumIntervalInSeconds: 100,
				BackoffCoefficient:       2,
				MaximumAttempts:          120,
			},
		},
		{
			name: "non-default MaximumIntervalInSeconds is not set",
			input: &commonpb.RetryPolicy{
				MaximumIntervalInSeconds: 1000,
			},
			want: &commonpb.RetryPolicy{
				InitialIntervalInSeconds: 1,
				MaximumIntervalInSeconds: 1000,
				BackoffCoefficient:       2,
				MaximumAttempts:          120,
			},
		},
		{
			name: "non-default BackoffCoefficient is not set",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1.5,
			},
			want: &commonpb.RetryPolicy{
				InitialIntervalInSeconds: 1,
				MaximumIntervalInSeconds: 100,
				BackoffCoefficient:       1.5,
				MaximumAttempts:          120,
			},
		},
		{
			name: "non-default Maximum attempts is not set",
			input: &commonpb.RetryPolicy{
				MaximumAttempts: 49,
			},
			want: &commonpb.RetryPolicy{
				InitialIntervalInSeconds: 1,
				MaximumIntervalInSeconds: 100,
				BackoffCoefficient:       2,
				MaximumAttempts:          49,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := EnsureRetryPolicyDefaults(tt.input, defaultRetryPolicy)
			assert.Equal(t, tt.want, got)
		})
	}
}
