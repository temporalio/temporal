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
