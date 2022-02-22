// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
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
		{
			name: "timeout nonretryable error - valid type",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
				},
			},
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "timeout nonretryable error - unspecified type",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_UNSPECIFIED.String(),
				},
			},
			wantErr:       true,
			wantErrString: "Invalid timeout type value: Unspecified.",
		},
		{
			name: "timeout nonretryable error - unknown type",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + "unknown",
				},
			},
			wantErr:       true,
			wantErrString: "Invalid timeout type value: unknown.",
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
		InitialInterval:            time.Second,
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
		initialIntervalInSecondsConfigKey:   2,
		maximumIntervalCoefficientConfigKey: 100.0,
		backoffCoefficientConfigKey:         4.0,
		maximumAttemptsConfigKey:            5,
	}

	defaultSettings := FromConfigToDefaultRetrySettings(options)
	assert.Equal(t, 2*time.Second, defaultSettings.InitialInterval)
	assert.Equal(t, 100.0, defaultSettings.MaximumIntervalCoefficient)
	assert.Equal(t, 4.0, defaultSettings.BackoffCoefficient)
	assert.Equal(t, int32(5), defaultSettings.MaximumAttempts)
}

func TestIsContextDeadlineExceededErr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond)
	require.True(t, IsContextDeadlineExceededErr(ctx.Err()))
	require.True(t, IsContextDeadlineExceededErr(serviceerror.NewDeadlineExceeded("something")))

	require.False(t, IsContextDeadlineExceededErr(errors.New("some random error")))

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	require.False(t, IsContextDeadlineExceededErr(ctx.Err()))
}

func TestIsContextCanceledErr(t *testing.T) {
	require.True(t, IsContextCanceledErr(serviceerror.NewCanceled("something")))
	require.False(t, IsContextCanceledErr(errors.New("some random error")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, IsContextCanceledErr(ctx.Err()))
}

func TestOverrideWorkflowRunTimeout_InfiniteRunTimeout_InfiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(0)
	executionTimeout := time.Duration(0)
	require.Equal(t, time.Duration(0), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_FiniteRunTimeout_InfiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(10)
	executionTimeout := time.Duration(0)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_InfiniteRunTimeout_FiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(0)
	executionTimeout := time.Duration(10)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_FiniteRunTimeout_FiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(100)
	executionTimeout := time.Duration(10)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))

	runTimeout = time.Duration(10)
	executionTimeout = time.Duration(100)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowTaskTimeout_Infinite(t *testing.T) {
	taskTimeout := time.Duration(0)
	runTimeout := time.Duration(100)
	defaultTimeout := time.Duration(20)
	defaultTimeoutFn := dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(20), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(10)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(0)
	defaultTimeout = time.Duration(30)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(30), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(0)
	defaultTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, MaxWorkflowTaskStartToCloseTimeout, OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))
}

func TestOverrideWorkflowTaskTimeout_Finite(t *testing.T) {
	taskTimeout := time.Duration(10)
	runTimeout := MaxWorkflowTaskStartToCloseTimeout - time.Duration(1)
	defaultTimeout := time.Duration(20)
	defaultTimeoutFn := dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = MaxWorkflowTaskStartToCloseTimeout - time.Duration(1)
	runTimeout = time.Duration(10)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(10)
	runTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	runTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, MaxWorkflowTaskStartToCloseTimeout, OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))
}
