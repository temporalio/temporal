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

package dynamicconfig

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/number"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	defaultInitialInterval            = time.Second
	defaultMaximumIntervalCoefficient = 100.0
	defaultBackoffCoefficient         = 2.0
	defaultMaximumAttempts            = 0

	initialIntervalInSecondsConfigKey   = "InitialIntervalInSeconds"
	maximumIntervalCoefficientConfigKey = "MaximumIntervalCoefficient"
	backoffCoefficientConfigKey         = "BackoffCoefficient"
	maximumAttemptsConfigKey            = "MaximumAttempts"
)

func GetDefaultRetryPolicyConfigOptions() map[string]any {
	return map[string]any{
		initialIntervalInSecondsConfigKey:   int(defaultInitialInterval.Seconds()),
		maximumIntervalCoefficientConfigKey: defaultMaximumIntervalCoefficient,
		backoffCoefficientConfigKey:         defaultBackoffCoefficient,
		maximumAttemptsConfigKey:            defaultMaximumAttempts,
	}
}

// DefaultRetrySettings indicates what the "default" retry settings
// are if it is not specified on an Activity or for any unset fields
// if a policy is explicitly set on a workflow
type DefaultRetrySettings struct {
	InitialInterval            time.Duration
	MaximumIntervalCoefficient float64
	BackoffCoefficient         float64
	MaximumAttempts            int32
}

func FromConfigToDefaultRetrySettings(options map[string]interface{}) DefaultRetrySettings {
	defaultSettings := DefaultRetrySettings{
		InitialInterval: defaultInitialInterval,
		MaximumAttempts: defaultMaximumAttempts,
	}

	if seconds, ok := options[initialIntervalInSecondsConfigKey]; ok {
		defaultSettings.InitialInterval = time.Duration(
			number.NewNumber(
				seconds,
			).GetIntOrDefault(int(defaultInitialInterval.Nanoseconds())),
		) * time.Second
	}

	if coefficient, ok := options[maximumIntervalCoefficientConfigKey]; ok {
		defaultSettings.MaximumIntervalCoefficient = number.NewNumber(
			coefficient,
		).GetFloatOrDefault(defaultMaximumIntervalCoefficient)
	}

	if coefficient, ok := options[backoffCoefficientConfigKey]; ok {
		defaultSettings.BackoffCoefficient = number.NewNumber(
			coefficient,
		).GetFloatOrDefault(defaultBackoffCoefficient)
	}

	if attempts, ok := options[maximumAttemptsConfigKey]; ok {
		defaultSettings.MaximumAttempts = int32(number.NewNumber(
			attempts,
		).GetIntOrDefault(defaultMaximumAttempts))
	}

	return defaultSettings
}

// EnsureRetryPolicyDefaults ensures the policy subfields, if not explicitly set, are set to the specified defaults
func EnsureRetryPolicyDefaults(originalPolicy *commonpb.RetryPolicy, defaultSettings DefaultRetrySettings) {
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
