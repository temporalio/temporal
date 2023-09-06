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

package timer

import (
	"errors"
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	// MaxAllowedTimer is the maximum allowed timer duration in the system
	// exported for integration tests
	MaxAllowedTimer = 100 * 365 * 24 * time.Hour
)

var (
	errNegativeDuration = errors.New("negative timer duration")
)

func ValidateAndCapTimer(delay *time.Duration) error {
	duration := timestamp.DurationValue(delay)
	if duration < 0 {
		return errNegativeDuration
	}

	// unix nano (max int64) is 2262-04-11T23:47:16.854775807Z
	// allowing 100 years timer is safe until 2162
	//
	// NOTE: we choose to cap the timer instead of returning error so that
	// existing workflows implementation using higher than allowed timer
	// can continue to run.
	if duration > MaxAllowedTimer {
		*delay = MaxAllowedTimer
	}
	return nil
}
