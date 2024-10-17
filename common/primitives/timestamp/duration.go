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

package timestamp

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	errNegativeDuration = fmt.Errorf("negative duration")
	errMismatchedSigns  = fmt.Errorf("duration has seconds and nanos with different signs")
)

func DurationValue(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func DurationPtr(td time.Duration) *durationpb.Duration {
	return durationpb.New(td)
}

func MinDurationPtr(d1 *durationpb.Duration, d2 *durationpb.Duration) *durationpb.Duration {
	res := min(DurationValue(d1), DurationValue(d2))
	return durationpb.New(res)
}

func DurationFromSeconds(s int64) *durationpb.Duration {
	return durationMultipleOf(s, time.Second)
}

func DurationFromMinutes(m int64) *durationpb.Duration {
	return durationMultipleOf(m, time.Minute)
}

func DurationFromHours(h int64) *durationpb.Duration {
	return durationMultipleOf(h, time.Hour)
}

func DurationFromDays(d int32) *durationpb.Duration {
	return durationMultipleOf(int64(d), time.Hour*24)
}

func durationMultipleOf(amt int64, mult time.Duration) *durationpb.Duration {
	return DurationPtr(time.Duration(amt) * mult)
}

// ValidateProtoDuration checks protobuf durations for two conditions:
//  1. the seconds and nanos fields have the same sign (to avoid serialization issues)
//  2. the golang representation of the duration is not negative
//
// nil durations are considered valid because they will be treated as the zero value.
// durationpb.CheckValid cannot be used directly because it will return an error for
// very large durations but we are okay with truncating these. durationpb.AsDuration()
// caps the upper bound for timers at 10,000 years to prevent overflow.
func ValidateProtoDuration(d *durationpb.Duration) error {
	if d == nil {
		// nil durations are converted to 0 value
		return nil
	}

	if (d.GetSeconds() > 0 && d.GetNanos() < 0) || (d.GetSeconds() < 0 && d.GetNanos() > 0) {
		return errMismatchedSigns
	}

	// this is a best effort conversion and will return the closest value in the event of overflow.
	if d.AsDuration() < 0 {
		return errNegativeDuration
	}

	return nil
}
