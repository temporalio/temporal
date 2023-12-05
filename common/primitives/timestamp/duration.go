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
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

func DurationPtr(td time.Duration) *durationpb.Duration {
	return durationpb.New(td)
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

func DaysFromDuration(duration *durationpb.Duration) int {
	return int(duration.AsDuration().Hours() / 24)
}

func DaysInt32FromDuration(duration *durationpb.Duration) int32 {
	return int32(DaysFromDuration(duration))
}
