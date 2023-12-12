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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TimePtr(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

func TimeValue(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func DurationValue(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func MinDurationPtr(d1 *durationpb.Duration, d2 *durationpb.Duration) *durationpb.Duration {
	res := min(DurationValue(d1), DurationValue(d2))
	return durationpb.New(res)
}

func RoundUp(d time.Duration) time.Duration {
	res := d.Truncate(time.Second)
	if res == d {
		return d
	}
	return res + time.Second
}

func UnixOrZeroTime(nanos int64) time.Time {
	if nanos <= 0 {
		nanos = 0
	}

	return time.Unix(0, nanos).UTC()
}

func UnixOrZeroTimePtr(nanos int64) *timestamppb.Timestamp {
	return TimePtr(UnixOrZeroTime(nanos))
}

func TimeNowPtrUtcAddDuration(t time.Duration) *timestamppb.Timestamp {
	return TimePtr(time.Now().UTC().Add(t))
}

func TimeNowPtrUtcAddSeconds(seconds int) *timestamppb.Timestamp {
	return TimePtr(time.Now().UTC().Add(time.Second * time.Duration(seconds)))
}

func TimeNowPtrUtc() *timestamppb.Timestamp {
	return TimePtr(time.Now().UTC())
}
