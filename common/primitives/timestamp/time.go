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

	"github.com/gogo/protobuf/types"

	"go.temporal.io/server/common/util"
)

// Timestamp provides easy conversions and utility comparison functions
// making go to proto time comparison straightforward
type Timestamp struct {
	// one of
	protoTime *types.Timestamp
	goTime    *time.Time
}

// TimestampFromProto returns a Timestamp from proto time
//noinspection GoNameStartsWithPackageName
func TimestampFromProto(ts *types.Timestamp) *Timestamp {
	// todo: should we validate against proto min/max time here?
	return &Timestamp{
		protoTime: &types.Timestamp{
			Seconds: ts.Seconds,
			Nanos:   ts.Nanos,
		},
	}
}

// TimestampFromTimePtr returns a Timestamp from a time.time
//noinspection GoNameStartsWithPackageName
func TimestampFromTime(t time.Time) *Timestamp {
	// todo: should we validate against proto min/max time here?
	c := time.Unix(0, t.UnixNano()).UTC()
	return &Timestamp{
		goTime: &c,
	}
}

// TimestampFromTimePtr returns a Timestamp from a time.time
//noinspection GoNameStartsWithPackageName
func TimestampFromTimePtr(t *time.Time) *Timestamp {
	// todo: should we validate against proto min/max time here?
	c := time.Unix(0, t.UnixNano()).UTC()
	return &Timestamp{
		goTime: &c,
	}
}

// TimestampNow returns a timestamp that represents Now()
//noinspection GoNameStartsWithPackageName
func TimestampNow() *Timestamp {
	return &Timestamp{
		protoTime: types.TimestampNow(),
	}
}

// TimestampNowAddSeconds returns a timestamp that represents Now() + some number of seconds
//noinspection GoNameStartsWithPackageName
func TimestampNowAddSeconds(seconds int64) *Timestamp {
	t := TimestampNow()
	t.protoTime.Seconds += seconds
	return t
}

// TimestampEpoch returns the unix epoch -TimestampFromTime(time.Unix(0, 0)).UTC()
//noinspection GoNameStartsWithPackageName
func TimestampEpoch() *Timestamp {
	epoch := time.Unix(0, 0).UTC()
	return TimestampFromTimePtr(&epoch)
}

// ToProto returns the proto representation
//noinspection GoReceiverNames
func (t *Timestamp) ToProto() *types.Timestamp {
	if t.protoTime != nil {
		return t.protoTime
	}

	proto, _ := types.TimestampProto(*t.goTime)
	return proto
}

// ToTime returns the time.time representation
//noinspection GoReceiverNames
func (t *Timestamp) ToTime() *time.Time {
	if t.goTime != nil {
		return t.goTime
	}

	goTime, _ := types.TimestampFromProto(t.protoTime)
	return &goTime
}

// Before returns true when t1 is after t2, false otherwise
//noinspection GoReceiverNames
func (t1 *Timestamp) After(t2 *Timestamp) bool {
	if t1.goTime != nil && t2.goTime != nil {
		// both go time
		return t1.goTime.After(*t2.goTime)

	}

	return t1.UnixNano() > t2.UnixNano()
}

// Before returns true when t1 is before t2, false otherwise
//noinspection GoReceiverNames
func (t1 *Timestamp) Before(t2 *Timestamp) bool {
	if t1.goTime != nil && t2.goTime != nil {
		// both go time
		return t1.goTime.Before(*t2.goTime)

	}

	return t1.UnixNano() < t2.UnixNano()
}

// SamesAs returns true when t1 is the same time as t2, false otherwise
//noinspection GoReceiverNames
func (t1 *Timestamp) SameAs(t2 *Timestamp) bool {
	if t1.goTime != nil && t2.goTime != nil {
		// both go time
		return t1.goTime.Equal(*t2.goTime)

	}

	return t1.UnixNano() == t2.UnixNano()
}

// UnixNano returns the int64 representation of nanoseconds since 1970 (see time.UnixNano)
//noinspection GoReceiverNames
func (t *Timestamp) UnixNano() int64 {
	if t.goTime != nil {
		return t.goTime.UnixNano()
	}

	return t.protoTime.Seconds*time.Second.Nanoseconds() + int64(t.protoTime.Nanos)
}

func TimePtr(t time.Time) *time.Time {
	return &t
}

func TimeValue(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func DurationValue(d *time.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return *d
}

func MinDurationPtr(d1 *time.Duration, d2 *time.Duration) *time.Duration {
	res := util.Min(DurationValue(d1), DurationValue(d2))
	return &res
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

func UnixOrZeroTimePtr(nanos int64) *time.Time {
	return TimePtr(UnixOrZeroTime(nanos))
}

func TimeNowPtrUtcAddDuration(t time.Duration) *time.Time {
	return TimePtr(time.Now().UTC().Add(t))
}

func TimeNowPtrUtcAddSeconds(seconds int) *time.Time {
	return TimePtr(time.Now().UTC().Add(time.Second * time.Duration(seconds)))
}

func TimeNowPtrUtc() *time.Time {
	return TimePtr(time.Now().UTC())
}
