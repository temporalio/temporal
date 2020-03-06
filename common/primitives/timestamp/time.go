// Copyright (c) 2020 Temporal Technologies, Inc.
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
)

var (
	// This is the maximal time value we support
	maxValidTimeGo    = time.Unix(0, MaxValidTimeNanoseconds)
	maxValidTimestamp = TimestampFromTime(&maxValidTimeGo)
)

const MaxValidTimeNanoseconds = (2 ^ (64 - 1)) - 1

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

// TimestampFromTime returns a Timestamp from a time.time
//noinspection GoNameStartsWithPackageName
func TimestampFromTime(t *time.Time) *Timestamp {
	// todo: should we validate against proto min/max time here?
	c := time.Unix(0, t.UnixNano())
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

// TimestampEpoch returns the unix epoch - TimestampFromTime(time.Unix(0, 0))
//noinspection GoNameStartsWithPackageName
func TimestampEpoch() *Timestamp {
	epoch := time.Unix(0, 0)
	return TimestampFromTime(&epoch)
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
