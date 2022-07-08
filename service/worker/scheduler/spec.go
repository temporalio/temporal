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

package scheduler

import (
	"math"
	"time"

	"github.com/dgryski/go-farm"

	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
)

type (
	compiledSpec struct {
		spec     *schedpb.ScheduleSpec
		tz       *time.Location
		calendar []*compiledCalendar
		excludes []*compiledCalendar
	}
)

func newCompiledSpec(spec *schedpb.ScheduleSpec) (*compiledSpec, error) {
	tz, err := loadTimezone(spec)
	if err != nil {
		return nil, err
	}

	cspec := &compiledSpec{
		spec:     spec,
		tz:       tz,
		calendar: make([]*compiledCalendar, len(spec.Calendar)),
		excludes: make([]*compiledCalendar, len(spec.ExcludeCalendar)),
	}

	for i, cal := range spec.Calendar {
		if cspec.calendar[i], err = newCompiledCalendar(cal, tz); err != nil {
			return nil, err
		}
	}
	for i, excal := range spec.ExcludeCalendar {
		if cspec.excludes[i], err = newCompiledCalendar(excal, tz); err != nil {
			return nil, err
		}
	}

	return cspec, nil
}

func loadTimezone(spec *schedpb.ScheduleSpec) (*time.Location, error) {
	if spec.TimezoneData != nil {
		return time.LoadLocationFromTZData(spec.TimezoneName, spec.TimezoneData)
	}
	return time.LoadLocation(spec.TimezoneName)
}

// Returns the earliest time that matches the schedule spec that is after the given time.
// Returns: nominal is the time that matches, pre-jitter. next is the nominal time with
// jitter applied. has is false if there is no matching time.
func (cs *compiledSpec) getNextTime(after time.Time) (nominal, next time.Time, has bool) {
	// If we're starting before the schedule's allowed time range, jump up to right before
	// it (so that we can still return the first second of the range if it happens to match).
	if cs.spec.StartTime != nil && after.Before(*cs.spec.StartTime) {
		after = cs.spec.StartTime.Add(-time.Second)
	}

	for {
		nominal = cs.rawNextTime(after)

		if nominal.IsZero() || (cs.spec.EndTime != nil && nominal.After(*cs.spec.EndTime)) {
			has = false
			return
		}

		// check against excludes
		if !cs.excluded(nominal) {
			break
		}

		after = nominal
	}

	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	// Ensure that jitter doesn't push this time past the _next_ nominal start time
	if following := cs.rawNextTime(nominal); !following.IsZero() {
		maxJitter = util.Min(maxJitter, following.Sub(nominal))
	}
	next = cs.addJitter(nominal, maxJitter)

	has = true
	return
}

// Returns the next matching time (without jitter), or the zero value if no time matches.
func (cs *compiledSpec) rawNextTime(after time.Time) (nominal time.Time) {
	var minTimestamp int64 = math.MaxInt64 // unix seconds-since-epoch as int64

	for _, cal := range cs.calendar {
		if next := cal.next(after); !next.IsZero() {
			nextTs := next.Unix()
			if nextTs < minTimestamp {
				minTimestamp = nextTs
			}
		}
	}

	ts := after.Unix()
	for _, iv := range cs.spec.Interval {
		next := cs.nextIntervalTime(iv, ts)
		if next < minTimestamp {
			minTimestamp = next
		}
	}

	if minTimestamp == math.MaxInt64 {
		return time.Time{}
	}
	return time.Unix(minTimestamp, 0).UTC()
}

// Returns the next matching time for a single interval spec.
func (cs *compiledSpec) nextIntervalTime(iv *schedpb.IntervalSpec, ts int64) int64 {
	interval := int64(timestamp.DurationValue(iv.Interval) / time.Second)
	if interval < 1 {
		interval = 1
	}
	phase := int64(timestamp.DurationValue(iv.Phase) / time.Second)
	if phase < 0 {
		phase = 0
	}
	return (((ts-phase)/interval)+1)*interval + phase
}

// Returns true if any exclude spec matches the time.
func (cs *compiledSpec) excluded(nominal time.Time) bool {
	for _, excal := range cs.excludes {
		if excal.matches(nominal) {
			return true
		}
	}
	return false
}

// Adds jitter to a nominal time, deterministically (by hashing the given time).
func (cs *compiledSpec) addJitter(nominal time.Time, maxJitter time.Duration) time.Time {
	if maxJitter < 0 {
		maxJitter = 0
	}

	bin, err := nominal.MarshalBinary()
	if err != nil {
		return nominal
	}

	// we want to fit the result of a multiply in 64 bits, and use 32 bits of hash, which
	// leaves 32 bits for the range. if we use nanoseconds or microseconds, our range is
	// limited to only a few seconds or hours. using milliseconds supports up to 49 days.
	fp := uint64(farm.Fingerprint32(bin))
	ms := uint64(maxJitter.Milliseconds())
	if ms > math.MaxUint32 {
		ms = math.MaxUint32
	}
	jitter := time.Duration((fp*ms)>>32) * time.Millisecond
	return nominal.Add(jitter)
}
