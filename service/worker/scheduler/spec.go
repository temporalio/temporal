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
)

const (
	defaultJitter = 1 * time.Second
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

func (cs *compiledSpec) getNextTime(
	state *schedpb.ScheduleState,
	after time.Time,
) (nominal, next time.Time, has bool) {
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

	// Ensure that jitter doesn't push this time past the _next_ nominal start time
	following := cs.rawNextTime(nominal)
	next = cs.addJitter(nominal, following.Sub(nominal))

	has = true
	return
}

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

func (cs *compiledSpec) excluded(nominal time.Time) bool {
	for _, excal := range cs.excludes {
		if excal.matches(nominal) {
			return true
		}
	}
	return false
}

func (cs *compiledSpec) addJitter(nominal time.Time, limit time.Duration) time.Time {
	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	if maxJitter == 0 {
		maxJitter = defaultJitter
	}
	if maxJitter > limit {
		maxJitter = limit
	}

	bin, err := nominal.MarshalBinary()
	if err != nil {
		return nominal
	}

	// we want to fit the result of a multiply in 64 bits, and use 32 bits of hash, which
	// leaves 32 bits for the range. if we use nanoseconds or microseconds, our range is
	// limited to only a few seconds or hours. using milliseconds supports up to 49 days.
	fp := int64(farm.Fingerprint32(bin))
	ms := maxJitter.Milliseconds()
	jitter := time.Duration((fp*ms)>>32) * time.Millisecond
	return nominal.Add(jitter)
}
