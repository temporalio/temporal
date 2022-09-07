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
	CompiledSpec struct {
		spec     *schedpb.ScheduleSpec
		tz       *time.Location
		calendar []*compiledCalendar
		excludes []*compiledCalendar
	}
)

func NewCompiledSpec(spec *schedpb.ScheduleSpec) (*CompiledSpec, error) {
	// make shallow copy so we can change some fields
	specCopy := *spec
	spec = &specCopy

	// parse CalendarSpecs to StructuredCalendarSpecs
	for _, cal := range spec.Calendar {
		structured, err := parseCalendarToStrucured(cal)
		if err != nil {
			return nil, err
		}
		spec.StructuredCalendar = append(spec.StructuredCalendar, structured)
	}
	spec.Calendar = nil

	// parse ExcludeCalendars
	for _, cal := range spec.ExcludeCalendar {
		structured, err := parseCalendarToStrucured(cal)
		if err != nil {
			return nil, err
		}
		spec.ExcludeStructuredCalendar = append(spec.ExcludeStructuredCalendar, structured)
	}
	spec.ExcludeCalendar = nil

	// parse CronStrings
	for _, cs := range spec.CronString {
		structured, interval, tz, err := parseCronString(cs)
		if err != nil {
			return nil, err
		}
		if tz != "" {
			if spec.TimezoneName != tz || spec.TimezoneData != nil {
				return nil, errConflictingTimezoneNames
			}
			spec.TimezoneName = tz
		}
		if structured != nil {
			spec.StructuredCalendar = append(spec.StructuredCalendar, structured)
		}
		if interval != nil {
			spec.Interval = append(spec.Interval, interval)
		}
	}
	spec.CronString = nil

	// validate intervals
	for _, interval := range spec.Interval {
		if err := validateInterval(interval); err != nil {
			return nil, err
		}
	}

	// load timezone
	tz, err := loadTimezone(spec)
	if err != nil {
		return nil, err
	}

	// compile StructuredCalendarSpecs
	ccs := make([]*compiledCalendar, len(spec.StructuredCalendar))
	for i, structured := range spec.StructuredCalendar {
		if ccs[i], err = newCompiledCalendar(structured, tz); err != nil {
			return nil, err
		}
	}

	// compile excludes
	excludes := make([]*compiledCalendar, len(spec.ExcludeStructuredCalendar))
	for i, excal := range spec.ExcludeStructuredCalendar {
		if excludes[i], err = newCompiledCalendar(excal, tz); err != nil {
			return nil, err
		}
	}

	cspec := &CompiledSpec{
		spec:     spec,
		tz:       tz,
		calendar: ccs,
		excludes: excludes,
	}

	return cspec, nil
}

func validateInterval(i *schedpb.IntervalSpec) error {
	iv, phase := timestamp.DurationValue(i.GetInterval()), timestamp.DurationValue(i.GetPhase())
	if iv < time.Second || phase < 0 || phase >= iv {
		return errMalformed
	}
	return nil
}

func loadTimezone(spec *schedpb.ScheduleSpec) (*time.Location, error) {
	if spec.TimezoneData != nil {
		return time.LoadLocationFromTZData(spec.TimezoneName, spec.TimezoneData)
	}
	return time.LoadLocation(spec.TimezoneName)
}

func (cs *CompiledSpec) CanonicalForm() *schedpb.ScheduleSpec {
	return cs.spec
}

// Returns the earliest time that matches the schedule spec that is after the given time.
// Returns: nominal is the time that matches, pre-jitter. next is the nominal time with
// jitter applied. has is false if there is no matching time.
func (cs *CompiledSpec) getNextTime(after time.Time) (nominal, next time.Time, has bool) {
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
func (cs *CompiledSpec) rawNextTime(after time.Time) (nominal time.Time) {
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
func (cs *CompiledSpec) nextIntervalTime(iv *schedpb.IntervalSpec, ts int64) int64 {
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
func (cs *CompiledSpec) excluded(nominal time.Time) bool {
	for _, excal := range cs.excludes {
		if excal.matches(nominal) {
			return true
		}
	}
	return false
}

// Adds jitter to a nominal time, deterministically (by hashing the given time).
func (cs *CompiledSpec) addJitter(nominal time.Time, maxJitter time.Duration) time.Time {
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
