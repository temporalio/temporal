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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	parseMode int

	compiledCalendar struct {
		// Time zone that this calendar spec is interpreted in
		tz *time.Location

		// Matching predicates for each field (two for date). As in Go's time
		// library, months start on 1 == January, day of month starts at 1, day
		// of week starts at 0 == Sunday. A time matches this compiled calendar
		// when all fields match.
		year, month, dayOfMonth, dayOfWeek, hour, minute, second func(int) bool
	}
)

const (
	// minCalendarYear is the smallest year that can appear in a calendar spec.
	minCalendarYear = 2000
	// maxCalendarYear is the latest year that will be recognized for calendar dates.
	// If you're still using Temporal in 2100 please change this constant and rebuild.
	maxCalendarYear = 2100

	// max length of one calendar comment field
	maxCommentLen = 200
)

const (
	// Modes for parsing range strings: all modes accept decimal integers
	parseModeInt parseMode = iota
	// parseModeYear is like parseModeInt but returns an empty range for the default
	parseModeYear
	// parseModeMonth also accepts month name prefixes (at least three letters)
	parseModeMonth
	// parseModeDow also accepts day-of-week prefixes (at least two letters)
	parseModeDow
)

var (
	errOutOfRange               = errors.New("out of range")
	errConflictingTimezoneNames = errors.New("conflicting timezone names")

	monthStrings = []string{
		"january",
		"february",
		"march",
		"april",
		"may",
		"june",
		"july",
		"august",
		"september",
		"october",
		"november",
		"december",
	}

	dowStrings = []string{
		"sunday",
		"monday",
		"tuesday",
		"wednesday",
		"thursday",
		"friday",
		"saturday",
	}
)

func newCompiledCalendar(cal *schedpb.StructuredCalendarSpec, tz *time.Location) *compiledCalendar {
	return &compiledCalendar{
		tz:         tz,
		year:       makeYearMatcher(cal.Year),
		month:      makeBitMatcher(cal.Month),
		dayOfMonth: makeBitMatcher(cal.DayOfMonth),
		dayOfWeek:  makeBitMatcher(cal.DayOfWeek),
		hour:       makeBitMatcher(cal.Hour),
		minute:     makeBitMatcher(cal.Minute),
		second:     makeBitMatcher(cal.Second),
	}
}

// Returns true if the given time matches this calendar spec.
func (cc *compiledCalendar) matches(ts time.Time) bool {
	// set time zone
	ts = ts.In(cc.tz)

	// get ymdhms from ts
	y, mo, d := ts.Date()
	h, m, s := ts.Clock()

	return cc.year(y) &&
		cc.month(int(mo)) &&
		cc.dayOfMonth(d) &&
		cc.dayOfWeek(int(ts.Weekday())) &&
		cc.hour(h) &&
		cc.minute(m) &&
		cc.second(s)
}

// Returns the earliest time that matches this calendar spec that is after the given time.
// All times are considered to have 1 second resolution.
func (cc *compiledCalendar) next(ts time.Time) time.Time {
	// set time zone
	ts = ts.In(cc.tz)

	// get ymdhms from ts
	y, mo, d := ts.Date()
	h, m, s := ts.Clock()
	dstoffset := 0 * time.Hour
	if ts.Add(-time.Hour).Hour() == h {
		// we're in the second copy of a dst repeated hour
		dstoffset = 1 * time.Hour
	}

	// looking for first matching time after ts, so add 1 second
	s++

Outer:
	for {
		// normalize after carries
		if s >= 60 {
			m, s = m+1, 0
		}
		if m >= 60 {
			prev := time.Date(y, mo, d, h, 0, 0, 0, cc.tz)
			h, m = h+1, 0
			next := time.Date(y, mo, d, h, 0, 0, 0, cc.tz)
			// if we moved to the next hour but it's two hours later, then we skipped over
			// a dst repeated hour. try it again with an offset.
			if dstoffset == 0 && next.Sub(prev) > time.Hour {
				h = h - 1
				dstoffset = 1 * time.Hour
			} else {
				dstoffset = 0
			}
		}
		if h >= 24 {
			d, h = d+1, 0
		}
		if d > daysInMonth(mo, y) {
			mo, d = mo+1, 1
		}
		if mo > time.December {
			y, mo = y+1, time.January
		}
		if y > maxCalendarYear {
			break Outer
		}
		// try to match year, month, etc. from outside in
		if !cc.year(y) {
			y, mo, d, h, m, s = y+1, time.January, 1, 0, 0, 0
			dstoffset = 0
			continue Outer
		}
		for !cc.month(int(mo)) {
			mo, d, h, m, s = mo+1, 1, 0, 0, 0
			dstoffset = 0
			if mo > time.December {
				continue Outer
			}
		}
		for !cc.dayOfMonth(d) || !cc.dayOfWeek(int(time.Date(y, mo, d, h, m, s, 0, cc.tz).Weekday())) {
			d, h, m, s = d+1, 0, 0, 0
			dstoffset = 0
			if d > daysInMonth(mo, y) {
				continue Outer
			}
		}
		for !cc.hour(h) {
			h, m, s = h+1, 0, 0
			dstoffset = 0
			if h >= 24 {
				continue Outer
			}
		}
		for !cc.minute(m) {
			m, s = m+1, 0
			if m >= 60 {
				continue Outer
			}
		}
		for !cc.second(s) {
			s = s + 1
			if s >= 60 {
				continue Outer
			}
		}
		// everything matches
		nextTs := time.Date(y, mo, d, h, m, s, 0, cc.tz)
		// we might have reached a nonexistent time that got jumped over by a dst transition.
		// we can tell if the hour is different from what we think it should be.
		if nextTs.Hour() != h {
			h, m, s = h+1, 0, 0
			continue Outer
		}
		return nextTs.Add(dstoffset)
	}

	// no more matching times (up to max we checked)
	return time.Time{}
}

func parseCalendarToStructured(cal *schedpb.CalendarSpec) (*schedpb.StructuredCalendarSpec, error) {
	var errs []string
	makeRangeOrNil := func(s, field, def string, min, max int, parseMode parseMode) []*schedpb.Range {
		r, err := makeRange(s, field, def, min, max, parseMode)
		if err != nil {
			errs = append(errs, err.Error())
		}
		return r
	}
	ss := &schedpb.StructuredCalendarSpec{
		Second:     makeRangeOrNil(cal.Second, "Second", "0", 0, 59, parseModeInt),
		Minute:     makeRangeOrNil(cal.Minute, "Minute", "0", 0, 59, parseModeInt),
		Hour:       makeRangeOrNil(cal.Hour, "Hour", "0", 0, 23, parseModeInt),
		DayOfWeek:  makeRangeOrNil(cal.DayOfWeek, "DayOfWeek", "*", 0, 7, parseModeDow),
		DayOfMonth: makeRangeOrNil(cal.DayOfMonth, "DayOfMonth", "*", 1, 31, parseModeInt),
		Month:      makeRangeOrNil(cal.Month, "Month", "*", 1, 12, parseModeMonth),
		Year:       makeRangeOrNil(cal.Year, "Year", "*", minCalendarYear, maxCalendarYear, parseModeYear),
		Comment:    cal.Comment,
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, ", "))
	}
	return ss, nil
}

func parseCronString(c string) (*schedpb.StructuredCalendarSpec, *schedpb.IntervalSpec, string, error) {
	var tzName string
	var comment string

	c = strings.TrimSpace(c)

	// split out timezone
	if strings.HasPrefix(c, "TZ=") || strings.HasPrefix(c, "CRON_TZ=") {
		tz, rest, found := strings.Cut(c, " ")
		if !found {
			return nil, nil, "", errors.New("CronString has time zone but missing fields")
		}
		c = rest
		_, tzName, _ = strings.Cut(tz, "=")
	}

	// split out comment
	c, comment, _ = strings.Cut(c, "#")
	c = strings.TrimSpace(c)
	comment = strings.TrimSpace(comment)

	// handle @every intervals
	if strings.HasPrefix(c, "@every") {
		iv, err := parseCronStringInterval(c)
		return nil, iv, "", err
	}

	// handle @hourly, etc.
	c = handlePredefinedCronStrings(c)

	// split fields
	cal := schedpb.CalendarSpec{Comment: comment}
	fields := strings.Fields(c)
	switch len(fields) {
	case 5:
		cal.Minute, cal.Hour, cal.DayOfMonth, cal.Month, cal.DayOfWeek = fields[0], fields[1], fields[2], fields[3], fields[4]
	case 6:
		cal.Minute, cal.Hour, cal.DayOfMonth, cal.Month, cal.DayOfWeek, cal.Year = fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
	case 7:
		cal.Second, cal.Minute, cal.Hour, cal.DayOfMonth, cal.Month, cal.DayOfWeek, cal.Year = fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]
	default:
		return nil, nil, "", errors.New("CronString does not have 5-7 fields")
	}

	structured, err := parseCalendarToStructured(&cal)
	if err != nil {
		return nil, nil, "", err
	}

	return structured, nil, tzName, nil
}

func parseCronStringInterval(c string) (*schedpb.IntervalSpec, error) {
	// split after @every
	_, interval, found := strings.Cut(c, " ")
	if !found {
		return nil, errors.New("CronString does not have interval after @every")
	}
	// allow @every 14h/3h
	interval, phase, _ := strings.Cut(interval, "/")
	intervalDuration, err := timestamp.ParseDuration(interval)
	if err != nil {
		return nil, err
	}
	if phase == "" {
		return &schedpb.IntervalSpec{Interval: durationpb.New(intervalDuration)}, nil
	}
	phaseDuration, err := timestamp.ParseDuration(phase)
	if err != nil {
		return nil, err
	}
	return &schedpb.IntervalSpec{Interval: durationpb.New(intervalDuration), Phase: durationpb.New(phaseDuration)}, nil
}

func handlePredefinedCronStrings(c string) string {
	switch c {
	case "@yearly", "@annually":
		return "0 0 1 1 *"
	case "@monthly":
		return "0 0 1 * *"
	case "@weekly":
		return "0 0 * * 0"
	case "@daily", "@midnight":
		return "0 0 * * *"
	case "@hourly":
		return "0 * * * *"
	default:
		return c
	}
}

func makeBitMatcher(ranges []*schedpb.Range) func(int) bool {
	var bits uint64
	add := func(i int) { bits |= 1 << i }
	iterateRanges(ranges, add)
	return func(v int) bool { return (1<<v)&bits != 0 }
}

func makeYearMatcher(ranges []*schedpb.Range) func(int) bool {
	if len(ranges) == 0 {
		// special case for year: all is represented as empty range list
		return func(int) bool { return true }
	}

	var values []int16
	add := func(i int) { values = append(values, int16(i)) }
	iterateRanges(ranges, add)
	return func(v int) bool {
		for _, value := range values {
			if int(value) == v {
				return true
			}
		}
		return false
	}
}

func iterateRanges(ranges []*schedpb.Range, f func(i int)) {
	for _, r := range ranges {
		start, end, step := int(r.GetStart()), int(r.GetEnd()), int(r.GetStep())
		if step == 0 {
			step = 1
		}
		if end < start {
			end = start
		}
		for ; start <= end; start += step {
			f(start)
		}
	}
}

// Parses the string into a Range.
// Accepts strings of the form:
//
//   - *        matches always
//   - x        matches when the field equals x
//   - x-z      matches when the field is between x and z inclusive
//   - x-z/y    matches when the field is between x and z inclusive, skipping by y
//   - x/y      matches when the field is between x and max inclusive, skipping by y
//   - j,k,l    matches when the field is one of the listed values/ranges
//
// Each comma-separated value can be a range, and any range can have a skip value, e.g.:
//
// - 1-5             matches 1,2,3,4,5
// - 1-5/2           matches 1,3,5
// - 3/5             matches 3,8,13,18,23,28 (assuming max=30)
// - 1-5/2,8         matches 1,3,5,8
// - 1-5/2,8-11      matches 1,3,5,8,9,10,11
// - 1-5/2,8-16/3,2  matches 1,2,3,5,8,11,14
//
// Calls f for all values that should be considered matching. Values don't have to appear
// in order, and f may be called out of order as well.
// Handles day-of-week names or month names according to parseMode.
// min and max are the complete range of expected values.
func makeRange(s, field, def string, min, max int, parseMode parseMode) ([]*schedpb.Range, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		s = def
	}
	if s == "*" && parseMode == parseModeYear {
		return nil, nil // special case for year: all is represented as empty range list
	}
	var ranges []*schedpb.Range
	for _, part := range strings.Split(s, ",") {
		var err error
		step := 1
		hasStep := false
		if strings.Contains(part, "/") {
			skipParts := strings.Split(part, "/")
			if len(skipParts) != 2 {
				return nil, fmt.Errorf("%s has too many slashes", field)
			}
			part = skipParts[0]
			step, err = strconv.Atoi(skipParts[1])
			if err != nil {
				return nil, err
			}
			if step < 1 {
				return nil, fmt.Errorf("%s has invalid Step", field)
			}
			hasStep = true
		}

		start, end := min, max
		if part != "*" {
			if strings.Contains(part, "-") {
				rangeParts := strings.Split(part, "-")
				if len(rangeParts) != 2 {
					return nil, fmt.Errorf("%s has too many dashes", field)
				}
				if start, err = parseValue(rangeParts[0], min, max, parseMode); err != nil {
					return nil, fmt.Errorf("%s Start is not in range [%d-%d]", field, min, max)
				}
				if end, err = parseValue(rangeParts[1], start, max, parseMode); err != nil {
					return nil, fmt.Errorf("%s End is before Start or not in range [%d-%d]", field, min, max)
				}
			} else {
				if start, err = parseValue(part, min, max, parseMode); err != nil {
					return nil, fmt.Errorf("%s is not in range [%d-%d]", field, min, max)
				}
				if !hasStep {
					// if / is present, a single value is treated as that value to the
					// end. otherwise a single value is just the single value.
					end = start
				}
			}
		}
		// Special handling for Sunday: Turn "7" into "0", which may require an extra range.
		// Consider some cases:
		// 0-7 or 1-7   can turn into 0-6
		// 3-7          has to turn into 0,3-6
		// 3-7/3        can turn into 3-6/3  (7 doesn't match)
		// 1-7/2        has to turn into 0,1-6/2
		// That is, we can use a single range and just turn the 7 into a 6 only if step == 1
		// and start == 0 or 1. Or if 7 isn't actually included. In other cases, we can add a
		// 0, and then turn the 7 into a 6 in whatever the original range was. If the original
		// range was just 7-7, then we're done.
		if parseMode == parseModeDow && end == 7 {
			if (7-start)%step == 0 && (step > 1 || step == 1 && start > 1) {
				ranges = append(ranges, &schedpb.Range{Start: int32(0)})
				if start == 7 {
					continue
				}
			}
			end = 6
		}
		if start == end {
			end = 0 // use default value so proto is smaller
		}
		if step == 1 {
			step = 0 // use default value so proto is smaller
		}
		ranges = append(ranges, &schedpb.Range{Start: int32(start), End: int32(end), Step: int32(step)})
	}
	return ranges, nil
}

// Parses a single value (integer or day-of-week or month name).
func parseValue(s string, min, max int, parseMode parseMode) (int, error) {
	if parseMode == parseModeMonth {
		if len(s) >= 3 {
			s = strings.ToLower(s)
			for i, month := range monthStrings {
				if strings.HasPrefix(month, s) {
					i++
					if i < min || i > max {
						return i, errOutOfRange
					}
					return i, nil
				}
			}
		}
	} else if parseMode == parseModeDow {
		if len(s) >= 2 {
			s = strings.ToLower(s)
			for i, dow := range dowStrings {
				if strings.HasPrefix(dow, s) {
					if i < min || i > max {
						return i, errOutOfRange
					}
					return i, nil
				}
			}
		}
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return i, err
	}
	if i < min || i > max {
		return i, errOutOfRange
	}
	return i, nil
}

// same as Go's version
func isLeapYear(y int) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

func daysInMonth(m time.Month, y int) int {
	if m == time.February {
		if isLeapYear(y) {
			return 29
		} else {
			return 28
		}
	}
	const bits = 0b1010110101010
	return 30 + (bits>>m)&1
}
