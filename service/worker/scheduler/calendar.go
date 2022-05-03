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
	"math"
	"strconv"
	"strings"
	"time"

	schedpb "go.temporal.io/api/schedule/v1"
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
	// minCalendarYear is the earliest year that will be recognized for calendar dates.
	minCalendarYear = 2000
	// maxCalendarYear is the latest year that will be recognized for calendar dates.
	// If you're still using Temporal in 2100 please change this constant and rebuild.
	maxCalendarYear = 2100
)

const (
	// Modes for parsing range strings: all modes accept decimal integers
	parseModeInt parseMode = iota
	// parseModeMonth also accepts month name prefixes (at least three letters)
	parseModeMonth
	// parseModeDow also accepts day-of-week prefixes (at least two letters)
	parseModeDow
)

var (
	errOutOfRange = errors.New("out of range")
	errMalformed  = errors.New("malformed expression")

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

func newCompiledCalendar(cal *schedpb.CalendarSpec, tz *time.Location) (*compiledCalendar, error) {
	cc := &compiledCalendar{tz: tz}
	var err error
	if cc.year, err = makeMatcher(cal.Year, "*", minCalendarYear, maxCalendarYear, parseModeInt); err != nil {
		return nil, err
	} else if cc.month, err = makeMatcher(cal.Month, "*", 1, 12, parseModeMonth); err != nil {
		return nil, err
	} else if cc.dayOfMonth, err = makeMatcher(cal.DayOfMonth, "*", 1, 31, parseModeInt); err != nil {
		return nil, err
	} else if cc.dayOfWeek, err = makeMatcher(cal.DayOfWeek, "*", 0, 7, parseModeDow); err != nil {
		return nil, err
	} else if cc.hour, err = makeMatcher(cal.Hour, "0", 0, 23, parseModeInt); err != nil {
		return nil, err
	} else if cc.minute, err = makeMatcher(cal.Minute, "0", 0, 59, parseModeInt); err != nil {
		return nil, err
	} else if cc.second, err = makeMatcher(cal.Second, "0", 0, 59, parseModeInt); err != nil {
		return nil, err
	}
	return cc, nil
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

// Returns a function that matches the given integer range/skip spec.
// The function _may_ return true for values that are out of range.
func makeMatcher(s, def string, min, max int, parseMode parseMode) (func(int) bool, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		s = def
	}
	// easy special cases
	if s == "*" {
		return func(int) bool { return true }, nil
	} else if s == "0" {
		return func(v int) bool { return v == 0 }, nil
	}
	if max <= 63 {
		return makeBitMatcher(s, min, max, parseMode)
	} else if max < math.MaxInt16 {
		return makeSliceMatcher(s, min, max, parseMode)
	}
	return nil, errOutOfRange
}

func makeBitMatcher(s string, min, max int, parseMode parseMode) (func(int) bool, error) {
	var bits uint64
	add := func(i int) { bits |= 1 << i }
	if err := parseStringSpec(s, min, max, parseMode, add); err != nil {
		return nil, err
	}
	if parseMode == parseModeDow {
		bits |= bits >> 7 // allow 7 or 0 for sunday
	}
	return func(v int) bool { return (1<<v)&bits != 0 }, nil
}

func makeSliceMatcher(s string, min, max int, parseMode parseMode) (func(int) bool, error) {
	var values []int16
	add := func(i int) { values = append(values, int16(i)) }
	if err := parseStringSpec(s, min, max, parseMode, add); err != nil {
		return nil, err
	}
	return func(v int) bool {
		for _, value := range values {
			if int(value) == v {
				return true
			}
		}
		return false
	}, nil
}

// Accepts strings of the form:
//   *        matches always
//   x        matches when the field equals x
//   x-z      matches when the field is between x and z inclusive
//   x-z/y    matches when the field is between x and z inclusive, skipping by y
//   x/y      matches when the field is between x and max inclusive, skipping by y
//   j,k,l    matches when the field is one of the listed values/ranges
// Each comma-separated value can be a range, and any range can have a skip value, e.g.:
//   1-5             matches 1,2,3,4,5
//   1-5/2           matches 1,3,5
//   3/5             matches 3,8,13,18,23,28 (assuming max=30)
//   1-5/2,8         matches 1,3,5,8
//   1-5/2,8-11      matches 1,3,5,8,9,10,11
//   1-5/2,8-16/3,2  matches 1,2,3,5,8,11,14
// Calls f for all values that should be considered matching. Values don't have to appear
// in order, and f may be called out of order as well.
// Handles day-of-week names or month names according to parseMode.
// min and max are the complete range of expected values.
func parseStringSpec(s string, min, max int, parseMode parseMode, f func(int)) error {
	for _, part := range strings.Split(s, ",") {
		var err error
		skipBy := 1
		hasSkipBy := false
		if strings.Contains(part, "/") {
			skipParts := strings.Split(part, "/")
			if len(skipParts) != 2 {
				return errMalformed
			}
			part = skipParts[0]
			skipBy, err = strconv.Atoi(skipParts[1])
			if err != nil {
				return err
			}
			if skipBy < 1 {
				return errMalformed
			}
			hasSkipBy = true
		}

		start, end := min, max
		if part != "*" {
			if strings.Contains(part, "-") {
				rangeParts := strings.Split(part, "-")
				if len(rangeParts) != 2 {
					return errMalformed
				}
				if start, err = parseValue(rangeParts[0], min, max, parseMode); err != nil {
					return err
				}
				if end, err = parseValue(rangeParts[1], start, max, parseMode); err != nil {
					return err
				}
			} else {
				if start, err = parseValue(part, min, max, parseMode); err != nil {
					return err
				}
				if !hasSkipBy {
					// if / is present, a single value is treated as that value to the
					// end. otherwise a single value is just the single value.
					end = start
				}
			}
		}

		for start <= end {
			f(start)
			start += skipBy
		}
	}
	return nil
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
