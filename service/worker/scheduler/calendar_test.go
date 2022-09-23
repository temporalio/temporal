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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

type calendarSuite struct {
	suite.Suite
	*require.Assertions
}

func TestCalendar(t *testing.T) {
	suite.Run(t, new(calendarSuite))
}

func (s *calendarSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *calendarSuite) mustCompileCalendarSpec(cal *schedpb.CalendarSpec, tz *time.Location) *compiledCalendar {
	scs, err := parseCalendarToStructured(cal)
	s.NoError(err)
	s.NotNil(scs)
	s.NoError(validateStructuredCalendar(scs))
	cc := newCompiledCalendar(scs, tz)
	return cc
}

func (s *calendarSuite) TestCalendarMatch() {
	pacific, err := time.LoadLocation("US/Pacific")
	s.NoError(err)

	// default is midnight once a day
	cc := s.mustCompileCalendarSpec(&schedpb.CalendarSpec{}, time.UTC)
	s.True(cc.matches(time.Date(2022, time.March, 17, 0, 0, 0, 0, time.UTC)))
	s.True(cc.matches(time.Date(2022, time.March, 18, 0, 0, 0, 0, time.UTC)))
	s.False(cc.matches(time.Date(2022, time.March, 18, 5, 15, 0, 0, time.UTC)))

	// match another tz
	s.False(cc.matches(time.Date(2022, time.March, 17, 0, 0, 0, 0, pacific)))
	s.True(cc.matches(time.Date(2022, time.March, 17, 17, 0, 0, 0, pacific)))

	cc = s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
		Minute: "5,9",
		Hour:   "*/2",
	}, time.UTC)
	s.True(cc.matches(time.Date(2022, time.March, 17, 14, 5, 0, 0, time.UTC)))
	s.False(cc.matches(time.Date(2022, time.March, 17, 14, 5, 33, 0, time.UTC)))
	s.False(cc.matches(time.Date(2022, time.March, 17, 14, 15, 0, 0, time.UTC)))
	s.True(cc.matches(time.Date(2022, time.March, 18, 3, 9, 0, 0, pacific)))
	s.False(cc.matches(time.Date(2022, time.March, 18, 3, 9, 0, 0, time.UTC)))

	cc = s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
		Second:     "55",
		Minute:     "55",
		Hour:       "5",
		DayOfWeek:  "wed-thurs",
		DayOfMonth: "2/2",
	}, pacific)
	s.False(cc.matches(time.Date(2022, time.March, 9, 5, 55, 55, 0, pacific)))
	s.True(cc.matches(time.Date(2022, time.March, 10, 5, 55, 55, 0, pacific)))
	s.False(cc.matches(time.Date(2022, time.March, 14, 5, 55, 55, 0, pacific)))
	s.True(cc.matches(time.Date(2022, time.March, 16, 5, 55, 55, 0, pacific)))
	s.False(cc.matches(time.Date(2022, time.February, 9, 5, 55, 55, 0, pacific)))
	s.True(cc.matches(time.Date(2022, time.February, 10, 5, 55, 55, 0, pacific)))
	s.False(cc.matches(time.Date(2022, time.February, 10, 1, 55, 55, 0, pacific)))
	// match another zone
	s.True(cc.matches(time.Date(2022, time.March, 10, 13, 55, 55, 0, time.UTC)))
	// offset changes between march 10 and 16
	s.False(cc.matches(time.Date(2022, time.March, 16, 13, 55, 55, 0, time.UTC)))
	// correct offset
	s.True(cc.matches(time.Date(2022, time.March, 16, 12, 55, 55, 0, time.UTC)))

	// different sunday representations
	for _, dow := range []string{"0", "7", "sun", "*", "0-3", "5-7", "5-7/2", "6-7", "2-7/5", "0-7/7", "0/7"} {
		cc = s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
			DayOfWeek: dow,
		}, time.UTC)
		s.True(cc.matches(time.Date(2022, time.March, 6, 0, 0, 0, 0, time.UTC)), dow) // sunday
	}
	for _, dow := range []string{"*", "5-7", "6-7", "2/4", "2-7/4"} {
		cc = s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
			DayOfWeek: dow,
		}, time.UTC)
		s.True(cc.matches(time.Date(2022, time.March, 5, 0, 0, 0, 0, time.UTC)), dow) // saturday
	}
	for _, dow := range []string{"0", "7", "sun", "5-7", "5-7/2", "6-7", "2-7/5", "0-7/7", "0/7"} {
		cc = s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
			DayOfWeek: dow,
		}, time.UTC)
		s.False(cc.matches(time.Date(2022, time.March, 7, 0, 0, 0, 0, time.UTC)), dow) // monday
	}
}

func (s *calendarSuite) TestParseCronString() {
	scs, iv, tz, err := parseCronString("5,9 */2 * * *")
	s.NoError(err)
	s.Equal(&schedpb.StructuredCalendarSpec{
		Second:     []*schedpb.Range{{Start: 0}},
		Minute:     []*schedpb.Range{{Start: 5}, {Start: 9}},
		Hour:       []*schedpb.Range{{Start: 0, End: 23, Step: 2}},
		DayOfMonth: []*schedpb.Range{{Start: 1, End: 31}},
		Month:      []*schedpb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedpb.Range{{Start: 0, End: 6}},
	}, scs)
	s.Nil(iv)
	s.Equal("", tz)

	_, _, _, err = parseCronString("0 1 2 3 4 1999")
	s.ErrorContains(err, "Year is not in range")

	scs, iv, tz, err = parseCronString("CRON_TZ=US/Pacific 55 55,57 5 2/2 * wed-thurs *  # explanation")
	s.NoError(err)
	s.Equal(&schedpb.StructuredCalendarSpec{
		Second:     []*schedpb.Range{{Start: 55}},
		Minute:     []*schedpb.Range{{Start: 55}, {Start: 57}},
		Hour:       []*schedpb.Range{{Start: 5}},
		DayOfMonth: []*schedpb.Range{{Start: 2, End: 31, Step: 2}},
		Month:      []*schedpb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedpb.Range{{Start: 3, End: 4}},
		Comment:    "explanation",
	}, scs)
	s.Nil(iv)
	s.Equal("US/Pacific", tz)

	scs, iv, tz, err = parseCronString("@monthly")
	s.NoError(err)
	s.Equal(&schedpb.StructuredCalendarSpec{
		Second:     []*schedpb.Range{{Start: 0}},
		Minute:     []*schedpb.Range{{Start: 0}},
		Hour:       []*schedpb.Range{{Start: 0}},
		DayOfMonth: []*schedpb.Range{{Start: 1}},
		Month:      []*schedpb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedpb.Range{{Start: 0, End: 6}},
	}, scs)
	s.Nil(iv)
	s.Equal("", tz)

	scs, iv, tz, err = parseCronString("@every 5d")
	s.NoError(err)
	s.Nil(scs)
	s.Equal(&schedpb.IntervalSpec{
		Interval: timestamp.DurationPtr(5 * 24 * time.Hour),
	}, iv)
	s.Equal("", tz)

	scs, iv, tz, err = parseCronString("@every 5h/45m")
	s.NoError(err)
	s.Nil(scs)
	s.Equal(&schedpb.IntervalSpec{
		Interval: timestamp.DurationPtr(5 * time.Hour),
		Phase:    timestamp.DurationPtr(45 * time.Minute),
	}, iv)
	s.Equal("", tz)
}

func (s *calendarSuite) TestCalendarNextBasic() {
	pacific, err := time.LoadLocation("US/Pacific")
	s.NoError(err)

	cc := s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
		Second:     "55",
		Minute:     "55",
		Hour:       "5",
		DayOfWeek:  "wed-thurs",
		DayOfMonth: "2/2",
	}, pacific)
	// only increment second
	next := cc.next(time.Date(2022, time.March, 2, 5, 55, 33, 0, pacific))
	s.True(time.Date(2022, time.March, 2, 5, 55, 55, 0, pacific).Equal(next))
	// increment minute, second
	next = cc.next(time.Date(2022, time.March, 2, 5, 33, 33, 0, pacific))
	s.True(time.Date(2022, time.March, 2, 5, 55, 55, 0, pacific).Equal(next))
	// increment hour, minute, second
	next = cc.next(time.Date(2022, time.March, 2, 3, 33, 33, 0, pacific))
	s.True(time.Date(2022, time.March, 2, 5, 55, 55, 0, pacific).Equal(next))
	// increment days
	next = cc.next(time.Date(2022, time.March, 1, 1, 11, 11, 0, pacific))
	s.True(time.Date(2022, time.March, 2, 5, 55, 55, 0, pacific).Equal(next))
	// from exact match
	next = cc.next(time.Date(2022, time.March, 2, 5, 55, 55, 0, pacific))
	s.True(time.Date(2022, time.March, 10, 5, 55, 55, 0, pacific).Equal(next))
	// crossing dst but not near it
	next = cc.next(time.Date(2022, time.March, 10, 5, 55, 55, 0, pacific))
	s.True(time.Date(2022, time.March, 16, 5, 55, 55, 0, pacific).Equal(next))
}

func (s *calendarSuite) TestGoDSTBehavior() {
	pacific, err := time.LoadLocation("US/Pacific")
	s.NoError(err)
	// The time package's behavior when given a nonexistent time like 2022-03-13T02:33 is
	// to return the previous hour. We depend on this, so check it here.
	t1 := time.Date(2022, time.March, 13, 1, 33, 33, 0, pacific)
	t2 := time.Date(2022, time.March, 13, 2, 33, 33, 0, pacific)
	s.True(t1.Equal(t2))
}

func (s *calendarSuite) checkSequence(cs string, start time.Time, seq ...time.Time) {
	s.T().Helper()
	scs, _, tzName, err := parseCronString(cs)
	s.NoError(err)
	tz, err := time.LoadLocation(tzName)
	s.NoError(err)
	cc := newCompiledCalendar(scs, tz)
	for _, exp := range seq {
		next := cc.next(start)
		s.True(exp.Equal(next))
		// every second between start and next should also end up at next
		for ts := start.Unix(); ts < next.Unix(); ts++ {
			s.True(exp.Equal(cc.next(time.Unix(ts, 0))), "failed on %v", ts)
		}
		start = next
	}
}

func (s *calendarSuite) TestCalendarNextDST() {
	pacific, err := time.LoadLocation("US/Pacific") // switches at 2am
	s.NoError(err)

	// spring forward
	s.checkSequence(
		"CRON_TZ=US/Pacific 33 33 2 * * * *",
		time.Date(2022, time.March, 11, 20, 0, 0, 0, pacific),
		time.Date(2022, time.March, 12, 2, 33, 33, 0, pacific),
		// march 13 has no 2:33:33
		time.Date(2022, time.March, 14, 2, 33, 33, 0, pacific),
	)

	// jump back
	s.checkSequence(
		"CRON_TZ=US/Pacific 33 33,44 1 * * * *",
		time.Date(2021, time.November, 7, 0, 15, 15, 0, pacific),
		time.Date(2021, time.November, 7, 1, 33, 33, 0, pacific),
		time.Date(2021, time.November, 7, 1, 44, 33, 0, pacific),
		// nov 7 has two 1:33:33s and 1:44:33s
		time.Date(2021, time.November, 7, 1, 33, 33, 0, pacific).Add(time.Hour),
		time.Date(2021, time.November, 7, 1, 44, 33, 0, pacific).Add(time.Hour),
		time.Date(2021, time.November, 8, 1, 33, 33, 0, pacific),
	)
}

func (s *calendarSuite) TestCalendarDSTStartInRepeatedHourButNotEnd() {
	loc, err := time.LoadLocation("Europe/London")
	s.NoError(err)
	cc := s.mustCompileCalendarSpec(&schedpb.CalendarSpec{
		Second:     "0",
		Minute:     "1",
		Hour:       "0",
		DayOfMonth: "2",
		Month:      "Jan",
		DayOfWeek:  "Sun",
	}, loc)
	next := cc.next(time.Date(2004, time.January, 1, 0, 0, 5, 0, loc))
	s.True(time.Date(2005, time.January, 2, 0, 1, 0, 0, loc).Equal(next))
	next = cc.next(time.Date(2004, time.October, 31, 1, 7, 3, 0, loc))
	s.True(time.Date(2005, time.January, 2, 0, 1, 0, 0, loc).Equal(next))
}

func (s *calendarSuite) TestMakeMatcher() {
	check := func(str string, min, max int, parseMode parseMode, expected ...int) {
		s.T().Helper()
		ranges, err := makeRange(str, "Test", str, min, max, parseMode)
		s.NoError(err)
		var m func(int) bool
		if max < 63 {
			m = makeBitMatcher(ranges)
		} else {
			m = makeYearMatcher(ranges)
		}
		for _, e := range expected {
			if e >= 0 {
				s.True(m(e), e)
			} else {
				s.False(m(-e), -e)
			}
		}
	}

	check("1,3,5-7", 0, 10, parseModeInt, 1, -2, 3, -4, 5, 6, 7, -8, -9, -10)
	check("2-4,8,1-5/2", 1, 15, parseModeInt, 1, 2, 3, 4, 5, -6, -7, 8, -9, -10, -11, -12, -13, -14, -15)
	check("February/5", 1, 12, parseModeMonth, -1, 2, -3, -4, -5, -6, 7, -8, -9, -10, -11, 12)
	check("*", 0, 7, parseModeDow, 0, 1, 2, 3, 4, 5, 6)
	check("0", 0, 7, parseModeDow, 0, -1, -2, -3, -4, -5, -6)
	check("6,7", 0, 7, parseModeDow, 0, -1, -2, -3, -4, -5, 6) // 7 means sunday (0)
	check("2020-2022,2024,2026/3", 2000, 2100, parseModeInt, -2019, 2020, 2021, 2022, -2023, 2024, -2025, 2026, -2027, -2028, 2029, -2030, 2032, 2098, -2101)
}

func (s *calendarSuite) TestMakeRange() {
	check := func(str string, min, max int, parseMode parseMode, expected ...*schedpb.Range) {
		s.T().Helper()
		ranges, err := makeRange(str, "Test", "", min, max, parseMode)
		s.NoError(err)
		s.EqualValues(expected, ranges)
	}
	checkErr := func(str string, min, max int, parseMode parseMode, expectedErr string) {
		s.T().Helper()
		_, err := makeRange(str, "Test", "", min, max, parseMode)
		s.ErrorContains(err, expectedErr)
	}

	check("13", 0, 59, parseModeInt, &schedpb.Range{Start: 13})
	checkErr("133", 0, 59, parseModeInt, "Test is not in range [0-59]")
	check("Sept", 1, 12, parseModeMonth, &schedpb.Range{Start: 9})
	check("13,18", 0, 59, parseModeInt, &schedpb.Range{Start: 13}, &schedpb.Range{Start: 18})
	check("13,18,44", 0, 59, parseModeInt, &schedpb.Range{Start: 13}, &schedpb.Range{Start: 18}, &schedpb.Range{Start: 44})
	checkErr("13,18,44,", 0, 59, parseModeInt, "Test is not in range") // not the most helpful error in this case but it has the field name
	check("13-18", 0, 59, parseModeInt, &schedpb.Range{Start: 13, End: 18})
	checkErr("18-13", 0, 59, parseModeInt, "End is before Start")
	checkErr("1,3,18-13", 0, 59, parseModeInt, "End is before Start")
	check("2-5,7-9,11", 0, 59, parseModeInt, &schedpb.Range{Start: 2, End: 5}, &schedpb.Range{Start: 7, End: 9}, &schedpb.Range{Start: 11})
	check("*", 5, 9, parseModeInt, &schedpb.Range{Start: 5, End: 9})
	check("*/3", 5, 9, parseModeInt, &schedpb.Range{Start: 5, End: 9, Step: 3})
	check("2/3", 0, 10, parseModeInt, &schedpb.Range{Start: 2, End: 10, Step: 3})
	checkErr("2/3/5", 0, 10, parseModeInt, "too many slashes")
	check("2-6/3", 0, 10, parseModeInt, &schedpb.Range{Start: 2, End: 6, Step: 3})
	check("2-6/4,7-8", 0, 10, parseModeInt, &schedpb.Range{Start: 2, End: 6, Step: 4}, &schedpb.Range{Start: 7, End: 8})
	check("mon-Friday", 0, 7, parseModeDow, &schedpb.Range{Start: 1, End: 5})
	checkErr("Fri-Tues", 0, 7, parseModeDow, "End is before Start")
	checkErr("1-5-7", 0, 7, parseModeDow, "too many dashes")
	checkErr("monday-", 0, 7, parseModeDow, "End is before Start")
}

func (s *calendarSuite) TestParseValue() {
	i, err := parseValue("1", 1, 10, parseModeInt)
	s.NoError(err)
	s.Equal(1, i)

	i, err = parseValue("29", 1, 30, parseModeInt)
	s.NoError(err)
	s.Equal(29, i)

	_, err = parseValue("29", 1, 12, parseModeInt)
	s.Error(err)

	_, err = parseValue("random text", 1, 31, parseModeInt)
	s.Error(err)

	i, err = parseValue("fri", 0, 7, parseModeDow)
	s.NoError(err)
	s.Equal(5, i)

	i, err = parseValue("August", 1, 12, parseModeMonth)
	s.NoError(err)
	s.Equal(8, i)
}

func (s *calendarSuite) TestDaysInMonth() {
	s.Equal(31, daysInMonth(time.January, 2022))
	s.Equal(28, daysInMonth(time.February, 2022))
	s.Equal(29, daysInMonth(time.February, 2024))
	s.Equal(29, daysInMonth(time.February, 2000))
	s.Equal(28, daysInMonth(time.February, 2100))
	s.Equal(31, daysInMonth(time.March, 2022))
	s.Equal(30, daysInMonth(time.April, 2022))
	s.Equal(31, daysInMonth(time.May, 2022))
	s.Equal(30, daysInMonth(time.June, 2022))
	s.Equal(31, daysInMonth(time.July, 2022))
	s.Equal(31, daysInMonth(time.August, 2022))
	s.Equal(30, daysInMonth(time.September, 2022))
	s.Equal(31, daysInMonth(time.October, 2022))
	s.Equal(30, daysInMonth(time.November, 2022))
	s.Equal(31, daysInMonth(time.December, 2022))
}

func FuzzCalendar(f *testing.F) {
	// partially random selection but including at least one with dst
	// transitions at midnight
	zones := []string{
		"UTC",
		"US/Pacific",
		"America/Montreal",
		"Asia/Urumqi",
		"Asia/Beirut",
		"America/Indiana/Knox",
		"Africa/Kinshasa",
		"America/Asuncion",
		"Europe/London",
		"Asia/Vientiane",
		"Cuba",
	}
	f.Fuzz(func(t *testing.T, s, m, h, dom, mo, y, dow string, tz uint, start int64) {
		name := zones[tz%uint(len(zones))]
		loc, err := time.LoadLocation(name)
		if err != nil {
			return
		}
		cal := &schedpb.CalendarSpec{
			Second:     s,
			Minute:     m,
			Hour:       h,
			DayOfMonth: dom,
			Month:      mo,
			Year:       y,
			DayOfWeek:  dow,
		}
		scs, err := parseCalendarToStructured(cal)
		if err != nil {
			return
		}
		cc := newCompiledCalendar(scs, loc)
		now := time.Unix(start, 0).In(loc)
		next := cc.next(now)
		if next.IsZero() {
			return
		}
		if next.Before(now) {
			t.Errorf("next %v not before now %v (for %+v)", next, now, cal)
		}
		gap := int(next.Sub(now) / time.Second)
		for i := 0; i < 1000; i++ {
			ts1 := now.Add(time.Duration(rand.Intn(gap)) * time.Second)
			if !cc.next(ts1).Equal(next) {
				t.Errorf("next(%v) = %v should equal next(%v) = %v (for %+v)", ts1, cc.next(ts1), now, next, cal)
			}
		}
		for ts1 := next; ts1.Sub(next) < 5*time.Hour; ts1 = ts1.Add(1234 * time.Second) {
			if !cc.next(ts1).After(next) {
				t.Errorf("next(%v) = %v should be after next(%v) = %v (for %+v)", ts1, cc.next(ts1), now, next, cal)
			}
		}
	})
}
