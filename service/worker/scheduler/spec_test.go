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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

type specSuite struct {
	suite.Suite
}

func TestSpec(t *testing.T) {
	suite.Run(t, new(specSuite))
}

func (s *specSuite) checkSequenceRaw(spec *schedpb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := newCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		next := cs.rawNextTime(start)
		s.Equal(exp, next)
		start = next
	}
}

func (s *specSuite) checkSequenceFull(spec *schedpb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := newCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		_, next, has := cs.getNextTime(start)
		if exp.IsZero() {
			s.Require().False(has)
			break
		}
		s.Require().True(has)
		s.Equal(exp, next)
		start = next
	}
}

func (s *specSuite) TestSpecIntervalBasic() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: timestamp.DurationPtr(90 * time.Minute)},
			},
		},
		time.Date(2022, 3, 23, 12, 53, 2, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 30, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 00, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecIntervalPhase() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{
					Interval: timestamp.DurationPtr(90 * time.Minute),
					Phase:    timestamp.DurationPtr(5*time.Minute + 44*time.Second),
				},
			},
		},
		time.Date(2022, 3, 23, 12, 53, 02, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 05, 44, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecIntervalMultiple() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{
					Interval: timestamp.DurationPtr(90 * time.Minute),
					Phase:    timestamp.DurationPtr(5*time.Minute + 44*time.Second),
				},
				{
					Interval: timestamp.DurationPtr(157 * time.Minute),
					Phase:    timestamp.DurationPtr(22*time.Minute + 11*time.Second),
				},
			},
		},
		time.Date(2022, 3, 23, 12, 53, 02, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 26, 11, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 03, 11, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 19, 35, 44, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecCalendarBasic() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "5,7", Minute: "23"},
			},
		},
		time.Date(2022, 3, 23, 3, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 7, 23, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecCalendarMultiple() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "5,7", Minute: "23"},
				{Hour: "11,13", Minute: "55"},
			},
		},
		time.Date(2022, 3, 23, 3, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 13, 55, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecMixedCalendarInterval() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "11,13", Minute: "55"},
			},
			Interval: []*schedpb.IntervalSpec{
				{
					Interval: timestamp.DurationPtr(90 * time.Minute),
					Phase:    timestamp.DurationPtr(7 * time.Minute),
				},
			},
		},
		time.Date(2022, 3, 23, 10, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 10, 37, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 07, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 37, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 07, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecExclude() {
	s.checkSequenceFull(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: timestamp.DurationPtr(90 * time.Minute)},
			},
			ExcludeCalendar: []*schedpb.CalendarSpec{
				{
					Hour:   "12-14",
					Minute: "*",
					Second: "*",
				},
			},
			Jitter: timestamp.DurationPtr(1 * time.Second),
		},
		time.Date(2022, 3, 23, 8, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 9, 00, 0, 235000000, time.UTC),
		time.Date(2022, 3, 23, 10, 30, 0, 139000000, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 89000000, time.UTC),
		time.Date(2022, 3, 23, 16, 30, 0, 687000000, time.UTC),
	)
}

func (s *specSuite) TestSpecStartTime() {
	s.checkSequenceFull(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: timestamp.DurationPtr(90 * time.Minute)},
			},
			StartTime: timestamp.TimePtr(time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)),
			Jitter:    timestamp.DurationPtr(1 * time.Second),
		},
		time.Date(2022, 3, 23, 8, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 89000000, time.UTC),
	)
}

func (s *specSuite) TestSpecEndTime() {
	s.checkSequenceFull(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: timestamp.DurationPtr(90 * time.Minute)},
			},
			EndTime: timestamp.TimePtr(time.Date(2022, 3, 23, 14, 0, 0, 0, time.UTC)),
			Jitter:  timestamp.DurationPtr(1 * time.Second),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Time{}, // end of sequence
	)
}

func (s *specSuite) TestSpecBoundedJitter() {
	s.checkSequenceFull(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: timestamp.DurationPtr(90 * time.Minute)},
			},
			Jitter: timestamp.DurationPtr(24 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC),
		time.Date(2022, 3, 23, 14, 22, 54, 562000000, time.UTC),
		time.Date(2022, 3, 23, 15, 8, 3, 724000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSingleRun() {
	s.checkSequenceFull(
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC),
	)
	s.checkSequenceFull(
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
			Jitter: timestamp.DurationPtr(1 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC),
	)
}
