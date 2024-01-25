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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	schedpb "go.temporal.io/api/schedule/v1"

	"go.temporal.io/server/common/testing/protorequire"
)

type specSuite struct {
	suite.Suite
	protorequire.ProtoAssertions

	specBuilder *SpecBuilder
}

func TestSpec(t *testing.T) {
	suite.Run(t, new(specSuite))
}

func (s *specSuite) SetupTest() {
	s.ProtoAssertions = protorequire.New(s.T())
	s.specBuilder = NewSpecBuilder()
}

func (s *specSuite) checkSequenceRaw(spec *schedpb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := s.specBuilder.NewCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		next := cs.rawNextTime(start)
		s.Equal(exp, next)
		start = next
	}
}

func (s *specSuite) checkSequenceFull(jitterSeed string, spec *schedpb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := s.specBuilder.NewCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		result := cs.getNextTime(jitterSeed, start)
		if exp.IsZero() {
			s.Require().True(
				result.Nominal.IsZero(),
				"exp %v nominal should be zero, got %v", exp, result.Nominal,
			)
			s.Require().True(result.Next.IsZero(), "next should be zero")
			break
		}
		s.Require().False(result.Nominal.IsZero())
		s.Require().False(result.Next.IsZero())
		s.Equal(exp, result.Next)
		start = result.Next
	}
}

func (s *specSuite) TestCanonicalize() {
	canonical, err := canonicalizeSpec(&schedpb.ScheduleSpec{})
	s.NoError(err)
	s.ProtoEqual(&schedpb.ScheduleSpec{}, canonical)

	canonical, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"@every 43h",
		},
	})
	s.NoError(err)
	s.ProtoEqual(&schedpb.ScheduleSpec{
		Interval: []*schedpb.IntervalSpec{{
			Interval: durationpb.New(43 * time.Hour),
		}},
	}, canonical)

	// negative interval
	_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		Interval: []*schedpb.IntervalSpec{{
			Interval: durationpb.New(-43 * time.Hour),
		}},
	})
	s.Error(err)

	// phase exceeds interval
	_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		Interval: []*schedpb.IntervalSpec{{
			Interval: durationpb.New(3 * time.Hour),
			Phase:    durationpb.New(4 * time.Hour),
		}},
	})
	s.Error(err)

	// various errors in ranges
	for _, scs := range []*schedpb.StructuredCalendarSpec{
		{Second: []*schedpb.Range{{Start: 100}}},
		{Second: []*schedpb.Range{{Start: -3}}},
		{Second: []*schedpb.Range{{Start: 30, End: 60}}},
		{Second: []*schedpb.Range{{Start: 30, End: 40, Step: -3}}},
		{Minute: []*schedpb.Range{{Start: 60}}},
		{Hour: []*schedpb.Range{{Start: 0, End: 24}}},
		{Hour: []*schedpb.Range{{Start: 24, End: 26}}},
		{Hour: []*schedpb.Range{{Start: 16, End: 12}}},
		{DayOfMonth: []*schedpb.Range{{Start: 0}}},
		{DayOfMonth: []*schedpb.Range{{End: 33}}},
		{Month: []*schedpb.Range{{Start: 0}}},
		{Month: []*schedpb.Range{{End: 13}}},
		{DayOfWeek: []*schedpb.Range{{Start: 7}}},
		{DayOfWeek: []*schedpb.Range{{Start: 6, End: 7}}},
		{Year: []*schedpb.Range{{Start: 1999}}},
		{Year: []*schedpb.Range{{Start: 2112}}},
	} {
		_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
			StructuredCalendar: []*schedpb.StructuredCalendarSpec{scs},
		})
		s.Error(err)
	}

	// check parsing and filling in defaults
	canonical, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		Calendar: []*schedpb.CalendarSpec{
			{Hour: "5,7", Minute: "23"},
		},
	})
	s.NoError(err)
	structured := []*schedpb.StructuredCalendarSpec{{
		Second:     []*schedpb.Range{{Start: 0}},
		Minute:     []*schedpb.Range{{Start: 23}},
		Hour:       []*schedpb.Range{{Start: 5}, {Start: 7}},
		DayOfMonth: []*schedpb.Range{{Start: 1, End: 31}},
		Month:      []*schedpb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedpb.Range{{Start: 0, End: 6}},
	}}
	s.ProtoEqual(&schedpb.ScheduleSpec{
		StructuredCalendar: structured,
	}, canonical)

	// no tz in cron string, leave spec alone
	canonical, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"23 5,7 * * *",
		},
		Jitter:       durationpb.New(5 * time.Minute),
		StartTime:    timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName: "Europe/London",
	})
	s.NoError(err)
	s.ProtoEqual(&schedpb.ScheduleSpec{
		StructuredCalendar: structured,
		Jitter:             durationpb.New(5 * time.Minute),
		StartTime:          timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName:       "Europe/London",
	}, canonical)

	// tz matches, ok
	canonical, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	})
	s.NoError(err)
	s.ProtoEqual(&schedpb.ScheduleSpec{
		StructuredCalendar: structured,
		TimezoneName:       "Europe/London",
	}, canonical)

	// tz mismatch, error
	_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	})
	s.Error(err)

	// tz mismatch between cron strings, error
	_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
	})
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"23 5,7 * * *",
		},
	})
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(&schedpb.ScheduleSpec{
		CronString: []string{
			"23 5,7 * * *",
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
	})
	s.Error(err)
}

func (s *specSuite) TestSpecIntervalBasic() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
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
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(5*time.Minute + 44*time.Second),
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
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(5*time.Minute + 44*time.Second),
				},
				{
					Interval: durationpb.New(157 * time.Minute),
					Phase:    durationpb.New(22*time.Minute + 11*time.Second),
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

func (s *specSuite) TestSpecCalendarAndCron() {
	s.checkSequenceRaw(
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "5,7", Minute: "23"},
			},
			CronString: []string{
				"55 11,13 * * *",
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
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(7 * time.Minute),
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
		"",
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			ExcludeCalendar: []*schedpb.CalendarSpec{
				{
					Hour:   "12-14",
					Minute: "*",
					Second: "*",
				},
			},
			Jitter: durationpb.New(1 * time.Second),
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
		"",
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			StartTime: timestamppb.New(time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)),
			Jitter:    durationpb.New(1 * time.Second),
		},
		time.Date(2022, 3, 23, 8, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 89000000, time.UTC),
	)
}

func (s *specSuite) TestSpecEndTime() {
	s.checkSequenceFull(
		"",
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			EndTime: timestamppb.New(time.Date(2022, 3, 23, 14, 0, 0, 0, time.UTC)),
			Jitter:  durationpb.New(1 * time.Second),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Time{}, // end of sequence
	)
}

func (s *specSuite) TestSpecBoundedJitter() {
	s.checkSequenceFull(
		"",
		&schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			Jitter: durationpb.New(24 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC),
		time.Date(2022, 3, 23, 14, 22, 54, 562000000, time.UTC),
		time.Date(2022, 3, 23, 15, 8, 3, 724000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSingleRun() {
	s.checkSequenceFull(
		"",
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC),
	)
	s.checkSequenceFull(
		"",
		&schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
			Jitter: durationpb.New(1 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSeed() {
	spec := &schedpb.ScheduleSpec{
		Interval: []*schedpb.IntervalSpec{
			{Interval: durationpb.New(24 * time.Hour)},
		},
		Jitter: durationpb.New(1 * time.Hour),
	}
	s.checkSequenceFull(
		"",
		spec,
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 0, 35, 24, 276000000, time.UTC),
	)
	s.checkSequenceFull(
		"seed-1",
		spec,
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 0, 6, 12, 519000000, time.UTC),
	)
	s.checkSequenceFull(
		"seed-2",
		spec,
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 0, 39, 16, 922000000, time.UTC),
	)
}
