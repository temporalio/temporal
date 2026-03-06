package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (s *specSuite) checkSequenceRaw(spec *schedulepb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := s.specBuilder.NewCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		next := cs.rawNextTime(start)
		s.Equal(exp, next)
		start = next
	}
}

func (s *specSuite) checkSequenceFull(jitterSeed string, spec *schedulepb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := s.specBuilder.NewCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		result := cs.GetNextTime(jitterSeed, start)
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
	canonical, err := canonicalizeSpec(&schedulepb.ScheduleSpec{})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{}, canonical)

	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"@every 43h",
		},
	})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(43 * time.Hour),
		}},
	}, canonical)

	// negative interval
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(-43 * time.Hour),
		}},
	})
	s.Error(err)

	// phase exceeds interval
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(3 * time.Hour),
			Phase:    durationpb.New(4 * time.Hour),
		}},
	})
	s.Error(err)

	// various errors in ranges
	for _, scs := range []*schedulepb.StructuredCalendarSpec{
		{Second: []*schedulepb.Range{{Start: 100}}},
		{Second: []*schedulepb.Range{{Start: -3}}},
		{Second: []*schedulepb.Range{{Start: 30, End: 60}}},
		{Second: []*schedulepb.Range{{Start: 30, End: 40, Step: -3}}},
		{Minute: []*schedulepb.Range{{Start: 60}}},
		{Hour: []*schedulepb.Range{{Start: 0, End: 24}}},
		{Hour: []*schedulepb.Range{{Start: 24, End: 26}}},
		{Hour: []*schedulepb.Range{{Start: 16, End: 12}}},
		{DayOfMonth: []*schedulepb.Range{{Start: 0}}},
		{DayOfMonth: []*schedulepb.Range{{End: 33}}},
		{Month: []*schedulepb.Range{{Start: 0}}},
		{Month: []*schedulepb.Range{{End: 13}}},
		{DayOfWeek: []*schedulepb.Range{{Start: 7}}},
		{DayOfWeek: []*schedulepb.Range{{Start: 6, End: 7}}},
		{Year: []*schedulepb.Range{{Start: 1999}}},
	} {
		_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{scs},
		})
		s.Error(err)
	}

	// check parsing and filling in defaults
	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{
			{Hour: "5,7", Minute: "23"},
		},
	})
	s.NoError(err)
	structured := []*schedulepb.StructuredCalendarSpec{{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 23}},
		Hour:       []*schedulepb.Range{{Start: 5}, {Start: 7}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6}},
	}}
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		StructuredCalendar: structured,
	}, canonical)

	// no tz in cron string, leave spec alone
	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"23 5,7 * * *",
		},
		Jitter:       durationpb.New(5 * time.Minute),
		StartTime:    timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName: "Europe/London",
	})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		StructuredCalendar: structured,
		Jitter:             durationpb.New(5 * time.Minute),
		StartTime:          timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName:       "Europe/London",
	}, canonical)

	// tz matches, ok
	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		StructuredCalendar: structured,
		TimezoneName:       "Europe/London",
	}, canonical)

	// tz mismatch, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	})
	s.Error(err)

	// tz mismatch between cron strings, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
	})
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"23 5,7 * * *",
		},
	})
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"23 5,7 * * *",
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
	})
	s.Error(err)
}

func (s *specSuite) TestSpecIntervalBasic() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
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
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
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
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
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
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
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
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
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
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
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
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "11,13", Minute: "55"},
			},
			Interval: []*schedulepb.IntervalSpec{
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
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			ExcludeCalendar: []*schedulepb.CalendarSpec{
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

func (s *specSuite) TestExcludeAll() {
	cs, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{
			{Interval: durationpb.New(7 * 24 * time.Hour)},
		},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			&schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"},
		},
	})
	s.NoError(err)
	s.Zero(cs.GetNextTime("", time.Date(2022, 3, 23, 12, 53, 2, 9, time.UTC)))
}

func (s *specSuite) TestSpecStartTime() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
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

func (s *specSuite) TestSpecStartTimeMinusOneSecond() {
	// This checks the bug fixed by FixStartTimeBug.
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(time.Hour)},
			},
			StartTime: timestamppb.New(time.Date(2022, 3, 23, 12, 0, 0, 456000000, time.UTC)),
		},
		time.Date(2022, 3, 23, 12, 00, 0, 123000000, time.UTC),
		time.Date(2022, 3, 23, 13, 00, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecEndTime() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
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
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
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
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC),
	)
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
			Jitter: durationpb.New(1 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSeed() {
	spec := &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{
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

func (s *specSuite) TestSpecFarFutureYear() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "12", Minute: "0", DayOfMonth: "1", Month: "1", Year: "2150"},
			},
		},
		time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Time{}, // returns zero time since 2150 is beyond calculation bound
	)
}
