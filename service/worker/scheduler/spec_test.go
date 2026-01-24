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

	canonical, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"@every 43h",
		},
	}.Build())
	s.NoError(err)
	s.ProtoEqual(schedulepb.ScheduleSpec_builder{
		Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
			Interval: durationpb.New(43 * time.Hour),
		}.Build()},
	}.Build(), canonical)

	// negative interval
	_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
			Interval: durationpb.New(-43 * time.Hour),
		}.Build()},
	}.Build())
	s.Error(err)

	// phase exceeds interval
	_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
			Interval: durationpb.New(3 * time.Hour),
			Phase:    durationpb.New(4 * time.Hour),
		}.Build()},
	}.Build())
	s.Error(err)

	// various errors in ranges
	for _, scs := range []*schedulepb.StructuredCalendarSpec{
		schedulepb.StructuredCalendarSpec_builder{Second: []*schedulepb.Range{schedulepb.Range_builder{Start: 100}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Second: []*schedulepb.Range{schedulepb.Range_builder{Start: -3}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Second: []*schedulepb.Range{schedulepb.Range_builder{Start: 30, End: 60}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Second: []*schedulepb.Range{schedulepb.Range_builder{Start: 30, End: 40, Step: -3}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Minute: []*schedulepb.Range{schedulepb.Range_builder{Start: 60}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Hour: []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 24}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Hour: []*schedulepb.Range{schedulepb.Range_builder{Start: 24, End: 26}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Hour: []*schedulepb.Range{schedulepb.Range_builder{Start: 16, End: 12}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{DayOfMonth: []*schedulepb.Range{schedulepb.Range_builder{Start: 0}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{DayOfMonth: []*schedulepb.Range{schedulepb.Range_builder{End: 33}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Month: []*schedulepb.Range{schedulepb.Range_builder{Start: 0}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Month: []*schedulepb.Range{schedulepb.Range_builder{End: 13}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{DayOfWeek: []*schedulepb.Range{schedulepb.Range_builder{Start: 7}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{DayOfWeek: []*schedulepb.Range{schedulepb.Range_builder{Start: 6, End: 7}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Year: []*schedulepb.Range{schedulepb.Range_builder{Start: 1999}.Build()}}.Build(),
		schedulepb.StructuredCalendarSpec_builder{Year: []*schedulepb.Range{schedulepb.Range_builder{Start: 2112}.Build()}}.Build(),
	} {
		_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{scs},
		}.Build())
		s.Error(err)
	}

	// check parsing and filling in defaults
	canonical, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		Calendar: []*schedulepb.CalendarSpec{
			schedulepb.CalendarSpec_builder{Hour: "5,7", Minute: "23"}.Build(),
		},
	}.Build())
	s.NoError(err)
	structured := []*schedulepb.StructuredCalendarSpec{schedulepb.StructuredCalendarSpec_builder{
		Second:     []*schedulepb.Range{schedulepb.Range_builder{Start: 0}.Build()},
		Minute:     []*schedulepb.Range{schedulepb.Range_builder{Start: 23}.Build()},
		Hour:       []*schedulepb.Range{schedulepb.Range_builder{Start: 5}.Build(), schedulepb.Range_builder{Start: 7}.Build()},
		DayOfMonth: []*schedulepb.Range{schedulepb.Range_builder{Start: 1, End: 31}.Build()},
		Month:      []*schedulepb.Range{schedulepb.Range_builder{Start: 1, End: 12}.Build()},
		DayOfWeek:  []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 6}.Build()},
	}.Build()}
	s.ProtoEqual(schedulepb.ScheduleSpec_builder{
		StructuredCalendar: structured,
	}.Build(), canonical)

	// no tz in cron string, leave spec alone
	canonical, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"23 5,7 * * *",
		},
		Jitter:       durationpb.New(5 * time.Minute),
		StartTime:    timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName: "Europe/London",
	}.Build())
	s.NoError(err)
	s.ProtoEqual(schedulepb.ScheduleSpec_builder{
		StructuredCalendar: structured,
		Jitter:             durationpb.New(5 * time.Minute),
		StartTime:          timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName:       "Europe/London",
	}.Build(), canonical)

	// tz matches, ok
	canonical, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	}.Build())
	s.NoError(err)
	s.ProtoEqual(schedulepb.ScheduleSpec_builder{
		StructuredCalendar: structured,
		TimezoneName:       "Europe/London",
	}.Build(), canonical)

	// tz mismatch, error
	_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	}.Build())
	s.Error(err)

	// tz mismatch between cron strings, error
	_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
	}.Build())
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"23 5,7 * * *",
		},
	}.Build())
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(schedulepb.ScheduleSpec_builder{
		CronString: []string{
			"23 5,7 * * *",
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
	}.Build())
	s.Error(err)
}

func (s *specSuite) TestSpecIntervalBasic() {
	s.checkSequenceRaw(
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(90 * time.Minute)}.Build(),
			},
		}.Build(),
		time.Date(2022, 3, 23, 12, 53, 2, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 30, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 00, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecIntervalPhase() {
	s.checkSequenceRaw(
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(5*time.Minute + 44*time.Second),
				}.Build(),
			},
		}.Build(),
		time.Date(2022, 3, 23, 12, 53, 02, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 05, 44, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecIntervalMultiple() {
	s.checkSequenceRaw(
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(5*time.Minute + 44*time.Second),
				}.Build(),
				schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(157 * time.Minute),
					Phase:    durationpb.New(22*time.Minute + 11*time.Second),
				}.Build(),
			},
		}.Build(),
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
		schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{Hour: "5,7", Minute: "23"}.Build(),
			},
		}.Build(),
		time.Date(2022, 3, 23, 3, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 7, 23, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecCalendarMultiple() {
	s.checkSequenceRaw(
		schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{Hour: "5,7", Minute: "23"}.Build(),
				schedulepb.CalendarSpec_builder{Hour: "11,13", Minute: "55"}.Build(),
			},
		}.Build(),
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
		schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{Hour: "5,7", Minute: "23"}.Build(),
			},
			CronString: []string{
				"55 11,13 * * *",
			},
		}.Build(),
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
		schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{Hour: "11,13", Minute: "55"}.Build(),
			},
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(7 * time.Minute),
				}.Build(),
			},
		}.Build(),
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
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(90 * time.Minute)}.Build(),
			},
			ExcludeCalendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{
					Hour:   "12-14",
					Minute: "*",
					Second: "*",
				}.Build(),
			},
			Jitter: durationpb.New(1 * time.Second),
		}.Build(),
		time.Date(2022, 3, 23, 8, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 9, 00, 0, 235000000, time.UTC),
		time.Date(2022, 3, 23, 10, 30, 0, 139000000, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 89000000, time.UTC),
		time.Date(2022, 3, 23, 16, 30, 0, 687000000, time.UTC),
	)
}

func (s *specSuite) TestExcludeAll() {
	cs, err := s.specBuilder.NewCompiledSpec(schedulepb.ScheduleSpec_builder{
		Interval: []*schedulepb.IntervalSpec{
			schedulepb.IntervalSpec_builder{Interval: durationpb.New(7 * 24 * time.Hour)}.Build(),
		},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			schedulepb.CalendarSpec_builder{Second: "*", Minute: "*", Hour: "*"}.Build(),
		},
	}.Build())
	s.NoError(err)
	s.Zero(cs.GetNextTime("", time.Date(2022, 3, 23, 12, 53, 2, 9, time.UTC)))
}

func (s *specSuite) TestSpecStartTime() {
	s.checkSequenceFull(
		"",
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(90 * time.Minute)}.Build(),
			},
			StartTime: timestamppb.New(time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)),
			Jitter:    durationpb.New(1 * time.Second),
		}.Build(),
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
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(time.Hour)}.Build(),
			},
			StartTime: timestamppb.New(time.Date(2022, 3, 23, 12, 0, 0, 456000000, time.UTC)),
		}.Build(),
		time.Date(2022, 3, 23, 12, 00, 0, 123000000, time.UTC),
		time.Date(2022, 3, 23, 13, 00, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecEndTime() {
	s.checkSequenceFull(
		"",
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(90 * time.Minute)}.Build(),
			},
			EndTime: timestamppb.New(time.Date(2022, 3, 23, 14, 0, 0, 0, time.UTC)),
			Jitter:  durationpb.New(1 * time.Second),
		}.Build(),
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Time{}, // end of sequence
	)
}

func (s *specSuite) TestSpecBoundedJitter() {
	s.checkSequenceFull(
		"",
		schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(90 * time.Minute)}.Build(),
			},
			Jitter: durationpb.New(24 * time.Hour),
		}.Build(),
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC),
		time.Date(2022, 3, 23, 14, 22, 54, 562000000, time.UTC),
		time.Date(2022, 3, 23, 15, 8, 3, 724000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSingleRun() {
	s.checkSequenceFull(
		"",
		schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"}.Build(),
			},
		}.Build(),
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC),
	)
	s.checkSequenceFull(
		"",
		schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"}.Build(),
			},
			Jitter: durationpb.New(1 * time.Hour),
		}.Build(),
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSeed() {
	spec := schedulepb.ScheduleSpec_builder{
		Interval: []*schedulepb.IntervalSpec{
			schedulepb.IntervalSpec_builder{Interval: durationpb.New(24 * time.Hour)}.Build(),
		},
		Jitter: durationpb.New(1 * time.Hour),
	}.Build()
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
