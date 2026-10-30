package scheduler

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var equivalenceTimezones = []string{
	"", "UTC", "Etc/GMT", "America/New_York", "Australia/Lord_Howe", "Europe/London", "Asia/Kolkata",
}

// DST transitions (UTC) for the zones above, used to bias query times.
var equivalenceTransitions = []time.Time{
	time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC),    // America/New_York spring forward
	time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC),   // America/New_York fall back
	time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),   // Australia/Lord_Howe 30-min fall back
	time.Date(2026, 10, 3, 15, 30, 0, 0, time.UTC), // Australia/Lord_Howe 30-min spring forward
	time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC),   // Europe/London
	time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC),  // Europe/London
	time.Date(2099, 12, 31, 23, 0, 0, 0, time.UTC),
	time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
}

func randRanges(r *rand.Rand, lo, hi int) []*schedulepb.Range {
	n := 1 + r.IntN(3)
	out := make([]*schedulepb.Range, n)
	for i := range out {
		start := lo + r.IntN(hi-lo+1)
		rng := &schedulepb.Range{Start: int32(start)}
		switch r.IntN(3) {
		case 1:
			rng.End = int32(start + r.IntN(hi-start+1))
		case 2:
			rng.End = int32(start + r.IntN(hi-start+1))
			rng.Step = int32(1 + r.IntN(hi-lo+1))
		default: // single value
		}
		out[i] = rng
	}
	return out
}

func randStructuredCalendar(r *rand.Rand) *schedulepb.StructuredCalendarSpec {
	cal := &schedulepb.StructuredCalendarSpec{
		Second:     randRanges(r, 0, 59),
		Minute:     randRanges(r, 0, 59),
		Hour:       randRanges(r, 0, 23),
		DayOfMonth: randRanges(r, 1, 31),
		Month:      randRanges(r, 1, 12),
		DayOfWeek:  randRanges(r, 0, 6),
	}
	// Bias toward dense fields so most specs produce many matches.
	for _, f := range []*[]*schedulepb.Range{&cal.Second, &cal.Minute, &cal.Hour, &cal.DayOfMonth, &cal.Month, &cal.DayOfWeek} {
		if r.IntN(2) == 0 {
			*f = nil
		}
	}
	if cal.Second == nil {
		cal.Second = []*schedulepb.Range{{Start: 0, End: 59}}
	}
	if cal.Minute == nil {
		cal.Minute = []*schedulepb.Range{{Start: 0, End: 59}}
	}
	if cal.Hour == nil {
		cal.Hour = []*schedulepb.Range{{Start: 0, End: 23}}
	}
	if cal.DayOfMonth == nil {
		cal.DayOfMonth = []*schedulepb.Range{{Start: 1, End: 31}}
	}
	if cal.Month == nil {
		cal.Month = []*schedulepb.Range{{Start: 1, End: 12}}
	}
	if cal.DayOfWeek == nil {
		cal.DayOfWeek = []*schedulepb.Range{{Start: 0, End: 6}}
	}
	if r.IntN(3) > 0 {
		cal.Year = randRanges(r, minCalendarYear, maxCalendarYear)
		if r.IntN(4) == 0 {
			cal.Year = []*schedulepb.Range{{Start: maxCalendarYear}}
		}
	}
	return cal
}

func randSpec(r *rand.Rand) *schedulepb.ScheduleSpec {
	spec := &schedulepb.ScheduleSpec{TimezoneName: equivalenceTimezones[r.IntN(len(equivalenceTimezones))]}
	for range 1 + r.IntN(3) {
		spec.StructuredCalendar = append(spec.StructuredCalendar, randStructuredCalendar(r))
	}
	if r.IntN(4) == 0 {
		spec.Interval = []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(time.Duration(1+r.IntN(90*24*3600)) * time.Second),
			Phase:    durationpb.New(time.Duration(r.IntN(3600)) * time.Second),
		}}
	}
	if r.IntN(5) == 0 {
		spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{randStructuredCalendar(r)}
	}
	switch r.IntN(3) {
	case 1:
		spec.Jitter = durationpb.New(-time.Duration(r.IntN(3600)) * time.Second)
	case 2:
		spec.Jitter = durationpb.New(time.Duration(r.IntN(3600*1000)) * time.Millisecond)
	default: // no jitter
	}
	if r.IntN(5) == 0 {
		spec.StartTime = timestamppb.New(randQueryTime(r))
	}
	if r.IntN(5) == 0 {
		spec.EndTime = timestamppb.New(randQueryTime(r))
	}
	return spec
}

func randQueryTime(r *rand.Rand) time.Time {
	if r.IntN(2) == 0 {
		base := equivalenceTransitions[r.IntN(len(equivalenceTransitions))]
		return base.Add(time.Duration(r.Int64N(int64(4*time.Hour))) - 2*time.Hour)
	}
	return time.Date(2000+r.IntN(100), 1, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(r.Int64N(int64(366 * 24 * time.Hour))))
}

// checkEquivalence compares GetNextTime with the pre-optimization reference over a
// sequence of calls. Returns false if the spec was rejected.
func checkEquivalence(t *testing.T, seed uint64) bool {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	spec := randSpec(r)
	// A small max bound keeps over-excluded specs fast and also checks the error path.
	cs, err := newSpecBuilderForTest(500, 2000).NewCompiledSpec(spec)
	if err != nil {
		return false
	}
	q := randQueryTime(r)
	for step := range 30 {
		want, wantErr := refGetNextTime(cs, "seed", q)
		got, gotErr := cs.GetNextTime("seed", q)
		require.Equal(t, wantErr, gotErr, "seed=%d step=%d after=%v spec=%v", seed, step, q, spec)
		require.Equal(t, want, got, "seed=%d step=%d after=%v spec=%v", seed, step, q, spec)
		if wantErr != nil || want.Next.IsZero() {
			break
		}
		q = want.Next
	}
	return true
}

func TestGetNextTimeEquivalence(t *testing.T) {
	compiled := 0
	for seed := range uint64(3000) {
		if checkEquivalence(t, seed) {
			compiled++
		}
	}
	// Guard against the generator silently producing only invalid specs.
	require.Greater(t, compiled, 2000)
}

func FuzzGetNextTimeEquivalence(f *testing.F) {
	for seed := range uint64(20) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed uint64) {
		checkEquivalence(t, seed)
	})
}
