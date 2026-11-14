package scheduler

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
)

// linearNextSetBit is the obvious O(limit) definition of nextSetBit: the first set bit in
// [from, limit), or limit if there is none.
func linearNextSetBit(bits uint64, from, limit int) int {
	for v := from; v < limit; v++ {
		if v < 64 && bits&(1<<v) != 0 {
			return v
		}
	}
	return limit
}

// TestNextSetBitMatchesLinearScan checks nextSetBit against a bit-by-bit scan over edge-case
// and random masks, for every from in [0, limit] and the limits next() uses (13 months,
// 24 hours, 60 minutes/seconds) plus the 64-bit boundary.
func TestNextSetBitMatchesLinearScan(t *testing.T) {
	masks := []uint64{0, ^uint64(0), 0x12a, 0x492, 0x0000_2000_4000_8001}
	for i := range 64 {
		masks = append(masks, 1<<i, ^uint64(0)<<i, ^uint64(0)>>i)
	}
	r := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		masks = append(masks, r.Uint64(), r.Uint64()&r.Uint64()&r.Uint64()) // dense and sparse
	}

	for _, limit := range []int{1, 7, 13, 24, 32, 60, 63, 64} {
		for _, bits := range masks {
			for from := 0; from <= limit; from++ {
				require.Equal(t, linearNextSetBit(bits, from, limit), nextSetBit(bits, from, limit),
					"bits=%#x from=%d limit=%d", bits, from, limit)
			}
		}
	}
}

// TestNextSetBitMatchesOldLoop checks each jump in next() against the loop it replaced.
// The old code, for a field value cur that did not match, ran
//
//	for !matcher(v) { v++; if v >= limit { break /* continue Outer */ } }
//
// The new code computes nextSetBit(bits, cur+1, limit). Both are built from the same
// ranges via makeBitMatcher/makeBits, as in newCompiledCalendar, so this also pins the
// invariant that the matcher and the stored bitmask agree.
func TestNextSetBitMatchesOldLoop(t *testing.T) {
	fields := []struct {
		name          string
		lo, hi, limit int
	}{
		{"month", 1, 12, int(time.December) + 1},
		{"hour", 0, 23, 24},
		{"minute", 0, 59, 60},
		{"second", 0, 59, 60},
	}
	oldLoop := func(matcher func(int) bool, cur, limit int) int {
		v := cur + 1
		for v < limit && !matcher(v) {
			v++
		}
		return v
	}

	r := rand.New(rand.NewPCG(3, 4))
	for _, f := range fields {
		rangeSets := [][]*schedulepb.Range{
			{{Start: int32(f.lo), End: int32(f.hi)}}, // "*"
			{{Start: int32(f.lo)}},                   // first value only
			{{Start: int32(f.hi)}},                   // last value only
		}
		for range 500 {
			rangeSets = append(rangeSets, randRanges(r, f.lo, f.hi))
		}
		for _, ranges := range rangeSets {
			matcher, bits := makeBitMatcher(ranges), makeBits(ranges)
			for cur := f.lo; cur <= f.hi; cur++ {
				if matcher(cur) {
					continue // next() only jumps when the current value does not match
				}
				want := oldLoop(matcher, cur, f.limit)
				got := nextSetBit(bits, cur+1, f.limit)
				require.Equal(t, want, got, "field=%s ranges=%v cur=%d", f.name, ranges, cur)
				if got < f.limit {
					require.True(t, matcher(got), "field=%s ranges=%v: jumped to non-matching %d", f.name, ranges, got)
				}
			}
		}
	}
}

// calendarEquivalenceZones mixes zones with ordinary DST, 30-minute DST (Lord_Howe), DST at
// midnight (Asuncion, Cuba, Beirut), non-hour offsets (Kolkata) and no DST at all.
var calendarEquivalenceZones = []string{
	"UTC",
	"America/New_York",
	"US/Pacific",
	"Europe/London",
	"Australia/Lord_Howe",
	"Asia/Kolkata",
	"America/Asuncion",
	"Asia/Beirut",
	"Cuba",
	"Africa/Kinshasa",
}

func loadEquivalenceZone(t testing.TB, i uint) *time.Location {
	loc, err := time.LoadLocation(calendarEquivalenceZones[i%uint(len(calendarEquivalenceZones))])
	require.NoError(t, err)
	return loc
}

// checkCalendarNextChain compares compiledCalendar.next with the pre-optimization
// refCalendarNext (spec_reference_test.go) over a chain of queries starting at q. Most
// queries continue from the previous result, so the chain walks consecutive matches;
// others jump by up to ±2h so they land inside DST gaps and repeated hours.
func checkCalendarNextChain(t testing.TB, r *rand.Rand, cc *compiledCalendar, q time.Time, steps int, desc any) {
	for step := range steps {
		want := refCalendarNext(cc, q)
		got := cc.next(q)
		require.Equal(t, want, got, "step=%d after=%v (%v) cal=%v", step, q, q.In(cc.tz), desc)
		if want.IsZero() {
			return
		}
		q = want
		if r.IntN(4) == 0 {
			q = q.Add(time.Duration(r.Int64N(int64(4*time.Hour))) - 2*time.Hour)
		}
	}
}

// TestCalendarNextEquivalence is a property test: for random structured calendars, time
// zones and query times, the optimized next() returns exactly what the old step-by-one
// implementation returned.
func TestCalendarNextEquivalence(t *testing.T) {
	for seed := range uint64(5000) {
		r := rand.New(rand.NewPCG(seed, seed^0x5851f42d4c957f2d))
		cal := randStructuredCalendar(r)
		cc := newCompiledCalendar(cal, loadEquivalenceZone(t, uint(r.IntN(len(calendarEquivalenceZones)))))
		checkCalendarNextChain(t, r, cc, randQueryTime(r), 40, cal)
	}
}

// FuzzCalendarNextEquivalence is the cron-string counterpart of TestCalendarNextEquivalence.
// Taking the fields as strings lets the coverage-guided fuzzer explore specs directly
// (unlike a single random seed). Run with:
//
//	go test -tags test_dep -run '^$' -fuzz '^FuzzCalendarNextEquivalence$' -fuzztime 60s ./service/worker/scheduler/
func FuzzCalendarNextEquivalence(f *testing.F) {
	nyFallBackSec := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC).Unix()
	nySpringForwardSec := time.Date(2026, 3, 8, 6, 30, 0, 0, time.UTC).Unix()
	// s, m, h, dom, mo, y, dow, tz index, start
	f.Add("0", "*/15", "*", "*", "3", "", "*", uint(0), time.Date(2026, 5, 20, 14, 33, 7, 0, time.UTC).Unix())
	f.Add("0", "0", "0", "1", "*/3", "", "*", uint(0), time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC).Unix())
	f.Add("30", "30", "1", "*", "*", "", "*", uint(1), nyFallBackSec)     // repeated hour
	f.Add("0", "30", "2", "*", "*", "", "*", uint(1), nySpringForwardSec) // skipped hour
	f.Add("0", "15,45", "*", "*", "*", "", "*", uint(4), nyFallBackSec)   // Lord_Howe 30-min DST
	f.Add("0", "0", "0", "*", "*", "", "*", uint(6), nySpringForwardSec)  // DST at midnight
	f.Add("0", "0", "12", "29", "2", "", "*", uint(0), int64(0))          // leap days only
	f.Add("0", "0", "0", "31", "2", "", "*", uint(0), int64(0))           // never matches
	f.Add("0", "0", "0", "1", "1", "2100", "*", uint(0), int64(0))        // far-future year
	f.Add("*", "*", "*", "*", "*", "", "*", uint(2), int64(1700000000))   // every second
	f.Add("0", "0", "9", "13", "*", "", "fri", uint(3), int64(1700000000))
	f.Add("59", "59", "23", "31", "12", "2030-2040/5", "*", uint(5), int64(1900000000))

	minStartSec := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	span := uint64(time.Date(maxCalendarYear+1, 1, 1, 0, 0, 0, 0, time.UTC).Unix() - minStartSec)

	f.Fuzz(func(t *testing.T, s, m, h, dom, mo, y, dow string, tz uint, start int64) {
		cal := &schedulepb.CalendarSpec{Second: s, Minute: m, Hour: h, DayOfMonth: dom, Month: mo, Year: y, DayOfWeek: dow}
		scs, err := parseCalendarToStructured(cal)
		if err != nil {
			return
		}
		cc := newCompiledCalendar(scs, loadEquivalenceZone(t, tz))
		// Keep start within the calendar's years: from the distant past the reference
		// implementation walks one year at a time and would take arbitrarily long.
		q := time.Unix(minStartSec+int64(uint64(start)%span), 0)
		r := rand.New(rand.NewPCG(uint64(start), uint64(tz)))
		checkCalendarNextChain(t, r, cc, q, 20, cal)
	})
}
