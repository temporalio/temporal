package scheduler

import (
	"math"
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
)

// Verbatim copies of the pre-optimization GetNextTime path, used as the reference in
// TestGetNextTimeEquivalence. V1 records GetNextTime results in workflow history, so the
// optimized path must produce byte-identical results. Do not modify.

func refCalendarNext(cc *compiledCalendar, ts time.Time) time.Time {
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

func refRawNextTime(cs *CompiledSpec, after time.Time) (nominal time.Time) {
	var minTimestamp int64 = math.MaxInt64 // unix seconds-since-epoch as int64

	for _, cal := range cs.calendar {
		if next := refCalendarNext(cal, after); !next.IsZero() {
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

func refGetNextTime(cs *CompiledSpec, jitterSeed string, after time.Time) (GetNextTimeResult, error) {
	// If we're starting before the schedule's allowed time range, jump up to right before
	// it (so that we can still return the first second of the range if it happens to match).
	// note: AsTime returns unix epoch on nil StartTime
	after = util.MaxTime(after, cs.spec.StartTime.AsTime().Add(-time.Second))

	pastEndTime := func(t time.Time) bool {
		return cs.spec.EndTime != nil && t.After(cs.spec.EndTime.AsTime()) || t.Year() > maxCalendarYear
	}
	warnIterations := cs.warnIterations()
	if warnIterations <= 0 {
		warnIterations = DefaultWarnIterations
	}
	maxIterations := cs.maxIterations()
	if maxIterations <= 0 {
		maxIterations = math.MaxInt // disabled: effectively unlimited
	}

	var warned bool
	var nominal time.Time
	for iterations := 0; nominal.IsZero() || cs.excluded(nominal); iterations++ {
		// Hard bound: stop an over-excluded / adversarial spec from spinning toward
		// maxCalendarYear. Disabled by default (maxIterations == math.MaxInt); an operator can
		// lower it to re-enable enforcement. Well-formed specs resolve in a handful of iterations.
		if iterations >= maxIterations {
			return GetNextTimeResult{ComputeLimitWarning: true}, ErrComputeLimitExceeded
		}
		if iterations >= warnIterations {
			warned = true
		}
		nominal = refRawNextTime(cs, after)
		after = nominal

		if nominal.IsZero() || pastEndTime(nominal) {
			return GetNextTimeResult{ComputeLimitWarning: warned}, nil
		}
	}

	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	// Ensure that jitter doesn't push this time past the _next_ nominal start time
	if following := refRawNextTime(cs, nominal); !following.IsZero() {
		maxJitter = min(maxJitter, following.Sub(nominal))
	}
	next := cs.addJitter(jitterSeed, nominal, maxJitter)

	return GetNextTimeResult{Nominal: nominal, Next: next, ComputeLimitWarning: warned}, nil
}
