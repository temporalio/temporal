package scheduler

import "time"

// gregorianJDN returns the Julian day number for a proleptic Gregorian civil date
// (year, month=1-12, day) using a standard Hatcher/Fliegel form.
func gregorianJDN(year, month, day int) int64 {
	a := (14 - month) / 12
	y := int64(year) + 4800 - int64(a)
	m := int64(month) + 12*int64(a) - 3
	return int64(day) + (153*m+2)/5 + 365*y + y/4 - y/100 + y/400 - 32045
}

// normalizePhaseInInterval returns phase reduced into [0, interval), matching
// the math used in nextIntervalTime.
func normalizePhaseInInterval(phase, interval int64) int64 {
	if interval < 1 {
		interval = 1
	}
	if phase < 0 {
		phase = 0
	}
	p := phase % interval
	if p < 0 {
		p += interval
	}
	return p
}

// nextCivilIntervalTick returns the next Unix time (second resolution) strictly
// after "after" for an interval that is a multiple of 86400 seconds, interpreted
// as steps of n whole calendar days in the schedule's timezone, each at the
// same local time of day as time.Unix(p,0) in that location, where p is
// phase mod interval (the first tick in [0,interval) aligned to 1970-01-01 UTC+offset).
// When the location is UTC (or nil), (0, false) is returned so the caller
// continues to use fixed-UTC-second interval math for backward compatibility.
func nextCivilIntervalTick(phase, interval int64, after time.Time, loc *time.Location) (int64, bool) {
	if interval%86400 != 0 || interval < 86400 {
		return 0, false
	}
	if loc == nil || loc == time.UTC {
		return 0, false
	}
	p := normalizePhaseInInterval(phase, interval)
	nDayStep := interval / 86400
	if nDayStep < 1 {
		return 0, false
	}

	// One tick in the current UTC-math grid is t ≡ p (mod interval). Use
	// 1970-01-01 00:00:00Z + p as a representative; local time-of-day for all
	// "same wall clock in zone" series is taken from that instant in loc.
	ref := time.Unix(p, 0).In(loc)
	h, m, s := ref.Clock()
	nsec := ref.Nanosecond()
	j0 := gregorianJDN(ref.Year(), int(ref.Month()), ref.Day())

	probe := after.Add(1 * time.Nanosecond)
	if !probe.After(after) {
		return 0, false
	}
	probeLoc := probe.In(loc)
	day0 := time.Date(probeLoc.Year(), probeLoc.Month(), probeLoc.Day(), 0, 0, 0, 0, loc)

	const maxDayScan = 20000
	for d := 0; d < maxDayScan; d++ {
		day := day0.AddDate(0, 0, d)
		cand := time.Date(day.Year(), day.Month(), day.Day(), h, m, s, nsec, loc)
		if !cand.After(after) {
			continue
		}
		jc := gregorianJDN(cand.Year(), int(cand.Month()), cand.Day())
		if (jc-j0)%nDayStep == 0 {
			return cand.Unix(), true
		}
	}
	return 0, false
}
