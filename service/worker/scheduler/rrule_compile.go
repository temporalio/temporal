package scheduler

import (
	"fmt"
	"math"
	"strings"
	"time"

	rrule "github.com/teambition/rrule-go"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func scheduleRRuleDTStart(spec *schedulepb.ScheduleSpec, loc *time.Location) time.Time {
	if spec.GetStartTime() != nil {
		return spec.GetStartTime().AsTime().In(loc)
	}
	return time.Unix(0, 0).In(loc)
}

// compileRRuleStrings parses and compiles rrule string bodies (no "RRULE:" prefix) with a
// single DTSTART derived from start_time (or the Unix epoch in the schedule time zone).
func compileRRuleStrings(spec *schedulepb.ScheduleSpec, loc *time.Location) ([]*rrule.RRule, error) {
	lines := spec.GetRrule()
	if len(lines) == 0 {
		return nil, nil
	}
	if len(lines) > maxRruleCount {
		return nil, fmt.Errorf("too many rrule lines: max %d", maxRruleCount)
	}
	dt := scheduleRRuleDTStart(spec, loc)
	out := make([]*rrule.RRule, 0, len(lines))
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			return nil, fmt.Errorf("rrule at index %d is empty", i)
		}
		if len(line) > maxRruleStringLen {
			return nil, fmt.Errorf("rrule at index %d is too long", i)
		}
		r, err := rrule.StrToRRule(line)
		if err != nil {
			return nil, err
		}
		r.DTStart(dt)
		out = append(out, r)
	}
	return out, nil
}

// nextRRuleTime returns the earliest Unix second from all RRULEs strictly after t, or
// MaxInt64 if there is no occurrence.
func nextRRuleTime(rrs []*rrule.RRule, after time.Time) int64 {
	var minTs int64 = math.MaxInt64
	for _, r := range rrs {
		if r == nil {
			continue
		}
		next := r.After(after, false)
		if next.IsZero() {
			continue
		}
		if u := next.UTC().Unix(); u < minTs {
			minTs = u
		}
	}
	return minTs
}
