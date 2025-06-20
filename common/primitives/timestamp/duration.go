package timestamp

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// 100 years. Maximum representable golang time.Duration is approximately 290 years (INT64_MAX * time.Nanosecond)
// ElasticSearch only supports datetimes up to 2262, so cap at 100 years to be safe.
const maxAllowedDuration = 100 * 365 * 24 * time.Hour

var (
	errNegativeDuration = fmt.Errorf("negative duration")
	errMismatchedSigns  = fmt.Errorf("duration has seconds and nanos with different signs")

	maxSeconds = maxAllowedDuration.Nanoseconds() / 1e9
)

func DurationValue(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func DurationPtr(td time.Duration) *durationpb.Duration {
	return durationpb.New(td)
}

func MinDurationPtr(d1 *durationpb.Duration, d2 *durationpb.Duration) *durationpb.Duration {
	res := min(DurationValue(d1), DurationValue(d2))
	return durationpb.New(res)
}

func DurationFromSeconds(s int64) *durationpb.Duration {
	return durationMultipleOf(s, time.Second)
}

func DurationFromMinutes(m int64) *durationpb.Duration {
	return durationMultipleOf(m, time.Minute)
}

func DurationFromHours(h int64) *durationpb.Duration {
	return durationMultipleOf(h, time.Hour)
}

func DurationFromDays(d int32) *durationpb.Duration {
	return durationMultipleOf(int64(d), time.Hour*24)
}

func durationMultipleOf(amt int64, mult time.Duration) *durationpb.Duration {
	return DurationPtr(time.Duration(amt) * mult)
}

// ValidateAndCapProtoDuration validates protobuf durations for two conditions:
//  1. the seconds and nanos fields have the same sign (to avoid serialization issues)
//  2. the golang representation of the duration is not negative
//
// Durations are capped to 250 years to prevent overflow and serialization errors.
// NB: to cap durations, the proto Seconds and Nanos fields are modified!
//
// nil durations are considered valid because they will be treated as the zero value.
// durationpb.CheckValid cannot be used directly because it will return an error for
// very large durations, but we are okay with truncating these.
func ValidateAndCapProtoDuration(d *durationpb.Duration) error {
	if d == nil {
		// nil durations are converted to 0 value
		return nil
	}

	if (d.GetSeconds() > 0 && d.GetNanos() < 0) || (d.GetSeconds() < 0 && d.GetNanos() > 0) {
		return errMismatchedSigns
	}

	// this is a best effort conversion and will return the closest value in the event of overflow.
	if d.AsDuration() < 0 {
		return errNegativeDuration
	}

	if d.AsDuration() > maxAllowedDuration {
		d.Seconds = maxSeconds
		d.Nanos = 0 // A year is always a round number of seconds.
	}

	return nil
}
