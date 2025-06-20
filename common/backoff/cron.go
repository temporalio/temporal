package backoff

import (
	"time"

	"github.com/robfig/cron/v3"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/convert"
)

// NoBackoff is used to represent backoff when no cron backoff is needed
const NoBackoff = time.Duration(-1)

// ValidateSchedule validates a cron schedule spec
func ValidateSchedule(cronSchedule string) error {
	if cronSchedule == "" {
		return nil
	}
	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return serviceerror.NewInvalidArgument("invalid CronSchedule.")
	}
	nextTime := schedule.Next(time.Now().UTC())
	if nextTime.IsZero() {
		// no time can be found to satisfy the schedule
		return serviceerror.NewInvalidArgument("invalid CronSchedule, no time can be found to satisfy the schedule")
	}
	return nil
}

// GetBackoffForNextSchedule calculates the backoff time for the next run given
// a cronSchedule, current scheduled time, and now.
func GetBackoffForNextSchedule(cronSchedule string, scheduledTime time.Time, now time.Time) time.Duration {
	if len(cronSchedule) == 0 {
		return NoBackoff
	}

	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return NoBackoff
	}

	scheduledUTCTime := scheduledTime.UTC()
	nowUTC := now.UTC()

	var nextScheduleTime time.Time
	if nowUTC.Before(scheduledUTCTime) {
		nextScheduleTime = scheduledUTCTime
	} else {
		nextScheduleTime = schedule.Next(scheduledUTCTime)
		// Calculate the next schedule start time which is nearest to now (right after now).
		for !nextScheduleTime.IsZero() && nextScheduleTime.Before(nowUTC) {
			nextScheduleTime = schedule.Next(nextScheduleTime)
		}
	}
	if nextScheduleTime.IsZero() {
		// no time can be found to satisfy the schedule
		return NoBackoff
	}

	backoffInterval := nextScheduleTime.Sub(nowUTC)
	roundedInterval := time.Second * time.Duration(convert.Int64Ceil(backoffInterval.Seconds()))
	return roundedInterval
}

// GetBackoffForNextScheduleNonNegative calculates the backoff time and ensures a non-negative duration.
func GetBackoffForNextScheduleNonNegative(cronSchedule string, scheduledTime time.Time, now time.Time) time.Duration {
	backoffDuration := GetBackoffForNextSchedule(cronSchedule, scheduledTime, now)
	if backoffDuration == NoBackoff || backoffDuration < 0 {
		backoffDuration = 0
	}
	return backoffDuration
}
