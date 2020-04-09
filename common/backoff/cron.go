package backoff

import (
	"math"
	"time"

	"github.com/robfig/cron"
	"go.temporal.io/temporal-proto/serviceerror"
)

// NoBackoff is used to represent backoff when no cron backoff is needed
const NoBackoff = time.Duration(-1)

// ValidateSchedule validates a cron schedule spec
func ValidateSchedule(cronSchedule string) error {
	if cronSchedule == "" {
		return nil
	}
	if _, err := cron.ParseStandard(cronSchedule); err != nil {
		return serviceerror.NewInvalidArgument("Invalid CronSchedule.")
	}
	return nil
}

// GetBackoffForNextSchedule calculates the backoff time for the next run given
// a cronSchedule, workflow start time and workflow close time
func GetBackoffForNextSchedule(cronSchedule string, startTime time.Time, closeTime time.Time) time.Duration {
	if len(cronSchedule) == 0 {
		return NoBackoff
	}

	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return NoBackoff
	}
	startUTCTime := startTime.In(time.UTC)
	closeUTCTime := closeTime.In(time.UTC)
	nextScheduleTime := schedule.Next(startUTCTime)
	// Calculate the next schedule start time which is nearest to the close time
	for nextScheduleTime.Before(closeUTCTime) {
		nextScheduleTime = schedule.Next(nextScheduleTime)
	}
	backoffInterval := nextScheduleTime.Sub(closeUTCTime)
	roundedInterval := time.Second * time.Duration(math.Ceil(backoffInterval.Seconds()))
	return roundedInterval
}

// GetBackoffForNextScheduleInSeconds calculates the backoff time in seconds for the
// next run given a cronSchedule and current time
func GetBackoffForNextScheduleInSeconds(cronSchedule string, startTime time.Time, closeTime time.Time) int32 {
	backoffDuration := GetBackoffForNextSchedule(cronSchedule, startTime, closeTime)
	if backoffDuration == NoBackoff {
		return 0
	}
	return int32(math.Ceil(backoffDuration.Seconds()))
}
