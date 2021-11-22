// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	if _, err := cron.ParseStandard(cronSchedule); err != nil {
		return serviceerror.NewInvalidArgument("Invalid CronSchedule.")
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
		for nextScheduleTime.Before(nowUTC) {
			nextScheduleTime = schedule.Next(nextScheduleTime)
		}
	}

	backoffInterval := nextScheduleTime.Sub(nowUTC)
	roundedInterval := time.Second * time.Duration(convert.Int64Ceil(backoffInterval.Seconds()))
	return roundedInterval
}

// GetBackoffForNextScheduleNonNegative calculates the backoff time and ensures a non-negative duration.
func GetBackoffForNextScheduleNonNegative(cronSchedule string, scheduledTime time.Time, now time.Time) *time.Duration {
	backoffDuration := GetBackoffForNextSchedule(cronSchedule, scheduledTime, now)
	if backoffDuration == NoBackoff || backoffDuration < 0 {
		backoffDuration = 0
	}
	return &backoffDuration
}
