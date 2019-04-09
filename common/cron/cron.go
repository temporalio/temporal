// Copyright (c) 2017 Uber Technologies, Inc.
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

package cron

import (
	"math"
	"time"

	"github.com/robfig/cron"
	workflow "github.com/uber/cadence/.gen/go/shared"
)

// NoBackoff is used to represent backoff when no cron backoff is needed
const NoBackoff = time.Duration(-1)

// ValidateSchedule validates a cron schedule spec
func ValidateSchedule(cronSchedule string) error {
	if cronSchedule == "" {
		return nil
	}
	if _, err := cron.Parse(cronSchedule); err != nil {
		return &workflow.BadRequestError{Message: "Invalid CronSchedule."}
	}
	return nil
}

// GetBackoffForNextSchedule calculates the backoff time for the next run given
// a cronSchedule and current time
func GetBackoffForNextSchedule(cronSchedule string, nowTime time.Time) time.Duration {
	if len(cronSchedule) == 0 {
		return NoBackoff
	}

	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return NoBackoff
	}

	nowTime = nowTime.In(time.UTC)
	backoffInterval := schedule.Next(nowTime).Sub(nowTime)
	roundedInterval := time.Second * time.Duration(math.Ceil(backoffInterval.Seconds()))
	return roundedInterval
}

// GetBackoffForNextScheduleInSeconds calculates the backoff time in seconds for the
// next run given a cronSchedule and current time
func GetBackoffForNextScheduleInSeconds(cronSchedule string, nowTime time.Time) int32 {
	backoffDuration := GetBackoffForNextSchedule(cronSchedule, nowTime)
	if backoffDuration == NoBackoff {
		return 0
	}
	return int32(math.Ceil(backoffDuration.Seconds()))
}
