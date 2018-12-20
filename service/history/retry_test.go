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

package history

import (
	"github.com/uber/cadence/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/cadence/common/persistence"
)

func Test_NextRetry(t *testing.T) {
	a := assert.New(t)
	now, _ := time.Parse(time.RFC3339, "2018-04-13T16:08:08+00:00")
	reason := "good-reason"

	// no retry without retry policy
	var version int64 = 59
	ai := &persistence.ActivityInfo{
		Version:                version,
		ScheduleToStartTimeout: 5,
		ScheduleToCloseTimeout: 30,
		StartToCloseTimeout:    25,
		HasRetryPolicy:         false,
		NonRetriableErrors:     []string{"bad-reason", "ugly-reason"},
	}
	a.Nil(prepareActivityNextRetry(version, ai, reason))

	// no retry if cancel requested
	ai.HasRetryPolicy = true
	ai.CancelRequested = true
	a.Nil(prepareActivityNextRetry(version, ai, reason))

	// no retry if both MaximumAttempts and ExpirationTime are not set
	ai.CancelRequested = false
	a.Nil(prepareActivityNextRetry(version, ai, reason))

	// no retry if MaximumAttempts is 1 (for initial attempt)
	ai.InitialInterval = 1
	ai.MaximumAttempts = 1
	a.Nil(prepareActivityNextRetry(version, ai, reason))

	// backoff retry, intervals: 1s, 2s, 4s, 8s.
	ai.MaximumAttempts = 5
	ai.BackoffCoefficient = 2
	retryTask := prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.NotNil(retryTask)
	a.Equal(version, retryTask.GetVersion())
	a.Equal(now.Add(time.Second), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)

	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.NotNil(retryTask)
	a.Equal(now.Add(time.Second*2), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)

	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.NotNil(retryTask)
	a.Equal(now.Add(time.Second*4), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)

	// test non-retriable error
	reason = "bad-reason"
	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.Nil(retryTask)
	reason = "good-reason"

	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.NotNil(retryTask)
	a.Equal(now.Add(time.Second*8), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)

	// no retry as max attempt reached
	a.Equal(ai.MaximumAttempts-1, ai.Attempt)
	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.Nil(retryTask)

	// increase max attempts, with max interval cap at 10s
	ai.MaximumAttempts = 6
	ai.MaximumInterval = 10
	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.NotNil(retryTask)
	a.Equal(now.Add(time.Second*10), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)

	// no retry because expiration time before next interval
	ai.MaximumAttempts = 8
	ai.ExpirationTime = now.Add(time.Second * 5)
	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.Nil(retryTask)

	// extend expiration, next interval should be 10s
	version += 10
	ai.ExpirationTime = now.Add(time.Minute)
	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.NotNil(retryTask)
	a.Equal(version, ai.Version)
	a.Equal(now.Add(time.Second*10), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)

	// with big max retry, math.Pow() could overflow, verify that it uses the MaxInterval
	ai.Attempt = 64
	ai.MaximumAttempts = 100
	retryTask = prepareActivityNextRetryWithNowTime(version, ai, reason, now)
	a.Equal(now.Add(time.Second*10), retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp)
}

func Test_NextCronSchedule(t *testing.T) {
	a := assert.New(t)

	// every day cron
	now, _ := time.Parse(time.RFC3339, "2018-12-17T08:08:00+00:00")
	cronSpec := "0 10 * * *"
	backoff := getBackoffForNextCronSchedule(cronSpec, now)
	a.Equal(time.Minute*112, backoff)
	backoff = getBackoffForNextCronSchedule(cronSpec, now.Add(backoff))
	a.Equal(time.Hour*24, backoff)

	// every hour cron
	now, _ = time.Parse(time.RFC3339, "2018-12-17T08:08:00+00:00")
	cronSpec = "0 * * * *"
	backoff = getBackoffForNextCronSchedule(cronSpec, now)
	a.Equal(time.Minute*52, backoff)
	backoff = getBackoffForNextCronSchedule(cronSpec, now.Add(backoff))
	a.Equal(time.Hour, backoff)

	// every minute cron
	now, _ = time.Parse(time.RFC3339, "2018-12-17T08:08:18+00:00")
	cronSpec = "* * * * *"
	backoff = getBackoffForNextCronSchedule(cronSpec, now)
	a.Equal(time.Second*42, backoff)
	backoff = getBackoffForNextCronSchedule(cronSpec, now.Add(backoff))
	a.Equal(time.Minute, backoff)

	// invalid cron spec
	cronSpec = "invalid-cron-spec"
	backoff = getBackoffForNextCronSchedule(cronSpec, now)
	a.Equal(common.NoRetryBackoff, backoff)
}
