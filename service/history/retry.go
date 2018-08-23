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
	"math"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

func prepareNextRetry(a *persistence.ActivityInfo, errReason string) persistence.Task {
	return prepareNextRetryWithNowTime(a, errReason, time.Now())
}

func prepareNextRetryWithNowTime(a *persistence.ActivityInfo, errReason string, now time.Time) persistence.Task {
	if !a.HasRetryPolicy || a.CancelRequested {
		return nil
	}

	backoffInterval := getBackoffInterval(a.Attempt, a.MaximumAttempts, a.InitialInterval, a.MaximumInterval, a.BackoffCoefficient, now, a.ExpirationTime, errReason, a.NonRetriableErrors)
	if backoffInterval == common.NoRetryBackoff {
		return nil
	}
	nextScheduleTime := now.Add(backoffInterval)

	// a retry is needed, update activity info for next retry
	a.Attempt++
	a.ScheduledTime = nextScheduleTime // update to next schedule time
	a.StartedID = common.EmptyEventID
	a.RequestID = ""
	a.StartedTime = time.Time{}
	a.LastHeartBeatUpdatedTime = time.Time{}
	// clear timer created bits except for ScheduleToClose
	a.TimerTaskStatus = TimerTaskStatusNone | (a.TimerTaskStatus & TimerTaskStatusCreatedScheduleToClose)

	return &persistence.ActivityRetryTimerTask{
		Version:             a.Version,
		VisibilityTimestamp: a.ScheduledTime,
		EventID:             a.ScheduleID,
		Attempt:             a.Attempt,
	}
}

func getBackoffInterval(currAttempt, maxAttempts, initInterval, maxInterval int32, backoffCoefficient float64, now, expirationTime time.Time, errReason string, nonRetriableErrors []string) time.Duration {
	if maxAttempts == 0 && expirationTime.IsZero() {
		return common.NoRetryBackoff
	}

	if maxAttempts > 0 && currAttempt >= maxAttempts-1 {
		// currAttempt starts from 0.
		// MaximumAttempts is the total attempts, including initial (non-retry) attempt.
		return common.NoRetryBackoff
	}

	nextInterval := int64(float64(initInterval) * math.Pow(backoffCoefficient, float64(currAttempt)))
	if nextInterval <= 0 {
		// math.Pow() could overflow
		if maxInterval > 0 {
			nextInterval = int64(maxInterval)
		} else {
			return common.NoRetryBackoff
		}
	}

	if maxInterval > 0 && nextInterval > int64(maxInterval) {
		// cap next interval to MaxInterval
		nextInterval = int64(maxInterval)
	}

	backoffInterval := time.Duration(nextInterval) * time.Second
	nextScheduleTime := now.Add(backoffInterval)
	if !expirationTime.IsZero() && nextScheduleTime.After(expirationTime) {
		return common.NoRetryBackoff
	}

	// check if error is non-retriable
	for _, er := range nonRetriableErrors {
		if er == errReason {
			return common.NoRetryBackoff
		}
	}

	return backoffInterval
}

func getTimeoutErrorReason(timeoutType shared.TimeoutType) string {
	return "timeout:" + timeoutType.String()
}
