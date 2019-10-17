// Copyright (c) 2019 Uber Technologies, Inc.
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

package canary

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	activityDelay                           = 4 * time.Second
	activityStartToCloseTimeoutThreshold    = 1 * time.Second
	activityScheduleToStartTimeoutThreshold = 1 * time.Second
	activityScheduleToCloseTimeoutThreshold = activityStartToCloseTimeoutThreshold + activityScheduleToStartTimeoutThreshold
)

func init() {
	registerWorkflow(timeoutWorkflow, wfTypeTimeout)
	registerActivity(timeoutActivity, activityTypeTimeout)
}

// timeoutWorkflow is the workflow implementation to test for querying workflow status
func timeoutWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeConcurrentExec, scheduledTimeNanos)
	if err != nil {
		return err
	}

	now := workflow.Now(ctx).UnixNano()
	activityOptions := newActivityOptions()
	activityOptions.StartToCloseTimeout = activityStartToCloseTimeoutThreshold
	activityOptions.ScheduleToStartTimeout = activityScheduleToStartTimeoutThreshold
	activityOptions.ScheduleToCloseTimeout = activityScheduleToCloseTimeoutThreshold
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)
	activityFuture := workflow.ExecuteActivity(activityCtx, timeoutActivity, now)

	activityErr := activityFuture.Get(ctx, nil)
	if activityErr != nil {
		if _, ok := activityErr.(*workflow.TimeoutError); !ok {
			workflow.GetLogger(ctx).Info("activity timeout failed", zap.Error(activityErr))
		} else {
			activityErr = nil
		}
	} else {
		activityErr = fmt.Errorf("activity timeout error expected, but no error received")
		workflow.GetLogger(ctx).Info("activity timeout failed", zap.Error(activityErr))
	}

	return profile.end(activityErr)
}

// timeoutActivity is the activity implementation for timeout test
func timeoutActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeConcurrentExec, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	timer := time.NewTimer(activityDelay)
	select {
	case <-timer.C:
	}
	timer.Stop()

	return nil
}
