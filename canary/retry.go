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

package canary

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/temporal"
	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

var (
	errRetryableActivityError = errors.New("Retry me")
	errUnexpectedProgress     = errors.New("Unexpected progress")
	errUnexpectedResult       = errors.New("Unexpected result")
)

func registerRetry(r registrar) {
	registerWorkflow(r, retryWorkflow, wfTypeRetry)
	registerActivity(r, retryOnTimeoutActivity, activityTypeRetryOnTimeout)
	registerActivity(r, retryOnFailureActivity, activityTypeRetryOnFailure)
}

func retryWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	profile, err := beginWorkflow(ctx, wfTypeRetry, scheduledTimeNanos)
	if err != nil {
		return err
	}

	info := workflow.GetInfo(ctx)
	now := workflow.Now(ctx).UnixNano()
	expiration := time.Duration(info.ExecutionStartToCloseTimeoutSeconds) * time.Second
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second * 5,
		BackoffCoefficient: 1,
		MaximumInterval:    time.Second * 5,
		ExpirationInterval: expiration,
		MaximumAttempts:    5,
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList:               taskListName,
		ScheduleToStartTimeout: expiration,
		StartToCloseTimeout:    expiration,
		HeartbeatTimeout:       5 * time.Second,
		RetryPolicy:            retryPolicy,
	})

	f1 := workflow.ExecuteActivity(activityCtx, activityTypeRetryOnTimeout, now)
	f2 := workflow.ExecuteActivity(activityCtx, activityTypeRetryOnFailure, now)

	var progress int
	err = f1.Get(ctx, &progress)
	if err != nil {
		workflow.GetLogger(ctx).Error("retryWorkflow failed", zap.Error(err))
		return profile.end(err)
	}

	if progress < 200 {
		workflow.GetLogger(ctx).Error("Unexpected activity progress.", zap.Int("Progress", progress))
		return profile.end(errUnexpectedProgress)
	}

	var result int32
	err = f2.Get(ctx, &result)
	if err != nil {
		workflow.GetLogger(ctx).Error("retryWorkflow failed", zap.Error(err))
		return profile.end(err)
	}

	if result < 3 {
		workflow.GetLogger(ctx).Error("Unexpected activity result.", zap.Int32("Result", result))
		return profile.end(errUnexpectedResult)
	}

	return profile.end(nil)

}

func retryOnTimeoutActivity(ctx context.Context, scheduledTimeNanos int64) (int, error) {
	var err error

	progress := 0
	if activity.HasHeartbeatDetails(ctx) {
		err = activity.GetHeartbeatDetails(ctx, &progress)
		if err != nil {
			activity.GetLogger(ctx).Error("GetProgress failed.", zap.Error(err))
			return 0, err
		}
	}

	info := activity.GetInfo(ctx)
	if info.Attempt < 3 {
		activity.RecordHeartbeat(ctx, info.Attempt*100)
		time.Sleep(2 * info.HeartbeatTimeout)
		// Currently we have a server bug which accepts completion from different attempt
		// For now fail the activity
		return 0, errRetryableActivityError
	}

	return progress, nil
}

func retryOnFailureActivity(ctx context.Context, scheduledTimeNanos int64) (int32, error) {
	info := activity.GetInfo(ctx)
	if info.Attempt < 3 {
		return 0, errRetryableActivityError
	}

	return info.Attempt, nil
}
