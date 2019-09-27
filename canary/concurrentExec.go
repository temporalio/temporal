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

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	// to force cuncurrent execution of activity and pagination on history events
	// we need to make this number larger than the default page size, which is 1000
	numConcurrentExec   = 25
	totalConcurrentExec = 250
)

func init() {
	registerWorkflow(concurrentExecWorkflow, wfTypeConcurrentExec)
	registerActivity(concurrentExecActivity, activityTypeConcurrentExec)
}

// concurrentExecWorkflow is the workflow implementation to test
// 1. client side events pagination when reconstructing workflow state
// 2. concurrent execution of activities
func concurrentExecWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeConcurrentExec, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	selector := workflow.NewSelector(ctx)
	errors := make([]error, totalConcurrentExec)

	doActivity := func(index int) {
		now := workflow.Now(ctx).UnixNano()
		activityCtx := workflow.WithActivityOptions(ctx, newActivityOptions())

		future := workflow.ExecuteActivity(activityCtx, activityTypeConcurrentExec, now)
		selector.AddFuture(future, func(f workflow.Future) {
			// do not care about the return value
			errors[index] = f.Get(activityCtx, nil)
		})
	}

	for index := 0; index < numConcurrentExec; index++ {
		doActivity(index)
	}

	for index := numConcurrentExec; index < totalConcurrentExec; index++ {
		selector.Select(ctx)
		doActivity(index)
	}

	for index := 0; index < numConcurrentExec; index++ {
		selector.Select(ctx)
	}

	for _, err := range errors {
		if err != nil {
			workflow.GetLogger(ctx).Info("concurrentExecActivity failed", zap.Error(err))
			return profile.end(err)
		}
	}

	return profile.end(nil)
}

// concurrentExecActivity is the activity implementation for concurrent execution test
func concurrentExecActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeConcurrentExec, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	// canary test do not require any actual work to be done, so just return
	return nil
}
