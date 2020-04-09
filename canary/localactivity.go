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
	"math/rand"
	"time"

	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
)

func registerLocalActivity(r registrar) {
	registerWorkflow(r, localActivityWorkfow, wfTypeLocalActivity)

	registerActivity(r, activityForCondition0, "")
	registerActivity(r, activityForCondition1, "")
	registerActivity(r, activityForCondition2, "")
	registerActivity(r, activityForCondition3, "")
	registerActivity(r, activityForCondition4, "")
}

type conditionAndAction struct {
	// condition is a function pointer to a local activity
	condition interface{}
	// action is a function pointer to a regular activity
	action interface{}
}

var checks = []conditionAndAction{
	{checkCondition0, activityForCondition0},
	{checkCondition1, activityForCondition1},
	{checkCondition2, activityForCondition2},
	{checkCondition3, activityForCondition3},
	{checkCondition4, activityForCondition4},
}

func localActivityWorkfow(ctx workflow.Context) (string, error) {
	profile, err := beginWorkflow(ctx, wfTypeSignal, workflow.Now(ctx).UnixNano())
	if err != nil {
		return "", err
	}

	logger := workflow.GetLogger(ctx)

	lao := workflow.LocalActivityOptions{
		// use short timeout as local activity is execute as function locally.
		ScheduleToCloseTimeout: time.Second,
	}
	ctx = workflow.WithLocalActivityOptions(ctx, lao)

	var data int32
	err = workflow.ExecuteLocalActivity(ctx, getConditionData).Get(ctx, &data)
	if err != nil {
		return "", profile.end(err)
	}
	logger.Sugar().Infof("Get condition data %v", data)

	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var actionFutures []workflow.Future

	for i, check := range checks {
		var conditionMeet bool
		err := workflow.ExecuteLocalActivity(ctx, check.condition, data).Get(ctx, &conditionMeet)
		if err != nil {
			return "", profile.end(err)
		}

		logger.Sugar().Infof("condition meet for %v: %v", i, conditionMeet)
		if conditionMeet {
			f := workflow.ExecuteActivity(ctx, check.action)
			actionFutures = append(actionFutures, f)
		}
	}

	var processResult string
	for _, f := range actionFutures {
		var actionResult string
		if err := f.Get(ctx, &actionResult); err != nil {
			return "", profile.end(err)
		}
		if len(processResult) > 0 {
			processResult += " and "
		}
		processResult += actionResult
	}

	logger.Sugar().Infof("Processed condition %v: %v", data, processResult)

	return processResult, profile.end(nil)
}

func getConditionData() (int32, error) {
	return rand.Int31n(100), nil
}

func checkCondition0(ctx context.Context, data int32) (bool, error) {
	return data < 10, nil
}

func checkCondition1(ctx context.Context, data int32) (bool, error) {
	return data >= 90, nil
}

func checkCondition2(ctx context.Context, data int32) (bool, error) {
	return data%2 == 0, nil
}

func checkCondition3(ctx context.Context, data int32) (bool, error) {
	return data%3 == 0, nil
}

func checkCondition4(ctx context.Context, data int32) (bool, error) {
	return data%5 == 0, nil
}

func activityForCondition0(ctx context.Context) (string, error) {
	activity.GetLogger(ctx).Info("process for condition 0")
	return "data < 10", nil
}

func activityForCondition1(ctx context.Context) (string, error) {
	activity.GetLogger(ctx).Info("process for condition 1")
	return "data >= 90", nil
}

func activityForCondition2(ctx context.Context) (string, error) {
	activity.GetLogger(ctx).Info("process for condition 2")
	return "data%2 == 0", nil
}

func activityForCondition3(ctx context.Context) (string, error) {
	activity.GetLogger(ctx).Info("process for condition 3")
	return "data%3 == 0", nil
}

func activityForCondition4(ctx context.Context) (string, error) {
	activity.GetLogger(ctx).Info("process for condition 4")
	return "data%5 == 0", nil
}
