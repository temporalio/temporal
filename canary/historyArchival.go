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
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/pborman/uuid"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	numHistoryArchivals = 5
	resultSize          = 1024 // 1KB
)

func init() {
	registerWorkflow(historyArchivalWorkflow, wfTypeHistoryArchival)
	registerActivity(historyArchivalActivity, activityTypeHistoryArchival)
	registerWorkflow(archivalExternalWorkflow, wfTypeArchivalExternal)
	registerActivity(largeResultActivity, activityTypeLargeResult)
}

func historyArchivalWorkflow(ctx workflow.Context, scheduledTimeNanos int64, _ string) error {
	profile, err := beginWorkflow(ctx, wfTypeHistoryArchival, scheduledTimeNanos)
	if err != nil {
		return err
	}
	ch := workflow.NewBufferedChannel(ctx, numHistoryArchivals)
	for i := 0; i < numHistoryArchivals; i++ {
		workflow.Go(ctx, func(ctx2 workflow.Context) {
			aCtx := workflow.WithActivityOptions(ctx2, newActivityOptions())
			err := workflow.ExecuteActivity(aCtx, activityTypeHistoryArchival, workflow.Now(ctx2).UnixNano()).Get(aCtx, nil)
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			ch.Send(ctx2, errStr)
		})
	}
	successfulArchivalsCount := 0
	for i := 0; i < numHistoryArchivals; i++ {
		var errStr string
		ch.Receive(ctx, &errStr)
		if errStr != "" {
			workflow.GetLogger(ctx).Error("at least one archival failed", zap.Int("success-count", successfulArchivalsCount), zap.String("err-string", errStr))
			return profile.end(errors.New(errStr))
		}
		successfulArchivalsCount++
	}

	return profile.end(nil)
}

func historyArchivalActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeHistoryArchival, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityArchivalContext(ctx).cadence
	execution, err := executeArchivalExeternalWorkflow(ctx, client, scheduledTimeNanos)
	if err != nil {
		return err
	}
	getHistoryReq := &shared.GetWorkflowExecutionHistoryRequest{
		Domain:    stringPtr(archivalDomain),
		Execution: execution,
	}

	failureReason := ""
	attempts := 0
	expireTime := time.Now().Add(activityTaskTimeout)
	for {
		<-time.After(5 * time.Second)
		if time.Now().After(expireTime) {
			break
		}
		attempts++
		bCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		history, err := client.Service.GetWorkflowExecutionHistory(bCtx, getHistoryReq)
		cancel()
		if err != nil {
			failureReason = fmt.Sprintf("error accessing history, %v", err.Error())
		} else if !history.GetArchived() {
			failureReason = "history is not archived"
		} else if len(history.History.Events) == 0 {
			failureReason = "got empty history"
		} else {
			return nil
		}
	}
	activity.GetLogger(ctx).Error("failed to get archived history within time limit",
		zap.String("failure_reason", failureReason),
		zap.String("domain", archivalDomain),
		zap.String("workflow_id", execution.GetWorkflowId()),
		zap.String("run_id", execution.GetRunId()),
		zap.Int("attempts", attempts))
	return fmt.Errorf("failed to get archived history within time limit, %v", failureReason)
}

func executeArchivalExeternalWorkflow(
	ctx context.Context,
	client cadenceClient,
	scheduledTimeNanos int64,
) (*shared.WorkflowExecution, error) {
	workflowID := fmt.Sprintf("%v.%v", wfTypeArchivalExternal, uuid.New())
	ops := newWorkflowOptions(workflowID, childWorkflowTimeout)
	ops.TaskList = archivalTaskListName
	workflowRun, err := client.ExecuteWorkflow(ctx, ops, wfTypeArchivalExternal, scheduledTimeNanos)
	if err != nil {
		return nil, err
	}

	if err := workflowRun.Get(ctx, nil); err != nil {
		return nil, err
	}
	return &shared.WorkflowExecution{
		WorkflowId: stringPtr(workflowID),
		RunId:      stringPtr(workflowRun.GetRunID()),
	}, nil
}

func archivalExternalWorkflow(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeArchivalExternal, scheduledTimeNanos)
	if err != nil {
		return err
	}
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	}
	var result []byte
	numActs := rand.Intn(10) + 1
	for i := 0; i != numActs; i++ {
		aCtx := workflow.WithActivityOptions(ctx, ao)
		if err = workflow.ExecuteActivity(aCtx, activityTypeLargeResult).Get(aCtx, &result); err != nil {
			break
		}
	}
	if err != nil {
		return profile.end(err)
	}
	return profile.end(nil)
}

func largeResultActivity() ([]byte, error) {
	return make([]byte, resultSize, resultSize), nil
}
