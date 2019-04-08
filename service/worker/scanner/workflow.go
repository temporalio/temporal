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

package scanner

import (
	"context"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	cclient "go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/service/worker/scanner/tasklist"
)

type contextKey int

const (
	scannerContextKey = contextKey(0)

	maxConcurrentActivityExecutionSize     = 10
	maxConcurrentDecisionTaskExecutionSize = 10
	infiniteDuration                       = 20 * 365 * 24 * time.Hour

	tlScannerWFID                 = "cadence-sys-tl-scanner"
	tlScannerWFTypeName           = "cadence-sys-tl-scanner-workflow"
	tlScannerTaskListName         = "cadence-sys-tl-scanner-tasklist-0"
	taskListScavengerActivityName = "cadence-sys-tl-scanner-scvg-activity"
)

var (
	tlScavengerHBInterval = 10 * time.Second

	tlScavengerActivityRetryPolicy = cadence.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 1.7,
		MaximumInterval:    5 * time.Minute,
		ExpirationInterval: infiniteDuration,
	}
	tlScannerWFStartOptions = cclient.StartWorkflowOptions{
		ID:                           tlScannerWFID,
		TaskList:                     tlScannerTaskListName,
		ExecutionStartToCloseTimeout: 5 * 24 * time.Hour,
		WorkflowIDReusePolicy:        cclient.WorkflowIDReusePolicyAllowDuplicate,
		CronSchedule:                 "0 */12 * * *",
	}
)

func init() {
	workflow.RegisterWithOptions(TaskListScannerWorkflow, workflow.RegisterOptions{Name: tlScannerWFTypeName})
	activity.RegisterWithOptions(TaskListScavengerActivity, activity.RegisterOptions{Name: taskListScavengerActivityName})
}

// TaskListScannerWorkflow is the workflow that runs the task-list scanner background daemon
func TaskListScannerWorkflow(ctx workflow.Context) error {
	opts := workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Minute,
		StartToCloseTimeout:    infiniteDuration,
		HeartbeatTimeout:       5 * time.Minute,
		RetryPolicy:            &tlScavengerActivityRetryPolicy,
	}
	future := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), taskListScavengerActivityName)
	return future.Get(ctx, nil)
}

// TaskListScavengerActivity is the activity that runs task list scavenger
func TaskListScavengerActivity(aCtx context.Context) error {
	ctx := aCtx.Value(scannerContextKey).(scannerContext)
	scavenger := tasklist.NewScavenger(ctx.taskDB, ctx.metricsClient, ctx.logger)
	ctx.logger.Info("Starting task list scavenger")
	scavenger.Start()
	for scavenger.Alive() {
		activity.RecordHeartbeat(aCtx)
		if aCtx.Err() != nil {
			ctx.logger.Infof("activity context error, stopping scavenger: %v", aCtx.Err())
			scavenger.Stop()
			return aCtx.Err()
		}
		time.Sleep(tlScavengerHBInterval)
	}
	return nil
}
