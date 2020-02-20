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

	"github.com/uber/cadence/service/worker/scanner/executions"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	cclient "go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/service/worker/scanner/history"
	"github.com/uber/cadence/service/worker/scanner/tasklist"
)

type (
	contextKey int
)

const (
	scannerContextKey = contextKey(0)

	maxConcurrentActivityExecutionSize     = 10
	maxConcurrentDecisionTaskExecutionSize = 10
	infiniteDuration                       = 20 * 365 * 24 * time.Hour

	tlScannerWFID                 = "cadence-sys-tl-scanner"
	tlScannerWFTypeName           = "cadence-sys-tl-scanner-workflow"
	tlScannerTaskListName         = "cadence-sys-tl-scanner-tasklist-0"
	taskListScavengerActivityName = "cadence-sys-tl-scanner-scvg-activity"

	historyScannerWFID           = "cadence-sys-history-scanner"
	historyScannerWFTypeName     = "cadence-sys-history-scanner-workflow"
	historyScannerTaskListName   = "cadence-sys-history-scanner-tasklist-0"
	historyScavengerActivityName = "cadence-sys-history-scanner-scvg-activity"

	executionsScannerWFID           = "cadence-sys-executions-scanner"
	executionsScannerWFTypeName     = "cadence-sys-executions-scanner-workflow"
	executionsScannerTaskListName   = "cadence-sys-executions-scanner-tasklist-0"
	executionsScavengerActivityName = "cadence-sys-executions-scanner-scvg-activity"
)

var (
	tlScavengerHBInterval         = 10 * time.Second
	executionsScavengerHBInterval = 10 * time.Second

	activityRetryPolicy = cadence.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 1.7,
		MaximumInterval:    5 * time.Minute,
		ExpirationInterval: infiniteDuration,
	}
	activityOptions = workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Minute,
		StartToCloseTimeout:    infiniteDuration,
		HeartbeatTimeout:       5 * time.Minute,
		RetryPolicy:            &activityRetryPolicy,
	}
	tlScannerWFStartOptions = cclient.StartWorkflowOptions{
		ID:                           tlScannerWFID,
		TaskList:                     tlScannerTaskListName,
		ExecutionStartToCloseTimeout: 5 * 24 * time.Hour,
		WorkflowIDReusePolicy:        cclient.WorkflowIDReusePolicyAllowDuplicate,
		CronSchedule:                 "0 */12 * * *",
	}
	historyScannerWFStartOptions = cclient.StartWorkflowOptions{
		ID:                           historyScannerWFID,
		TaskList:                     historyScannerTaskListName,
		ExecutionStartToCloseTimeout: infiniteDuration,
		WorkflowIDReusePolicy:        cclient.WorkflowIDReusePolicyAllowDuplicate,
		CronSchedule:                 "0 */12 * * *",
	}
	executionsScannerWFStartOptions = cclient.StartWorkflowOptions{
		ID:                           executionsScannerWFID,
		TaskList:                     executionsScannerTaskListName,
		ExecutionStartToCloseTimeout: infiniteDuration,
		WorkflowIDReusePolicy:        cclient.WorkflowIDReusePolicyAllowDuplicate,
		CronSchedule:                 "0 */12 * * *",
	}
)

func init() {
	workflow.RegisterWithOptions(TaskListScannerWorkflow, workflow.RegisterOptions{Name: tlScannerWFTypeName})
	workflow.RegisterWithOptions(HistoryScannerWorkflow, workflow.RegisterOptions{Name: historyScannerWFTypeName})
	workflow.RegisterWithOptions(ExecutionsScannerWorkflow, workflow.RegisterOptions{Name: executionsScannerWFTypeName})

	activity.RegisterWithOptions(TaskListScavengerActivity, activity.RegisterOptions{Name: taskListScavengerActivityName})
	activity.RegisterWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
	activity.RegisterWithOptions(ExecutionsScavengerActivity, activity.RegisterOptions{Name: executionsScavengerActivityName})
}

// TaskListScannerWorkflow is the workflow that runs the task-list scanner background daemon
func TaskListScannerWorkflow(
	ctx workflow.Context,
) error {

	future := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, activityOptions), taskListScavengerActivityName)
	return future.Get(ctx, nil)
}

// HistoryScannerWorkflow is the workflow that runs the history scanner background daemon
func HistoryScannerWorkflow(
	ctx workflow.Context,
) error {

	future := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, activityOptions),
		historyScavengerActivityName,
	)
	return future.Get(ctx, nil)
}

// ExecutionsScannerWorkflow is the workflow that runs the executions scanner background daemon
func ExecutionsScannerWorkflow(
	ctx workflow.Context,
	executionsScannerWorkflowParams executions.ScannerWorkflowParams,
) error {

	future := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, activityOptions), executionsScavengerActivityName, executionsScannerWorkflowParams)
	return future.Get(ctx, nil)
}

// HistoryScavengerActivity is the activity that runs history scavenger
func HistoryScavengerActivity(
	activityCtx context.Context,
) (history.ScavengerHeartbeatDetails, error) {

	ctx := activityCtx.Value(scannerContextKey).(scannerContext)
	rps := ctx.cfg.PersistenceMaxQPS()

	hbd := history.ScavengerHeartbeatDetails{}
	if activity.HasHeartbeatDetails(activityCtx) {
		if err := activity.GetHeartbeatDetails(activityCtx, &hbd); err != nil {
			ctx.GetLogger().Error("Failed to recover from last heartbeat, start over from beginning", tag.Error(err))
		}
	}

	scavenger := history.NewScavenger(
		ctx.GetHistoryManager(),
		rps,
		ctx.GetHistoryClient(),
		hbd,
		ctx.GetMetricsClient(),
		ctx.GetLogger(),
	)
	return scavenger.Run(activityCtx)
}

// TaskListScavengerActivity is the activity that runs task list scavenger
func TaskListScavengerActivity(
	activityCtx context.Context,
) error {

	ctx := activityCtx.Value(scannerContextKey).(scannerContext)
	scavenger := tasklist.NewScavenger(ctx.GetTaskManager(), ctx.GetMetricsClient(), ctx.GetLogger())
	ctx.GetLogger().Info("Starting task list scavenger")
	scavenger.Start()
	for scavenger.Alive() {
		activity.RecordHeartbeat(activityCtx)
		if activityCtx.Err() != nil {
			ctx.GetLogger().Info("activity context error, stopping scavenger", tag.Error(activityCtx.Err()))
			scavenger.Stop()
			return activityCtx.Err()
		}
		time.Sleep(tlScavengerHBInterval)
	}
	return nil
}

// ExecutionsScavengerActivity is the activity that runs executions scavenger
func ExecutionsScavengerActivity(
	activityCtx context.Context,
	executionsScannerWorkflowParams executions.ScannerWorkflowParams,
) error {

	ctx := activityCtx.Value(scannerContextKey).(scannerContext)
	scavenger := executions.NewScavenger(executionsScannerWorkflowParams, ctx.GetFrontendClient(), ctx.GetHistoryManager(), ctx.GetMetricsClient(), ctx.GetLogger())
	ctx.GetLogger().Info("Starting executions scavenger")
	scavenger.Start()
	for scavenger.Alive() {
		activity.RecordHeartbeat(activityCtx)
		if activityCtx.Err() != nil {
			ctx.GetLogger().Info("activity context error, stopping scavenger", tag.Error(activityCtx.Err()))
			scavenger.Stop()
			return activityCtx.Err()
		}
		time.Sleep(executionsScavengerHBInterval)
	}
	return nil
}
