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

package scanner

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/worker/scanner/executions"
	"go.temporal.io/server/service/worker/scanner/history"
	"go.temporal.io/server/service/worker/scanner/taskqueue"
)

const (
	infiniteDuration = 20 * 365 * 24 * time.Hour

	tqScannerWFID                  = "temporal-sys-tq-scanner"
	tqScannerWFTypeName            = "temporal-sys-tq-scanner-workflow"
	tqScannerTaskQueueName         = "temporal-sys-tq-scanner-taskqueue-0"
	taskQueueScavengerActivityName = "temporal-sys-tq-scanner-scvg-activity"

	historyScannerWFID           = "temporal-sys-history-scanner"
	historyScannerWFTypeName     = "temporal-sys-history-scanner-workflow"
	historyScannerTaskQueueName  = "temporal-sys-history-scanner-taskqueue-0"
	historyScavengerActivityName = "temporal-sys-history-scanner-scvg-activity"

	executionsScannerWFID           = "temporal-sys-executions-scanner"
	executionsScannerWFTypeName     = "temporal-sys-executions-scanner-workflow"
	executionsScannerTaskQueueName  = "temporal-sys-executions-scanner-taskqueue-0"
	executionsScavengerActivityName = "temporal-sys-executions-scanner-scvg-activity"
)

type (
	scannerContextKeyType struct{}
)

var (
	scannerContextKey             = scannerContextKeyType{}
	tlScavengerHBInterval         = 10 * time.Second
	executionsScavengerHBInterval = 10 * time.Second

	activityRetryPolicy = temporal.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 1.7,
		MaximumInterval:    5 * time.Minute,
	}
	activityOptions = workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Minute,
		StartToCloseTimeout:    infiniteDuration,
		HeartbeatTimeout:       5 * time.Minute,
		RetryPolicy:            &activityRetryPolicy,
	}
	tlScannerWFStartOptions = client.StartWorkflowOptions{
		ID:                    tqScannerWFID,
		TaskQueue:             tqScannerTaskQueueName,
		WorkflowRunTimeout:    5 * 24 * time.Hour,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          "0 */12 * * *",
	}
	historyScannerWFStartOptions = client.StartWorkflowOptions{
		ID:                    historyScannerWFID,
		TaskQueue:             historyScannerTaskQueueName,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          "0 */12 * * *",
	}
	executionsScannerWFStartOptions = client.StartWorkflowOptions{
		ID:                    executionsScannerWFID,
		TaskQueue:             executionsScannerTaskQueueName,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          "0 */12 * * *",
	}
)

// TaskQueueScannerWorkflow is the workflow that runs the task queue scanner background daemon
func TaskQueueScannerWorkflow(
	ctx workflow.Context,
) error {

	future := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, activityOptions), taskQueueScavengerActivityName)
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
) error {
	future := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, activityOptions), executionsScavengerActivityName)
	return future.Get(ctx, nil)
}

// HistoryScavengerActivity is the activity that runs history scavenger
func HistoryScavengerActivity(
	activityCtx context.Context,
) (history.ScavengerHeartbeatDetails, error) {

	ctx := activityCtx.Value(scannerContextKey).(scannerContext)
	rps := ctx.cfg.PersistenceMaxQPS()
	numShards := ctx.cfg.Persistence.NumHistoryShards

	hbd := history.ScavengerHeartbeatDetails{}
	if activity.HasHeartbeatDetails(activityCtx) {
		if err := activity.GetHeartbeatDetails(activityCtx, &hbd); err != nil {
			ctx.logger.Error("Failed to recover from last heartbeat, start over from beginning", tag.Error(err))
		}
	}

	scavenger := history.NewScavenger(
		numShards,
		ctx.executionManager,
		rps,
		ctx.historyClient,
		hbd,
		ctx.metricsClient,
		ctx.logger,
	)
	return scavenger.Run(activityCtx)
}

// TaskQueueScavengerActivity is the activity that runs task queue scavenger
func TaskQueueScavengerActivity(
	activityCtx context.Context,
) error {
	ctx := activityCtx.Value(scannerContextKey).(scannerContext)
	scavenger := taskqueue.NewScavenger(ctx.taskManager, ctx.metricsClient, ctx.logger)
	ctx.logger.Info("Starting task queue scavenger")
	scavenger.Start()
	for scavenger.Alive() {
		activity.RecordHeartbeat(activityCtx)
		if activityCtx.Err() != nil {
			ctx.logger.Info("activity context error, stopping scavenger", tag.Error(activityCtx.Err()))
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
) error {
	ctx := activityCtx.Value(scannerContextKey).(scannerContext)

	metricsClient := ctx.metricsClient
	scavenger := executions.NewScavenger(
		ctx.cfg.Persistence.NumHistoryShards,
		ctx.executionManager,
		metricsClient,
		ctx.logger,
	)
	scavenger.Start()
	for scavenger.Alive() {
		activity.RecordHeartbeat(activityCtx)
		if activityCtx.Err() != nil {
			ctx.logger.Info("activity context error, stopping scavenger", tag.Error(activityCtx.Err()))
			scavenger.Stop()
			return activityCtx.Err()
		}
		time.Sleep(executionsScavengerHBInterval)
	}
	return nil
}
