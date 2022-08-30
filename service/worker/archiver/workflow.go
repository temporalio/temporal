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

package archiver

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type dynamicConfigResult struct {
	ArchiverConcurrency   int
	ArchivalsPerIteration int
	TimelimitPerIteration time.Duration
}

func archivalWorkflow(ctx workflow.Context, carryover []ArchiveRequest) error {
	return archivalWorkflowHelper(ctx, globalLogger, globalMetricsHandler, globalConfig, nil, nil, carryover)
}

func archivalWorkflowHelper(
	ctx workflow.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *Config,
	handler Handler, // enables tests to inject mocks
	pump Pump, // enables tests to inject mocks
	carryover []ArchiveRequest,
) error {
	metricsHandler = NewReplayMetricsHandler(metricsHandler, ctx)
	metricsHandler = metricsHandler.WithTags(metrics.OperationTag(metrics.ArchiverArchivalWorkflowOperation))
	metricsHandler.Counter(metrics.ArchiverWorkflowStartedCount.MetricName.String()).Record(1)
	startTime := workflow.Now(ctx)
	workflowInfo := workflow.GetInfo(ctx)
	logger = log.With(
		logger,
		tag.WorkflowID(workflowInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(workflowInfo.WorkflowExecution.RunID),
		tag.WorkflowTaskQueueName(workflowInfo.TaskQueueName),
		tag.WorkflowType(workflowInfo.WorkflowType.Name))
	logger = log.NewReplayLogger(logger, ctx, false)

	logger.Info("archival system workflow started")
	var dcResult dynamicConfigResult
	_ = workflow.SideEffect(
		ctx,
		func(ctx workflow.Context) interface{} {
			timeLimit := config.TimeLimitPerArchivalIteration()
			maxTimeLimit := MaxArchivalIterationTimeout()
			if timeLimit > maxTimeLimit {
				timeLimit = maxTimeLimit
			}
			return dynamicConfigResult{
				ArchiverConcurrency:   config.ArchiverConcurrency(),
				ArchivalsPerIteration: config.ArchivalsPerIteration(),
				TimelimitPerIteration: timeLimit,
			}
		}).Get(&dcResult)
	requestCh := workflow.NewBufferedChannel(ctx, dcResult.ArchivalsPerIteration)
	if handler == nil {
		handler = NewHandler(ctx, logger, metricsHandler, dcResult.ArchiverConcurrency, requestCh)
	}
	handlerStartTime := workflow.Now(ctx)
	handler.Start()
	signalCh := workflow.GetSignalChannel(ctx, signalName)
	if pump == nil {
		pump = NewPump(ctx, logger, metricsHandler, carryover, dcResult.TimelimitPerIteration, dcResult.ArchivalsPerIteration, requestCh, signalCh)
	}
	pumpResult := pump.Run()
	metricsHandler.Counter(metrics.ArchiverNumPumpedRequestsCount.MetricName.String()).Record(int64(len(pumpResult.PumpedHashes)))
	handledHashes := handler.Finished()
	handlerEndTime := workflow.Now(ctx)
	metricsHandler.Timer(metrics.ArchiverHandleAllRequestsLatency.MetricName.String()).Record(handlerEndTime.Sub(handlerStartTime))
	metricsHandler.Counter(metrics.ArchiverNumHandledRequestsCount.MetricName.String()).Record(int64(len(handledHashes)))

	if !hashesEqual(pumpResult.PumpedHashes, handledHashes) {
		logger.Error("handled archival requests do not match pumped archival requests")
		metricsHandler.Counter(metrics.ArchiverPumpedNotEqualHandledCount.MetricName.String()).Record(1)
	}
	if pumpResult.TimeoutWithoutSignals {
		logger.Info("workflow stopping because pump did not get any signals within timeout threshold")
		metricsHandler.Counter(metrics.ArchiverWorkflowStoppingCount.MetricName.String()).Record(1)
		endTime := workflow.Now(ctx).Sub(startTime)
		metricsHandler.Timer(metrics.ServiceLatency.MetricName.String()).Record(endTime)
		return nil
	}
	for {
		var request ArchiveRequest
		if ok := signalCh.ReceiveAsync(&request); !ok {
			break
		}
		pumpResult.UnhandledCarryover = append(pumpResult.UnhandledCarryover, request)
	}
	logger.Info("archival system workflow continue as new")
	ctx = workflow.WithWorkflowRunTimeout(ctx, workflowRunTimeout)
	ctx = workflow.WithWorkflowTaskTimeout(ctx, workflowTaskTimeout)
	endTime := workflow.Now(ctx).Sub(startTime)
	metricsHandler.Timer(metrics.ServiceLatency.MetricName.String()).Record(endTime)
	return workflow.NewContinueAsNewError(ctx, archivalWorkflowFnName, pumpResult.UnhandledCarryover)
}
