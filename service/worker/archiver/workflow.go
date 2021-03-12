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
	return archivalWorkflowHelper(ctx, globalLogger, globalMetricsClient, globalConfig, nil, nil, carryover)
}

func archivalWorkflowHelper(
	ctx workflow.Context,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
	handler Handler, // enables tests to inject mocks
	pump Pump, // enables tests to inject mocks
	carryover []ArchiveRequest,
) error {
	metricsClient = NewReplayMetricsClient(metricsClient, ctx)
	metricsClient.IncCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount)
	sw := metricsClient.StartTimer(metrics.ArchiverArchivalWorkflowScope, metrics.ServiceLatency)
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
		handler = NewHandler(ctx, logger, metricsClient, dcResult.ArchiverConcurrency, requestCh)
	}
	handlerSW := metricsClient.StartTimer(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency)
	handler.Start()
	signalCh := workflow.GetSignalChannel(ctx, signalName)
	if pump == nil {
		pump = NewPump(ctx, logger, metricsClient, carryover, dcResult.TimelimitPerIteration, dcResult.ArchivalsPerIteration, requestCh, signalCh)
	}
	pumpResult := pump.Run()
	metricsClient.AddCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(len(pumpResult.PumpedHashes)))
	handledHashes := handler.Finished()
	handlerSW.Stop()
	metricsClient.AddCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(len(handledHashes)))
	if !hashesEqual(pumpResult.PumpedHashes, handledHashes) {
		logger.Error("handled archival requests do not match pumped archival requests")
		metricsClient.IncCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverPumpedNotEqualHandledCount)
	}
	if pumpResult.TimeoutWithoutSignals {
		logger.Info("workflow stopping because pump did not get any signals within timeout threshold")
		metricsClient.IncCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStoppingCount)
		sw.Stop()
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
	sw.Stop()
	return workflow.NewContinueAsNewError(ctx, archivalWorkflowFnName, pumpResult.UnhandledCarryover)
}
