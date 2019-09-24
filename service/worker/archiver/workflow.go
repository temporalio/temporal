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

package archiver

import (
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/cadence/workflow"
)

type (
	dynamicConfigResult struct {
		ArchiverConcurrency   int
		ArchivalsPerIteration int
		TimelimitPerIteration time.Duration
	}
)

func archiveHistoryWorkflow(ctx workflow.Context, carryover []ArchiveHistoryRequest) error {
	return archivalWorkflowHelper(
		ctx,
		globalLogger,
		globalMetricsClient,
		globalConfig,
		nil,
		nil,
		convertHistoryRequestSlice(carryover),
		GetHistoryRequestReceiver(),
		NewHistoryRequestProcessor(
			globalLogger,
			NewReplayMetricsScope(globalMetricsClient.Scope(metrics.HistoryArchivalHandlerScope), ctx),
		),
	)
}

func archivalWorkflowHelper(
	ctx workflow.Context,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
	handler Handler, // enables tests to inject mocks
	pump Pump, // enables tests to inject mocks
	carryover []interface{},
	receiver RequestReceiver,
	processor RequestProcessor,
) error {
	metricsClient = NewReplayMetricsClient(metricsClient, ctx)
	workflowScope := metricsClient.Scope(metrics.HistoryArchivalWorkflowScope)
	workflowScope.IncCounter(metrics.ArchiverWorkflowStartedCount)
	sw := workflowScope.StartTimer(metrics.CadenceLatency)
	workflowInfo := workflow.GetInfo(ctx)
	logger = logger.WithTags(
		tag.WorkflowID(workflowInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(workflowInfo.WorkflowExecution.RunID),
		tag.WorkflowTaskListName(workflowInfo.TaskListName),
		tag.WorkflowType(workflowInfo.WorkflowType.Name))
	logger = loggerimpl.NewReplayLogger(logger, ctx, false)

	logger.Info("archival system workflow started")
	dcResult := getDynamicConfig(ctx, config)
	requestCh := workflow.NewBufferedChannel(ctx, dcResult.ArchivalsPerIteration)
	if handler == nil {
		handler = NewHandler(ctx, logger, metricsClient.Scope(metrics.HistoryArchivalHandlerScope), dcResult.ArchiverConcurrency, requestCh, receiver, processor)

	}
	handlerSW := workflowScope.StartTimer(metrics.ArchiverHandleAllRequestsLatency)
	handler.Start()
	signalCh := workflow.GetSignalChannel(ctx, archiveHistorySignalName)
	if pump == nil {
		pump = NewPump(ctx, logger, metricsClient.Scope(metrics.HistoryArchivalPumpScope), carryover, dcResult.TimelimitPerIteration, dcResult.ArchivalsPerIteration, requestCh, signalCh, receiver)
	}
	pumpResult := pump.Run()
	workflowScope.AddCounter(metrics.ArchiverNumPumpedRequestsCount, int64(len(pumpResult.PumpedHashes)))
	handledHashes := handler.Finished()
	handlerSW.Stop()
	workflowScope.AddCounter(metrics.ArchiverNumHandledRequestsCount, int64(len(handledHashes)))
	if !hashesEqual(pumpResult.PumpedHashes, handledHashes) {
		logger.Error("handled archival requests do not match pumped archival requests")
		workflowScope.IncCounter(metrics.ArchiverPumpedNotEqualHandledCount)
	}
	if pumpResult.TimeoutWithoutSignals {
		logger.Info("workflow stopping because pump did not get any signals within timeout threshold")
		workflowScope.IncCounter(metrics.ArchiverWorkflowStoppingCount)
		sw.Stop()
		return nil
	}
	for {
		request, ok := receiver.ReceiveAsync(signalCh)
		if !ok {
			break
		}
		pumpResult.UnhandledCarryover = append(pumpResult.UnhandledCarryover, request)
	}
	logger.Info("archival system workflow continue as new")
	ctx = workflow.WithExecutionStartToCloseTimeout(ctx, workflowStartToCloseTimeout)
	ctx = workflow.WithWorkflowTaskStartToCloseTimeout(ctx, workflowTaskStartToCloseTimeout)
	sw.Stop()
	return workflow.NewContinueAsNewError(ctx, archiveHistoryWorkflowFnName, pumpResult.UnhandledCarryover)
}

func getDynamicConfig(ctx workflow.Context, config *Config) dynamicConfigResult {
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
	return dcResult
}
