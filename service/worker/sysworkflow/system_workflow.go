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

package sysworkflow

import (
	"context"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/logging"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
	"time"
)

// SystemWorkflow is the system workflow code
func SystemWorkflow(ctx workflow.Context) error {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	logger := workflow.GetLogger(ctx)
	scope := workflow.GetMetricsScope(ctx).Tagged(map[string]string{SystemWorkflowIDTag: id})
	ch := workflow.GetSignalChannel(ctx, SignalName)

	logger.Info("started new system workflow")
	signalsHandled := 0
	for ; signalsHandled < SignalsUntilContinueAsNew; signalsHandled++ {
		var signal signal
		if more := ch.Receive(ctx, &signal); !more {
			scope.Counter(ChannelClosedUnexpectedlyError).Inc(1)
			logger.Error("cadence channel was unexpectedly closed")
			break
		}
		selectSystemTask(scope, signal, ctx, logger)
	}

	for {
		var signal signal
		if ok := ch.ReceiveAsync(&signal); !ok {
			break
		}
		selectSystemTask(scope, signal, ctx, logger)
		signalsHandled++
	}

	logger.Info("completed current set of iterations, continuing as new",
		zap.Int(logging.TagIterationsUntilContinueAsNew, signalsHandled))

	ctx = workflow.WithExecutionStartToCloseTimeout(ctx, WorkflowStartToCloseTimeout)
	ctx = workflow.WithWorkflowTaskStartToCloseTimeout(ctx, DecisionTaskStartToCloseTimeout)
	return workflow.NewContinueAsNewError(ctx, SystemWorkflowFnName)
}

func selectSystemTask(scope tally.Scope, signal signal, ctx workflow.Context, logger *zap.Logger) {
	scope.Counter(HandledSignalCount).Inc(1)

	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 10,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			MaximumInterval:          time.Minute,
			ExpirationInterval:       time.Hour * 24 * 30,
			MaximumAttempts:          0,
			NonRetriableErrorReasons: []string{},
		},
	}

	actCtx := workflow.WithActivityOptions(ctx, ao)
	switch signal.RequestType {
	case archivalRequest:
		if err := workflow.ExecuteActivity(
			actCtx,
			ArchivalActivityFnName,
			*signal.ArchiveRequest,
		).Get(ctx, nil); err != nil {
			scope.Counter(ArchivalFailureErr)
			logger.Error("failed to execute archival activity", zap.Error(err))
		}
	case backfillRequest:
		if err := workflow.ExecuteActivity(
			actCtx,
			BackfillActivityFnName,
			*signal.BackillRequest,
		).Get(ctx, nil); err != nil {
			scope.Counter(BackfillFailureErr)
			logger.Error("failed to backfill", zap.Error(err))
		}
	default:
		scope.Counter(UnknownSignalTypeErr).Inc(1)
		logger.Error("received unknown request type")
	}
}

// ArchivalActivity is the archival activity code
func ArchivalActivity(ctx context.Context, request ArchiveRequest) error {
	// TODO: write this activity
	return nil
}

// BackfillActivity is the backfill activity code
func BackfillActivity(_ context.Context, _ BackfillRequest) error {
	// TODO: write this activity
	return nil
}
