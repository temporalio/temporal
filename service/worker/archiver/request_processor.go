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
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

type (
	// RequestProcessor is for processing archive requests
	RequestProcessor interface {
		Process(ctx workflow.Context, request interface{})
	}

	historyRequestProcessor struct {
		logger       log.Logger
		metricsScope metrics.Scope
	}
)

// NewHistoryRequestProcessor returns a new processor for history requests
func NewHistoryRequestProcessor(logger log.Logger, metricsScope metrics.Scope) RequestProcessor {
	return &historyRequestProcessor{
		logger:       logger,
		metricsScope: metricsScope,
	}
}

func (p *historyRequestProcessor) Process(ctx workflow.Context, request interface{}) {
	sw := p.metricsScope.StartTimer(metrics.ArchiverHandleRequestLatency)
	logger := tagLoggerWithArchiveHistoryRequest(p.logger, request.(ArchiveHistoryRequest))
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 1 * time.Minute,
		StartToCloseTimeout:    1 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			ExpirationInterval:       5 * time.Minute,
			NonRetriableErrorReasons: uploadHistoryActivityNonRetryableErrors,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, ao)
	uploadSW := p.metricsScope.StartTimer(metrics.ArchiverUploadWithRetriesLatency)
	err := workflow.ExecuteActivity(actCtx, uploadHistoryActivityFnName, request).Get(actCtx, nil)
	if err != nil {
		logger.Error("failed to archive history, will move on to deleting history without archiving", tag.Error(err))
		p.metricsScope.IncCounter(metrics.ArchiverUploadFailedAllRetriesCount)
	} else {
		p.metricsScope.IncCounter(metrics.ArchiverUploadSuccessCount)
	}
	uploadSW.Stop()

	lao := workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			ExpirationInterval:       5 * time.Minute,
			NonRetriableErrorReasons: deleteHistoryActivityNonRetryableErrors,
		},
	}
	deleteSW := p.metricsScope.StartTimer(metrics.ArchiverDeleteWithRetriesLatency)
	localActCtx := workflow.WithLocalActivityOptions(ctx, lao)
	err = workflow.ExecuteLocalActivity(localActCtx, deleteHistoryActivity, request).Get(localActCtx, nil)
	if err != nil {
		logger.Error("deleting history failed, this means zombie histories are left", tag.Error(err))
		p.metricsScope.IncCounter(metrics.ArchiverDeleteFailedAllRetriesCount)
	} else {
		p.metricsScope.IncCounter(metrics.ArchiverDeleteSuccessCount)
	}
	deleteSW.Stop()
	sw.Stop()
}
