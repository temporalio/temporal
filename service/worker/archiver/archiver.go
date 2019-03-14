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

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

type (
	// Archiver is used to process archival requests
	Archiver interface {
		Start()
		Finished() []uint64
	}

	archiver struct {
		ctx           workflow.Context
		logger        bark.Logger
		metricsClient metrics.Client
		concurrency   int
		requestCh     workflow.Channel
		resultCh      workflow.Channel
	}
)

// NewArchiver returns a new Archiver
func NewArchiver(
	ctx workflow.Context,
	logger bark.Logger,
	metricsClient metrics.Client,
	concurrency int,
	requestCh workflow.Channel,
) Archiver {
	return &archiver{
		ctx:           ctx,
		logger:        logger,
		metricsClient: metricsClient,
		concurrency:   concurrency,
		requestCh:     requestCh,
		resultCh:      workflow.NewChannel(ctx),
	}
}

// Start spawns concurrency count of coroutine to handle archivals (does not block).
func (a *archiver) Start() {
	a.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverStartedCount)
	for i := 0; i < a.concurrency; i++ {
		workflow.Go(a.ctx, func(ctx workflow.Context) {
			a.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount)
			var handledHashes []uint64
			for {
				var request ArchiveRequest
				if more := a.requestCh.Receive(ctx, &request); !more {
					break
				}
				handleRequest(ctx, a.logger, a.metricsClient, request)
				handledHashes = append(handledHashes, hashArchiveRequest(request))
			}
			a.resultCh.Send(a.ctx, handledHashes)
			a.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount)
		})
	}
}

// Finished will block until all work has been finished.
// Returns hashes of requests handled.
func (a *archiver) Finished() []uint64 {
	var handledHashes []uint64
	for i := 0; i < a.concurrency; i++ {
		var subResult []uint64
		a.resultCh.Receive(a.ctx, &subResult)
		handledHashes = append(handledHashes, subResult...)
	}
	a.resultCh.Close()
	a.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverStoppedCount)
	return handledHashes
}

func handleRequest(ctx workflow.Context, logger bark.Logger, metricsClient metrics.Client, request ArchiveRequest) {
	sw := metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverHandleRequestLatency)
	defer sw.Stop()

	logger = tagLoggerWithRequest(logger, request)
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 10 * time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       2 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			ExpirationInterval:       10 * time.Minute,
			NonRetriableErrorReasons: uploadHistoryActivityNonRetryableErrors,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, ao)
	uploadSW := metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverUploadWithRetriesLatency)
	if err := workflow.ExecuteActivity(actCtx, uploadHistoryActivityFnName, request).Get(actCtx, nil); err != nil {
		logger.WithField(logging.TagErr, err).Error("failed to upload history, moving on to deleting history without archiving")
		metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount)
	} else {
		metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount)
	}
	uploadSW.Stop()

	lao := workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			ExpirationInterval:       3 * time.Minute,
			NonRetriableErrorReasons: deleteHistoryActivityNonRetryableErrors,
		},
	}
	deleteSW := metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverDeleteWithRetriesLatency)
	localActCtx := workflow.WithLocalActivityOptions(ctx, lao)
	err := workflow.ExecuteLocalActivity(localActCtx, deleteHistoryActivity, request).Get(localActCtx, nil)
	if err == nil {
		metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteLocalSuccessCount)
		return
	}
	metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteLocalFailedAllRetriesCount)
	logger.WithField(logging.TagErr, err).Warn("deleting history though local activity failed, attempting to run as normal activity")
	ao = workflow.ActivityOptions{
		ScheduleToStartTimeout: 10 * time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       2 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			ExpirationInterval:       10 * time.Minute,
			NonRetriableErrorReasons: deleteHistoryActivityNonRetryableErrors,
		},
	}
	actCtx = workflow.WithActivityOptions(ctx, ao)
	if err := workflow.ExecuteActivity(actCtx, deleteHistoryActivityFnName, request).Get(actCtx, nil); err != nil {
		logger.WithField(logging.TagErr, err).Error("failed to delete history, this means zombie histories are left")
		metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteFailedAllRetriesCount)
	} else {
		metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount)
	}
	deleteSW.Stop()
}
