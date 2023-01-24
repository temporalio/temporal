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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination handler_mock.go

package archiver

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// Handler is used to process archival requests
	Handler interface {
		Start()
		Finished() []uint64
	}

	handler struct {
		ctx            workflow.Context
		logger         log.Logger
		metricsHandler metrics.Handler
		concurrency    int
		requestCh      workflow.Channel
		resultCh       workflow.Channel
	}
)

// NewHandler returns a new Handler
func NewHandler(
	ctx workflow.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	concurrency int,
	requestCh workflow.Channel,
) Handler {
	return &handler{
		ctx:            ctx,
		logger:         logger,
		metricsHandler: metricsHandler.WithTags(metrics.OperationTag(metrics.ArchiverScope)),
		concurrency:    concurrency,
		requestCh:      requestCh,
		resultCh:       workflow.NewChannel(ctx),
	}
}

// Start spawns concurrency count of coroutine to handle archivals (does not block).
func (h *handler) Start() {
	h.metricsHandler.Counter(metrics.ArchiverStartedCount.GetMetricName()).Record(1)
	for i := 0; i < h.concurrency; i++ {
		workflow.Go(h.ctx, func(ctx workflow.Context) {
			h.metricsHandler.Counter(metrics.ArchiverCoroutineStartedCount.GetMetricName()).Record(1)
			var handledHashes []uint64
			for {
				var request ArchiveRequest
				if more := h.requestCh.Receive(ctx, &request); !more {
					break
				}
				h.handleRequest(ctx, &request)
				handledHashes = append(handledHashes, hash(request))
			}
			h.resultCh.Send(ctx, handledHashes)
			h.metricsHandler.Counter(metrics.ArchiverCoroutineStoppedCount.GetMetricName()).Record(1)
		})
	}
}

// Finished will block until all work has been finished.
// Returns hashes of requests handled.
func (h *handler) Finished() []uint64 {
	var handledHashes []uint64
	for i := 0; i < h.concurrency; i++ {
		var subResult []uint64
		h.resultCh.Receive(h.ctx, &subResult)
		handledHashes = append(handledHashes, subResult...)
	}
	h.metricsHandler.Counter(metrics.ArchiverStoppedCount.GetMetricName()).Record(1)
	return handledHashes
}

func (h *handler) handleRequest(ctx workflow.Context, request *ArchiveRequest) {
	var pendingRequests []workflow.Channel
	for _, target := range request.Targets {
		doneCh := workflow.NewChannel(ctx)
		pendingRequests = append(pendingRequests, doneCh)
		switch target {
		case ArchiveTargetHistory:
			workflow.Go(ctx, func(ctx workflow.Context) {
				h.handleHistoryRequest(ctx, request)
				doneCh.Close()
			})
		case ArchiveTargetVisibility:
			workflow.Go(ctx, func(ctx workflow.Context) {
				h.handleVisibilityRequest(ctx, request)
				doneCh.Close()
			})
		default:
			doneCh.Close()
		}
	}

	for _, doneCh := range pendingRequests {
		doneCh.Receive(ctx, nil)
	}
}

func (h *handler) handleHistoryRequest(ctx workflow.Context, request *ArchiveRequest) {

	startTime := time.Now().UTC()
	logger := tagLoggerWithHistoryRequest(h.logger, request)
	ao := workflow.ActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Minute,
		StartToCloseTimeout:    1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, ao)
	err := workflow.ExecuteActivity(actCtx, uploadHistoryActivityFnName, *request).Get(actCtx, nil)
	if err != nil {
		logger.Error("failed to archive history, will move on to deleting history without archiving", tag.Error(err))
		h.metricsHandler.Counter(metrics.ArchiverUploadFailedAllRetriesCount.GetMetricName()).Record(1)
	} else {
		h.metricsHandler.Counter(metrics.ArchiverUploadSuccessCount.GetMetricName()).Record(1)
	}
	h.metricsHandler.Timer(metrics.ArchiverUploadWithRetriesLatency.GetMetricName()).Record(time.Since(startTime))

	lao := workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
		},
	}

	deleteTime := time.Now().UTC()
	localActCtx := workflow.WithLocalActivityOptions(ctx, lao)
	err = workflow.ExecuteLocalActivity(localActCtx, deleteHistoryActivity, *request).Get(localActCtx, nil)
	if err != nil {
		logger.Error("deleting workflow execution failed all retires, skip workflow deletion", tag.Error(err))
		h.metricsHandler.Counter(metrics.ArchiverDeleteFailedAllRetriesCount.GetMetricName()).Record(1)
	} else {
		h.metricsHandler.Counter(metrics.ArchiverDeleteSuccessCount.GetMetricName()).Record(1)
	}
	h.metricsHandler.Timer(metrics.ArchiverDeleteWithRetriesLatency.GetMetricName()).Record(time.Since(deleteTime))
	h.metricsHandler.Timer(metrics.ArchiverHandleHistoryRequestLatency.GetMetricName()).Record(time.Since(startTime))
}

func (h *handler) handleVisibilityRequest(ctx workflow.Context, request *ArchiveRequest) {
	startTime := time.Now().UTC()
	logger := tagLoggerWithVisibilityRequest(h.logger, request)
	ao := workflow.ActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Minute,
		StartToCloseTimeout:    1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, ao)
	err := workflow.ExecuteActivity(actCtx, archiveVisibilityActivityFnName, *request).Get(actCtx, nil)
	if err != nil {
		logger.Error("failed to archive workflow visibility record", tag.Error(err))
		h.metricsHandler.Counter(metrics.ArchiverHandleVisibilityFailedAllRetiresCount.GetMetricName()).Record(1)
	} else {
		h.metricsHandler.Counter(metrics.ArchiverHandleVisibilitySuccessCount.GetMetricName()).Record(1)
	}
	h.metricsHandler.Timer(metrics.ArchiverHandleVisibilityRequestLatency.GetMetricName()).Record(time.Since(startTime))
}
