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
		ctx           workflow.Context
		logger        log.Logger
		metricsClient metrics.Client
		concurrency   int
		requestCh     workflow.Channel
		resultCh      workflow.Channel
	}
)

// NewHandler returns a new Handler
func NewHandler(
	ctx workflow.Context,
	logger log.Logger,
	metricsClient metrics.Client,
	concurrency int,
	requestCh workflow.Channel,
) Handler {
	return &handler{
		ctx:           ctx,
		logger:        logger,
		metricsClient: metricsClient,
		concurrency:   concurrency,
		requestCh:     requestCh,
		resultCh:      workflow.NewChannel(ctx),
	}
}

// Start spawns concurrency count of coroutine to handle archivals (does not block).
func (h *handler) Start() {
	h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverStartedCount)
	for i := 0; i < h.concurrency; i++ {
		workflow.Go(h.ctx, func(ctx workflow.Context) {
			h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount)
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
			h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount)
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
	h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverStoppedCount)
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
	sw := h.metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverHandleHistoryRequestLatency)
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
	uploadSW := h.metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverUploadWithRetriesLatency)
	err := workflow.ExecuteActivity(actCtx, uploadHistoryActivityFnName, *request).Get(actCtx, nil)
	if err != nil {
		logger.Error("failed to archive history, will move on to deleting history without archiving", tag.Error(err))
		h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount)
	} else {
		h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount)
	}
	uploadSW.Stop()

	lao := workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
		},
	}

	deleteSW := h.metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverDeleteWithRetriesLatency)
	localActCtx := workflow.WithLocalActivityOptions(ctx, lao)
	err = workflow.ExecuteLocalActivity(localActCtx, deleteHistoryActivity, *request).Get(localActCtx, nil)
	if err != nil {
		logger.Error("deleting history failed, this means zombie histories are left", tag.Error(err))
		h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteFailedAllRetriesCount)
	} else {
		h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount)
	}
	deleteSW.Stop()
	sw.Stop()
}

func (h *handler) handleVisibilityRequest(ctx workflow.Context, request *ArchiveRequest) {
	sw := h.metricsClient.StartTimer(metrics.ArchiverScope, metrics.ArchiverHandleVisibilityRequestLatency)
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
		h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilityFailedAllRetiresCount)
	} else {
		h.metricsClient.IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount)
	}
	sw.Stop()
}
