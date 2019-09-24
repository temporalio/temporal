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
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/cadence/workflow"
)

type (
	// Handler is used to process archival requests
	Handler interface {
		Start()
		Finished() []uint64
	}

	handler struct {
		ctx          workflow.Context
		logger       log.Logger
		metricsScope metrics.Scope
		concurrency  int
		requestCh    workflow.Channel
		resultCh     workflow.Channel
		receiver     RequestReceiver
		processor    RequestProcessor
	}
)

// NewHandler returns a new Handler
func NewHandler(
	ctx workflow.Context,
	logger log.Logger,
	metricsScope metrics.Scope,
	concurrency int,
	requestCh workflow.Channel,
	receiver RequestReceiver,
	processor RequestProcessor,
) Handler {
	return &handler{
		ctx:          ctx,
		logger:       logger,
		metricsScope: metricsScope,
		concurrency:  concurrency,
		requestCh:    requestCh,
		resultCh:     workflow.NewChannel(ctx),
		receiver:     receiver,
		processor:    processor,
	}
}

// Start spawns concurrency count of coroutine to handle archivals (does not block).
func (h *handler) Start() {
	h.metricsScope.IncCounter(metrics.ArchiverStartedCount)
	for i := 0; i < h.concurrency; i++ {
		workflow.Go(h.ctx, func(ctx workflow.Context) {
			h.metricsScope.IncCounter(metrics.ArchiverCoroutineStartedCount)
			var handledHashes []uint64
			for {
				request, more := h.receiver.Receive(ctx, h.requestCh)
				if !more {
					break
				}
				h.processor.Process(ctx, request)
				handledHashes = append(handledHashes, hash(request))
			}
			h.resultCh.Send(ctx, handledHashes)
			h.metricsScope.IncCounter(metrics.ArchiverCoroutineStoppedCount)
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
	h.metricsScope.IncCounter(metrics.ArchiverStoppedCount)
	return handledHashes
}
