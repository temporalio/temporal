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

package update

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

const libraryName = "go.temporal.io/service/history/workflow/update"

type (
	instrumentation struct {
		log     log.Logger
		metrics metrics.Handler
		tracer  trace.Tracer
	}
)

var (
	noopInstrumentation = instrumentation{
		log:     log.NewNoopLogger(),
		metrics: metrics.NoopMetricsHandler,
		tracer:  trace.NewNoopTracerProvider().Tracer(libraryName),
	}
)

func invalidArgf(tmpl string, args ...any) error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf(tmpl, args...))
}

func internalErrorf(tmpl string, args ...any) error {
	return serviceerror.NewInternal(fmt.Sprintf(tmpl, args...))
}

func (i *instrumentation) countRequestMsg() {
	i.oneOf(metrics.MessageTypeRequestWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countAcceptanceMsg() {
	i.oneOf(metrics.MessageTypeAcceptWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countRejectionMsg() {
	i.oneOf(metrics.MessageTypeRejectWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countResponseMsg() {
	i.oneOf(metrics.MessageTypeRespondWorkflowExecutionUpdateCounter.Name())
}

func (i *instrumentation) countRateLimited() {
	i.oneOf(metrics.WorkflowExecutionUpdateRequestRateLimited.Name())
}

func (i *instrumentation) countTooMany() {
	i.oneOf(metrics.WorkflowExecutionUpdateTooMany.Name())
}

func (i *instrumentation) countAborted() {
	i.oneOf(metrics.WorkflowExecutionUpdateAborted.Name())
}

func (i *instrumentation) countSent() {
	i.oneOf(metrics.WorkflowExecutionUpdateSentToWorker.Name())
}

func (i *instrumentation) countSentAgain() {
	i.oneOf(metrics.WorkflowExecutionUpdateSentToWorkerAgain.Name())
}

func (i *instrumentation) invalidStateTransition(updateID string, msg proto.Message, state state) {
	i.oneOf(metrics.InvalidStateTransitionWorkflowExecutionUpdateCounter.Name())
	i.log.Error("invalid state transition attempted",
		tag.NewStringTag("update-id", updateID),
		tag.NewStringTag("message", fmt.Sprintf("%T", msg)),
		tag.NewStringTag("state", state.String()))
}

func (i *instrumentation) updateRegistrySize(size int) {
	i.metrics.Histogram(metrics.WorkflowExecutionUpdateRegistrySize.Name(), metrics.Bytes).Record(int64(size))
}

func (i *instrumentation) oneOf(counterName string) {
	i.metrics.Counter(counterName).Record(1)
}

func (i *instrumentation) stateChange(updateID string, from, to state) {
	i.log.Debug("update state change",
		tag.NewStringTag("update-id", updateID),
		tag.NewStringTag("from-state", from.String()),
		tag.NewStringTag("to-state", to.String()),
	)
}
