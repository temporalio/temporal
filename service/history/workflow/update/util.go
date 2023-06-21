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

// CountRequestMsg adds 1 to the update request message counter
func (i *instrumentation) CountRequestMsg() {
	i.countMessage(metrics.MessageTypeRequestWorkflowExecutionUpdateCounter.GetMetricName())
}

// CountAcceptanceMsg adds 1 to the update acceptance message counter
func (i *instrumentation) CountAcceptanceMsg() {
	i.countMessage(metrics.MessageTypeAcceptWorkflowExecutionUpdateCounter.GetMetricName())
}

// CountRejectionMsg counter adds 1 to the update rejection message counter
func (i *instrumentation) CountRejectionMsg() {
	i.countMessage(metrics.MessageTypeRejectWorkflowExecutionUpdateCounter.GetMetricName())
}

// CountResponseMsg counter adds 1 to the update response message counter
func (i *instrumentation) CountResponseMsg() {
	i.countMessage(metrics.MessageTypeRespondWorkflowExecutionUpdateCounter.GetMetricName())
}

// CountInvalidStateTransition counter adds 1 to invalid update state machine transition counter
func (i *instrumentation) CountInvalidStateTransition() {
	i.countMessage(metrics.InvalidStateTransitionWorkflowExecutionUpdateCounter.GetMetricName())
}

func (i *instrumentation) countMessage(ctrName string) {
	i.metrics.Counter(ctrName).Record(1)
}

// StateChange records instrumentation info about an Update state change
func (i *instrumentation) StateChange(updateID string, from, to state) {
	i.log.Debug("update state change",
		tag.NewStringTag("update-id", updateID),
		tag.NewStringTag("from-state", from.String()),
		tag.NewStringTag("to-state", to.String()),
	)
}
