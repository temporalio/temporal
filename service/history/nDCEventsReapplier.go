// Copyright (c) 2019 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCEventsReapplier_mock.go

package history

import (
	ctx "context"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

type (
	nDCEventsReapplier interface {
		reapplyEvents(
			ctx ctx.Context,
			msBuilder mutableState,
			historyEvents []*workflow.HistoryEvent,
		) error
	}

	nDCEventsReapplierImpl struct {
		metricsClient metrics.Client
		logger        log.Logger
	}
)

func newNDCEventsReapplier(
	metricsClient metrics.Client,
	logger log.Logger,
) *nDCEventsReapplierImpl {

	return &nDCEventsReapplierImpl{
		metricsClient: metricsClient,
		logger:        logger,
	}
}

func (r *nDCEventsReapplierImpl) reapplyEvents(
	ctx ctx.Context,
	msBuilder mutableState,
	historyEvents []*workflow.HistoryEvent,
) error {

	var reapplyEvents []*workflow.HistoryEvent
	// TODO: need to implement Reapply policy
	for _, event := range historyEvents {
		switch event.GetEventType() {
		case workflow.EventTypeWorkflowExecutionSignaled:
			reapplyEvents = append(reapplyEvents, event)
		}
	}

	if len(reapplyEvents) == 0 {
		return nil
	}

	if !msBuilder.IsWorkflowExecutionRunning() {
		// TODO when https://github.com/uber/cadence/issues/2420 is finished
		//  reset to workflow finish event
		//  ignore this case for now
		return nil
	}

	// TODO: need to have signal deduplicate logic
	for _, event := range reapplyEvents {
		signal := event.GetWorkflowExecutionSignaledEventAttributes()
		if _, err := msBuilder.AddWorkflowExecutionSignaled(
			signal.GetSignalName(),
			signal.GetInput(),
			signal.GetIdentity(),
		); err != nil {
			return err
		}
	}
	return nil
}
