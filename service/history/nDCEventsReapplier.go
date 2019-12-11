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

	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	nDCEventsReapplier interface {
		reapplyEvents(
			ctx ctx.Context,
			msBuilder mutableState,
			historyEvents []*workflow.HistoryEvent,
			runID string,
		) ([]*workflow.HistoryEvent, error)
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
	runID string,
) ([]*workflow.HistoryEvent, error) {

	var toReapplyEvents []*workflow.HistoryEvent
	for _, event := range historyEvents {
		switch event.GetEventType() {
		case workflow.EventTypeWorkflowExecutionSignaled:
			dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
			if msBuilder.IsResourceDuplicated(dedupResource) {
				// skip already applied event
				continue
			}
			toReapplyEvents = append(toReapplyEvents, event)
		}
	}

	if len(toReapplyEvents) == 0 {
		return nil, nil
	}

	if !msBuilder.IsWorkflowExecutionRunning() {
		// TODO when https://github.com/uber/cadence/issues/2420 is finished
		//  reset to workflow finish event
		//  ignore this case for now
		return nil, nil
	}

	var reappliedEvents []*workflow.HistoryEvent
	for _, event := range toReapplyEvents {
		signal := event.GetWorkflowExecutionSignaledEventAttributes()
		if _, err := msBuilder.AddWorkflowExecutionSignaled(
			signal.GetSignalName(),
			signal.GetInput(),
			signal.GetIdentity(),
		); err != nil {
			return nil, err
		}
		deDupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
		msBuilder.UpdateDuplicatedResource(deDupResource)
		reappliedEvents = append(reappliedEvents, event)
	}
	return reappliedEvents, nil
}
