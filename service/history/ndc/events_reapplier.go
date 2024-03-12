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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination events_reapplier_mock.go

package ndc

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/workflow"
)

type (
	EventsReapplier interface {
		ReapplyEvents(
			ctx context.Context,
			ms workflow.MutableState,
			historyEvents []*historypb.HistoryEvent,
			runID string,
		) ([]*historypb.HistoryEvent, error)
	}

	EventsReapplierImpl struct {
		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

func NewEventsReapplier(
	metricsHandler metrics.Handler,
	logger log.Logger,
) *EventsReapplierImpl {

	return &EventsReapplierImpl{
		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

// TODO (dan) this function is almost identical to reapplyEvents in workflow_resetter.go: unify them.
func (r *EventsReapplierImpl) ReapplyEvents(
	ctx context.Context,
	ms workflow.MutableState,
	historyEvents []*historypb.HistoryEvent,
	runID string,
) ([]*historypb.HistoryEvent, error) {
	// TODO (dan) Reapply updates
	var reappliedEvents []*historypb.HistoryEvent
	for _, event := range historyEvents {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
			if ms.IsResourceDuplicated(dedupResource) {
				continue
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REQUESTED:
			// Test coverage: TestNDCEventReapplicationSuite/TestReapplyEvents_AppliedEvent_Update
			dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
			if ms.IsResourceDuplicated(dedupResource) {
				continue
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			// Test coverage: TestNDCEventReapplicationSuite/TestReapplyEvents_AppliedEvent_Update
			dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
			if ms.IsResourceDuplicated(dedupResource) {
				continue
			}
			reappliedEvents = append(reappliedEvents, event)
		}
	}

	if len(reappliedEvents) == 0 {
		return nil, nil
	}

	// sanity check workflow still running
	if !ms.IsWorkflowExecutionRunning() {
		return nil, serviceerror.NewInternal("unable to reapply events to closed workflow.")
	}

	shouldScheduleWorkflowTask := false
	for _, event := range reappliedEvents {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signal := event.GetWorkflowExecutionSignaledEventAttributes()
			shouldScheduleWorkflowTask = shouldScheduleWorkflowTask || !signal.GetSkipGenerateWorkflowTask()
			if _, err := ms.AddWorkflowExecutionSignaled(
				signal.GetSignalName(),
				signal.GetInput(),
				signal.GetIdentity(),
				signal.GetHeader(),
				signal.GetSkipGenerateWorkflowTask(),
			); err != nil {
				return nil, err
			}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REQUESTED:
			attr := event.GetWorkflowExecutionUpdateRequestedEventAttributes()
			if _, err := ms.AddWorkflowExecutionUpdateRequestedEvent(
				attr.GetRequest(),
				attr.Origin,
			); err != nil {
				return nil, err
			}
			shouldScheduleWorkflowTask = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			attr := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			if _, err := ms.AddWorkflowExecutionUpdateRequestedEvent(
				attr.GetAcceptedRequest(),
				enumspb.UPDATE_REQUESTED_EVENT_ORIGIN_REAPPLY,
			); err != nil {
				return nil, err
			}
			shouldScheduleWorkflowTask = true
		}
		deDupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
		ms.UpdateDuplicatedResource(deDupResource)
	}

	// After reapply event, checking if we should schedule a workflow task
	if ms.IsWorkflowPendingOnWorkflowTaskBackoff() {
		// Do not create workflow task when the workflow has first workflow task backoff and execution is not started yet
		return reappliedEvents, nil
	}
	if !shouldScheduleWorkflowTask {
		// Do not create workflow task when all reapplied signals had SkipGenerateWorkflowTask=true flag set
		return reappliedEvents, nil
	}

	if !ms.HasPendingWorkflowTask() {
		if _, err := ms.AddWorkflowTaskScheduledEvent(
			false,
			enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		); err != nil {
			return nil, err
		}
	}
	return reappliedEvents, nil
}
