package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/telemetry"
)

// WorkflowCloseObservation is the normalized, non-payload-bearing form of a workflow close event.
type WorkflowCloseObservation struct {
	Outcome        string
	SuccessorRunID string
	EventTime      time.Time
}

// NormalizeWorkflowCloseEvent maps supported workflow close history events to one stable shape.
func NormalizeWorkflowCloseEvent(event *historypb.HistoryEvent) (WorkflowCloseObservation, error) {
	if event == nil {
		return WorkflowCloseObservation{}, errors.New("workflow close telemetry: event is nil")
	}
	observed := WorkflowCloseObservation{}
	if event.GetEventTime() != nil {
		observed.EventTime = event.GetEventTime().AsTime()
	}
	switch {
	case event.GetWorkflowExecutionCompletedEventAttributes() != nil:
		observed.Outcome = telemetry.WorkflowCloseOutcomeCompleted
		observed.SuccessorRunID = event.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId()
	case event.GetWorkflowExecutionFailedEventAttributes() != nil:
		observed.Outcome = telemetry.WorkflowCloseOutcomeFailed
		observed.SuccessorRunID = event.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId()
	case event.GetWorkflowExecutionCanceledEventAttributes() != nil:
		observed.Outcome = telemetry.WorkflowCloseOutcomeCanceled
	case event.GetWorkflowExecutionTerminatedEventAttributes() != nil:
		observed.Outcome = telemetry.WorkflowCloseOutcomeTerminated
	case event.GetWorkflowExecutionTimedOutEventAttributes() != nil:
		observed.Outcome = telemetry.WorkflowCloseOutcomeTimedOut
		observed.SuccessorRunID = event.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId()
	case event.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil:
		observed.Outcome = telemetry.WorkflowCloseOutcomeContinuedAsNew
		observed.SuccessorRunID = event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
	default:
		return WorkflowCloseObservation{}, fmt.Errorf("workflow close telemetry: unsupported or incomplete event %s", event.GetEventType())
	}
	return observed, nil
}

// EmitWorkflowExecutionClosed reports one normalized workflow close observation.
func EmitWorkflowExecutionClosed(ctx context.Context, key definition.WorkflowKey, event *historypb.HistoryEvent) error {
	observed, err := NormalizeWorkflowCloseEvent(event)
	if err != nil {
		return err
	}
	options := []trace.EventOption{trace.WithAttributes(
		telemetry.AttrWorkflowID.String(key.WorkflowID),
		telemetry.AttrRunID.String(key.RunID),
		telemetry.AttrNamespaceID.String(key.NamespaceID),
		telemetry.AttrWorkflowCloseOutcome.String(observed.Outcome),
		telemetry.AttrWorkflowSuccessorRunID.String(observed.SuccessorRunID),
	)}
	if !observed.EventTime.IsZero() {
		options = append(options, trace.WithTimestamp(observed.EventTime))
	}
	trace.SpanFromContext(ctx).AddEvent(telemetry.EventWorkflowExecutionClosed, options...)
	return nil
}
