//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCEventsReapplier_mock.go

package history

import (
	"context"

	eventpb "go.temporal.io/temporal-proto/event"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	nDCEventsReapplier interface {
		reapplyEvents(
			ctx context.Context,
			msBuilder mutableState,
			historyEvents []*eventpb.HistoryEvent,
			runID string,
		) ([]*eventpb.HistoryEvent, error)
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
	_ context.Context,
	msBuilder mutableState,
	historyEvents []*eventpb.HistoryEvent,
	runID string,
) ([]*eventpb.HistoryEvent, error) {

	var reappliedEvents []*eventpb.HistoryEvent
	for _, event := range historyEvents {
		switch event.GetEventType() {
		case eventpb.EventType_WorkflowExecutionSignaled:
			dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
			if msBuilder.IsResourceDuplicated(dedupResource) {
				// skip already applied event
				continue
			}
			reappliedEvents = append(reappliedEvents, event)
		}
	}

	if len(reappliedEvents) == 0 {
		return nil, nil
	}

	// sanity check workflow still running
	if !msBuilder.IsWorkflowExecutionRunning() {
		return nil, serviceerror.NewInternal("unable to reapply events to closed workflow.")
	}

	for _, event := range reappliedEvents {
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
	}
	return reappliedEvents, nil
}
