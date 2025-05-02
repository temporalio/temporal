package update

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/effect"
)

type (
	// UpdateStore represents the update package's requirements for reading Updates from the store.
	UpdateStore interface {
		VisitUpdates(visitor func(updID string, updInfo *persistencespb.UpdateInfo))
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)
		GetCurrentVersion() int64
		IsWorkflowExecutionRunning() bool
	}

	// EventStore is the interface that an Update needs to read and write events
	// and to be notified when buffered writes have been flushed. It is the
	// expectation of this code that writes to EventStore will return before the
	// data has been made durable. Callbacks attached to the EventStore via
	// OnAfterCommit and OnAfterRollback *must* be called after the EventStore
	// state is successfully written or is discarded.
	EventStore interface {
		effect.Controller

		// AddWorkflowExecutionUpdateAcceptedEvent writes an Update accepted
		// event. The data may not be durable when this function returns.
		AddWorkflowExecutionUpdateAcceptedEvent(
			updateID string,
			acceptedRequestMessageId string,
			acceptedRequestSequencingEventId int64,
			acceptedRequest *updatepb.Request,
		) (*historypb.HistoryEvent, error)

		// AddWorkflowExecutionUpdateCompletedEvent writes an Update completed
		// event. The data may not be durable when this function returns.
		AddWorkflowExecutionUpdateCompletedEvent(
			acceptedEventID int64,
			resp *updatepb.Response,
		) (*historypb.HistoryEvent, error)

		// CanAddEvent returns true if an event can be added to the EventStore.
		CanAddEvent() bool
	}
)
