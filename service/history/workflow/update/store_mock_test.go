package update_test

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

var (
	emptyUpdateStore = mockUpdateStore{
		VisitUpdatesFunc: func(func(updID string, updInfo *persistencespb.UpdateInfo)) {},
		GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
			return nil, serviceerror.NewNotFound("not found")
		},
	}
	eventStoreUnused update.EventStore
)

type mockUpdateStore struct {
	update.UpdateStore
	VisitUpdatesFunc               func(visitor func(updID string, updInfo *persistencespb.UpdateInfo))
	GetUpdateOutcomeFunc           func(context.Context, string) (*updatepb.Outcome, error)
	GetCurrentVersionFunc          func() int64
	IsWorkflowExecutionRunningFunc func() bool
}

func (m mockUpdateStore) VisitUpdates(
	visitor func(updID string, updInfo *persistencespb.UpdateInfo),
) {
	if m.VisitUpdatesFunc != nil {
		m.VisitUpdatesFunc(visitor)
	}
}

func (m mockUpdateStore) GetUpdateOutcome(
	ctx context.Context,
	updateID string,
) (*updatepb.Outcome, error) {
	return m.GetUpdateOutcomeFunc(ctx, updateID)
}

func (m mockUpdateStore) GetCurrentVersion() int64 {
	if m.GetCurrentVersionFunc == nil {
		return 0
	}
	return m.GetCurrentVersionFunc()
}

func (m mockUpdateStore) IsWorkflowExecutionRunning() bool {
	if m.IsWorkflowExecutionRunningFunc == nil {
		return true
	}
	return m.IsWorkflowExecutionRunningFunc()
}

type mockEventStore struct {
	effect.Controller
	AddWorkflowExecutionUpdateAcceptedEventFunc func(
		updateID string,
		acceptedRequestMessageId string,
		acceptedRequestSequencingEventId int64,
		acceptedRequest *updatepb.Request,
	) (*historypb.HistoryEvent, error)

	AddWorkflowExecutionUpdateCompletedEventFunc func(
		acceptedEventID int64,
		resp *updatepb.Response,
	) (*historypb.HistoryEvent, error)

	CanAddEventFunc func() bool
}

func (m mockEventStore) AddWorkflowExecutionUpdateAcceptedEvent(
	updateID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateAcceptedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateAcceptedEventFunc(updateID, acceptedRequestMessageId, acceptedRequestSequencingEventId, acceptedRequest)
	}
	return &historypb.HistoryEvent{EventId: testAcceptedEventID}, nil
}

func (m mockEventStore) AddWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	resp *updatepb.Response,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateCompletedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateCompletedEventFunc(acceptedEventID, resp)
	}
	return &historypb.HistoryEvent{}, nil
}

func (m mockEventStore) CanAddEvent() bool {
	if m.CanAddEventFunc != nil {
		return m.CanAddEventFunc()
	}
	return true
}
