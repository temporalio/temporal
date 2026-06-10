package update_test

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
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

	AddWorkflowExecutionOptionsUpdatedEventFunc func(
		versioningOverride *workflowpb.VersioningOverride,
		unsetVersioningOverride bool,
		attachRequestID string,
		attachCompletionCallbacks []*commonpb.Callback,
		links []*commonpb.Link,
		identity string,
		priority *commonpb.Priority,
		timeSkippingConfig *workflowpb.TimeSkippingConfig,
		workflowUpdateOptions []*historypb.WorkflowExecutionOptionsUpdatedEventAttributes_WorkflowUpdateOptionsUpdate,
	) (*historypb.HistoryEvent, error)

	CanAddEventFunc  func() bool
	HasRequestIDFunc func(requestID string) bool
}

func (m mockEventStore) AddWorkflowExecutionOptionsUpdatedEvent(
	versioningOverride *workflowpb.VersioningOverride,
	unsetVersioningOverride bool,
	attachRequestID string,
	attachCompletionCallbacks []*commonpb.Callback,
	links []*commonpb.Link,
	identity string,
	priority *commonpb.Priority,
	timeSkippingConfig *workflowpb.TimeSkippingConfig,
	workflowUpdateOptions []*historypb.WorkflowExecutionOptionsUpdatedEventAttributes_WorkflowUpdateOptionsUpdate,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionOptionsUpdatedEventFunc != nil {
		return m.AddWorkflowExecutionOptionsUpdatedEventFunc(versioningOverride, unsetVersioningOverride, attachRequestID, attachCompletionCallbacks, links, identity, priority, timeSkippingConfig, workflowUpdateOptions)
	}
	return &historypb.HistoryEvent{}, nil
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

func (m mockEventStore) RejectWorkflowExecutionUpdate(_ string, _ *failurepb.Failure) error {
	return nil
}

func (m mockEventStore) HasRequestID(requestID string) bool {
	if m.HasRequestIDFunc != nil {
		return m.HasRequestIDFunc(requestID)
	}
	return false
}
