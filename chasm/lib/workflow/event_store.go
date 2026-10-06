package workflow

import (
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventStore is the history event store that workflow components add events to. MSPointer
// implements it with mutable state. A workflow created by NewNativeWorkflow implements it with
// events held in its CHASM tree.
type EventStore interface {
	// AddHistoryEvent adds an event to history. While a workflow task is started the event is
	// buffered: its ID is common.BufferedEventID until the workflow task closes.
	AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent
}

var _ EventStore = chasm.MSPointer{}

func (w *Workflow) eventStore(ctx chasm.MutableContext) EventStore {
	if state, ok := w.Native.TryGet(ctx); ok {
		return nativeEventStore{ctx: ctx, w: w, state: state}
	}
	return w.MSPointer
}

func (w *Workflow) runTimeout(ctx chasm.Context) time.Duration {
	if state, ok := w.Native.TryGet(ctx); ok {
		return state.GetWorkflowRunTimeout().AsDuration()
	}
	return w.WorkflowRunTimeout()
}

func (w *Workflow) taskQueue(ctx chasm.Context) string {
	if state, ok := w.Native.TryGet(ctx); ok {
		return state.GetTaskQueue()
	}
	return w.WorkflowTaskQueue()
}

// History returns a native workflow's history events in event ID order.
func (w *Workflow) History(ctx chasm.Context) []*historypb.HistoryEvent {
	ids := make([]int64, 0, len(w.NativeHistory))
	for id := range w.NativeHistory {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	events := make([]*historypb.HistoryEvent, len(ids))
	for i, id := range ids {
		events[i] = w.NativeHistory[id].Get(ctx)
	}
	return events
}

// HistoryEvent returns a native workflow's history event.
func (w *Workflow) HistoryEvent(ctx chasm.Context, eventID int64) (*historypb.HistoryEvent, bool) {
	field, ok := w.NativeHistory[eventID]
	if !ok {
		return nil, false
	}
	return field.Get(ctx), true
}

type nativeEventStore struct {
	ctx   chasm.MutableContext
	w     *Workflow
	state *nativeWorkflowState
}

func (s nativeEventStore) AddHistoryEvent(
	t enumspb.EventType,
	setAttributes func(*historypb.HistoryEvent),
) *historypb.HistoryEvent {
	if s.state.GetWorkflowTaskStartedEventId() != 0 {
		event := &historypb.HistoryEvent{
			EventId:   common.BufferedEventID,
			EventType: t,
			EventTime: timestamppb.New(s.ctx.Now(s.w)),
		}
		setAttributes(event)
		s.state.BufferedEvents = append(s.state.BufferedEvents, event)
		return event
	}
	event := s.w.appendEvent(s.ctx, s.state, t, setAttributes)
	if def, ok := workflowContextFromChasm(s.ctx).registry.EventDefinitionByEventType(t); ok && def.IsWorkflowTaskTrigger() {
		s.w.scheduleWorkflowTask(s.ctx, s.state, 1)
	}
	return event
}

func (w *Workflow) appendEvent(
	ctx chasm.MutableContext,
	state *nativeWorkflowState,
	t enumspb.EventType,
	setAttributes func(*historypb.HistoryEvent),
) *historypb.HistoryEvent {
	event := &historypb.HistoryEvent{EventType: t, EventTime: timestamppb.New(ctx.Now(w))}
	setAttributes(event)
	w.appendEvents(ctx, state, []*historypb.HistoryEvent{event})
	return event
}

// appendEvents adds events to history, assigning event IDs. A buffered activity outcome event
// refers to the buffered ActivityTaskStarted event preceding it, so its started event ID is
// assigned here, as the server does when it flushes buffered events.
func (w *Workflow) appendEvents(ctx chasm.MutableContext, state *nativeWorkflowState, events []*historypb.HistoryEvent) {
	startedEventIDs := map[int64]int64{} // scheduled event ID -> started event ID
	for _, event := range events {
		event.EventId = state.NextEventId
		state.NextEventId++
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			startedEventIDs[attrs.GetScheduledEventId()] = event.EventId
		}
		setBufferedStartedEventID(event, startedEventIDs)
		w.NativeHistory[event.EventId] = chasm.NewDataField(ctx, event)
		state.HistorySizeBytes += int64(proto.Size(event))
	}
}

// flushBufferedEvents adds buffered events to history and reports whether there were any.
func (w *Workflow) flushBufferedEvents(ctx chasm.MutableContext, state *nativeWorkflowState) bool {
	if len(state.BufferedEvents) == 0 {
		return false
	}
	w.appendEvents(ctx, state, state.BufferedEvents)
	state.BufferedEvents = nil
	return true
}

type activityOutcomeAttributes interface {
	GetScheduledEventId() int64
	GetStartedEventId() int64
}

func setBufferedStartedEventID(event *historypb.HistoryEvent, startedEventIDs map[int64]int64) {
	var attrs activityOutcomeAttributes
	var set func(int64)
	switch {
	case event.GetActivityTaskCompletedEventAttributes() != nil:
		a := event.GetActivityTaskCompletedEventAttributes()
		attrs, set = a, func(id int64) { a.StartedEventId = id }
	case event.GetActivityTaskFailedEventAttributes() != nil:
		a := event.GetActivityTaskFailedEventAttributes()
		attrs, set = a, func(id int64) { a.StartedEventId = id }
	case event.GetActivityTaskTimedOutEventAttributes() != nil:
		a := event.GetActivityTaskTimedOutEventAttributes()
		attrs, set = a, func(id int64) { a.StartedEventId = id }
	case event.GetActivityTaskCanceledEventAttributes() != nil:
		a := event.GetActivityTaskCanceledEventAttributes()
		attrs, set = a, func(id int64) { a.StartedEventId = id }
	default:
		return
	}
	if attrs.GetStartedEventId() == common.BufferedEventID {
		set(startedEventIDs[attrs.GetScheduledEventId()])
	}
}
