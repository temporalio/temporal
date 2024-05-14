// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package historybuilder

import (
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

type StubHandler struct{}

func (h StubHandler) WithTags(...metrics.Tag) metrics.Handler {
	return &h
}

func (h StubHandler) Counter(_ string) metrics.CounterIface {
	return nil
}

func (h StubHandler) Gauge(_ string) metrics.GaugeIface {
	return nil
}

func (h StubHandler) Timer(_ string) metrics.TimerIface {
	return nil
}

func (h StubHandler) Histogram(_ string, _ metrics.MetricUnit) metrics.HistogramIface {
	return nil
}

func (h StubHandler) Stop(_ log.Logger) {}

func TestHistoryBuilder_IsDirty(t *testing.T) {
	hb := HistoryBuilder{EventStore: EventStore{}}
	if hb.IsDirty() {
		t.Fatal("newly created history is dirty")
	}
}

func TestHistoryBuilder_AddWorkflowExecutionStartedEvent(t *testing.T) {
	ns := "some-namespace"
	t.Run("When ParentExecutionInfo is nil should not include in attributes", func(t *testing.T) {
		hb := HistoryBuilder{}
		startReq := &workflowservice.StartWorkflowExecutionRequest{}
		req := &historyservice.StartWorkflowExecutionRequest{StartRequest: startReq}
		startTime := time.Date(2023, 12, 27, 1, 11, 00, 00, time.UTC)
		e := hb.AddWorkflowExecutionStartedEvent(
			startTime,
			req,
			nil,
			"prev-run-id",
			"first-run-id",
			"original-run-id",
		)
		if e == nil {
			t.Fatal("added event is nil")
		}
		if attrs, ok := e.Attributes.(*historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes); ok {
			if attrs.WorkflowExecutionStartedEventAttributes.ParentWorkflowExecution != nil {
				t.Errorf(
					"expected attributes.ParentWorkflowExecution nil got %v",
					attrs.WorkflowExecutionStartedEventAttributes.ParentWorkflowExecution,
				)
			}
		} else {
			t.Error("wrong attributes type")
		}
	})

	t.Run("When ParentExecutionInfo is not nil should copy values to attributes", func(t *testing.T) {
		hb := HistoryBuilder{}
		parentInfo := &workflow.ParentExecutionInfo{Namespace: ns}
		startReq := &workflowservice.StartWorkflowExecutionRequest{}
		req := &historyservice.StartWorkflowExecutionRequest{StartRequest: startReq, ParentExecutionInfo: parentInfo}

		startTime := time.Date(2023, 12, 27, 1, 11, 00, 00, time.UTC)
		e := hb.AddWorkflowExecutionStartedEvent(
			startTime,
			req,
			nil,
			"prev-run-id",
			"first-run-id",
			"original-run-id",
		)
		if e == nil {
			t.Fatal("added event is nil")
		}
		if attrs, ok := e.Attributes.(*historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes); ok {
			if attrs.WorkflowExecutionStartedEventAttributes.ParentWorkflowNamespace != ns {
				t.Errorf("expected attributes.ParentWorkflowNamespace %s got %s",
					ns,
					attrs.WorkflowExecutionStartedEventAttributes.ParentWorkflowNamespace,
				)
			}
		} else {
			t.Error("wrong workflow attributes type")
		}

		if len(hb.memLatestBatch) != 1 {
			t.Fatalf("event was not stored in the buffer, expected len 1 got %d", len(hb.memLatestBatch))
		}
	})
}

func TestHistoryBuilder_FlushBufferToCurrentBatch(t *testing.T) {
	t.Run("when no events in dbBufferBatch or meBufferBatch will return scheduledIDToStartedID", func(t *testing.T) {
		hb := HistoryBuilder{
			EventStore{scheduledIDToStartedID: make(map[int64]int64)},
			EventFactory{},
		}
		hb.scheduledIDToStartedID[71] = 42

		schedlued := hb.FlushBufferToCurrentBatch()
		if len(schedlued) != len(hb.scheduledIDToStartedID) {
			t.Errorf("wrong scheduled2started map")
		}
		if schedlued[71] != 42 {
			t.Errorf("wrong value in map, expected 42 got %d", schedlued[42])
		}
	})

	t.Run("if workflow finished should clean dbBufferBatch and memBufferBatch and exit", func(t *testing.T) {
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 12})
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		hb.AddActivityTaskStartedEvent(42, 1, "request-id-1", "identity-1", nil, nil, 0)
		hb.workflowFinished = true
		hb.FlushBufferToCurrentBatch()
		if hb.memBufferBatch != nil || hb.dbBufferBatch != nil {
			t.Fatalf(
				"memBufferBatch and dbBufferBatch were not cleared,expected nil, nil got %v %v",
				hb.memEventsBatches,
				hb.dbBufferBatch,
			)
		}
	})
	t.Run("when there are events in both memBufferBatch and dbBufferBatch will move all to latest", func(t *testing.T) {
		assertEventsWired := func(scheduled map[int64]int64) {
			t.Helper()
			if len(scheduled) != 1 {
				t.Fatalf("expected one scheduledToStartedIds event got %d", len(scheduled))
			}
			startedId, ok := scheduled[42]
			if !ok {
				t.Fatal("scheduledToStartedIds event not found")
			}
			if startedId != 13 {
				t.Fatalf("wrong started id expected 42 got %d", startedId)
			}
		}

		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 12})
		// add event to memLatestBatch
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		if len(hb.memLatestBatch) != 1 {
			t.Errorf("expected 1 event in memLatestBatch got %d", len(hb.memLatestBatch))
		}
		// add event to memBufferBatch
		hb.AddActivityTaskStartedEvent(42, 1, "request-id-1", "identity-1", nil, nil, 0)
		if len(hb.memBufferBatch) != 1 {
			t.Errorf("expected 1 event in memBufferBatch got %d", len(hb.memBufferBatch))
		}

		scheduledToStartedIds := hb.FlushBufferToCurrentBatch()
		assertEventsWired(scheduledToStartedIds)
		if len(hb.memLatestBatch) != 2 {
			t.Errorf("wrong size of memLatestBatch expected 2 got %d", len(hb.memLatestBatch))
		}
		if len(hb.memBufferBatch) != 0 {
			t.Errorf("wrong size of memBufferBatch expected 0 got %d", len(hb.memBufferBatch))
		}
	})

	t.Run("when there is ACTIVITY_TASK_COMPLETED event will move it to the end", func(t *testing.T) {
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 12})
		hb.AddActivityTaskCompletedEvent(14, 13, "activity-completed", nil)
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		hb.AddActivityTaskStartedEvent(42, 1, "request-id-1", "identity-1", nil, nil, 0)
		hb.FlushBufferToCurrentBatch()
		if len(hb.memLatestBatch) != 3 {
			t.Fatalf("wrong length of memLatestBatch after Flush expected 3 got %d", len(hb.memLatestBatch))
		}
		if hb.memLatestBatch[2].EventType != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			t.Fatal("ACTIVITY_TASK_COMPLETED was not moved to the end")
		}
		if hb.memLatestBatch[0].EventType != enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			t.Fatal("ACTIVITY_TASK_SCHEDULED was moved")
		}
		if hb.memLatestBatch[1].EventType != enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED {
			t.Fatal("ACTIVITY_TASK_SCHEDULED was moved")
		}
	})

	t.Run("when ActivityTaskCompletedEvent in history will link event to started", func(t *testing.T) {
		hb := newSUT()
		hb.AddActivityTaskStartedEvent(eventConfig{scheduledId: 42})
		hb.AddActivityTaskCanceledEvent()
		scheduledToStarted := hb.FlushBufferToCurrentBatch()
		scheduled, ok := scheduledToStarted[42]
		if !ok {
			t.Fatalf("event not in map %v", scheduled)
		}
	})
}

func TestHistoryBuilder_Finish(t *testing.T) {

	t.Run("finish empty", func(t *testing.T) {
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 42})
		_, err := hb.Finish(false)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("finish with one vent in dbBufferBatch", func(t *testing.T) {
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 50})
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		hb.AddActivityTaskStartedEvent(42, 1, "request-id-1", "identity-1", nil, nil, 0)
		result, err := hb.Finish(false)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.DBEventsBatches[0]) < 2 {
			t.Errorf("expected len 2 got %d", len(result.DBEventsBatches))
			t.Errorf("DBEventsBatches %v", result.DBEventsBatches)
			t.Fatalf("DBEventsBatches too short expected 2 got %d", len(result.DBEventsBatches))
		}
	})
}

func TestHistoryBuilder_GetAndRemoveTimerFireEvent(t *testing.T) {

	assertTimerExpiredEventForId := func(event *historypb.HistoryEvent, timerId string) {
		t.Helper()
		if event == nil {
			t.Fatalf("did not receive timer fired event")
		}
		attrs, ok := event.Attributes.(*historypb.HistoryEvent_TimerFiredEventAttributes)
		if !ok {
			t.Fatal("wrong attributes set")
		}
		if attrs.TimerFiredEventAttributes.TimerId != timerId {
			t.Fatalf(
				"wrong timer event removed, expected %q got %q",
				timerId,
				attrs.TimerFiredEventAttributes.TimerId,
			)
		}
	}

	t.Run("no timer fired event present does nothing", func(t *testing.T) {
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 50})
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		hb.AddActivityTaskScheduledEvent(
			32,
			&commandpb.ScheduleActivityTaskCommandAttributes{},
		)
		hb.AddActivityTaskStartedEvent(42, 1, "request-id-1", "identity-1", nil, nil, 0)
		memBufferSize := len(hb.memBufferBatch)
		dbBufferSize := len(hb.dbBufferBatch)

		result := hb.GetAndRemoveTimerFireEvent("timer-1")

		if result != nil {
			t.Errorf("got timer event expected nothing got %v", result)
		}
		if len(hb.memBufferBatch) != memBufferSize {
			t.Errorf("memBuffer has changed expected %d got %d", memBufferSize, len(hb.memBufferBatch))
		}
		if len(hb.dbBufferBatch) != dbBufferSize {
			t.Errorf("dbBuffer has changed expected %d got %d", dbBufferSize, len(hb.dbBufferBatch))
		}
	})

	t.Run("should remove only the matching timer event from memBufferBatch", func(t *testing.T) {
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 50})
		hb.AddTimerFiredEvent(32, "timer-1")
		hb.AddTimerFiredEvent(32, "timer-2")
		hb.AddTimerFiredEvent(32, "timer-3")
		result := hb.GetAndRemoveTimerFireEvent("timer-2")
		assertTimerExpiredEventForId(result, "timer-2")
		if len(hb.memBufferBatch) != 2 {
			t.Errorf("memBufferBatch did not properly shrink, expected 2 got %d", len(hb.memBufferBatch))
		}
	})

	t.Run("should remove only the matching timer event from dbBufferBatch", func(t *testing.T) {
		dbBufferBatch := newDbBufferWithTimerEvents("timer-1", "timer-2", "timer-3")
		hb := newHistoryBuilderFromConfig(builderConfig{nextEventId: 50, dbBufferBatch: dbBufferBatch})
		result := hb.GetAndRemoveTimerFireEvent("timer-1")
		assertTimerExpiredEventForId(result, "timer-1")
		if len(hb.dbBufferBatch) != 2 {
			t.Errorf("dbBufferBatch did not properly shrink, expected 2 got %d", len(hb.dbBufferBatch))
		}
	})
}

func TestHistoryBuilder_HasBufferEvents(t *testing.T) {
	newHistoryBuilderWithEventsInDbBuffer := func() *HistoryBuilder {
		return newHistoryBuilderFromConfig(builderConfig{dbBufferBatch: newDbBufferWithTimerEvents("timer-1")})
	}
	newHistoryBuilderWithEventsInMemBuffer := func() *sutTestingAdapter {
		sut := newSUT()
		sut.AddActivityTaskTimedOutEvent()
		return &sut
	}
	t.Run("when there are no event in any buffer HasBufferEvents should produce false", func(t *testing.T) {
		hb := newHistoryBuilder()
		if hb.HasBufferEvents() {
			t.Error("empty history has events")
		}
	})
	t.Run("when there is event in dbBuffer HasBufferEvents should be true", func(t *testing.T) {
		hb := newHistoryBuilderWithEventsInDbBuffer()
		if hb.HasBufferEvents() == false {
			t.Error("when event in DbBuffer expected HasBufferEvents to return true got false")
		}
	})
	t.Run("when there is an event in memBuffer HasBufferEvents should be true", func(t *testing.T) {
		hb := newHistoryBuilderWithEventsInMemBuffer()
		if hb.HasBufferEvents() == false {
			t.Error("when event in MemBuffer expected HasBufferEvents to return true got false")
		}
	})
}

func TestHistoryBuilder_HasAnyBufferedEvent(t *testing.T) {
	isTaskStartedEvent := func(e *historypb.HistoryEvent) bool {
		return e.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
	}
	matching := func(timerId string) func(e *historypb.HistoryEvent) bool {
		return func(e *historypb.HistoryEvent) bool {
			return e.GetTimerFiredEventAttributes().GetTimerId() == timerId
		}
	}

	t.Run("when there is an event in memBufferBatch which matches filter should produce true", func(t *testing.T) {
		historyBuilder := newSUT()
		historyBuilder.AddActivityTaskCompletedEvent()
		historyBuilder.AddActivityTaskStartedEvent()
		historyBuilder.AddActivityTaskCompletedEvent()
		if historyBuilder.HasAnyBufferedEvent(isTaskStartedEvent) == false {
			t.Error(
				"ActivityTaskScheduledEvent in history, expected HasAnyBufferedEvent(isTaskStarted) to return true got false",
			)
		}
	})

	t.Run("when there are no matching events in any batch should produce false", func(t *testing.T) {
		historyBuilder := historyBuilderWithTimerEventsInDbBuffer()
		historyBuilder.AddActivityTaskCompletedEvent()
		historyBuilder.AddActivityTaskCompletedEvent()
		if historyBuilder.HasAnyBufferedEvent(isTaskStartedEvent) {
			t.Error(
				"no events in history, expected HasAnyBufferedEvent(isTaskStarted) to return false got true",
			)
		}
	})

	t.Run("when there is an event in dbBufferBatch which matches filter should produce true", func(t *testing.T) {
		hb := historyBuilderWithTimerEventsInDbBuffer("timer-1", "timer-2", "timer-3")
		if hb.HasAnyBufferedEvent(matching("timer-2")) == false {
			t.Error(
				"ActivityTaskScheduledEvent in history, expected HasAnyBufferedEvent(isTaskStarted) to return true got false",
			)
		}
	})
}

func TestHistoryBuilder_NumBufferedEvents(t *testing.T) {
	t.Run("when buffers are empty should produce 0", func(t *testing.T) {
		s := newSUT()
		if s.HistoryBuilder.NumBufferedEvents() != 0 {
			t.Error("empty history has buffered events")
		}
	})

	t.Run("should count events in dbBuffer and memBuffer", func(t *testing.T) {
		s := historyBuilderWithTimerEventsInDbBuffer("timer-1", "timer-2", "timer-3", "timer-4")
		s.AddActivityTaskStartedEvent()
		s.AddActivityTaskStartedEvent()
		s.AddActivityTaskStartedEvent()
		s.AddActivityTaskCompletedEvent()
		s.AddActivityTaskCompletedEvent()
		if s.NumBufferedEvents() != 9 {
			t.Errorf("wrong event count expected 9 got %d", s.HistoryBuilder.NumBufferedEvents())
		}

	})
}

func TestHistoryBuilder_AddDifferentEvents_WorkflowFinishEvents(t *testing.T) {
	sut := newSUT()
	assertWorkflowFinished := func(t *testing.T, s sutTestingAdapter) {
		t.Helper()
		if s.workflowFinished == false {
			t.Errorf("expected workflow to be finished")
		}
	}
	assertWorkflowNotFinished := func(t *testing.T, s sutTestingAdapter) {
		t.Helper()
		if s.workflowFinished {
			t.Errorf("expected workflow not to be finished")
		}
	}
	usecase := []struct {
		description string
		action      func(...eventConfig) *historypb.HistoryEvent
		assertion   func(*testing.T, sutTestingAdapter)
	}{
		{
			"When AddWorkflowExecutionStartedEvent ",
			sut.AddWorkflowExecutionStartedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowTaskScheduledEvent added ",
			sut.AddWorkflowTaskScheduledEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowTaskStartedEvent added ",
			sut.AddWorkflowTaskStartedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowTaskCompletedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskCompletedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowTaskTimedOutEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskTimedOutEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowTaskFailedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskFailedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ActivityTaskScheduledEvent added it will be placed in memLatestBatch",
			sut.AddActivityTaskScheduledEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ActivityTaskStartedEvent added it will be placed in memBuffer",
			sut.AddActivityTaskStartedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When AddActivityTaskCompletedEvent added it will be placed in memBuffer",
			sut.AddActivityTaskCompletedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ActivityTaskFailedEvent added it will be placed in memBuffer",
			sut.AddActivityTaskFailedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ActivityTaskTimedOutEvent added it will be placed in memBuffer",
			sut.AddActivityTaskTimedOutEvent,
			assertWorkflowNotFinished,
		},
		{
			"When CompletedWorkflowEvent added it will be placed in memLatestBatch",
			sut.AddCompletedWorkflowEvent,
			assertWorkflowFinished,
		},
		{
			"When FailWorkflowEvent added it will be placed in memLatestBatch",
			sut.AddFailWorkflowEvent,
			assertWorkflowFinished,
		},
		{
			"When TimeoutWorkflowEvent added it will be placed in memLatestBatch",
			sut.AddTimeoutWorkflowEvent,
			assertWorkflowFinished,
		},
		{
			"When WorkflowExecutionTerminatedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionTerminatedEvent,
			assertWorkflowFinished,
		},
		{
			"When WorkflowExecutionUpdateAcceptedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionUpdateAcceptedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowExecutionUpdateCompletedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionUpdateCompletedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ContinuedAsNewEvent added it will be placed in memLatestBatch",
			sut.AddContinuedAsNewEvent,
			assertWorkflowFinished,
		},
		{
			"When TimerStartedEvent added it will be placed in memLatestBatch",
			sut.AddTimerStartedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When TimerFiredEvent added it will be placed in memBuffer",
			sut.AddTimerFiredEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ActivityTaskCancelRequestedEvent added it will be placed in memLatestBatch",
			sut.AddActivityTaskCancelRequestedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When  ActivityTaskCanceledEvent added it will be placed in memBuffer",
			sut.AddActivityTaskCanceledEvent,
			assertWorkflowNotFinished,
		},
		{
			"When  TimerCanceledEvent added it will be placed in memLatestBatch",
			sut.AddTimerCanceledEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowExecutionCancelRequestedEvent added it will be placed in memBuffer",
			sut.AddWorkflowExecutionCancelRequestedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowExecutionCanceledEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionCanceledEvent,
			assertWorkflowFinished,
		},
		{
			"When RequestCancelExternalWorkflowExecutionInitiatedEvent added it will be placed in memLatestBatch",
			sut.AddRequestCancelExternalWorkflowExecutionInitiatedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When RequestCancelExternalWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddRequestCancelExternalWorkflowExecutionFailedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ExternalWorkflowExecutionCancelRequested added it will be placed in MemBuffer",
			sut.AddExternalWorkflowExecutionCancelRequested,
			assertWorkflowNotFinished,
		},
		{
			"When SignalExternalWorkflowExecutionInitiatedEvent added it will be placed in memLatestBatch",
			sut.AddSignalExternalWorkflowExecutionInitiatedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When UpsertWorkflowSearchAttributesEvent added it will be placed in memLatestBatch",
			sut.AddUpsertWorkflowSearchAttributesEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowPropertiesModifiedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowPropertiesModifiedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When SignalExternalWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddSignalExternalWorkflowExecutionFailedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ExternalWorkflowExecutionSignaled added it will be placed in MemBuffer",
			sut.AddExternalWorkflowExecutionSignaled,
			assertWorkflowNotFinished,
		},
		{
			"When MarkerRecordedEvent added it will be placed in memLatestBatch",
			sut.AddMarkerRecordedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When WorkflowExecutionSignaledEvent added it will be placed in MemBuffer",
			sut.AddWorkflowExecutionSignaledEvent,
			assertWorkflowNotFinished,
		},
		{
			"When StartChildWorkflowExecutionInitiatedEvent added it will be placed in memLatestBatch",
			sut.AddStartChildWorkflowExecutionInitiatedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ChildWorkflowExecutionStartedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionStartedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When StartChildWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddStartChildWorkflowExecutionFailedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ChildWorkflowExecutionCompletedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionCompletedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ChildWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionFailedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ChildWorkflowExecutionCanceledEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionCanceledEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ChildWorkflowExecutionTerminatedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionTerminatedEvent,
			assertWorkflowNotFinished,
		},
		{
			"When ChildWorkflowExecutionTimedOutEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionTimedOutEvent,
			assertWorkflowNotFinished,
		},
	}
	for _, uc := range usecase {
		t.Run(uc.description, func(t *testing.T) {
			sut.ResetHistoryBuilder()
			uc.action()
			uc.assertion(t, sut)
		})
	}
}

func TestHistoryBuilder_AddDifferentEvents(t *testing.T) {
	sut := newSUT()
	usecase := []struct {
		description string
		action      func(...eventConfig) *historypb.HistoryEvent
		assertion   func(*testing.T, sutTestingAdapter)
	}{
		{
			"When AddWorkflowExecutionStartedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionStartedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowTaskScheduledEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskScheduledEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowTaskStartedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskStartedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowTaskCompletedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskCompletedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowTaskTimedOutEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskTimedOutEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowTaskFailedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowTaskFailedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When ActivityTaskScheduledEvent added it will be placed in memLatestBatch",
			sut.AddActivityTaskScheduledEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When ActivityTaskStartedEvent added it will be placed in memBuffer",
			sut.AddActivityTaskStartedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When AddActivityTaskCompletedEvent added it will be placed in memBuffer",
			sut.AddActivityTaskCompletedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ActivityTaskFailedEvent added it will be placed in memBuffer",
			sut.AddActivityTaskFailedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ActivityTaskTimedOutEvent added it will be placed in memBuffer",
			sut.AddActivityTaskTimedOutEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When CompletedWorkflowEvent added it will be placed in memLatestBatch",
			sut.AddCompletedWorkflowEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When FailWorkflowEvent added it will be placed in memLatestBatch",
			sut.AddFailWorkflowEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When TimeoutWorkflowEvent added it will be placed in memLatestBatch",
			sut.AddTimeoutWorkflowEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowExecutionTerminatedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionTerminatedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowExecutionUpdateAcceptedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionUpdateAcceptedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowExecutionUpdateCompletedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionUpdateCompletedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When ContinuedAsNewEvent added it will be placed in memLatestBatch",
			sut.AddContinuedAsNewEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When TimerStartedEvent added it will be placed in memLatestBatch",
			sut.AddTimerStartedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When TimerFiredEvent added it will be placed in memBuffer",
			sut.AddTimerFiredEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ActivityTaskCancelRequestedEvent added it will be placed in memLatestBatch",
			sut.AddActivityTaskCancelRequestedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When  ActivityTaskCanceledEvent added it will be placed in memBuffer",
			sut.AddActivityTaskCanceledEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When  TimerCanceledEvent added it will be placed in memLatestBatch",
			sut.AddTimerCanceledEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowExecutionCancelRequestedEvent added it will be placed in memBuffer",
			sut.AddWorkflowExecutionCancelRequestedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When WorkflowExecutionCanceledEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowExecutionCanceledEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When RequestCancelExternalWorkflowExecutionInitiatedEvent added it will be placed in memLatestBatch",
			sut.AddRequestCancelExternalWorkflowExecutionInitiatedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When RequestCancelExternalWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddRequestCancelExternalWorkflowExecutionFailedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ExternalWorkflowExecutionCancelRequested added it will be placed in MemBuffer",
			sut.AddExternalWorkflowExecutionCancelRequested,
			assertEventFoundInMemBuffer,
		},
		{
			"When SignalExternalWorkflowExecutionInitiatedEvent added it will be placed in memLatestBatch",
			sut.AddSignalExternalWorkflowExecutionInitiatedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When UpsertWorkflowSearchAttributesEvent added it will be placed in memLatestBatch",
			sut.AddUpsertWorkflowSearchAttributesEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowPropertiesModifiedEvent added it will be placed in memLatestBatch",
			sut.AddWorkflowPropertiesModifiedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When SignalExternalWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddSignalExternalWorkflowExecutionFailedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ExternalWorkflowExecutionSignaled added it will be placed in MemBuffer",
			sut.AddExternalWorkflowExecutionSignaled,
			assertEventFoundInMemBuffer,
		},
		{
			"When MarkerRecordedEvent added it will be placed in memLatestBatch",
			sut.AddMarkerRecordedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When WorkflowExecutionSignaledEvent added it will be placed in MemBuffer",
			sut.AddWorkflowExecutionSignaledEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When StartChildWorkflowExecutionInitiatedEvent added it will be placed in memLatestBatch",
			sut.AddStartChildWorkflowExecutionInitiatedEvent,
			assertEventFoundInLatestBatch,
		},
		{
			"When ChildWorkflowExecutionStartedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionStartedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When StartChildWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddStartChildWorkflowExecutionFailedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ChildWorkflowExecutionCompletedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionCompletedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ChildWorkflowExecutionFailedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionFailedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ChildWorkflowExecutionCanceledEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionCanceledEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ChildWorkflowExecutionTerminatedEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionTerminatedEvent,
			assertEventFoundInMemBuffer,
		},
		{
			"When ChildWorkflowExecutionTimedOutEvent added it will be placed in MemBuffer",
			sut.AddChildWorkflowExecutionTimedOutEvent,
			assertEventFoundInMemBuffer,
		},
	}
	for _, uc := range usecase {
		t.Run(uc.description, func(t *testing.T) {
			sut.ResetHistoryBuilder()
			uc.action()
			uc.assertion(t, sut)
		})
	}
}

func TestHistoryBuilder_AddDifferentEvents_AssignmentEventId(t *testing.T) {
	t.Run("Adding batched event will set event id to next event id allocated by HistoryBuilder", func(t *testing.T) {
		historyBuilder := newSUTFromConfig(builderConfig{nextEventId: 76})
		event := historyBuilder.AddWorkflowTaskScheduledEvent()
		if event.EventId != 76 {
			t.Fatalf("wrong event id, expected 64 got %d", event.EventId)
		}
	})
	t.Run("Adding buffered event will set event Id to common.BufferedEventID", func(t *testing.T) {
		historyBuilder := newSUT()
		event := historyBuilder.AddWorkflowExecutionCancelRequestedEvent()
		if event.EventId != common.BufferedEventID {
			t.Fatalf("wrong event id, expected 64 got %d", event.EventId)
		}
	})
}

func TestHistoryBuilder_FlushBufferToCurrentBatch_WiringEvents(t *testing.T) {
	t.Run("should allocate event id for each buffered event", func(t *testing.T) {
		sut := newSUTFromConfig(builderConfig{nextEventId: 98})
		taskStartedEvent := sut.AddActivityTaskStartedEvent()
		taskCompletedEvent := sut.AddActivityTaskCompletedEvent()
		if taskStartedEvent.EventId != common.BufferedEventID {
			t.Errorf(
				"wrong task started event id, expected %d got %d",
				common.BufferedEventID,
				taskStartedEvent.EventId,
			)
		}
		if taskCompletedEvent.EventId != common.BufferedEventID {
			t.Errorf(
				"wrong task completed event id, expected %d got %d",
				common.BufferedEventID,
				taskCompletedEvent.EventId,
			)
		}

		sut.FlushBufferToCurrentBatch()

		if taskStartedEvent.EventId != 98 {
			t.Errorf("wrong task started event id, expected 42 got %d", taskStartedEvent.EventId)
		}
		if taskCompletedEvent.EventId != 99 {
			t.Errorf("wrong task completed event id, expected 43 got %d", taskCompletedEvent.EventId)
		}
	})

	t.Run("for every Activity event should point to its causal event", func(t *testing.T) {
		sut := newSUTFromConfig(builderConfig{nextEventId: 98})
		started := sut.AddActivityTaskStartedEvent(eventConfig{scheduledId: 2})
		completed := sut.AddActivityTaskCompletedEvent(eventConfig{scheduledId: 2, startedId: 22})
		failed := sut.AddActivityTaskFailedEvent(eventConfig{scheduledId: 2, startedId: 25})
		timedOut := sut.AddActivityTaskTimedOutEvent(eventConfig{scheduledId: 2, startedId: 26})
		canceled := sut.AddActivityTaskCanceledEvent(eventConfig{scheduledId: 2})

		scheduledToStarted := sut.FlushBufferToCurrentBatch()

		if scheduledToStarted[2] != started.EventId {
			t.Errorf(
				"wrong in scheduledToStarted[2] expected %d, got %d",
				started.EventId,
				scheduledToStarted[2],
			)
		}
		attrs1 := completed.GetActivityTaskCompletedEventAttributes()
		if attrs1.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in completed event expected 98, got %d",
				attrs1.StartedEventId,
			)
		}
		attrs2 := failed.GetActivityTaskFailedEventAttributes()
		if attrs2.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in failed event expected %d, got %d",
				started.EventId,
				attrs2.StartedEventId,
			)
		}
		attrs3 := timedOut.GetActivityTaskTimedOutEventAttributes()
		if attrs3.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in timedOut event expected %d, got %d",
				started.EventId,
				attrs3.StartedEventId,
			)
		}
		attrs4 := canceled.GetActivityTaskCanceledEventAttributes()
		if attrs4 != nil && attrs4.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in canceled event expected %d, got %d",
				started.EventId,
				attrs4.StartedEventId,
			)
		}
	})

	t.Run("for every Child Workflow event should point to its causal event", func(t *testing.T) {
		sut := newSUTFromConfig(builderConfig{nextEventId: 202})
		started := sut.AddChildWorkflowExecutionStartedEvent(eventConfig{initiatedId: 42})
		completed := sut.AddChildWorkflowExecutionCompletedEvent(eventConfig{initiatedId: 42, startedId: 78})
		failed := sut.AddChildWorkflowExecutionFailedEvent(eventConfig{initiatedId: 42, startedId: 90})
		timedOut := sut.AddChildWorkflowExecutionTimedOutEvent(eventConfig{initiatedId: 42, startedId: 93})
		canceled := sut.AddChildWorkflowExecutionCanceledEvent(eventConfig{initiatedId: 42, startedId: 98})
		terminated := sut.AddChildWorkflowExecutionTerminatedEvent(eventConfig{initiatedId: 42, startedId: 100})

		scheduledToStarted := sut.FlushBufferToCurrentBatch()

		if scheduledToStarted[42] != started.EventId {
			t.Errorf(
				"wrong id in scheduledToStarted[2] expected %d, got %d",
				started.EventId,
				scheduledToStarted[42],
			)
		}
		attr1 := completed.GetChildWorkflowExecutionCompletedEventAttributes()
		if attr1.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in completed event expected %d, got %d",
				started.EventId,
				attr1.StartedEventId,
			)
		}
		attr2 := failed.GetChildWorkflowExecutionFailedEventAttributes()
		if attr2.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in failed expected %d, got %d",
				started.EventId,
				attr2.StartedEventId,
			)
		}
		attr3 := timedOut.GetChildWorkflowExecutionTimedOutEventAttributes()
		if attr3.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in timedOut expected %d, got %d",
				started.EventId,
				attr3.StartedEventId,
			)
		}
		attr4 := canceled.GetChildWorkflowExecutionCanceledEventAttributes()
		if attr4.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in canceled expected %d, got %d",
				started.EventId,
				attr4.StartedEventId,
			)
		}
		attr5 := terminated.GetChildWorkflowExecutionTerminatedEventAttributes()
		if attr5.StartedEventId != started.EventId {
			t.Errorf(
				"wrong started event id in terminated expected %d, got %d",
				started.EventId,
				attr5.StartedEventId,
			)
		}

	})
}

func TestHistoryBuilder_HasActivityFinishEvent(t *testing.T) {
	t.Run("will produce false if can't find event with given id", func(t *testing.T) {
		sut := newSUT()
		sut.AddActivityTaskCompletedEvent(eventConfig{scheduledId: 49})
		sut.AddActivityTaskFailedEvent(eventConfig{scheduledId: 72})
		sut.AddActivityTaskTimedOutEvent(eventConfig{scheduledId: 98})
		sut.AddActivityTaskCanceledEvent(eventConfig{scheduledId: 10})
		if sut.HasActivityFinishEvent(17) {
			t.Error("produced true while there is no event with given id")
		}
	})

	t.Run("will produce false if event not an activity finish event", func(t *testing.T) {
		sut := newSUT()
		sut.AddActivityTaskStartedEvent(eventConfig{scheduledId: 61})
		sut.AddActivityTaskCompletedEvent(eventConfig{scheduledId: 49})
		sut.AddActivityTaskFailedEvent(eventConfig{scheduledId: 72})
		if sut.HasActivityFinishEvent(61) {
			t.Error("produced true while not an activity finish event")
		}
	})

	t.Run("can find completed event if present", func(t *testing.T) {
		sut := newSUT()
		sut.AddActivityTaskCompletedEvent(eventConfig{scheduledId: 49})
		if sut.HasActivityFinishEvent(49) == false {
			t.Errorf("could not find activity task completed with scheduledId 49")
		}
	})

	t.Run("can find failed event if present", func(t *testing.T) {
		sut := newSUT()
		sut.AddActivityTaskFailedEvent(eventConfig{scheduledId: 72})
		if sut.HasActivityFinishEvent(72) == false {
			t.Errorf("could not find activity task failed with scheduledId 72")
		}
	})

	t.Run("can find failed event if present", func(t *testing.T) {
		sut := newSUT()
		sut.AddActivityTaskTimedOutEvent(eventConfig{scheduledId: 98})
		if sut.HasActivityFinishEvent(98) == false {
			t.Errorf("could not find activity task timedOut with scheduledId 98")
		}
	})

	t.Run("can find failed event if present", func(t *testing.T) {
		sut := newSUT()
		sut.AddActivityTaskCanceledEvent(eventConfig{scheduledId: 10})
		if sut.HasActivityFinishEvent(10) == false {
			t.Errorf("could not find activity task cancelled with scheduledId 98")
		}
	})
}

func newDbBufferWithTimerEvents(ids ...string) []*historypb.HistoryEvent {
	timerEvent := func(tid string) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
			Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
				TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{TimerId: tid},
			},
		}
	}

	dbBufferBatch := make([]*historypb.HistoryEvent, 0)
	for _, tid := range ids {
		dbBufferBatch = append(dbBufferBatch, timerEvent(tid))
	}
	return dbBufferBatch
}

func assertEventFoundInLatestBatch(t *testing.T, sut sutTestingAdapter) {
	t.Helper()
	if len(sut.memLatestBatch) < 1 {
		t.Error("event is missing from memLatestBatch")
	}
}

func assertEventFoundInMemBuffer(t *testing.T, sut sutTestingAdapter) {
	t.Helper()
	if len(sut.memBufferBatch) < 1 {
		t.Error("event is missing from memBufferBatch")
	}
}

type builderConfig struct {
	nextEventId   int64
	dbBufferBatch []*historypb.HistoryEvent
}

func newHistoryBuilderFromConfig(config builderConfig) *HistoryBuilder {
	ts := clock.NewRealTimeSource()
	tig := func(n int) ([]int64, error) { return []int64{1, 2, 3, 4, 5}, nil }
	return New(ts, tig, int64(101), config.nextEventId, config.dbBufferBatch, StubHandler{})
}

func newHistoryBuilder() *HistoryBuilder {
	return newHistoryBuilderFromConfig(builderConfig{nextEventId: 42})
}

func historyBuilderWithTimerEventsInDbBuffer(timers ...string) sutTestingAdapter {
	bufferWithTimerEvents := newDbBufferWithTimerEvents(timers...)
	return newSUTFromConfig(builderConfig{dbBufferBatch: bufferWithTimerEvents})
}

type eventConfig struct {
	startedId   int64
	scheduledId int64
	initiatedId int64
}

type sutTestingAdapter struct {
	*HistoryBuilder
	today time.Time
}

func newSUT() sutTestingAdapter {
	today := time.Date(2023, 12, 27, 1, 11, 00, 00, time.UTC)
	hb := newHistoryBuilder()
	s := sutTestingAdapter{HistoryBuilder: hb, today: today}
	return s
}

func newSUTFromConfig(config builderConfig) sutTestingAdapter {
	today := time.Date(2023, 12, 27, 1, 11, 00, 00, time.UTC)
	hb := newHistoryBuilderFromConfig(config)
	s := sutTestingAdapter{HistoryBuilder: hb, today: today}
	return s
}

func (s *sutTestingAdapter) ResetHistoryBuilder() {
	s.HistoryBuilder = newHistoryBuilder()
}

func (s *sutTestingAdapter) AddWorkflowExecutionStartedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	parentInfo := &workflow.ParentExecutionInfo{Namespace: "ns-1"}
	startReq := &workflowservice.StartWorkflowExecutionRequest{}
	req := &historyservice.StartWorkflowExecutionRequest{StartRequest: startReq, ParentExecutionInfo: parentInfo}
	return s.HistoryBuilder.AddWorkflowExecutionStartedEvent(s.today, req, nil, "prev-run-1", "first-run-1", "original-run-1")
}

func (s *sutTestingAdapter) AddWorkflowTaskStartedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowTaskStartedEvent(64, "request-1", "identity-1", s.today, false, 100, nil, 0)
}

func (s *sutTestingAdapter) AddWorkflowTaskCompletedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowTaskCompletedEvent(
		64,
		32,
		"identity-1",
		"checksum",
		nil,
		nil,
		nil,
	)
}

func (s *sutTestingAdapter) AddActivityTaskFailedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	cfg := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddActivityTaskFailedEvent(
		cfg.scheduledId,
		cfg.startedId,
		nil,
		enumspb.RETRY_STATE_IN_PROGRESS,
		"identity-1",
	)
}

func (s *sutTestingAdapter) AddActivityTaskScheduledEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddActivityTaskScheduledEvent(
		64,
		&commandpb.ScheduleActivityTaskCommandAttributes{},
	)
}

func (s *sutTestingAdapter) AddWorkflowTaskFailedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowTaskFailedEvent(64,
		32,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES,
		nil,
		"identity-1",
		"base-run-1",
		"new-run-1",
		0,
		"checksum",
	)
}

func (s *sutTestingAdapter) AddWorkflowTaskTimedOutEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowTaskTimedOutEvent(64, 32, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)
}

func (s *sutTestingAdapter) AddWorkflowTaskScheduledEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowTaskScheduledEvent(nil, nil, 1, s.today)
}

func (s *sutTestingAdapter) AddActivityTaskStartedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddActivityTaskStartedEvent(config.scheduledId, 1, "request-1", "identity-1", nil, nil, 0)
}

func (s *sutTestingAdapter) AddActivityTaskCompletedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddActivityTaskCompletedEvent(config.scheduledId, config.scheduledId, "identity-1", nil)
}

func (s *sutTestingAdapter) AddActivityTaskTimedOutEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddActivityTaskTimedOutEvent(config.scheduledId, config.startedId, nil, enumspb.RETRY_STATE_IN_PROGRESS)
}

func (s *sutTestingAdapter) AddCompletedWorkflowEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.CompleteWorkflowExecutionCommandAttributes{
		Result: nil,
	}
	return s.HistoryBuilder.AddCompletedWorkflowEvent(64, attrs, "new-run-1")
}

func (s *sutTestingAdapter) AddFailWorkflowEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.FailWorkflowExecutionCommandAttributes{Failure: nil}
	event, _ := s.HistoryBuilder.AddFailWorkflowEvent(
		64,
		enumspb.RETRY_STATE_IN_PROGRESS,
		attrs,
		"new-run-11",
	)
	return event
}

func (s *sutTestingAdapter) AddTimeoutWorkflowEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddTimeoutWorkflowEvent(enumspb.RETRY_STATE_IN_PROGRESS, "new-rung-1")
}

func (s *sutTestingAdapter) AddWorkflowExecutionTerminatedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowExecutionTerminatedEvent("no reason to terminate", nil, "identity-secret")
}

func (s *sutTestingAdapter) AddWorkflowExecutionUpdateAcceptedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowExecutionUpdateAcceptedEvent("instance-1", "accepted-1", 42, nil)
}

func (s *sutTestingAdapter) AddWorkflowExecutionUpdateCompletedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	event, _ := s.HistoryBuilder.AddWorkflowExecutionUpdateCompletedEvent(64, nil)
	return event
}

func (s *sutTestingAdapter) AddContinuedAsNewEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{}
	return s.HistoryBuilder.AddContinuedAsNewEvent(64, "new-run-5", attrs)
}

func (s *sutTestingAdapter) AddTimerStartedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.StartTimerCommandAttributes{}
	return s.HistoryBuilder.AddTimerStartedEvent(64, attrs)
}

func (s *sutTestingAdapter) AddTimerFiredEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddTimerFiredEvent(64, "timer-1")
}

func (s *sutTestingAdapter) AddActivityTaskCancelRequestedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddActivityTaskCancelRequestedEvent(64, config.scheduledId)
}

func (s *sutTestingAdapter) AddActivityTaskCanceledEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddActivityTaskCanceledEvent(
		config.scheduledId,
		config.startedId,
		37,
		nil,
		"identity-1",
	)
}

func (s *sutTestingAdapter) AddTimerCanceledEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddTimerCanceledEvent(64, 32, "timer-1", "identity-hidden")
}

func (s *sutTestingAdapter) AddWorkflowExecutionCancelRequestedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{},
	}
	return s.HistoryBuilder.AddWorkflowExecutionCancelRequestedEvent(request)
}

func (s *sutTestingAdapter) AddWorkflowExecutionCanceledEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.CancelWorkflowExecutionCommandAttributes{}
	return s.HistoryBuilder.AddWorkflowExecutionCanceledEvent(64, attrs)
}

func (s *sutTestingAdapter) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{}
	return s.HistoryBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(64, attrs, namespace.ID("some-id"))
}

func (s *sutTestingAdapter) AddRequestCancelExternalWorkflowExecutionFailedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		64,
		32,
		namespace.Name("ns-1"),
		namespace.ID("target"),
		"workflow-1",
		"run-1",
		enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
	)
}

func (s *sutTestingAdapter) AddExternalWorkflowExecutionCancelRequested(...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddExternalWorkflowExecutionCancelRequested(
		64,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		"wf-1",
		"run-1",
	)
}

func (s *sutTestingAdapter) AddSignalExternalWorkflowExecutionInitiatedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
		Execution: &commonpb.WorkflowExecution{},
	}
	return s.HistoryBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(64, attrs, namespace.ID("ns-target"))
}

func (s *sutTestingAdapter) AddUpsertWorkflowSearchAttributesEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{}
	return s.HistoryBuilder.AddUpsertWorkflowSearchAttributesEvent(64, attrs)
}

func (s *sutTestingAdapter) AddWorkflowPropertiesModifiedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.ModifyWorkflowPropertiesCommandAttributes{}
	return s.HistoryBuilder.AddWorkflowPropertiesModifiedEvent(64, attrs)
}

func (s *sutTestingAdapter) AddSignalExternalWorkflowExecutionFailedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
		64,
		32,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		"workflow-1",
		"run-1",
		"out-of-control",
		enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
	)
}

func (s *sutTestingAdapter) AddExternalWorkflowExecutionSignaled(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddExternalWorkflowExecutionSignaled(
		64,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		"workflow-1",
		"run-1",
		"out-of-control",
	)
}

func (s *sutTestingAdapter) AddMarkerRecordedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.RecordMarkerCommandAttributes{}
	return s.HistoryBuilder.AddMarkerRecordedEvent(64, attrs)
}

func (s *sutTestingAdapter) AddWorkflowExecutionSignaledEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddWorkflowExecutionSignaledEvent(
		"signal-name",
		nil,
		"identity-1",
		nil,
		false,
		nil,
	)
}

func (s *sutTestingAdapter) AddStartChildWorkflowExecutionInitiatedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	attrs := &commandpb.StartChildWorkflowExecutionCommandAttributes{}
	return s.HistoryBuilder.AddStartChildWorkflowExecutionInitiatedEvent(64, attrs, namespace.ID("ns-target"))
}

func (s *sutTestingAdapter) AddChildWorkflowExecutionStartedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddChildWorkflowExecutionStartedEvent(
		config.initiatedId,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		nil,
		nil,
		nil,
	)
}

func (s *sutTestingAdapter) AddStartChildWorkflowExecutionFailedEvent(_ ...eventConfig) *historypb.HistoryEvent {
	return s.HistoryBuilder.AddStartChildWorkflowExecutionFailedEvent(
		64,
		32,
		enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		"workflow-1",
		nil,
		"no-control",
	)
}

func (s *sutTestingAdapter) AddChildWorkflowExecutionCompletedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddChildWorkflowExecutionCompletedEvent(
		config.initiatedId,
		config.startedId,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		nil,
		nil,
		nil,
	)
}

func (s *sutTestingAdapter) AddChildWorkflowExecutionFailedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddChildWorkflowExecutionFailedEvent(
		config.initiatedId,
		config.startedId,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		nil,
		nil,
		nil,
		enumspb.RETRY_STATE_UNSPECIFIED,
	)
}

func (s *sutTestingAdapter) AddChildWorkflowExecutionCanceledEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddChildWorkflowExecutionCanceledEvent(
		config.initiatedId,
		config.startedId,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		nil,
		nil,
		nil,
	)
}

func (s *sutTestingAdapter) AddChildWorkflowExecutionTerminatedEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddChildWorkflowExecutionTerminatedEvent(
		config.initiatedId,
		config.startedId,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		nil,
		nil,
	)
}

func (s *sutTestingAdapter) AddChildWorkflowExecutionTimedOutEvent(optionalConfig ...eventConfig) *historypb.HistoryEvent {
	config := getConfigOrDefault(optionalConfig)
	return s.HistoryBuilder.AddChildWorkflowExecutionTimedOutEvent(
		config.initiatedId,
		config.initiatedId,
		namespace.Name("target"),
		namespace.ID("ns-target"),
		nil,
		nil,
		enumspb.RETRY_STATE_UNSPECIFIED,
	)
}

func getConfigOrDefault(config []eventConfig) eventConfig {
	if len(config) > 1 {
		panic("only one config can be provided")
	}
	cfg := eventConfig{scheduledId: 64, startedId: 32}
	if len(config) == 1 {
		if config[0].scheduledId != 0 {
			cfg.scheduledId = config[0].scheduledId
		}
		if config[0].startedId != 0 {
			cfg.startedId = config[0].startedId
		}
		if config[0].initiatedId != 0 {
			cfg.initiatedId = config[0].initiatedId
		}
	}
	return cfg
}
