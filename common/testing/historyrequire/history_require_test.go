package historyrequire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/testvars"
)

// sampleCompactHistory is used in almost every test.
func sampleCompactHistory(t *testing.T) ([]*historypb.HistoryEvent, []map[string]any) {
	hr := New(t)
	hes, attrs := hr.parseHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskFailed
  5 v1 WorkflowTaskScheduled
  6 v1 WorkflowTaskStarted
  7 v1 WorkflowTaskCompleted
  8 v1 ActivityTaskScheduled
  9 v1 ActivityTaskStarted
 10 v1 ActivityTaskCompleted
 11 v1 WorkflowTaskScheduled
 12 v1 WorkflowTaskStarted
 13 v1 WorkflowTaskCompleted`)

	require.Len(t, hes, 13)
	require.Len(t, attrs, 13)
	for _, attr := range attrs {
		require.Nil(t, attr)
	}
	return hes, attrs
}

// Sanity check for history parser.
func TestSampleCompactHistory(t *testing.T) {
	sampleCompactHistory(t)
}

// Use this test to generate new sample history.
func TestPrintHistoryEvents(t *testing.T) {
	tv := testvars.New(t)

	generator := test.InitializeHistoryEventGenerator(tv.NamespaceName(), tv.NamespaceID(), 0)

	var historyEvents []*historypb.HistoryEvent
	for generator.HasNextVertex() {
		events := generator.GetNextVertices()
		for _, event := range events {
			historyEvent := event.GetData().(*historypb.HistoryEvent)
			historyEvents = append(historyEvents, historyEvent)
		}
	}

	hr := New(t)
	hr.PrintHistoryEvents(historyEvents)
	hr.PrintHistoryEventsCompact(historyEvents)
}

func TestEqualHistoryEventsWithVersion(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	hr.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskFailed
  5 v1 WorkflowTaskScheduled
  6 v1 WorkflowTaskStarted
  7 v1 WorkflowTaskCompleted
  8 v1 ActivityTaskScheduled
  9 v1 ActivityTaskStarted
 10 v1 ActivityTaskCompleted
 11 v1 WorkflowTaskScheduled
 12 v1 WorkflowTaskStarted
 13 v1 WorkflowTaskCompleted`, historyEvents)
}

func TestEqualHistoryEventsWithoutVersion(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	for _, event := range historyEvents {
		event.Version = 0
	}

	hr.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 ActivityTaskScheduled
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted`, historyEvents)
}

func TestEqualHistoryEventsWithoutEventID(t *testing.T) {
	hr := New(t)

	historyEvents, _ := sampleCompactHistory(t)

	for _, event := range historyEvents {
		event.EventId = 0
		event.Version = 0
	}

	hr.EqualHistoryEvents(`
WorkflowExecutionStarted
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskFailed
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskCompleted
ActivityTaskScheduled
ActivityTaskStarted
ActivityTaskCompleted
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskCompleted`, historyEvents)
}

func TestEqualHistoryEventsSuffix(t *testing.T) {
	hr := New(t)

	historyEvents, _ := sampleCompactHistory(t)

	for _, event := range historyEvents {
		event.EventId = 0
		event.Version = 0
	}

	hr.EqualHistoryEventsSuffix(`
ActivityTaskCompleted
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskCompleted`, historyEvents)
}

func TestEqualHistoryEventsPrefix(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	for _, event := range historyEvents {
		event.EventId = 0
		event.Version = 0
	}

	hr.EqualHistoryEventsPrefix(`
WorkflowExecutionStarted
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskFailed
WorkflowTaskScheduled`, historyEvents)
}

func TestParsePartialHistoryEvents(t *testing.T) {
	hr := New(t)

	historyEvents, attrs := hr.parseHistory(`
  7 WorkflowTaskCompleted
  8 ActivityTaskScheduled
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted`)

	require.Len(t, historyEvents, 7)
	require.Len(t, attrs, 7)

	for i, event := range historyEvents {
		require.Equal(t, int64(i+7), event.EventId)
	}

	hr.EqualHistoryEvents(`
  7 WorkflowTaskCompleted
  8 ActivityTaskScheduled
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted`, historyEvents)
}

func TestContainsHistoryEventsWithoutEventID(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	historyEvents[3].Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
		WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			Identity: "Was wollen wir trinken",
		},
	}

	hr.ContainsHistoryEvents(`
WorkflowTaskFailed {"Identity": "Was wollen wir trinken"}
WorkflowTaskScheduled
WorkflowTaskStarted
`, historyEvents)
}

func TestContainsHistoryEventsWithEventID(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	historyEvents[4].Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			Attempt: 2208,
		},
	}

	hr.ContainsHistoryEvents(`
4 WorkflowTaskFailed
5 WorkflowTaskScheduled {"Attempt": 2208}
6 WorkflowTaskStarted
`, historyEvents)
}

func TestWaitForHistoryEvents(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	attempt := 0
	hr.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 ActivityTaskScheduled
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted`, func() []*historypb.HistoryEvent {
		if attempt < 13 {
			attempt++
		}
		t.Logf("Attempt %d", attempt)
		return historyEvents[:attempt]
	}, 1*time.Second, 10*time.Millisecond)
}

func TestWaitForHistoryEventsSuffix(t *testing.T) {
	hr := New(t)
	historyEvents, _ := sampleCompactHistory(t)

	attempt := 0
	hr.WaitForHistoryEventsSuffix(`
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted`, func() []*historypb.HistoryEvent {
		if attempt < 13 {
			attempt++
		}
		t.Logf("Attempt %d", attempt)
		return historyEvents[:attempt]
	}, 1*time.Second, 10*time.Millisecond)
}

func TestEmptyHistoryEvents(t *testing.T) {
	hr := New(t)
	emptyHistoryEvents := make([]*historypb.HistoryEvent, 0)

	hr.EqualHistoryEvents(``, emptyHistoryEvents)
	hr.EqualHistoryEvents(``, nil)
	hr.EqualHistoryEvents(`
`, emptyHistoryEvents)
	hr.EqualHistoryEvents(`
`, nil)

	hr.EqualHistoryEventsPrefix(``, emptyHistoryEvents)
	hr.EqualHistoryEventsPrefix(``, nil)
	hr.EqualHistoryEventsPrefix(`
`, emptyHistoryEvents)
	hr.EqualHistoryEventsPrefix(`
`, nil)

	hr.EqualHistoryEventsSuffix(``, emptyHistoryEvents)
	hr.EqualHistoryEventsSuffix(``, nil)
	hr.EqualHistoryEventsSuffix(`
`, emptyHistoryEvents)
	hr.EqualHistoryEventsSuffix(`
`, nil)

	hr.ContainsHistoryEvents(``, emptyHistoryEvents)
	hr.ContainsHistoryEvents(``, nil)
	hr.ContainsHistoryEvents(`
`, emptyHistoryEvents)
	hr.ContainsHistoryEvents(`
`, nil)
}
