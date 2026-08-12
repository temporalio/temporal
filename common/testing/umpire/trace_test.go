package umpire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceRecorderNormalizesRedactsAndDefensivelySnapshots(t *testing.T) {
	recorder := NewTraceRecorder(TraceOptions{MaxEvents: 10, MaxBytes: 4096})
	require.NoError(t, recorder.Record(TraceEvent{
		Key:    "2",
		Kind:   TraceFact,
		Name:   "WorkflowStarted",
		Causes: []string{"1", "0", "1"},
		Fields: map[string]string{
			"payload":       "secret payload",
			"authorization": "secret token",
			"workflow":      "workflow-id",
		},
	}))

	trace := recorder.Snapshot()
	require.Equal(t, []string{"0", "1"}, trace.Events[0].Causes)
	require.Equal(t, TraceRedacted, trace.Events[0].Fields["payload"])
	require.Equal(t, TraceRedacted, trace.Events[0].Fields["authorization"])
	require.Equal(t, "workflow-id", trace.Events[0].Fields["workflow"])
	trace.Events[0].Fields["workflow"] = "changed"
	require.Equal(t, "workflow-id", recorder.Snapshot().Events[0].Fields["workflow"])
}

func TestTraceRecorderRejectsInvalidDuplicateAndOverLimitEventsAtomically(t *testing.T) {
	recorder := NewTraceRecorder(TraceOptions{MaxEvents: 1, MaxBytes: 4096})
	require.ErrorIs(t, recorder.Record(TraceEvent{Kind: TraceFact, Name: "missing-key"}), ErrTraceEvent)
	require.NoError(t, recorder.Record(TraceEvent{Key: "one", Kind: TraceFact, Name: "first"}))
	require.ErrorIs(t, recorder.Record(TraceEvent{Key: "one", Kind: TraceFact, Name: "duplicate"}), ErrTraceEvent)
	require.ErrorIs(t, recorder.Record(TraceEvent{Key: "two", Kind: TraceAction, Name: "second"}), ErrTraceLimit)
	require.Len(t, recorder.Snapshot().Events, 1)

	byteLimited := NewTraceRecorder(TraceOptions{MaxEvents: 10, MaxBytes: 32})
	require.ErrorIs(t, byteLimited.Record(TraceEvent{Key: "large", Kind: TraceFact, Name: "event-with-a-long-name"}), ErrTraceLimit)
	require.Empty(t, byteLimited.Snapshot().Events)
}

func TestCompareTraceRefinementAcceptsExtrasAndChecksOrdering(t *testing.T) {
	actual := Trace{Events: []TraceEvent{
		{Key: "start", Kind: TraceFact, Name: "WorkflowStarted"},
		{Key: "extra", Kind: TraceTransition, Name: "WorkflowTaskScheduled", Causes: []string{"start"}},
		{Key: "complete", Kind: TraceFact, Name: "WorkflowCompleted", Causes: []string{"start"}},
	}}
	spec := TraceRefinement{
		Required: []TracePattern{
			{Kind: TraceFact, Name: "WorkflowStarted"},
			{Kind: TraceFact, Name: "WorkflowCompleted"},
		},
		Forbidden:   []TracePattern{{Kind: TraceVerdict, Name: "violation"}},
		AllowExtras: true,
	}

	require.NoError(t, CompareTraceRefinement(spec, actual))

	err := CompareTraceRefinement(TraceRefinement{Required: []TracePattern{
		{Kind: TraceFact, Name: "WorkflowCompleted"},
		{Kind: TraceFact, Name: "WorkflowStarted"},
	}, AllowExtras: true}, actual)
	require.ErrorContains(t, err, "missing or misordered")
	var mismatch *TraceMismatch
	require.ErrorAs(t, err, &mismatch)
}

func TestCompareTraceRefinementRejectsForbiddenAndInvalidCausality(t *testing.T) {
	actual := Trace{Events: []TraceEvent{
		{Key: "violation", Kind: TraceVerdict, Name: "violation"},
		{Key: "start", Kind: TraceFact, Name: "started", Causes: []string{"future"}},
		{Key: "future", Kind: TraceFact, Name: "future"},
	}}

	err := CompareTraceRefinement(TraceRefinement{Forbidden: []TracePattern{{Kind: TraceVerdict, Name: "violation"}}, AllowExtras: true}, actual)
	require.ErrorContains(t, err, "forbidden")

	err = CompareTraceRefinement(TraceRefinement{AllowExtras: true}, Trace{Events: actual.Events[1:]})
	require.ErrorContains(t, err, "cause")
}

func TestWriteTraceFileAtomicallyPersistsCompleteTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traces", "trace.json")
	trace := Trace{Complete: true, Events: []TraceEvent{{Key: "one", Kind: TraceFact, Name: "started"}}}

	require.NoError(t, WriteTraceFile(path, trace))
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var got Trace
	require.NoError(t, json.Unmarshal(payload, &got))
	require.Equal(t, trace, got)
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
