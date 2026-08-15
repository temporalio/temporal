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

func TestEvidenceOrderingRejectsClockSkewAndAcceptsCausality(t *testing.T) {
	skewed := Trace{Complete: true, Events: []TraceEvent{
		{Key: "telemetry", Kind: TraceFact, Name: "Started", Source: TelemetryEvidence, ClockDomain: "telemetry", SourceSequence: 20, Fields: map[string]string{"timestamp": "later"}},
		{Key: "history", Kind: TraceFact, Name: "Completed", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 1, Fields: map[string]string{"timestamp": "earlier"}},
	}}
	spec := TraceRefinement{Required: []TracePattern{{Kind: TraceFact, Name: "Started"}, {Kind: TraceFact, Name: "Completed"}}, AllowExtras: true}

	err := CompareTraceRefinementWithEvidence(spec, skewed, InProcessProfile())
	require.ErrorContains(t, err, ErrTraceOrder.Error())

	skewed.Events[1].Causes = []string{"telemetry"}
	require.NoError(t, CompareTraceRefinementWithEvidence(spec, skewed, InProcessProfile()))
}

func TestEvidenceRefinementRejectsIncompleteOrUndeclaredEvidence(t *testing.T) {
	spec := TraceRefinement{Required: []TracePattern{{Kind: TraceFact, Name: "Started"}}}
	trace := Trace{Events: []TraceEvent{{Key: "started", Kind: TraceFact, Name: "Started", Source: HistoryEvidence}}}

	require.ErrorContains(t, CompareTraceRefinementWithEvidence(spec, trace, HistoryProfile()), "incomplete")
	trace.Complete = true
	trace.Events[0].Source = TelemetryEvidence
	require.ErrorContains(t, CompareTraceRefinementWithEvidence(spec, trace, HistoryProfile()), "unavailable")
}

func TestEvidenceOrderingUsesOnlySequencesInsideOneClockDomain(t *testing.T) {
	trace := Trace{Complete: true, Events: []TraceEvent{
		{Key: "one", Kind: TraceFact, Name: "One", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 4},
		{Key: "two", Kind: TraceFact, Name: "Two", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 5},
	}}

	ordered, err := TraceOrderedBefore(trace, HistoryProfile(), "one", "two")
	require.NoError(t, err)
	require.True(t, ordered)
}

func TestEvidenceOrderingRejectsConflictingCausalAndSequenceEvidence(t *testing.T) {
	trace := Trace{Complete: true, Events: []TraceEvent{
		{Key: "one", Kind: TraceFact, Name: "One", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 5},
		{Key: "two", Kind: TraceFact, Name: "Two", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 4, Causes: []string{"one"}},
	}}

	_, err := TraceOrderedBefore(trace, HistoryProfile(), "one", "two")
	require.ErrorContains(t, err, "causal references conflict")
}

func TestEvidenceOrderingRejectsReverseAndDanglingCausalEvidence(t *testing.T) {
	profile := HistoryProfile()
	reverse := Trace{Complete: true, Events: []TraceEvent{
		{Key: "two", Kind: TraceFact, Name: "Two", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 5},
		{Key: "one", Kind: TraceFact, Name: "One", Source: HistoryEvidence, ClockDomain: "history", SourceSequence: 4, Causes: []string{"two"}},
	}}

	_, err := TraceOrderedBefore(reverse, profile, "one", "two")
	require.ErrorContains(t, err, "causal references conflict")

	dangling := Trace{Complete: true, Events: []TraceEvent{
		{Key: "one", Kind: TraceFact, Name: "One", Source: HistoryEvidence, Causes: []string{"missing"}},
	}}
	_, err = TraceOrderedBefore(dangling, profile, "one", "missing")
	require.ErrorContains(t, err, "cause")
}

func TestCompareCausalFootprintSelectsActionWindow(t *testing.T) {
	actual := Trace{Events: []TraceEvent{
		{Key: "before", Kind: TraceFact, Name: "Before"},
		{Key: "start", Kind: TraceAction, Name: "complete", Fields: map[string]string{"outcome": ExecutionOutcomeStarted}},
		{Key: "scheduled", Kind: TraceFact, Name: "Scheduled", Causes: []string{"start"}},
		{Key: "extra", Kind: TraceTransition, Name: "Started", Causes: []string{"start"}},
		{Key: "terminal", Kind: TraceFact, Name: "Terminal", Causes: []string{"start"}},
		{Key: "finish", Kind: TraceAction, Name: "complete", Causes: []string{"start"}, Fields: map[string]string{"outcome": ExecutionOutcomeSucceeded}},
	}}
	spec := CausalFootprint{
		Action: "complete",
		Refinement: TraceRefinement{
			Required:    []TracePattern{{Kind: TraceFact, Name: "Scheduled"}, {Kind: TraceFact, Name: "Terminal"}},
			Forbidden:   []TracePattern{{Kind: TraceVerdict, Name: "failure"}},
			AllowExtras: true,
		},
		Causal: []TracePattern{{Kind: TraceFact, Name: "Scheduled"}, {Kind: TraceFact, Name: "Terminal"}},
	}
	require.NoError(t, CompareCausalFootprint(spec, actual))
}

func TestCompareCausalFootprintReportsStableMismatches(t *testing.T) {
	base := CausalFootprint{
		Action: "complete",
		Refinement: TraceRefinement{
			Required:    []TracePattern{{Kind: TraceFact, Name: "Terminal"}},
			AllowExtras: true,
		},
		Causal: []TracePattern{{Kind: TraceFact, Name: "Terminal"}},
	}
	tests := []struct {
		name   string
		trace  Trace
		reason string
	}{
		{name: "missing start", trace: Trace{}, reason: "window start is missing"},
		{name: "missing finish", trace: Trace{Events: []TraceEvent{{Key: "start", Kind: TraceAction, Name: "complete", Fields: map[string]string{"outcome": ExecutionOutcomeStarted}}}}, reason: "window finish is missing"},
		{name: "causal disconnect", trace: Trace{Events: []TraceEvent{
			{Key: "start", Kind: TraceAction, Name: "complete", Fields: map[string]string{"outcome": ExecutionOutcomeStarted}},
			{Key: "terminal", Kind: TraceFact, Name: "Terminal"},
			{Key: "finish", Kind: TraceAction, Name: "complete", Causes: []string{"start"}, Fields: map[string]string{"outcome": ExecutionOutcomeSucceeded}},
		}}, reason: "causally disconnected"},
		{name: "duplicate semantic event", trace: Trace{Events: []TraceEvent{
			{Key: "start", Kind: TraceAction, Name: "complete", Fields: map[string]string{"outcome": ExecutionOutcomeStarted}},
			{Key: "terminal-1", Kind: TraceFact, Name: "Terminal", Causes: []string{"start"}},
			{Key: "terminal-2", Kind: TraceFact, Name: "Terminal", Causes: []string{"start"}},
			{Key: "finish", Kind: TraceAction, Name: "complete", Causes: []string{"start"}, Fields: map[string]string{"outcome": ExecutionOutcomeSucceeded}},
		}}, reason: "duplicate semantic observation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := CompareCausalFootprint(base, test.trace)
			require.ErrorContains(t, err, test.reason)
			var mismatch *TraceMismatch
			require.ErrorAs(t, err, &mismatch)
		})
	}
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
