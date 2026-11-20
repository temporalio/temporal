package internal

import (
	"slices"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BufferedStartState is the lifecycle state encoded by a BufferedStart's
// Attempt, RunId, and Completed fields.
type BufferedStartState int

const (
	BufferedStartInvalid BufferedStartState = iota
	// BufferedStartUnprocessed awaits its first ProcessBuffer pass (Attempt 0).
	BufferedStartUnprocessed
	// BufferedStartDeferred waits for an overlapping action to change state
	// (Attempt -1).
	BufferedStartDeferred
	// BufferedStartSelected was selected for execution and has no recorded run
	// yet. It may be backing off before its next attempt.
	BufferedStartSelected
	// BufferedStartRunning has a recorded run and no completion.
	BufferedStartRunning
	// BufferedStartCompleted has a recorded run and its completion.
	BufferedStartCompleted
	// BufferedStartCompletedUnrecorded received its completion before the
	// Execute task recorded its run. The run is recorded later.
	BufferedStartCompletedUnrecorded
)

func (s BufferedStartState) String() string {
	switch s {
	case BufferedStartUnprocessed:
		return "Unprocessed"
	case BufferedStartDeferred:
		return "Deferred"
	case BufferedStartSelected:
		return "Selected"
	case BufferedStartRunning:
		return "Running"
	case BufferedStartCompleted:
		return "Completed"
	case BufferedStartCompletedUnrecorded:
		return "CompletedUnrecorded"
	default:
		return "Invalid"
	}
}

// RunRecorded reports whether the Execute task has recorded the start's run.
func (s BufferedStartState) RunRecorded() bool {
	return s == BufferedStartRunning || s == BufferedStartCompleted
}

// ClassifyBufferedStart returns the lifecycle state of start. RunId and
// Completed decide the post-execution states, and Attempt decides the rest.
func ClassifyBufferedStart(start *schedulespb.BufferedStart) BufferedStartState {
	hasRun := start.GetRunId() != ""
	hasCompletion := start.GetCompleted() != nil
	switch attempt := start.GetAttempt(); {
	case hasRun && hasCompletion:
		return BufferedStartCompleted
	case hasRun:
		return BufferedStartRunning
	case hasCompletion:
		return BufferedStartCompletedUnrecorded
	case attempt == -1:
		return BufferedStartDeferred
	case attempt == 0:
		return BufferedStartUnprocessed
	case attempt > 0:
		return BufferedStartSelected
	default:
		return BufferedStartInvalid
	}
}

// MarkStartDeferred holds an unprocessed start until an overlapping action
// changes state.
func MarkStartDeferred(logger log.Logger, start *schedulespb.BufferedStart) {
	assertTransition(logger, start, BufferedStartDeferred, BufferedStartUnprocessed)
	start.Attempt = -1
}

// MarkStartUnprocessed returns a deferred start to ProcessBuffer.
func MarkStartUnprocessed(logger log.Logger, start *schedulespb.BufferedStart) {
	assertTransition(logger, start, BufferedStartUnprocessed, BufferedStartDeferred)
	start.Attempt = 0
}

// MarkStartSelected marks a start for its first execution attempt.
func MarkStartSelected(logger log.Logger, start *schedulespb.BufferedStart) {
	assertTransition(logger, start, BufferedStartSelected, BufferedStartUnprocessed, BufferedStartDeferred)
	start.Attempt = 1
}

// MarkStartRetrying advances a start that failed with a retryable error to its
// next attempt. Its completion may already have arrived if an earlier attempt
// started the run but its response was lost.
func MarkStartRetrying(logger log.Logger, start *schedulespb.BufferedStart, backoffTime *timestamppb.Timestamp) {
	assertTransition(logger, start, BufferedStartSelected, BufferedStartSelected, BufferedStartCompletedUnrecorded)
	start.Attempt++
	start.BackoffTime = backoffTime
}

// MarkStartRunning records the run created for a start.
func MarkStartRunning(
	logger log.Logger,
	start *schedulespb.BufferedStart,
	runID string,
	startTime *timestamppb.Timestamp,
) {
	assertTransition(logger, start, BufferedStartRunning, BufferedStartSelected, BufferedStartCompletedUnrecorded)
	start.RunId = runID
	start.StartTime = startTime
}

// MarkStartCompleted records a start's completion. The completion can arrive
// before the Execute task records the run.
func MarkStartCompleted(logger log.Logger, start *schedulespb.BufferedStart, result *schedulespb.CompletedResult) {
	assertTransition(logger, start, BufferedStartCompleted, BufferedStartRunning, BufferedStartSelected)
	start.Completed = result
}

func assertTransition(
	logger log.Logger,
	start *schedulespb.BufferedStart,
	to BufferedStartState,
	from ...BufferedStartState,
) {
	state := ClassifyBufferedStart(start)
	softassert.That(logger, slices.Contains(from, state), "invalid buffered start transition",
		tag.RequestID(start.GetRequestId()),
		tag.NewStringerTag("from", state),
		tag.NewStringerTag("to", to),
	)
}
