package internal

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// errorCountingLogger counts Error calls, which is how softassert reports a
// failed assertion.
type errorCountingLogger struct {
	log.Logger
	errors int
}

func (l *errorCountingLogger) Error(string, ...tag.Tag) { l.errors++ }

func newBufferedStart(state BufferedStartState) *schedulespb.BufferedStart {
	completed := &schedulespb.CompletedResult{}
	switch state {
	case BufferedStartUnprocessed:
		return &schedulespb.BufferedStart{}
	case BufferedStartDeferred:
		return &schedulespb.BufferedStart{Attempt: -1}
	case BufferedStartSelected:
		return &schedulespb.BufferedStart{Attempt: 1}
	case BufferedStartRunning:
		return &schedulespb.BufferedStart{Attempt: 1, RunId: "run"}
	case BufferedStartCompleted:
		return &schedulespb.BufferedStart{Attempt: 1, RunId: "run", Completed: completed}
	case BufferedStartCompletedUnrecorded:
		return &schedulespb.BufferedStart{Attempt: 1, Completed: completed}
	default:
		return &schedulespb.BufferedStart{Attempt: -2}
	}
}

var allBufferedStartStates = []BufferedStartState{
	BufferedStartInvalid,
	BufferedStartUnprocessed,
	BufferedStartDeferred,
	BufferedStartSelected,
	BufferedStartRunning,
	BufferedStartCompleted,
	BufferedStartCompletedUnrecorded,
}

func TestClassifyBufferedStart(t *testing.T) {
	for _, state := range allBufferedStartStates {
		t.Run(state.String(), func(t *testing.T) {
			require.Equal(t, state, ClassifyBufferedStart(newBufferedStart(state)))
		})
	}

	t.Run("retrying start stays selected", func(t *testing.T) {
		start := &schedulespb.BufferedStart{Attempt: 3, BackoffTime: timestamppb.Now()}
		require.Equal(t, BufferedStartSelected, ClassifyBufferedStart(start))
	})

	t.Run("recorded run takes precedence over attempt", func(t *testing.T) {
		start := &schedulespb.BufferedStart{RunId: "run"}
		require.Equal(t, BufferedStartRunning, ClassifyBufferedStart(start))
	})
}

func TestBufferedStartTransitions(t *testing.T) {
	transitions := []struct {
		name  string
		apply func(log.Logger, *schedulespb.BufferedStart)
		from  []BufferedStartState
	}{
		{
			name:  "deferred",
			apply: MarkStartDeferred,
			from:  []BufferedStartState{BufferedStartUnprocessed},
		},
		{
			name:  "unprocessed",
			apply: MarkStartUnprocessed,
			from:  []BufferedStartState{BufferedStartDeferred},
		},
		{
			name:  "selected",
			apply: MarkStartSelected,
			from:  []BufferedStartState{BufferedStartUnprocessed, BufferedStartDeferred},
		},
		{
			name: "retrying",
			apply: func(logger log.Logger, start *schedulespb.BufferedStart) {
				MarkStartRetrying(logger, start, timestamppb.Now())
			},
			from: []BufferedStartState{BufferedStartSelected, BufferedStartCompletedUnrecorded},
		},
		{
			name: "running",
			apply: func(logger log.Logger, start *schedulespb.BufferedStart) {
				MarkStartRunning(logger, start, "run", timestamppb.Now())
			},
			from: []BufferedStartState{BufferedStartSelected, BufferedStartCompletedUnrecorded},
		},
		{
			name: "completed",
			apply: func(logger log.Logger, start *schedulespb.BufferedStart) {
				MarkStartCompleted(logger, start, &schedulespb.CompletedResult{})
			},
			from: []BufferedStartState{BufferedStartRunning, BufferedStartSelected},
		},
	}

	for _, tt := range transitions {
		for _, from := range allBufferedStartStates {
			t.Run(tt.name+"/from "+from.String(), func(t *testing.T) {
				logger := &errorCountingLogger{Logger: log.NewNoopLogger()}
				tt.apply(logger, newBufferedStart(from))

				wantErrors := 1
				if slices.Contains(tt.from, from) {
					wantErrors = 0
				}
				require.Equal(t, wantErrors, logger.errors)
			})
		}
	}
}

func TestBufferedStartTransitionTargets(t *testing.T) {
	logger := &errorCountingLogger{Logger: log.NewNoopLogger()}
	completed := &schedulespb.CompletedResult{}

	start := newBufferedStart(BufferedStartUnprocessed)
	MarkStartDeferred(logger, start)
	require.Equal(t, BufferedStartDeferred, ClassifyBufferedStart(start))
	MarkStartUnprocessed(logger, start)
	require.Equal(t, BufferedStartUnprocessed, ClassifyBufferedStart(start))
	MarkStartSelected(logger, start)
	require.Equal(t, BufferedStartSelected, ClassifyBufferedStart(start))

	backoff := timestamppb.Now()
	MarkStartRetrying(logger, start, backoff)
	require.Equal(t, int64(2), start.GetAttempt())
	require.Equal(t, backoff, start.GetBackoffTime())

	MarkStartCompleted(logger, start, completed)
	require.Equal(t, BufferedStartCompletedUnrecorded, ClassifyBufferedStart(start))
	MarkStartRunning(logger, start, "run", timestamppb.Now())
	require.Equal(t, BufferedStartCompleted, ClassifyBufferedStart(start))

	require.Zero(t, logger.errors)
}
