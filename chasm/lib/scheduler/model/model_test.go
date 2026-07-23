package model_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/scheduler/model"
)

var testStart = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestTransitionLifecycle(t *testing.T) {
	t.Parallel()
	config := testConfig()
	state := model.State{}

	created := transition(t, config, state, model.Create{Paused: true, Notes: "initially paused"})
	require.Equal(t, model.LifecycleRunning, created.State.Lifecycle)
	require.True(t, created.State.Paused)
	require.Equal(t, int64(1), created.State.ConflictToken)
	require.Equal(t, []model.Effect{model.ScheduleWakeup{At: testStart.Add(time.Minute)}}, created.Effects)

	described := transition(t, config, created.State, model.Describe{})
	require.Equal(t, model.Observe(created.State), described.Response.(model.DescribeResponse).Observable)

	triggered := transition(t, config, created.State, model.Trigger{ID: "manual-1"})
	require.Len(t, triggered.State.Pending, 1)
	require.Equal(t, model.StartWorkflow{Action: triggered.State.Pending[0]}, triggered.Effects[0])

	unpaused := transition(t, config, triggered.State, model.Unpause{Note: "resume"})
	require.False(t, unpaused.State.Paused)
	require.Equal(t, "resume", unpaused.State.Notes)
	require.Equal(t, model.ScheduleWakeup{At: testStart.Add(time.Minute)}, unpaused.Effects[0])

	started := transition(t, config, unpaused.State, model.StartSucceeded{ActionID: "manual-1", RunID: "run-1"})
	require.Empty(t, started.State.Pending)
	require.Equal(t, "run-1", started.State.Running[0].RunID)
	require.Equal(t, int64(1), started.State.ActionCount)

	completed := transition(t, config, started.State, model.CompleteWorkflow{ActionID: "manual-1"})
	require.Empty(t, completed.State.Running)
	require.Len(t, completed.State.Recent, 1)

	deleted := transition(t, config, completed.State, model.Delete{})
	require.Equal(t, model.LifecycleClosed, deleted.State.Lifecycle)
	require.Equal(t, []model.Effect{model.CloseSchedule{}}, deleted.Effects)
}

func TestTransitionAdvanceAndCatchup(t *testing.T) {
	t.Parallel()
	config := testConfig()
	created := transition(t, config, model.State{}, model.Create{})

	advanced := transition(t, config, created.State, model.AdvanceTime{Time: testStart.Add(3 * time.Minute)})
	require.Equal(t, testStart.Add(3*time.Minute), advanced.State.HighWatermark)
	require.Len(t, advanced.State.Pending, 3)
	require.Len(t, advanced.Effects, 4)
	require.Equal(t, model.ScheduleWakeup{At: testStart.Add(4 * time.Minute)}, advanced.Effects[3])

	config.CatchupWindow = time.Minute
	advanced = transition(t, config, created.State, model.AdvanceTime{Time: testStart.Add(3 * time.Minute)})
	require.Len(t, advanced.State.Pending, 2)
	require.Equal(t, testStart.Add(2*time.Minute), advanced.State.Pending[0].NominalTime)
	require.Equal(t, testStart.Add(3*time.Minute), advanced.State.Pending[1].NominalTime)
}

func TestTransitionPausedAdvanceSkipsElapsedActions(t *testing.T) {
	t.Parallel()
	config := testConfig()
	created := transition(t, config, model.State{}, model.Create{Paused: true})

	advanced := transition(t, config, created.State, model.AdvanceTime{Time: testStart.Add(3 * time.Minute)})
	require.Empty(t, advanced.State.Pending)
	require.Equal(t, testStart.Add(3*time.Minute), advanced.State.HighWatermark)
	require.Equal(t, []model.Effect{
		model.ScheduleWakeup{At: testStart.Add(4 * time.Minute)},
	}, advanced.Effects)

	unpaused := transition(t, config, advanced.State, model.Unpause{})
	require.Empty(t, unpaused.State.Pending)
	require.Equal(t, []model.Effect{
		model.ScheduleWakeup{At: testStart.Add(4 * time.Minute)},
	}, unpaused.Effects)
}

func TestTransitionErrorsPreserveInput(t *testing.T) {
	t.Parallel()
	config := testConfig()
	running := transition(t, config, model.State{}, model.Create{}).State
	closed := transition(t, config, running, model.Delete{}).State

	tests := []struct {
		name  string
		state model.State
		event model.Event
		err   error
	}{
		{name: "describe absent", event: model.Describe{}, err: model.ErrNotCreated},
		{name: "create twice", state: running, event: model.Create{}, err: model.ErrAlreadyCreated},
		{name: "mutate closed", state: closed, event: model.Pause{}, err: model.ErrClosed},
		{name: "backwards time", state: running, event: model.AdvanceTime{Time: testStart.Add(-time.Second)}, err: model.ErrInvalidEvent},
		{name: "empty trigger", state: running, event: model.Trigger{}, err: model.ErrInvalidEvent},
		{name: "unknown start", state: running, event: model.StartSucceeded{ActionID: "missing", RunID: "run"}, err: model.ErrInvalidEvent},
		{name: "unknown completion", state: running, event: model.CompleteWorkflow{ActionID: "missing"}, err: model.ErrInvalidEvent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := cloneForTest(test.state)
			outcome, err := model.Transition(config, test.state, test.event)
			require.ErrorIs(t, err, test.err)
			require.Equal(t, before, test.state)
			require.Equal(t, before, outcome.State)
			require.Empty(t, outcome.Effects)
		})
	}
}

func TestTransitionDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	config := testConfig()
	state := model.State{
		Now:       testStart,
		Lifecycle: model.LifecycleRunning,
		Pending:   []model.Action{{ID: "pending"}},
		Running:   []model.Action{{ID: "running", RunID: "run"}},
		Recent:    []model.Action{{ID: "recent", RunID: "old-run"}},
	}
	before := cloneForTest(state)

	_, err := model.Transition(config, state, model.CompleteWorkflow{ActionID: "running"})
	require.NoError(t, err)
	require.Equal(t, before, state)
}

func testConfig() model.Config {
	return model.Config{
		StartTime:         testStart,
		Interval:          time.Minute,
		CatchupWindow:     time.Hour,
		RecentActionLimit: 10,
		MaxGenerated:      20,
		OverlapPolicy:     enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
}

func transition(t *testing.T, config model.Config, state model.State, event model.Event) model.Outcome {
	t.Helper()
	outcome, err := model.Transition(config, state, event)
	require.NoError(t, err)
	return outcome
}

func cloneForTest(state model.State) model.State {
	state.Pending = append([]model.Action(nil), state.Pending...)
	state.Running = append([]model.Action(nil), state.Running...)
	state.Recent = append([]model.Action(nil), state.Recent...)
	return state
}

func requireModeledError(t *testing.T, err error) {
	t.Helper()
	require.True(t,
		errors.Is(err, model.ErrAlreadyCreated) ||
			errors.Is(err, model.ErrNotCreated) ||
			errors.Is(err, model.ErrClosed) ||
			errors.Is(err, model.ErrInvalidEvent),
		"unexpected error: %v", err,
	)
}
