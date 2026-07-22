package model_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/scheduler/model"
)

func TestTransitionBoundedExplorer(t *testing.T) {
	config := testConfig()
	type node struct {
		state model.State
		path  []model.Event
	}
	frontier := []node{{}}
	seen := make(map[string]struct{})

	for depth := 0; depth < 5; depth++ {
		next := make([]node, 0)
		for _, current := range frontier {
			for _, event := range explorerEvents(current.state) {
				before := cloneForTest(current.state)
				first, err := model.Transition(config, current.state, event)
				second, secondErr := model.Transition(config, current.state, event)
				require.Equal(t, errString(err), errString(secondErr), "path=%v event=%T", current.path, event)
				require.Equal(t, first, second, "path=%v event=%T", current.path, event)
				require.Equal(t, before, current.state, "path=%v event=%T", current.path, event)
				if err != nil {
					requireModeledError(t, err)
					continue
				}
				requireStateInvariants(t, config, first.State)
				path := append(append([]model.Event(nil), current.path...), event)
				replayed := replay(t, config, path)
				require.Equal(t, first.State, replayed)
				key := fmt.Sprintf("%+v", first.State)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				next = append(next, node{state: first.State, path: path})
			}
		}
		frontier = next
	}

	require.Greater(t, len(seen), 20)
}

func explorerEvents(state model.State) []model.Event {
	events := []model.Event{
		model.Create{},
		model.Describe{},
		model.Pause{Note: "paused"},
		model.Unpause{Note: "running"},
		model.Trigger{ID: "manual"},
		model.AdvanceTime{Time: testStart.Add(time.Minute)},
		model.AdvanceTime{Time: testStart.Add(2 * time.Minute)},
		model.Delete{},
	}
	if len(state.Pending) > 0 {
		events = append(events, model.StartSucceeded{ActionID: state.Pending[0].ID, RunID: "run"})
	}
	if len(state.Running) > 0 {
		events = append(events, model.CompleteWorkflow{ActionID: state.Running[0].ID})
	}
	return events
}

func replay(t *testing.T, config model.Config, path []model.Event) model.State {
	t.Helper()
	state := model.State{}
	for _, event := range path {
		outcome, err := model.Transition(config, state, event)
		require.NoError(t, err)
		state = outcome.State
	}
	return state
}

type helperTestingT interface {
	require.TestingT
	Helper()
}

func requireStateInvariants(t helperTestingT, config model.Config, state model.State) {
	t.Helper()
	require.False(t, state.Now.Before(config.StartTime))
	require.LessOrEqual(t, len(state.Recent), config.RecentActionLimit)
	ids := make(map[string]struct{})
	for _, actions := range [][]model.Action{state.Pending, state.Running, state.Recent} {
		for _, action := range actions {
			require.NotEmpty(t, action.ID)
			_, exists := ids[action.ID]
			require.False(t, exists, "duplicate action ID %q", action.ID)
			ids[action.ID] = struct{}{}
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
