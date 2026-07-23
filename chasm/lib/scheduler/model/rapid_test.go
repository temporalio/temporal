package model_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/scheduler/model"
	"pgregory.net/rapid"
)

func TestTransitionRapidTraces(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		config := testConfig()
		config.Interval = time.Duration(rapid.IntRange(1, 5).Draw(t, "interval-minutes")) * time.Minute
		config.RecentActionLimit = rapid.IntRange(0, 5).Draw(t, "recent-limit")
		state := transitionRapid(t, config, model.State{}, model.Create{
			Paused: rapid.Bool().Draw(t, "initially-paused"),
		}).State
		triggerID := 0

		t.Repeat(map[string]func(*rapid.T){
			"advance": func(t *rapid.T) {
				minutes := rapid.IntRange(0, 3).Draw(t, "minutes")
				state = transitionRapid(t, config, state, model.AdvanceTime{Time: state.Now.Add(time.Duration(minutes) * time.Minute)}).State
			},
			"pause": func(t *rapid.T) {
				state = transitionRapid(t, config, state, model.Pause{Note: "paused"}).State
			},
			"unpause": func(t *rapid.T) {
				state = transitionRapid(t, config, state, model.Unpause{Note: "running"}).State
			},
			"trigger": func(t *rapid.T) {
				triggerID++
				state = transitionRapid(t, config, state, model.Trigger{ID: fmt.Sprintf("manual-%d", triggerID)}).State
			},
			"start": func(t *rapid.T) {
				if len(state.Pending) == 0 {
					t.Skip("no pending action")
				}
				action := rapid.SampledFrom(state.Pending).Draw(t, "pending-action")
				state = transitionRapid(t, config, state, model.StartSucceeded{ActionID: action.ID, RunID: "run-" + action.ID}).State
			},
			"complete": func(t *rapid.T) {
				if len(state.Running) == 0 {
					t.Skip("no running action")
				}
				action := rapid.SampledFrom(state.Running).Draw(t, "running-action")
				state = transitionRapid(t, config, state, model.CompleteWorkflow{ActionID: action.ID}).State
			},
			"describe": func(t *rapid.T) {
				outcome := transitionRapid(t, config, state, model.Describe{})
				require.Equal(t, model.Observe(state), outcome.Response.(model.DescribeResponse).Observable)
			},
			"": func(t *rapid.T) {
				requireStateInvariants(t, config, state)
			},
		})
	})
}

func transitionRapid(t *rapid.T, config model.Config, state model.State, event model.Event) model.Outcome {
	t.Helper()
	outcome, err := model.Transition(config, state, event)
	require.NoError(t, err)
	return outcome
}
