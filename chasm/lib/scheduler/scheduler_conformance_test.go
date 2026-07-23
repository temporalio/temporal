package scheduler_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/model"
	"pgregory.net/rapid"
)

const schedulerConformanceDrainLimit = 100

type schedulerConformance struct {
	env             *schedulerPropertyEnv
	state           model.State
	config          model.Config
	requestByAction map[string]string
}

func newSchedulerConformance(t *rapid.T, paused bool) *schedulerConformance {
	t.Helper()
	runner := &schedulerConformance{
		env:             newSchedulerPropertyEnv(t, paused),
		requestByAction: make(map[string]string),
		config: model.Config{
			StartTime: schedulerPropertyStartTime, Interval: defaultInterval, CatchupWindow: defaultCatchupWindow,
			RecentActionLimit: scheduler.RecentActionCount, MaxGenerated: 8,
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}
	outcome, err := model.Transition(runner.config, runner.state, model.Create{Paused: paused})
	require.NoError(t, err)
	runner.state = outcome.State
	runner.quiesce(t, outcome.Effects, 0)
	return runner
}

func (r *schedulerConformance) apply(t *rapid.T, event model.Event) {
	t.Helper()
	beforeCalls := len(r.env.services.StartCalls())
	outcome, err := model.Transition(r.config, r.state, event)
	require.NoError(t, err)
	r.state = outcome.State

	switch event := event.(type) {
	case model.Pause:
		r.env.setPaused(t, true)
	case model.Unpause:
		r.env.setPaused(t, false)
	case model.Trigger:
		r.env.trigger(t)
	case model.AdvanceTime:
		r.env.timeSource.Update(event.Time)
	case model.CompleteWorkflow:
		r.env.complete(t, r.requestByAction[event.ActionID])
	case model.Describe:
	default:
		t.Fatalf("unsupported conformance event %T", event)
	}
	r.quiesce(t, outcome.Effects, beforeCalls)
}

func (r *schedulerConformance) quiesce(t *rapid.T, effects []model.Effect, beforeCalls int) {
	t.Helper()
	r.env.drain(t, schedulerConformanceDrainLimit)
	starts := modelStartEffects(effects)
	calls := r.env.services.StartCalls()[beforeCalls:]
	require.Len(t, calls, len(starts), "model and Scheduler emitted different start counts")
	for index, start := range starts {
		call := calls[index]
		require.NoError(t, call.Err)
		r.requestByAction[start.Action.ID] = call.Request.GetRequestId()
		outcome, err := model.Transition(r.config, r.state, model.StartSucceeded{ActionID: start.Action.ID, RunID: call.Response.GetRunId()})
		require.NoError(t, err)
		r.state = outcome.State
	}
	r.requireObservation(t)
}

func (r *schedulerConformance) requireObservation(t *rapid.T) {
	t.Helper()
	description := r.env.describe(t)
	info := description.GetInfo()
	want := model.Observe(r.state)
	require.Equal(t, want.Paused, description.GetSchedule().GetState().GetPaused())
	require.Equal(t, want.Notes, description.GetSchedule().GetState().GetNotes())
	require.Equal(t, want.ConflictToken, schedulerConflictToken(description.GetConflictToken()))
	require.Equal(t, int64(want.Pending), info.GetBufferSize())
	require.Len(t, info.GetRunningWorkflows(), want.Running)
	require.Equal(t, want.ActionCount, info.GetActionCount())

	completed := 0
	for _, action := range info.GetRecentActions() {
		if action.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			completed++
		}
	}
	require.Equal(t, want.Recent, completed)
}

func modelStartEffects(effects []model.Effect) []model.StartWorkflow {
	var starts []model.StartWorkflow
	for _, effect := range effects {
		if start, ok := effect.(model.StartWorkflow); ok {
			starts = append(starts, start)
		}
	}
	return starts
}

func schedulerConflictToken(token []byte) int64 {
	if len(token) != 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(token))
}

func TestSchedulerModelConformanceProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		runner := newSchedulerConformance(t, rapid.Bool().Draw(t, "initially-paused"))
		triggerID := 0
		t.Repeat(map[string]func(*rapid.T){
			"pause": func(t *rapid.T) {
				if runner.state.Paused {
					t.Skip("already paused")
				}
				runner.apply(t, model.Pause{Note: "property pause"})
			},
			"unpause": func(t *rapid.T) {
				if !runner.state.Paused {
					t.Skip("already unpaused")
				}
				runner.apply(t, model.Unpause{Note: "property unpause"})
			},
			"trigger": func(t *rapid.T) {
				if len(runner.state.Running) >= 4 {
					t.Skip("running action bound reached")
				}
				triggerID++
				runner.apply(t, model.Trigger{ID: "trigger-" + time.Duration(triggerID).String()})
			},
			"advance": func(t *rapid.T) {
				if !runner.state.Paused && len(runner.state.Running) >= 4 {
					t.Skip("running action bound reached")
				}
				runner.apply(t, model.AdvanceTime{Time: runner.state.Now.Add(defaultInterval)})
			},
			"complete": func(t *rapid.T) {
				if len(runner.state.Running) == 0 {
					t.Skip("no running action")
				}
				action := rapid.SampledFrom(runner.state.Running).Draw(t, "running-action")
				runner.apply(t, model.CompleteWorkflow{ActionID: action.ID})
			},
			"describe": func(t *rapid.T) { runner.apply(t, model.Describe{}) },
		})
	})
}
