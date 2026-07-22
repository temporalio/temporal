package model

import (
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

func Transition(config Config, input State, event Event) (Outcome, error) {
	state := cloneState(input)
	outcome := Outcome{State: state}

	switch event := event.(type) {
	case Create:
		if state.Lifecycle != LifecycleAbsent {
			return Outcome{State: input}, ErrAlreadyCreated
		}
		state.Now = config.StartTime
		state.Lifecycle = LifecycleRunning
		state.Paused = event.Paused
		state.Notes = event.Notes
		state.ConflictToken = 1
		state.HighWatermark = config.StartTime
		state.OverlapPolicy = normalizeOverlapPolicy(config.OverlapPolicy)
		state.LimitedActions = config.LimitedActions
		state.RemainingActions = config.RemainingActions
		outcome.State = state
		outcome.Response = MutationResponse{ConflictToken: state.ConflictToken}
		outcome.Effects = nextWakeup(config, state)
		return outcome, nil
	case Describe:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		outcome.Response = DescribeResponse{Observable: Observe(state)}
		return outcome, nil
	case Pause:
		return setPaused(config, input, state, true, event.Note)
	case Unpause:
		return setPaused(config, input, state, false, event.Note)
	case Update:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		state.Paused = event.Paused
		state.Notes = event.Notes
		state.OverlapPolicy = normalizeOverlapPolicy(event.OverlapPolicy)
		state.ConflictToken++
		return Outcome{
			State:    state,
			Response: MutationResponse{ConflictToken: state.ConflictToken},
			Effects:  nextWakeup(config, state),
		}, nil
	case Trigger:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		if event.ID == "" || hasAction(state, event.ID) {
			return Outcome{State: input}, fmt.Errorf("%w: invalid or duplicate trigger ID", ErrInvalidEvent)
		}
		action := Action{ID: event.ID, NominalTime: state.Now, Manual: true}
		state, outcome.Effects = enqueueAction(state, action)
		state.ConflictToken++
		outcome.State = state
		outcome.Response = MutationResponse{ConflictToken: state.ConflictToken}
		return outcome, nil
	case Backfill:
		return backfill(config, input, state, event)
	case AdvanceTime:
		return advance(config, input, state, event.Time)
	case StartSucceeded:
		return startSucceeded(input, state, event)
	case StartFailed:
		return startFailed(config, input, state, event)
	case CompleteWorkflow:
		return completeWorkflow(config, input, state, event)
	case Delete:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		state.Lifecycle = LifecycleClosed
		state.Pending = nil
		state.Running = nil
		state.ConflictToken++
		outcome.State = state
		outcome.Response = MutationResponse{ConflictToken: state.ConflictToken}
		outcome.Effects = []Effect{CloseSchedule{}}
		return outcome, nil
	default:
		return Outcome{State: input}, fmt.Errorf("%w: %T", ErrInvalidEvent, event)
	}
}

func backfill(config Config, input, state State, event Backfill) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	if event.ID == "" || event.Start.IsZero() || event.End.Before(event.Start) {
		return Outcome{State: input}, fmt.Errorf("%w: invalid backfill", ErrInvalidEvent)
	}
	policy := normalizeOverlapPolicy(event.OverlapPolicy)
	if event.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = state.OverlapPolicy
	}
	previousPolicy := state.OverlapPolicy
	state.OverlapPolicy = policy
	maxGenerated := config.MaxGenerated
	if maxGenerated <= 0 {
		maxGenerated = 1000
	}
	var effects []Effect
	for next, ok := matchingTimeAtOrAfter(config, event.Start); ok && !next.After(event.End); next = next.Add(config.Interval) {
		if len(effects) >= maxGenerated {
			return Outcome{State: input}, fmt.Errorf("%w: generated action limit exceeded", ErrInvalidEvent)
		}
		action := Action{ID: fmt.Sprintf("backfill:%s:%d", event.ID, next.UnixNano()), NominalTime: next, Manual: true}
		var actionEffects []Effect
		state, actionEffects = enqueueAction(state, action)
		effects = append(effects, actionEffects...)
	}
	state.OverlapPolicy = previousPolicy
	state.ConflictToken++
	return Outcome{State: state, Effects: effects, Response: MutationResponse{ConflictToken: state.ConflictToken}}, nil
}

func setPaused(config Config, input, state State, paused bool, note string) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	state.Paused = paused
	state.Notes = note
	state.ConflictToken++
	outcome := Outcome{
		State:    state,
		Response: MutationResponse{ConflictToken: state.ConflictToken},
	}
	outcome.Effects = nextWakeup(config, state)
	return outcome, nil
}

func startSucceeded(input, state State, event StartSucceeded) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	if event.RunID == "" {
		return Outcome{State: input}, fmt.Errorf("%w: empty run ID", ErrInvalidEvent)
	}
	action, remaining, ok := takeAction(state.Pending, event.ActionID)
	if !ok {
		return Outcome{State: input}, fmt.Errorf("%w: action %q is not pending", ErrInvalidEvent, event.ActionID)
	}
	action.RunID = event.RunID
	state.Pending = remaining
	state.Running = append(state.Running, action)
	state.ActionCount++
	return Outcome{State: state}, nil
}

func completeWorkflow(config Config, input, state State, event CompleteWorkflow) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	action, remaining, ok := takeAction(state.Running, event.ActionID)
	if !ok {
		return Outcome{State: input}, fmt.Errorf("%w: action %q is not running", ErrInvalidEvent, event.ActionID)
	}
	state.Running = remaining
	state.Recent = append(state.Recent, action)
	if limit := config.RecentActionLimit; limit >= 0 && len(state.Recent) > limit {
		state.Recent = slices.Delete(state.Recent, 0, len(state.Recent)-limit)
	}
	state, effects := releaseDeferred(state)
	return Outcome{State: state, Effects: effects}, nil
}

func advance(config Config, input, state State, now time.Time) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	if now.Before(state.Now) {
		return Outcome{State: input}, fmt.Errorf("%w: time moved backwards", ErrInvalidEvent)
	}
	state.Now = now
	if state.Paused {
		state.HighWatermark = now
		return Outcome{State: state, Effects: nextWakeup(config, state)}, nil
	}
	if state.LimitedActions && state.RemainingActions <= 0 {
		state.HighWatermark = now
		return Outcome{State: state}, nil
	}
	if config.CatchupWindow > 0 {
		oldest := now.Add(-config.CatchupWindow).Add(-config.Interval)
		if state.HighWatermark.Before(oldest) {
			state.HighWatermark = oldest
		}
	}

	maxGenerated := config.MaxGenerated
	if maxGenerated <= 0 {
		maxGenerated = 1000
	}
	effects := retryEffects(state, now)
	generated := 0
	for next, ok := matchingTimeAfter(config, state.HighWatermark); ok && !next.After(now); next, ok = matchingTimeAfter(config, state.HighWatermark) {
		state.HighWatermark = next
		if config.CatchupWindow > 0 && next.Before(now.Add(-config.CatchupWindow)) {
			continue
		}
		if state.LimitedActions && state.RemainingActions <= 0 {
			state.HighWatermark = now
			break
		}
		if generated >= maxGenerated {
			return Outcome{State: input}, fmt.Errorf("%w: generated action limit exceeded", ErrInvalidEvent)
		}
		generated++
		action := Action{ID: scheduledActionID(next), NominalTime: next}
		var actionEffects []Effect
		state, actionEffects = enqueueAction(state, action)
		effects = append(effects, actionEffects...)
		if state.LimitedActions {
			state.RemainingActions--
		}
	}
	effects = append(effects, nextWakeup(config, state)...)
	return Outcome{State: state, Effects: effects}, nil
}

func startFailed(config Config, input, state State, event StartFailed) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	action, index, ok := findAction(state.Pending, event.ActionID)
	if !ok || !action.Dispatched {
		return Outcome{State: input}, fmt.Errorf("%w: action %q is not starting", ErrInvalidEvent, event.ActionID)
	}
	switch event.Class {
	case StartFailureRetryable, StartFailureRateLimited:
		if !event.RetryAt.After(state.Now) {
			return Outcome{State: input}, fmt.Errorf("%w: retry deadline must be in the future", ErrInvalidEvent)
		}
		action.RetryAt = event.RetryAt
		state.Pending[index] = action
		return Outcome{State: state, Effects: []Effect{ScheduleWakeup{At: event.RetryAt}}}, nil
	case StartFailureNonRetryable:
		state.Pending = slices.Delete(state.Pending, index, index+1)
		if len(state.Pending) == 0 {
			state.Pending = nil
		}
		state, effects := releaseDeferred(state)
		return Outcome{State: state, Effects: effects}, nil
	default:
		return Outcome{State: input}, fmt.Errorf("%w: unknown start failure class", ErrInvalidEvent)
	}
}

func enqueueAction(state State, action Action) (State, []Effect) {
	busy := len(state.Running) > 0 || len(state.Pending) > 0
	policy := normalizeOverlapPolicy(state.OverlapPolicy)
	if !busy || policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		action.Dispatched = true
		action.Attempt = 1
		state.Pending = append(state.Pending, action)
		return state, []Effect{StartWorkflow{Action: action}}
	}

	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
		state.OverlapSkipped++
		return state, nil
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
		for _, pending := range state.Pending {
			if !pending.Dispatched {
				state.OverlapSkipped++
				return state, nil
			}
		}
		state.Pending = append(state.Pending, action)
		return state, nil
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
		state.Pending = append(state.Pending, action)
		return state, nil
	case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
		state.Pending = append(state.Pending, action)
		effects := make([]Effect, len(state.Running))
		for i, running := range state.Running {
			effects[i] = CancelWorkflow{ActionID: running.ID}
		}
		return state, effects
	case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		state.Pending = append(state.Pending, action)
		effects := make([]Effect, len(state.Running))
		for i, running := range state.Running {
			effects[i] = TerminateWorkflow{ActionID: running.ID}
		}
		return state, effects
	default:
		return state, nil
	}
}

func releaseDeferred(state State) (State, []Effect) {
	if len(state.Running) > 0 {
		return state, nil
	}
	for i, action := range state.Pending {
		if action.Dispatched {
			continue
		}
		action.Dispatched = true
		action.Attempt = 1
		state.Pending[i] = action
		return state, []Effect{StartWorkflow{Action: action}}
	}
	return state, nil
}

func retryEffects(state State, now time.Time) []Effect {
	var effects []Effect
	for i, action := range state.Pending {
		if action.RetryAt.IsZero() || action.RetryAt.After(now) {
			continue
		}
		action.RetryAt = time.Time{}
		action.Attempt++
		state.Pending[i] = action
		effects = append(effects, StartWorkflow{Action: action})
	}
	return effects
}

func findAction(actions []Action, id string) (Action, int, bool) {
	for i, action := range actions {
		if action.ID == id {
			return action, i, true
		}
	}
	return Action{}, 0, false
}

func normalizeOverlapPolicy(policy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}

func requireRunning(state State) error {
	switch state.Lifecycle {
	case LifecycleAbsent:
		return ErrNotCreated
	case LifecycleClosed:
		return ErrClosed
	default:
		return nil
	}
}

func cloneState(state State) State {
	state.Pending = slices.Clone(state.Pending)
	state.Running = slices.Clone(state.Running)
	state.Recent = slices.Clone(state.Recent)
	return state
}

func nextWakeup(config Config, state State) []Effect {
	next, ok := matchingTimeAfter(config, state.HighWatermark)
	if !ok {
		return nil
	}
	return []Effect{ScheduleWakeup{At: next}}
}

func matchingTimeAfter(config Config, after time.Time) (time.Time, bool) {
	if config.Interval <= 0 {
		return time.Time{}, false
	}
	anchor := config.StartTime.Truncate(config.Interval).Add(config.Phase % config.Interval)
	if !config.SpecStart.IsZero() && anchor.Before(config.SpecStart) {
		steps := config.SpecStart.Sub(anchor) / config.Interval
		anchor = anchor.Add(steps * config.Interval)
		if anchor.Before(config.SpecStart) {
			anchor = anchor.Add(config.Interval)
		}
	}
	if !anchor.After(after) {
		steps := after.Sub(anchor)/config.Interval + 1
		anchor = anchor.Add(steps * config.Interval)
	}
	if !config.SpecEnd.IsZero() && anchor.After(config.SpecEnd) {
		return time.Time{}, false
	}
	return anchor, true
}

func matchingTimeAtOrAfter(config Config, at time.Time) (time.Time, bool) {
	if config.Interval <= 0 {
		return time.Time{}, false
	}
	anchor := config.StartTime.Truncate(config.Interval).Add(config.Phase % config.Interval)
	if at.Before(anchor) {
		steps := -int((anchor.Sub(at) + config.Interval - 1) / config.Interval)
		anchor = anchor.Add(time.Duration(steps) * config.Interval)
	}
	if anchor.Before(at) {
		anchor = anchor.Add(config.Interval)
	}
	if !config.SpecStart.IsZero() && anchor.Before(config.SpecStart) {
		return matchingTimeAfter(config, config.SpecStart.Add(-time.Nanosecond))
	}
	if !config.SpecEnd.IsZero() && anchor.After(config.SpecEnd) {
		return time.Time{}, false
	}
	return anchor, true
}

func hasAction(state State, id string) bool {
	for _, actions := range [][]Action{state.Pending, state.Running, state.Recent} {
		for _, action := range actions {
			if action.ID == id {
				return true
			}
		}
	}
	return false
}

func takeAction(actions []Action, id string) (Action, []Action, bool) {
	for i, action := range actions {
		if action.ID == id {
			remaining := slices.Delete(slices.Clone(actions), i, i+1)
			if len(remaining) == 0 {
				remaining = nil
			}
			return action, remaining, true
		}
	}
	return Action{}, actions, false
}

func scheduledActionID(nominal time.Time) string {
	return fmt.Sprintf("scheduled:%d", nominal.UnixNano())
}
