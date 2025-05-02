package hsm

import (
	"errors"
	"fmt"
	"slices"
)

// ErrInvalidTransition is returned from [Transition.Apply] on an invalid state transition.
var ErrInvalidTransition = errors.New("invalid transition")

// A TaskRegenerator is invoked to regenerate tasks post state-based replication or when refreshing all tasks for a
// workflow.
type TaskRegenerator interface {
	RegenerateTasks(*Node) ([]Task, error)
}

// A StateMachine is anything that can get and set a comparable state S and re-generate tasks based on current state.
// It is meant to be used with [Transition] objects to safely transition their state on a given event.
type StateMachine[S comparable] interface {
	TaskRegenerator
	State() S
	SetState(S)
}

// TransitionOutput is output produced for a single transition.
type TransitionOutput struct {
	Tasks []Task
}

// Transition represents a state machine transition for a machine of type SM with state S and event E.
type Transition[S comparable, SM StateMachine[S], E any] struct {
	// Source states that are valid for this transition.
	Sources []S
	// Destination state to transition to.
	Destination S
	// Function to apply the transition. Mutate the state machine object here and return tasks.
	apply func(SM, E) (TransitionOutput, error)
}

// NewTransition creates a new [Transition] from the given source states to a destination state for a given event.
// The apply function is called after verifying the transition is possible and setting the destination state.
func NewTransition[S comparable, SM StateMachine[S], E any](src []S, dst S, apply func(SM, E) (TransitionOutput, error)) Transition[S, SM, E] {
	return Transition[S, SM, E]{
		Sources:     src,
		Destination: dst,
		apply:       apply,
	}
}

// Possible returns a boolean indicating whether the transition is possible for the current state.
func (t Transition[S, SM, E]) Possible(sm SM) bool {
	return slices.Contains(t.Sources, sm.State())
}

// Apply applies a transition event to the given state machine changing the state machine's state to the transition's
// Destination on success.
func (t Transition[S, SM, E]) Apply(sm SM, event E) (TransitionOutput, error) {
	prevState := sm.State()
	if !t.Possible(sm) {
		return TransitionOutput{}, fmt.Errorf("%w from %v: %v", ErrInvalidTransition, prevState, event)
	}

	sm.SetState(t.Destination)
	return t.apply(sm, event)
}
