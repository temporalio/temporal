package chasm

import (
	"errors"
	"fmt"
	"slices"
)

// ErrInvalidTransition is returned from [Transition.Apply] on an invalid status transition.
var ErrInvalidTransition = errors.New("invalid transition")

// A StateMachine is anything that can get and set a comparable status S and re-generate tasks based on current status.
// It is meant to be used with [Transition] objects to safely transition their status on a given event.
type StateMachine[S comparable] interface {
	Status() S
	SetStatus(S)
}

// Transition represents a state machine transition for a machine of type SM with status S and event E.
type Transition[S comparable, SM StateMachine[S], E any] struct {
	// Source statuses that are valid for this transition.
	Sources []S
	// Destination status to transition to.
	Destination S
	// Function to apply the transition. Mutate the status machine object here and schedule tasks.
	apply func(SM, MutableContext, E) error
}

// NewTransition creates a new [Transition] from the given source statuses to a destination status for a given event.
// The apply function is called after verifying the transition is possible and setting the destination status.
func NewTransition[S comparable, SM StateMachine[S], E any](src []S, dst S, apply func(SM, MutableContext, E) error) Transition[S, SM, E] {
	return Transition[S, SM, E]{
		Sources:     src,
		Destination: dst,
		apply:       apply,
	}
}

// Possible returns a boolean indicating whether the transition is possible for the current status.
func (t Transition[S, SM, E]) Possible(sm SM) bool {
	return slices.Contains(t.Sources, sm.Status())
}

// Apply applies a transition event to the given status machine changing the status machine's status to the transition's
// Destination on success.
func (t Transition[S, SM, E]) Apply(ctx MutableContext, sm SM, event E) error {
	prevStatus := sm.Status()
	if !t.Possible(sm) {
		return fmt.Errorf("%w from %v: %v", ErrInvalidTransition, prevStatus, event)
	}

	sm.SetStatus(t.Destination)
	return t.apply(sm, ctx, event)
}
