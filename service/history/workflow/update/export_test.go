package update

import (
	"go.temporal.io/server/common/effect"
)

var (
	// while we *could* write the unit test code to walk an Update through a
	// series of message deliveries to get to the right state, it's much faster
	// just to instantiate directly into the desired state.
	NewAccepted  = newAccepted
	NewCompleted = newCompleted
	AbortFailure = acceptedUpdateCompletedWorkflowFailure
)

// ObserveCompletion exports withOnComplete to unit tests
func ObserveCompletion(b *bool) updateOpt {
	return withCompletionCallback(func() { *b = true })
}

func (u *Update) IsSent() bool { return u.isSent() }

func (u *Update) ID() string { return u.id }

func CompletedCount(r Registry) int { return r.(*registry).completedCount }

func (u *Update) Abort(reason AbortReason, effects effect.Controller) { u.abort(reason, effects) }
