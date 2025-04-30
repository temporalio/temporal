package transitionhistory

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

func CopyVersionedTransitions(
	transitions []*persistencespb.VersionedTransition,
) []*persistencespb.VersionedTransition {
	if transitions == nil {
		return nil
	}
	copied := make([]*persistencespb.VersionedTransition, len(transitions))
	for i, t := range transitions {
		copied[i] = CopyVersionedTransition(t)
	}
	return copied
}

func CopyVersionedTransition(
	transition *persistencespb.VersionedTransition,
) *persistencespb.VersionedTransition {
	if transition == nil {
		return nil
	}
	return common.CloneProto(transition)
}

func LastVersionedTransition(
	transitions []*persistencespb.VersionedTransition,
) *persistencespb.VersionedTransition {
	if len(transitions) == 0 {
		// transition history is not enabled
		return nil
	}
	return transitions[len(transitions)-1]
}

// Compare compares two VersionedTransition structs.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
//
// A VersionedTransition is considered less than another
// if its NamespaceFailoverVersion is less than the other's.
// Or if the NamespaceFailoverVersion is the same, then the TransitionCount is compared.
// Nil is considered the same as EmptyVersionedTransition, thus smaller than any non-empty versioned transition.
func Compare(
	a, b *persistencespb.VersionedTransition,
) int {
	if a.GetNamespaceFailoverVersion() < b.GetNamespaceFailoverVersion() {
		return -1
	}
	if a.GetNamespaceFailoverVersion() > b.GetNamespaceFailoverVersion() {
		return 1
	}

	if a.GetTransitionCount() < b.GetTransitionCount() {
		return -1
	}
	if a.GetTransitionCount() > b.GetTransitionCount() {
		return 1
	}

	return 0
}
