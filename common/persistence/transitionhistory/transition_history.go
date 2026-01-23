package transitionhistory

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/consts"
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

// StalenessCheck verifies that refVersionedTransition is contained in the given transition history.
//
// When a task or API request is being processed, the history is compared with the imprinted state reference to verify
// that the state is not stale or that the task/request itself is not stale. For example, if the state has a history of
// `[{v: 1, t: 3}, {v: 2, t: 5}]`, task A `{v: 2, t: 4}` **is not** referencing stale state because for version `2`
// transitions `4-5` are valid, while task B `{v: 2, t: 6}` **is** referencing stale state because the transition count
// is out of range for version `2`. Furthermore, task C `{v: 1, t: 4}` itself is stale because it is referencing an
// impossible state, likely due to post split-brain reconciliation.
// NOTE: This function should only be used when there is reloading logic on top of it, since the error returned is a
// terminal error.
func StalenessCheck(
	history []*persistencespb.VersionedTransition,
	refVersionedTransition *persistencespb.VersionedTransition,
) error {
	if len(history) == 0 {
		return serviceerror.NewInternal("state has empty transition history")
	}
	idx, minTransitionCount, maxTransitionCount := transitionHistoryRangeForVersion(history, refVersionedTransition.NamespaceFailoverVersion)
	if idx == -1 {
		lastItem := history[len(history)-1]
		if lastItem.NamespaceFailoverVersion < refVersionedTransition.NamespaceFailoverVersion {
			return fmt.Errorf(
				"%w: state namespace failover version < ref namespace failover version: %v < %v",
				consts.ErrStaleState,
				lastItem.NamespaceFailoverVersion,
				refVersionedTransition.NamespaceFailoverVersion,
			)
		}
		return fmt.Errorf(
			"%w: state namespace failover version > ref namespace failover version: %v > %v",
			consts.ErrStaleReference,
			lastItem.NamespaceFailoverVersion,
			refVersionedTransition.NamespaceFailoverVersion,
		)
	}
	if idx == len(history)-1 && maxTransitionCount < refVersionedTransition.TransitionCount {
		return fmt.Errorf(
			"%w: state transition count < ref transition count: %v < %v",
			consts.ErrStaleState,
			maxTransitionCount,
			refVersionedTransition.TransitionCount,
		)
	}
	if minTransitionCount > refVersionedTransition.TransitionCount || maxTransitionCount < refVersionedTransition.TransitionCount {
		return fmt.Errorf(
			"%w: ref transition count out of range for version %v: %v not in [%v, %v]",
			consts.ErrStaleReference,
			refVersionedTransition.NamespaceFailoverVersion,
			refVersionedTransition.TransitionCount,
			minTransitionCount,
			maxTransitionCount,
		)
	}
	return nil
}

// transitionHistoryRangeForVersion finds the index and transition count range in the given history for the given version.
func transitionHistoryRangeForVersion(
	history []*persistencespb.VersionedTransition,
	version int64,
) (idx int, minTransitionCount int64, maxTransitionCount int64) {
	prevVersionMaxTransitionCount := int64(-1)
	for i, item := range history {
		if item.NamespaceFailoverVersion == version {
			return i, prevVersionMaxTransitionCount + 1, item.TransitionCount
		}
		prevVersionMaxTransitionCount = item.TransitionCount
	}
	return -1, 0, 0
}
