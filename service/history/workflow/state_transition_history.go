package workflow

import (
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
)

var (
	// EmptyVersionedTransition is the zero value for VersionedTransition.
	// It's not a valid versioned transition for a workflow, and should only
	// be used for representing the absence of a versioned transition.
	// EmptyVersionedTransition is also considered less than any non-empty versioned transition.
	EmptyVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: common.EmptyVersion,
		TransitionCount:          0,
	}
)

// UpdatedTransitionHistory takes a slice of transition history and returns a new slice that includes the max state
// transition count for the given version.
// If the given version is the version of the last history item, that item will be replaced in the returned slice with
// an item containing the modified transition count.
// Does not modify the history slice in place.
func UpdatedTransitionHistory(
	history []*persistencespb.VersionedTransition,
	namespaceFailoverVersion int64,
) []*persistencespb.VersionedTransition {
	if len(history) == 0 {
		return []*persistencespb.VersionedTransition{
			{
				NamespaceFailoverVersion: namespaceFailoverVersion,
				TransitionCount:          1,
			},
		}
	}

	lastTransitionCount := history[len(history)-1].TransitionCount
	if history[len(history)-1].NamespaceFailoverVersion == namespaceFailoverVersion {
		history = history[:len(history)-1]
	}
	return append(history, &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: namespaceFailoverVersion,
		TransitionCount:          lastTransitionCount + 1,
	})
}

// transitionHistoryRangeForVersion finds the index and transition count range in the given history for the given version.
func transitionHistoryRangeForVersion(history []*persistencespb.VersionedTransition, version int64) (idx int, min int64, max int64) {
	prevVersionMaxTransitionCount := int64(-1)
	for i, item := range history {
		if item.NamespaceFailoverVersion == version {
			return i, prevVersionMaxTransitionCount + 1, item.TransitionCount
		}
		prevVersionMaxTransitionCount = item.TransitionCount
	}
	return -1, 0, 0
}

// TransitionHistoryStalenessCheck verifies that ref namespace failover version and transition count is contained in
// the given transition history.
//
// When a task or API request is being processed, the history is compared with the imprinted state reference to verify
// that the state is not stale or that the task/request itself is not stale. For example, if the state has a history of
// `[{v: 1, t: 3}, {v: 2, t: 5}]`, task A `{v: 2, t: 4}` **is not** referencing stale state because for version `2`
// transitions `4-5` are valid, while task B `{v: 2, t: 6}` **is** referencing stale state because the transition count
// is out of range for version `2`. Furthermore, task C `{v: 1, t: 4}` itself is stale because it is referencing an
// impossible state, likely due to post split-brain reconciliation.
// NOTE: This function should only be used when there is reloading logic on top of it, since the error returned is a
// terminal error.
func TransitionHistoryStalenessCheck(
	history []*persistencespb.VersionedTransition,
	refVersionedTransition *persistencespb.VersionedTransition,
) error {
	if len(history) == 0 {
		return queues.NewUnprocessableTaskError("state has empty transition history")
	}
	idx, min, max := transitionHistoryRangeForVersion(history, refVersionedTransition.NamespaceFailoverVersion)
	if idx == -1 {
		lastItem := history[len(history)-1]
		if lastItem.NamespaceFailoverVersion < refVersionedTransition.NamespaceFailoverVersion {
			return fmt.Errorf("%w: state namespace failover version < ref namespace failover version", consts.ErrStaleState)
		}
		return fmt.Errorf("%w: state namespace failover version > ref namespace failover version", consts.ErrStaleReference)
	}
	if idx == len(history)-1 && refVersionedTransition.TransitionCount > max {
		return fmt.Errorf("%w: state transition count < ref transition count", consts.ErrStaleState)
	}
	if min > refVersionedTransition.TransitionCount || max < refVersionedTransition.TransitionCount {
		return fmt.Errorf("%w: ref transition count out of range for version %v", consts.ErrStaleReference, refVersionedTransition.NamespaceFailoverVersion)
	}
	return nil
}
