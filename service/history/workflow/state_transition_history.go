package workflow

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
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
