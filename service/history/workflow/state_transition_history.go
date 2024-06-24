// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
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

// CompareVersionedTransition compares two VersionedTransition structs.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
//
// A VersionedTransition  is considered less than another
// if its NamespaceFailoverVersion is less than the other's.
// Or if the NamespaceFailoverVersion is the same, then the TransitionCount is compared.
func CompareVersionedTransition(
	a, b *persistencespb.VersionedTransition,
) int {
	if a.NamespaceFailoverVersion < b.NamespaceFailoverVersion {
		return -1
	}
	if a.NamespaceFailoverVersion > b.NamespaceFailoverVersion {
		return 1
	}

	if a.TransitionCount < b.TransitionCount {
		return -1
	}
	if a.TransitionCount > b.TransitionCount {
		return 1
	}

	return 0
}
