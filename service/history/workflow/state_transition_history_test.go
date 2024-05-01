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

package workflow_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow"
)

func TestUpdatedTranstionHistory(t *testing.T) {
	var hist []*persistencespb.VersionedTransition
	hist = workflow.UpdatedTransitionHistory(hist, 1, 4)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{{NamespaceFailoverVersion: 1, MaxTransitionCount: 4}},
		hist,
	)
	hist = workflow.UpdatedTransitionHistory(hist, 1, 5)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{{NamespaceFailoverVersion: 1, MaxTransitionCount: 5}},
		hist,
	)
	hist = workflow.UpdatedTransitionHistory(hist, 2, 6)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, MaxTransitionCount: 5},
			{NamespaceFailoverVersion: 2, MaxTransitionCount: 6},
		},
		hist,
	)
}

func TestTransitionHistoryStalenessCheck(t *testing.T) {
	var hist []*persistencespb.VersionedTransition
	hist = []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, MaxTransitionCount: 3},
		{NamespaceFailoverVersion: 3, MaxTransitionCount: 6},
	}

	// sv == tv, range(sc) < tc
	require.ErrorIs(t, workflow.TransitionHistoryStalenessCheck(hist, 3, 7), consts.ErrStaleState)
	// sv == tv, range(sc) contains tc
	require.NoError(t, workflow.TransitionHistoryStalenessCheck(hist, 3, 4))
	// sv == tv, range(sc) > tc
	require.ErrorIs(t, workflow.TransitionHistoryStalenessCheck(hist, 3, 3), consts.ErrStaleReference)

	// sv < tv
	require.ErrorIs(t, workflow.TransitionHistoryStalenessCheck(hist, 4, 4), consts.ErrStaleState)

	// sv does not contain tv
	require.ErrorIs(t, workflow.TransitionHistoryStalenessCheck(hist, 2, 4), consts.ErrStaleReference)

	// sv > tv, range(sc) does not contain tc
	require.ErrorIs(t, workflow.TransitionHistoryStalenessCheck(hist, 1, 4), consts.ErrStaleReference)
	// sv > tv, range(sc) contains tc
	require.NoError(t, workflow.TransitionHistoryStalenessCheck(hist, 1, 3))
}
