package transitionhistory

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/consts"
)

func TestCompareVersionedTransition(t *testing.T) {
	b := persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 2,
		TransitionCount:          2,
	}.Build()

	testCases := []struct {
		name           string
		a              *persistencespb.VersionedTransition
		expectedResult int
	}{
		{
			name: "equal",
			a: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 2,
				TransitionCount:          2,
			}.Build(),
			expectedResult: 0,
		},
		{
			name: "same version, smaller transition count",
			a: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 2,
				TransitionCount:          1,
			}.Build(),
			expectedResult: -1,
		},
		{
			name: "same version, larger transition count",
			a: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 2,
				TransitionCount:          3,
			}.Build(),
			expectedResult: 1,
		},
		{
			name: "lower version",
			a: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 1,
				TransitionCount:          10,
			}.Build(),
			expectedResult: -1,
		},
		{
			name: "higher version",
			a: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 3,
				TransitionCount:          1,
			}.Build(),
			expectedResult: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedResult, Compare(tc.a, b))
		})
	}
}

func TestTransitionHistoryStalenessCheck(t *testing.T) {
	var hist []*persistencespb.VersionedTransition
	hist = []*persistencespb.VersionedTransition{
		persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 3}.Build(),
		persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 3, TransitionCount: 6}.Build(),
	}

	// sv == tv, range(sc) < tc
	require.ErrorIs(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 3,
		TransitionCount:          7,
	}.Build()), consts.ErrStaleState)
	// sv == tv, range(sc) contains tc
	require.NoError(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 3,
		TransitionCount:          4,
	}.Build()))
	// sv == tv, range(sc) > tc
	require.ErrorIs(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 3,
		TransitionCount:          3,
	}.Build()), consts.ErrStaleReference)

	// sv < tv
	require.ErrorIs(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 4,
		TransitionCount:          4,
	}.Build()), consts.ErrStaleState)

	// sv does not contain tv
	require.ErrorIs(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 2,
		TransitionCount:          4,
	}.Build()), consts.ErrStaleReference)

	// sv > tv, range(sc) does not contain tc
	require.ErrorIs(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 1,
		TransitionCount:          4,
	}.Build()), consts.ErrStaleReference)
	// sv > tv, range(sc) contains tc
	require.NoError(t, StalenessCheck(hist, persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: 1,
		TransitionCount:          3,
	}.Build()))
}
