package tests

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests/testcore"
)

func TestFunctionalTestEntries(t *testing.T) {
	entries := FunctionalTestEntries()
	require.Len(t, entries, 137)

	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name
	}
	require.True(t, slices.IsSorted(names))

	entries[0].Name = "modified"
	entries = FunctionalTestEntries()
	require.Equal(t, "TestAcquireShardSuite", entries[0].Name)
}

func TestSelectFunctionalTestEntries(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		entries, err := selectFunctionalTestEntries()
		require.NoError(t, err)
		require.Len(t, entries, 137)
	})

	t.Run("exact names retain registry order", func(t *testing.T) {
		entries, err := selectFunctionalTestEntries(WithFunctionalTestNames(
			"TestWorkflowTestSuite",
			"TestActivityTestSuite",
		))
		require.NoError(t, err)
		require.Equal(t, []string{"TestActivityTestSuite", "TestWorkflowTestSuite"}, functionalTestEntryNames(entries))
	})

	t.Run("exact names are copied at option construction", func(t *testing.T) {
		names := []string{"TestWorkflowTestSuite", "TestActivityTestSuite"}
		option := WithFunctionalTestNames(names...)
		names[0] = "missing"

		entries, err := selectFunctionalTestEntries(option)
		require.NoError(t, err)
		require.Equal(t, []string{"TestActivityTestSuite", "TestWorkflowTestSuite"}, functionalTestEntryNames(entries))
	})

	t.Run("regex", func(t *testing.T) {
		entries, err := selectFunctionalTestEntries(WithFunctionalTestNameRegex("^TestScheduleV1"))
		require.NoError(t, err)
		require.Equal(t, []string{"TestScheduleV1", "TestScheduleV1WorkflowPauseInteraction"}, functionalTestEntryNames(entries))
	})

	t.Run("filters intersect", func(t *testing.T) {
		entries, err := selectFunctionalTestEntries(
			WithFunctionalTestNameRegex("^TestScheduleV1"),
			WithFunctionalTestNames("TestScheduleV1WorkflowPauseInteraction"),
		)
		require.NoError(t, err)
		require.Equal(t, []string{"TestScheduleV1WorkflowPauseInteraction"}, functionalTestEntryNames(entries))
	})

	t.Run("predicate permits empty selection", func(t *testing.T) {
		entries, err := selectFunctionalTestEntries(WithFunctionalTestPredicate(func(FunctionalTestEntry) bool { return false }))
		require.NoError(t, err)
		require.Empty(t, entries)
	})
}

func TestSelectFunctionalTestEntriesErrors(t *testing.T) {
	t.Run("unknown exact names are reported in option order", func(t *testing.T) {
		_, err := selectFunctionalTestEntries(
			WithFunctionalTestNames("missing"),
			WithFunctionalTestNameRegex("["),
		)
		require.EqualError(t, err, `unknown functional test entry "missing"`)
	})

	t.Run("invalid regex is reported in option order", func(t *testing.T) {
		_, err := selectFunctionalTestEntries(
			WithFunctionalTestNameRegex("["),
			WithFunctionalTestNames("missing"),
		)
		require.ErrorContains(t, err, `invalid functional test name regex "["`)
	})

	t.Run("non-predicate empty selections fail", func(t *testing.T) {
		_, err := selectFunctionalTestEntries(WithFunctionalTestNameRegex("^does-not-exist$"))
		require.EqualError(t, err, "functional test selection is empty; use WithFunctionalTestPredicate to allow it")
	})

	t.Run("empty exact selector fails", func(t *testing.T) {
		_, err := selectFunctionalTestEntries(WithFunctionalTestNames())
		require.EqualError(t, err, "functional test selection is empty; use WithFunctionalTestPredicate to allow it")
	})

	t.Run("nil predicate fails", func(t *testing.T) {
		_, err := selectFunctionalTestEntries(WithFunctionalTestPredicate(nil))
		require.EqualError(t, err, "functional test predicate is nil")
	})
}

func TestRunFunctionalTestEntriesUsesLogicalEntryNames(t *testing.T) {
	var names []string
	runFunctionalTestEntries(t, []FunctionalTestEntry{
		{
			Name: "first",
			run: func(t *testing.T) {
				names = append(names, t.Name())
			},
		},
		{
			Name: "second",
			run: func(t *testing.T) {
				names = append(names, t.Name())
			},
		},
	})
	require.Len(t, names, 2)
	require.True(t, strings.HasSuffix(names[0], "/first"))
	require.True(t, strings.HasSuffix(names[1], "/second"))
}

type noClusterFactory struct{}

func (noClusterFactory) NewCluster(*testing.T, *testcore.ClusterConfig, log.Logger) (testcore.Cluster, error) {
	panic("unexpected cluster creation")
}

func TestScheduleWorkerServiceSelectionUsesLogicalName(t *testing.T) {
	names := []string{"TestScheduleV1", "TestScheduleV1WorkflowPauseInteraction"}
	for _, name := range names {
		t.Run("direct/"+name, func(t *testing.T) {
			require.True(t, scheduleNameNeedsWorkerService(name))
		})
	}
	require.False(t, scheduleNameNeedsWorkerService("TestScheduleCHASM"))

	originalEntries := functionalTestEntries
	functionalTestEntries = make([]FunctionalTestEntry, 0, len(names))
	for _, name := range names {
		name := name
		functionalTestEntries = append(functionalTestEntries, FunctionalTestEntry{
			Name: name,
			run: func(t *testing.T) {
				require.Equal(t, name, testcore.LogicalTestName(t))
				require.True(t, scheduleNeedsWorkerService(t))
			},
		})
	}
	t.Cleanup(func() { functionalTestEntries = originalEntries })

	t.Run("caller-owned", func(t *testing.T) {
		Run(t, noClusterFactory{}, WithFunctionalTestNames(names...))
	})
}

func functionalTestEntryNames(entries []FunctionalTestEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name
	}
	return names
}
