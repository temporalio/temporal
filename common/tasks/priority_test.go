package tasks

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// Callers iterate PriorityOrder to visit every priority, so a priority missing from it is
// one whose work never gets looked at.
func TestPriorityOrderCoversEveryNamedPriority(t *testing.T) {
	require.Len(t, PriorityOrder, len(PriorityName),
		"a priority added to PriorityName must also be ordered")
	for _, priority := range PriorityOrder {
		require.Contains(t, PriorityName, priority)
	}
}

// A lower Priority is more urgent, so ascending value is most urgent first.
func TestPriorityOrderIsMostUrgentFirst(t *testing.T) {
	require.True(t, slices.IsSorted(PriorityOrder), "PriorityOrder must ascend by value")
	require.Equal(t, PriorityHigh, PriorityOrder[0])
	require.Equal(t, PriorityPreemptable, PriorityOrder[len(PriorityOrder)-1])
}
