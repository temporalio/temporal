package fc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

func TestMakeWakePriority(t *testing.T) {
	t.Parallel()
	age := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	require.Less(t, makeWakePriority(1, age.Add(24*time.Hour)), makeWakePriority(2, age))
	require.Less(t, makeWakePriority(7, age), makeWakePriority(7, age.Add(time.Millisecond)))
	require.Equal(t, makeWakePriority(0, age), makeWakePriority(-1, age))
	require.Less(t, makeWakePriority(0, age), makeWakePriority(1, age))
	require.Less(t, makeWakePriority(254, age), makeWakePriority(255, age))
	require.Equal(t, makeWakePriority(255, age), makeWakePriority(256, age))
}

func TestBetterWakePriority(t *testing.T) {
	t.Parallel()
	age := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	priority := makeWakePriority(7, age)

	require.False(t, betterWakePriority(priority, priority))
	require.False(t, betterWakePriority(priority, makeWakePriority(7, age.Add(59*time.Second+999*time.Millisecond))))
	require.False(t, betterWakePriority(priority, makeWakePriority(7, age.Add(60*time.Second))))
	require.True(t, betterWakePriority(priority, makeWakePriority(7, age.Add(60*time.Second+time.Millisecond))))
	require.False(t, betterWakePriority(makeWakePriority(7, age.Add(61*time.Second)), priority))
	require.True(t, betterWakePriority(makeWakePriority(7, age.Add(24*time.Hour)), makeWakePriority(8, age)))
	require.False(t, betterWakePriority(makeWakePriority(8, age), makeWakePriority(7, age.Add(24*time.Hour))))
}

func TestSortLimiters(t *testing.T) {
	t.Parallel()
	a := Limiter{Type: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "a", Config: "config-a", ConfigVersion: 1}
	b := Limiter{Type: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "b", Config: "config-b", ConfigVersion: 2}
	c := Limiter{
		Type: enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT, Key: "a", Source: LimiterSourceConfig,
		Config: "config-c", ConfigVersion: 3,
	}
	want := []Limiter{a, b, c}
	for _, input := range [][]Limiter{
		{a, b, c}, {a, c, b}, {b, a, c}, {b, c, a}, {c, a, b}, {c, b, a},
	} {
		sortLimiters(input)
		require.Equal(t, want, input)
	}
}
