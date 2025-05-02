package collection_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/collection"
)

func TestIndexedTakeList(t *testing.T) {
	t.Parallel()

	type Value struct{ ID int }
	values := []Value{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}
	indexer := func(v Value) int { return v.ID }

	list := collection.NewIndexedTakeList(values, indexer)

	t.Run("take absent", func(t *testing.T) {
		_, ok := list.Take(999)
		require.False(t, ok)
	})

	t.Run("take present", func(t *testing.T) {
		v, ok := list.Take(3)
		require.True(t, ok)
		require.Equal(t, 3, v.ID)
	})

	t.Run("take taken", func(t *testing.T) {
		_, ok := list.Take(3)
		require.False(t, ok)
	})

	t.Run("take remaining", func(t *testing.T) {
		allowed := []int{0, 1, 2, 4}
		remaining := list.TakeRemaining()
		require.Len(t, remaining, len(allowed))
		for _, v := range remaining {
			require.Contains(t, allowed, v.ID)
		}
	})

	t.Run("now empty", func(t *testing.T) {
		require.Empty(t, list.TakeRemaining())
	})
}
