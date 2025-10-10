package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepeatSlice(t *testing.T) {
	t.Run("when input slice is nil should return nil", func(t *testing.T) {
		got := RepeatSlice[int](nil, 5)
		require.Nil(t, got, "RepeatSlice produced non-nil slice from nil input")
	})
	t.Run("when input slice is empty should return empty", func(t *testing.T) {
		empty := []int{}
		got := RepeatSlice(empty, 5)
		require.Empty(t, got, "RepeatSlice filled empty slice")
	})
	t.Run("when requested repeat number equal 0 should return empty slice", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		got := RepeatSlice(xs, 0)
		require.Empty(t, got, "RepeatSlice with repeat count 0 returned non-empty slice")
	})
	t.Run("when requested repeat number is less than 0 should return empty slice", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		got := RepeatSlice(xs, -1)
		require.Empty(t, got, "RepeatSlice with repeat count -1 returned non-empty slice")
	})
	t.Run("when requested repeat number is 3 should return slice three times the input", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		got := RepeatSlice(xs, 3)
		require.Len(t, got, len(xs)*3, "RepeatSlice produced slice of wrong length: expected %d got %d", len(xs)*3, len(got))
		for i, v := range got {
			require.Equal(t, xs[i%len(xs)], v, "RepeatSlice wrong value in result: expected %d at index %d but got %d", xs[i%len(xs)], i, v)
		}
	})
	t.Run("should not change the input slice when truncating", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		_ = RepeatSlice(xs, 0)
		require.Len(t, xs, 5, "Repeat slice truncated the original slice: expected {1, 2, 3, 4, 5}, got %v", xs)
	})
	t.Run("should not change the input slice when replicating", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		_ = RepeatSlice(xs, 5)
		require.Len(t, xs, 5, "Repeat slice changed the original slice: expected {1, 2, 3, 4, 5}, got %v", xs)
	})
}

func TestMapSlice(t *testing.T) {
	t.Run("when given nil as slice should return nil", func(t *testing.T) {
		ys := MapSlice(nil, func(x int) uint32 { return uint32(x) })
		require.Nil(t, ys, "mapping over nil produced non nil got %v", ys)
	})
	t.Run("when given an empty slice should return empty slice", func(t *testing.T) {
		xs := []int{}
		var ys []uint32
		ys = MapSlice(xs, func(x int) uint32 { return uint32(x) })
		require.Empty(t, ys, "mapping over empty slice produced non empty slice got %v", ys)
	})
	t.Run("when given a slice and a function should apply function to every element of the original slice", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		ys := MapSlice(xs, func(x int) int { return x + 1 })
		for i, y := range ys {
			require.Equal(t, xs[i]+1, y, "mapping over slice did not apply function expected {2, 3, 4, 5} got %v", ys)
		}
	})
}
