package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitSet(t *testing.T) {
	var bs bitSet

	// setting and reading individual bits
	require.False(t, bs.get(0))

	bs = bs.set(0)
	require.Equal(t, bitSet{1}, bs)
	require.Equal(t, int32(1), bs.len())
	require.True(t, bs.get(0))
	require.False(t, bs.get(1))

	bs = bs.set(5)
	require.Equal(t, bitSet{0b100001}, bs)
	require.Equal(t, int32(6), bs.len())
	require.True(t, bs.get(5))
	require.False(t, bs.get(4))

	// bit in second word
	bs = bs.set(64)
	require.Equal(t, bitSet{0b100001, 1}, bs)
	require.Equal(t, int32(65), bs.len())
	require.True(t, bs.get(64))
	require.False(t, bs.get(65))

	// bit at word boundary
	bs = bs.set(63)
	require.Equal(t, bitSet{0b100001 | (1 << 63), 1}, bs)
	require.Equal(t, int32(65), bs.len())

	// clear high bit, read should drop back
	bs = bs.clear(64)
	require.Equal(t, bitSet{0b100001 | (1 << 63)}, bs) // trailing zero word trimmed
	require.Equal(t, int32(64), bs.len())
	require.False(t, bs.get(64))

	// clear all bits one by one
	bs = bs.clear(63)
	bs = bs.clear(5)
	bs = bs.clear(0)
	require.Empty(t, bs)
	require.Equal(t, int32(0), bs.len())
	require.False(t, bs.get(0))

	// clearing a bit that's already clear or out of range is a no-op
	bs = bs.clear(999)
	require.Empty(t, bs)

	// read on nil returns 0
	var nilbs bitSet
	require.Equal(t, int32(0), nilbs.len())
}
