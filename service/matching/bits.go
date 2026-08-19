package matching

import "math/bits"

// bitSet encapsulates simple bit manipulation functions on a uint64 slice.
type bitSet []uint64

// len returns the index of the highest set bit.
func (bs bitSet) len() int32 {
	i := len(bs) - 1
	if i < 0 {
		return 0
	}
	return int32(bits.Len64(bs[i]) + i*64)
}

// get returns true if a specific bit is set.
func (bs bitSet) get(i int32) bool {
	if len(bs) < int(i)/64+1 {
		return false
	}
	return bs[i/64]&(1<<(i%64)) != 0
}

// set sets a bit, growing the slice if necessary.
// note: caller must assign the result, as with append.
func (bs bitSet) set(i int32) bitSet {
	for len(bs) < int(i)/64+1 {
		bs = append(bs, 0)
	}
	bs[i/64] |= 1 << (i % 64)
	return bs
}

// clear clears a bit (and shrinks the slice to the minimum required).
// note: caller must assign the result, as with append.
func (bs bitSet) clear(i int32) bitSet {
	if len(bs) < int(i)/64+1 {
		return bs
	}
	bs[i/64] &^= 1 << (i % 64)
	for len(bs) > 0 && bs[len(bs)-1] == 0 {
		bs = bs[:len(bs)-1]
	}
	return bs
}
