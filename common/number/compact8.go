package number

import "math/bits"

// Compact8 is an unsigned 8-bit compact integer encoding with 12 mantissa
// values per exponent level (instead of the standard power-of-2 split).
//
// For a byte value b, let e = b / 12 and m = b % 12:
//
//	b=0:          0                        (zero)
//	e=0, m>=1:    m << (offset+1)          (subnormal)
//	e>=1:         (12+m) << (e+offset)     (normalized)
//
// This gives approximately 8.3% relative precision (12 values per octave)
// and a representable range from 32 to 503316480 with offset 4.
type Compact8 = uint8

const compact8offset = 4

// DecodeCompact8 converts a Compact8 value to an int64.
func DecodeCompact8(b Compact8) int64 {
	if b == 0 {
		return 0
	}
	e := int(b / 12)
	m := int(b % 12)
	if e == 0 {
		return int64(m) << (compact8offset + 1)
	}
	return int64(12+m) << (e + compact8offset)
}

// EncodeCompact8 encodes a non-negative int64 into Compact8 representation.
// The value is rounded down to the nearest representable value.
// Negative values go to 0 and values above the maximum representable go to 255.
func EncodeCompact8(value int64) Compact8 {
	if value <= 0 {
		return 0
	}

	uval := uint64(value)
	bitLen := bits.Len64(uval)

	// Find shift such that uval >> shift is in [12, 23].
	// This extracts the significand for the normalized representation.
	shift := max(bitLen-5, 0)
	sig := int(uval >> uint(shift))
	if sig >= 24 {
		shift++
		sig = int(uval >> uint(shift))
	}

	e := shift - compact8offset

	if e >= 1 {
		m := sig - 12
		b := e*12 + m
		if b > 255 {
			return 255
		}
		return Compact8(b)
	}

	// Subnormal: value = m << (offset + 1), m in [1, 11]
	m := int(uval >> (compact8offset + 1))
	if m < 1 {
		return 0
	}
	if m > 11 {
		m = 11
	}
	return Compact8(m)
}

// UpdateCompact8 returns the Compact8 encoding of value, but with hysteresis:
// it sticks to prev unless the new code is significantly closer. This prevents
// oscillation when the underlying value fluctuates near a bucket boundary.
func UpdateCompact8(value int64, prev Compact8) Compact8 {
	newCode := EncodeCompact8(value)
	if newCode == prev {
		return prev
	}
	newDist := value - DecodeCompact8(newCode) // always >= 0 (round-down)
	oldDist := DecodeCompact8(prev) - value
	if oldDist < 0 {
		oldDist = -oldDist
	}
	// Require the new code to be closer by at least half a bucket width
	// (at the smaller of the two exponent levels). This shifts the
	// transition point from the midpoint to the 3/4 mark of the gap,
	// creating a dead zone that prevents chatter.
	e := max(1, min(int(prev/12), int(newCode/12)))
	margin := int64(1) << (e + compact8offset - 1)
	if newDist < oldDist-margin {
		return newCode
	}
	return prev
}
