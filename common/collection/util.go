package collection

import (
	"encoding/binary"
	"encoding/hex"
)

// UUIDHashCode is a hash function for hashing string uuid
// if the uuid is malformed, then the hash function always
// returns 0 as the hash value
func UUIDHashCode(input interface{}) uint32 {
	key, ok := input.(string)
	if !ok {
		return 0
	}
	if len(key) != UUIDStringLength {
		return 0
	}
	// Use the first 4 bytes of the uuid as the hash
	b, err := hex.DecodeString(key[:8])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

// MinInt returns the min of given two integers
func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// MaxInt returns the max of given two integers
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinInt64 returns the min of given two integers
func MinInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

// MaxInt64 returns the max of given two integers
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
