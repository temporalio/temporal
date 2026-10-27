package ringpop

import (
	"math/bits"
	"time"

	"github.com/dgryski/go-farm"
)

// presence describes when a joining or leaving host is present in the ring for a given key.
// With a gradual change window, a host may be present for some keys and not others, depending
// on the key and the current time. Hosts without a presence are present for all keys.
//
// Note that the result of a lookup with some hosts filtered out is the same as a lookup on a
// ring that only contains the included hosts, so this is equivalent to (and much cheaper than)
// building a separate ring for each subset of hosts.
type presence struct {
	// If joinStart is non-zero, the host is present for no keys before joinStart and for all
	// keys at and after joinEnd. In between, it becomes present for each key at a different
	// time.
	joinStart, joinEnd time.Time
	// If leaveStart is non-zero, the host is present for all keys before leaveStart and for no
	// keys at and after leaveEnd. In between, it stops being present for each key at a
	// different time.
	leaveStart, leaveEnd time.Time
}

func (p *presence) present(keyHash uint32, now time.Time) bool {
	if !p.joinStart.IsZero() && now.Before(switchTime(p.joinStart, p.joinEnd, keyHash)) {
		return false
	}
	if !p.leaveStart.IsZero() && !now.Before(switchTime(p.leaveStart, p.leaveEnd, keyHash)) {
		return false
	}
	return true
}

// gradualEnd returns the end of the latest gradual window in p, or the zero time.
func (p *presence) gradualEnd() time.Time {
	var end time.Time
	if p.joinEnd.After(p.joinStart) {
		end = p.joinEnd
	}
	if p.leaveEnd.After(p.leaveStart) && p.leaveEnd.After(end) {
		end = p.leaveEnd
	}
	return end
}

// gradualKeyHash returns the hash used to pick the switch time for a key. It must not be
// correlated with the hash used to place keys on the ring: the keys that move to or from one
// host lie in a small number of short arcs of the ring, and if switch times were derived from
// ring position, they would be clustered into short bursts instead of spread over the window.
// The ring uses farm.Fingerprint32, so we use farm.Fingerprint64, which is an unrelated
// algorithm. (Fingerprint functions, unlike farm's Hash functions, are guaranteed stable
// across versions, which matters since all hosts must agree on switch times.)
func gradualKeyHash(key string) uint32 {
	return uint32(farm.Fingerprint64([]byte(key)) >> 32)
}

// switchTime returns the time within [start, end] at which a key with the given hash
// switches. Switch times are distributed uniformly over the window. This uses only integer
// arithmetic so that all hosts compute exactly the same time.
func switchTime(start, end time.Time, keyHash uint32) time.Time {
	window := end.Sub(start)
	if window <= 0 {
		return start
	}
	// offset = window * (keyHash+1) / 2^32, which is in [0, window].
	hi, lo := bits.Mul64(uint64(window), uint64(keyHash)+1)
	offset := hi<<32 | lo>>32
	return start.Add(time.Duration(offset))
}
