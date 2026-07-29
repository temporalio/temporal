package dynamicconfig

import (
	"math"

	"github.com/dgryski/go-farm"
)

// RolloutAccepts reports whether key falls within the given percentage [0,100]
// using a stable hash, so dialing the percent up is monotonic: a key accepted
// at percent P is accepted at every percent >= P.
//
// The hash algorithm and the bytes of key are a load-bearing rollout key:
// changing either re-shuffles every cohort and breaks monotonicity for
// in-flight rollouts. Callers are responsible for constructing key (including
// any separator bytes between fields) and must not change that construction
// once a rollout is in progress.
func RolloutAccepts(key []byte, percent int) bool {
	if percent >= 100 {
		return true
	}
	if percent <= 0 {
		return false
	}
	threshold := uint32(float64(percent) / 100.0 * float64(math.MaxUint32))
	return farm.Fingerprint32(key) < threshold
}
