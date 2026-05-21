package common

import "hash/fnv"

// RolloutAccepts reports whether (namespace, businessID) falls within the
// given percentage [0,100] using a stable hash, so dialing the percent up is
// monotonic.
//
// The hash function and its inputs (algorithm, input order, separator byte)
// are a load-bearing rollout key: changing any of them re-shuffles every
// namespace's cohort and breaks monotonicity for in-flight rollouts.
func RolloutAccepts(namespace, businessID string, percent int) bool {
	if percent >= 100 {
		return true
	}
	if percent <= 0 {
		return false
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(businessID))
	return int(h.Sum32()%100) < percent
}
