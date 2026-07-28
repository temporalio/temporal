package matching

import (
	"hash/maphash"
	"maps"

	commonpb "go.temporal.io/api/common/v1"
)

const (
	// minWeight * strideFactor must be >= 1
	strideFactor = 1000
	minWeight    = 0.001
)

type fairnessWeightOverrides map[string]float32

func getEffectiveWeight(overrides fairnessWeightOverrides, pri *commonpb.Priority) float32 {
	key := pri.GetFairnessKey()
	weight, ok := overrides[key]
	if !ok {
		weight = pri.GetFairnessWeight()
	}
	// zero means default weight (1.0). negative doesn't make sense, map it to 1.0 also.
	if weight <= 0.0 {
		weight = 1.0
	} else {
		weight = max(weight, minWeight)
	}
	return weight
}

// ditherPass spreads a new/reset key's starting pass deterministically over its initial
// stride [base, base+inc), based on a stable hash of the key. This keeps low-weight
// (large-stride) keys from all clumping at the ack level after a counter reset (e.g.
// partition movement), which would otherwise let them block dispatch of higher-weight
// keys. Established keys are unaffected: GetPass returns max(base, prev+inc), and a key
// already above the ack level has prev >= base, so prev+inc > base+(inc-1) always wins.
func ditherPass(seed maphash.Seed, key string, base, inc int64) int64 {
	return base + int64(maphash.Comparable(seed, key)%uint64(inc))
}

func mergeFairnessWeightOverrides(
	existing fairnessWeightOverrides,
	set fairnessWeightOverrides,
	unset []string,
	maxFairnessKeyWeightOverrides int,
) (fairnessWeightOverrides, error) {
	if len(existing) == 0 {
		// Validation already made sure that no keys of unset and set equal.
		return set, nil
	}

	res := maps.Clone(existing)

	for _, k := range unset {
		delete(res, k)
	}

	maps.Copy(res, set)

	if len(res) > maxFairnessKeyWeightOverrides {
		return nil, errFairnessOverridesUpdateRejected
	}

	return res, nil
}
