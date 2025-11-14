package matching

import (
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
