package matching

import commonpb "go.temporal.io/api/common/v1"

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
	existing map[string]float32,
	set map[string]float32,
	unset []string,
	maxFairnessKeyWeightOverrides int,
) (map[string]float32, error) {
	res := make(map[string]float32, len(existing))
	for k, v := range existing {
		res[k] = v
	}

	for _, k := range unset {
		delete(res, k)
	}

	for k, w := range set {
		res[k] = w
	}

	if len(res) > maxFairnessKeyWeightOverrides {
		return nil, errFairnessOverridesUpdateRejected
	}

	return res, nil
}
