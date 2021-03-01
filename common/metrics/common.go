package metrics

import "time"

const (
	distributionToTimerRatio = int(time.Millisecond / time.Nanosecond)
)

func mergeMapToRight(src map[string]string, dest map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
}

func getMetricDefs(serviceIdx ServiceIdx) map[int]metricDefinition {
	defs := make(map[int]metricDefinition)
	for idx, def := range MetricDefs[Common] {
		defs[idx] = def
	}

	for idx, def := range MetricDefs[serviceIdx] {
		defs[idx] = def
	}

	return defs
}
