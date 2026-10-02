package health

import (
	enumspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
)

type SignalReader interface {
	LatencyQuantile(quantile float64) (float64, bool)
	LatencyQuantileByGroup(groupName string, quantile float64) (float64, bool)
	ErrorRatio() (float64, bool)
	ErrorRatioByGroup(groupName string) (float64, bool)
}

// Evaluate compares the reader's signals against the settings and returns one check per
// configured threshold. Checks are named after the source, so callers evaluating more than
// one source (e.g. gRPC and persistence) get names that stay distinct in a single response.
func Evaluate(reader SignalReader, settings Settings, source Source) []*healthspb.HealthCheck {
	var checks []*healthspb.HealthCheck

	// //////////////////
	// overall latency
	// //////////////////

	for _, qt := range settings.Overall.QuantileThresholds {
		latency, found := reader.LatencyQuantile(qt.Quantile)
		if !found {
			continue
		}

		checks = append(checks, errorIfOverThreshold(
			source.overallCheckType(CheckTypeLatency, qt.Quantile),
			latency,
			float64(qt.Threshold.Milliseconds()),
			settings.Overall.Enforced,
		))
	}

	// //////////////////
	// overall error ratio
	// //////////////////

	if settings.Overall.ErrorRatioThreshold != nil {
		errorRatio, found := reader.ErrorRatio()
		if found {
			checks = append(checks, errorIfOverThreshold(
				source.overallCheckType(CheckTypeErrorRatio, 0),
				errorRatio,
				settings.Overall.ErrorRatioThreshold.Threshold,
				settings.Overall.Enforced,
			))
		}
	}

	// //////////////////
	// groups
	// //////////////////

	for _, group := range settings.Groups {
		for _, qt := range group.Thresholds.QuantileThresholds {
			latency, found := reader.LatencyQuantileByGroup(group.Name, qt.Quantile)
			if !found {
				continue
			}

			checks = append(checks, errorIfOverThreshold(
				source.groupCheckType(group.Name, CheckTypeLatency, qt.Quantile),
				latency,
				float64(qt.Threshold.Milliseconds()),
				group.Thresholds.Enforced,
			))
		}

		if group.Thresholds.ErrorRatioThreshold != nil {
			errorRatio, found := reader.ErrorRatioByGroup(group.Name)
			if found {
				checks = append(checks, errorIfOverThreshold(
					source.groupCheckType(group.Name, CheckTypeErrorRatio, 0),
					errorRatio,
					group.Thresholds.ErrorRatioThreshold.Threshold,
					group.Thresholds.Enforced,
				))
			}
		}
	}

	return checks
}

// RollupState returns the state and the unenforced state
func RollupState(checks []*healthspb.HealthCheck) (enumspb.HealthState, enumspb.HealthState) {
	state := enumspb.HEALTH_STATE_SERVING
	unenforcedState := enumspb.HEALTH_STATE_SERVING

	for _, check := range checks {
		if check.State == enumspb.HEALTH_STATE_SERVING {
			continue
		}

		// an unhealthy check always counts towards the unenforced state
		unenforcedState = enumspb.HEALTH_STATE_NOT_SERVING

		if check.Enforced {
			state = enumspb.HEALTH_STATE_NOT_SERVING
		}
	}

	return state, unenforcedState
}

func errorIfOverThreshold(checkType string, value float64, threshold float64, enforced bool) *healthspb.HealthCheck {
	state := enumspb.HEALTH_STATE_SERVING
	if value > threshold {
		state = enumspb.HEALTH_STATE_NOT_SERVING
	}

	return &healthspb.HealthCheck{
		CheckType: checkType,
		State:     state,
		Value:     value,
		Threshold: threshold,
		Enforced:  enforced,
	}
}
