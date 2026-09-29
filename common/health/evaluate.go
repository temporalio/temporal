package health

import (
	"fmt"

	enumspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
)

type SignalReader interface {
	LatencyQuantile(quantile float64) (float64, bool)
	LatencyQuantileByGroup(groupName string, quantile float64) (float64, bool)
	ErrorRatio() (float64, bool)
	ErrorRatioByGroup(groupName string) (float64, bool)
}

// Evaluate returns the check results, state, and unenforced state
func Evaluate(reader SignalReader, settings Settings, label string) ([]*healthspb.HealthCheck, enumspb.HealthState, enumspb.HealthState) {
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
			CheckTypeRPCLatencyOverall+fmt.Sprintf("_P%0.2f", 100.0*qt.Quantile),
			latency,
			float64(qt.Threshold.Milliseconds()),
			fmt.Sprintf("history service overall percentile latency (P%0.2f < %dms, enforced: %t)", 100.0*qt.Quantile, qt.Threshold.Milliseconds(), settings.Overall.Enforced),
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
				CheckTypeRPCErrorRatioOverall,
				errorRatio,
				settings.Overall.ErrorRatioThreshold.Threshold,
				fmt.Sprintf("history service overall error ratio (< %0.2f, enforced: %t)", settings.Overall.ErrorRatioThreshold.Threshold, settings.Overall.Enforced),
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
				fmt.Sprintf("%s_%s_P%0.2f", CheckTypeRPCLatencyGroup, group.Name, 100.0*qt.Quantile),
				latency,
				float64(qt.Threshold.Milliseconds()),
				fmt.Sprintf("history service %s group percentile latency (P%0.2f < %dms, enforced: %t)", group.Name, 100.0*qt.Quantile, qt.Threshold.Milliseconds(), group.Thresholds.Enforced),
				group.Thresholds.Enforced,
			))
		}

		if group.Thresholds.ErrorRatioThreshold != nil {
			errorRatio, found := reader.ErrorRatioByGroup(group.Name)
			if found {
				checks = append(checks, errorIfOverThreshold(
					fmt.Sprintf("%s_%s", CheckTypeRPCErrorRatioGroup, group.Name),
					errorRatio,
					group.Thresholds.ErrorRatioThreshold.Threshold,
					fmt.Sprintf("history service %s group error ratio (< %0.2f, enforced: %t)", group.Name, group.Thresholds.ErrorRatioThreshold.Threshold, group.Thresholds.Enforced),
					group.Thresholds.Enforced,
				))
			}
		}
	}

	// //////////////////
	// state
	// //////////////////

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

	return checks, state, unenforcedState
}

func errorIfOverThreshold(checkType string, value float64, threshold float64, message string, enforced bool) *healthspb.HealthCheck {
	state := enumspb.HEALTH_STATE_SERVING
	if value > threshold {
		state = enumspb.HEALTH_STATE_NOT_SERVING
	}

	return &healthspb.HealthCheck{
		CheckType: checkType,
		State:     state,
		Value:     value,
		Threshold: threshold,
		Message:   message,
		Enforced:  enforced,
	}
}
