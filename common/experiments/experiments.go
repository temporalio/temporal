package experiments

import (
	"context"
	"strings"

	"go.temporal.io/server/common/headers"
	"google.golang.org/grpc/metadata"
)

// Experimental feature names that can be enabled via the x-temporal-experimental header
const (
	// ChasmScheduler enables the CHASM (V2) scheduler implementation for new schedules
	ChasmScheduler = "chasm-sch"
)

// IsExperimentEnabled checks if a specific experiment is present in the x-temporal-experimental header
func IsExperimentEnabled(ctx context.Context, experiment string) bool {
	experimentalValues := metadata.ValueFromIncomingContext(ctx, headers.ExperimentalHeaderName)

	for _, headerValue := range experimentalValues {
		if headerValue == "" {
			continue
		}
		// Split by comma in case multiple experiments are sent
		requestedExperiments := strings.Split(headerValue, ",")
		for _, requested := range requestedExperiments {
			requested = strings.TrimSpace(requested)
			if strings.EqualFold(requested, experiment) {
				return true
			}
		}
	}

	return false
}
