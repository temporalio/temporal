package debug

import (
	"os"
	"strconv"
)

const otelDebugModeEnvVar = "TEMPORAL_OTEL_DEBUG"

// OTelDebugMode reports whether the TEMPORAL_OTEL_DEBUG environment variable enables
// verbose OpenTelemetry tracing.
func OTelDebugMode() bool {
	isDebug, err := strconv.ParseBool(os.Getenv(otelDebugModeEnvVar))
	if err != nil {
		return false
	}
	return isDebug
}
