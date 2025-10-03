package interceptor

import "fmt"

// PreTelemetryError wraps errors that occur in interceptors before TelemetryInterceptor.
// This allows early interceptors (like ServiceErrorInterceptor) to detect and handle
// errors from pre-telemetry interceptors for metrics and logging purposes.
type PreTelemetryError struct {
	Cause  error  // The original error
	Source string // Which interceptor created this error (for debugging)
}

func (e *PreTelemetryError) Error() string {
	if e.Source != "" {
		return fmt.Sprintf("[%s] %v", e.Source, e.Cause)
	}
	return e.Cause.Error()
}

func (e *PreTelemetryError) Unwrap() error {
	return e.Cause
}

// NewPreTelemetryError wraps an error to mark it as originating before TelemetryInterceptor.
// This allows ServiceErrorInterceptor to detect and emit metrics/logs for pre-telemetry errors.
func NewPreTelemetryError(source string, err error) error {
	if err == nil {
		return nil
	}
	return &PreTelemetryError{
		Cause:  err,
		Source: source,
	}
}
