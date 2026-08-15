package umpiretest

import (
	"os"
	"sync"
)

var (
	processInstrumentationOnce sync.Once
	processInstrumentationErr  error
)

// ConfigureProcessInstrumentation enables required process-wide observation exactly once.
func ConfigureProcessInstrumentation() error {
	processInstrumentationOnce.Do(func() {
		processInstrumentationErr = os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
	})
	return processInstrumentationErr
}
