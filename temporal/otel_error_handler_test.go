package temporal

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
)

func TestOTELLoggerErrorHandlerUsesNewestRegistration(t *testing.T) {
	handler := otelLoggerErrorHandler{}
	firstLogger := &otelErrorHandlerTestLogger{}
	secondLogger := &otelErrorHandlerTestLogger{}
	handler.add(&otelLoggerErrorHandlerRegistration{logger: firstLogger})
	handler.add(&otelLoggerErrorHandlerRegistration{logger: secondLogger})

	handler.Handle(errors.New("test error"))

	require.Equal(t, int32(0), firstLogger.warns.Load())
	require.Equal(t, int32(1), secondLogger.warns.Load())
}

func TestOTELLoggerErrorHandlerFallsBackAfterRemovingNewestRegistration(t *testing.T) {
	handler := otelLoggerErrorHandler{}
	firstLogger := &otelErrorHandlerTestLogger{}
	secondLogger := &otelErrorHandlerTestLogger{}
	firstRegistration := &otelLoggerErrorHandlerRegistration{logger: firstLogger}
	secondRegistration := &otelLoggerErrorHandlerRegistration{logger: secondLogger}
	handler.add(firstRegistration)
	handler.add(secondRegistration)

	handler.remove(secondRegistration)
	handler.Handle(errors.New("test error"))

	require.Equal(t, int32(1), firstLogger.warns.Load())
	require.Equal(t, int32(0), secondLogger.warns.Load())
}

func TestOTELLoggerErrorHandlerKeepsNewestAfterRemovingOlderRegistration(t *testing.T) {
	handler := otelLoggerErrorHandler{}
	firstLogger := &otelErrorHandlerTestLogger{}
	secondLogger := &otelErrorHandlerTestLogger{}
	firstRegistration := &otelLoggerErrorHandlerRegistration{logger: firstLogger}
	handler.add(firstRegistration)
	handler.add(&otelLoggerErrorHandlerRegistration{logger: secondLogger})

	handler.remove(firstRegistration)
	handler.Handle(errors.New("test error"))

	require.Equal(t, int32(0), firstLogger.warns.Load())
	require.Equal(t, int32(1), secondLogger.warns.Load())
}

func TestOTELLoggerErrorHandlerRemoveReleasesLogger(t *testing.T) {
	handler := otelLoggerErrorHandler{}
	logger := &otelErrorHandlerTestLogger{}
	loggerRef := weak.Make(logger)
	registration := &otelLoggerErrorHandlerRegistration{logger: logger}
	handler.add(registration)

	logger = nil
	handler.remove(registration)

	require.Eventually(t, func() bool {
		runtime.GC()
		return loggerRef.Value() == nil
	}, time.Second, 10*time.Millisecond)
	runtime.KeepAlive(registration)
}

type otelErrorHandlerTestLogger struct {
	warns   atomic.Int32
	padding [16]byte
}

func (l *otelErrorHandlerTestLogger) Debug(string, ...tag.Tag)  {}
func (l *otelErrorHandlerTestLogger) Info(string, ...tag.Tag)   {}
func (l *otelErrorHandlerTestLogger) Warn(string, ...tag.Tag)   { l.warns.Add(1) }
func (l *otelErrorHandlerTestLogger) Error(string, ...tag.Tag)  {}
func (l *otelErrorHandlerTestLogger) DPanic(string, ...tag.Tag) {}
func (l *otelErrorHandlerTestLogger) Panic(string, ...tag.Tag)  {}
func (l *otelErrorHandlerTestLogger) Fatal(string, ...tag.Tag)  {}
