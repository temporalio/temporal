package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.temporal.io/server/common/log"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"
)

type runAlreadyExistsCheckingStore struct {
	*mockp.MockExecutionStore
}

func (runAlreadyExistsCheckingStore) SupportsCheckRunAlreadyExists() bool {
	return true
}

func TestTelemetryExecutionStore_SupportsCheckRunAlreadyExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	tracer := noop.NewTracerProvider().Tracer("test")
	require.True(t, newTelemetryExecutionStore(runAlreadyExistsCheckingStore{mockp.NewMockExecutionStore(ctrl)}, log.NewNoopLogger(), tracer).SupportsCheckRunAlreadyExists())
	require.False(t, newTelemetryExecutionStore(mockp.NewMockExecutionStore(ctrl), log.NewNoopLogger(), tracer).SupportsCheckRunAlreadyExists())
}
