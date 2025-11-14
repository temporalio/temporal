package persistence

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
)

func TestExecutionPersistenceClient_DataLossMetrics_EmittedOnDataLossError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	// Enable data loss metrics
	enableDataLossMetrics := func() bool { return true }

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		enableDataLossMetrics,
	)

	ctx := context.Background()
	request := &GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace-id",
		WorkflowID:  "test-workflow-id",
		RunID:       "test-run-id",
	}

	// Mock the underlying call to return DataLoss error
	dataLossErr := serviceerror.NewDataLoss("test data loss error")
	mockExecutionManager.EXPECT().
		GetWorkflowExecution(ctx, request).
		Return(nil, dataLossErr)

	// Start capturing metrics
	capture := metricsHandler.StartCapture()

	// Call the method
	_, err := client.GetWorkflowExecution(ctx, request)

	// Verify error is returned
	require.Error(t, err)
	assert.True(t, errors.Is(err, dataLossErr))

	// Verify data loss metrics are emitted
	snapshot := capture.Snapshot()
	dataLossMetrics := snapshot[metrics.DataLossCounter.Name()]
	require.Len(t, dataLossMetrics, 1)

	recording := dataLossMetrics[0]
	assert.Equal(t, int64(1), recording.Value)

	// Verify tags are properly set (caller comes from context headers, which defaults to _unknown_)
	tags := recording.Tags
	assert.Equal(t, "_unknown_", tags[metrics.NamespaceTag("").Key])
	assert.Equal(t, "test-workflow-id", tags["workflow_id"])
	assert.Equal(t, "test-run-id", tags["run_id"])
	assert.Equal(t, metrics.PersistenceGetWorkflowExecutionScope, tags[metrics.OperationTag("").Key])
	assert.Equal(t, "test data loss error", tags["error"])
}

func TestExecutionPersistenceClient_DataLossMetrics_NotEmittedWhenDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	// Disable data loss metrics
	enableDataLossMetrics := func() bool { return false }

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		enableDataLossMetrics,
	)

	ctx := context.Background()
	request := &GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace-id",
		WorkflowID:  "test-workflow-id",
		RunID:       "test-run-id",
	}

	// Mock the underlying call to return DataLoss error
	dataLossErr := serviceerror.NewDataLoss("test data loss error")
	mockExecutionManager.EXPECT().
		GetWorkflowExecution(ctx, request).
		Return(nil, dataLossErr)

	// Start capturing metrics
	capture := metricsHandler.StartCapture()

	// Call the method
	_, err := client.GetWorkflowExecution(ctx, request)

	// Verify error is returned
	require.Error(t, err)

	// Verify no data loss metrics are emitted when disabled
	snapshot := capture.Snapshot()
	dataLossMetrics := snapshot[metrics.DataLossCounter.Name()]
	assert.Empty(t, dataLossMetrics)
}

func TestExecutionPersistenceClient_DataLossMetrics_NotEmittedOnNonDataLossError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	// Enable data loss metrics
	enableDataLossMetrics := func() bool { return true }

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		enableDataLossMetrics,
	)

	ctx := context.Background()
	request := &GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace-id",
		WorkflowID:  "test-workflow-id",
		RunID:       "test-run-id",
	}

	// Mock the underlying call to return a non-DataLoss error
	unavailableErr := serviceerror.NewUnavailable("service unavailable")
	mockExecutionManager.EXPECT().
		GetWorkflowExecution(ctx, request).
		Return(nil, unavailableErr)

	// Start capturing metrics
	capture := metricsHandler.StartCapture()

	// Call the method
	_, err := client.GetWorkflowExecution(ctx, request)

	// Verify error is returned
	require.Error(t, err)

	// Verify no data loss metrics are emitted for non-DataLoss errors
	snapshot := capture.Snapshot()
	dataLossMetrics := snapshot[metrics.DataLossCounter.Name()]
	assert.Empty(t, dataLossMetrics)
}

func TestExecutionPersistenceClient_DataLossMetrics_WithWrappedDataLossError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	// Enable data loss metrics
	enableDataLossMetrics := func() bool { return true }

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		enableDataLossMetrics,
	)

	ctx := context.Background()
	// Use a simpler request without complex nested types
	request := &GetCurrentExecutionRequest{
		ShardID:     1,
		NamespaceID: "test-namespace-id",
		WorkflowID:  "test-workflow-id",
	}

	// Mock the underlying call to return wrapped DataLoss error
	dataLossErr := serviceerror.NewDataLoss("original data loss error")
	wrappedErr := errors.Join(errors.New("wrapper error"), dataLossErr)
	mockExecutionManager.EXPECT().
		GetCurrentExecution(ctx, request).
		Return(nil, wrappedErr)

	// Start capturing metrics
	capture := metricsHandler.StartCapture()

	// Call the method
	_, err := client.GetCurrentExecution(ctx, request)

	// Verify error is returned
	require.Error(t, err)

	// Verify data loss metrics are emitted even for wrapped DataLoss errors
	snapshot := capture.Snapshot()
	dataLossMetrics := snapshot[metrics.DataLossCounter.Name()]
	require.Len(t, dataLossMetrics, 1)

	recording := dataLossMetrics[0]
	assert.Equal(t, int64(1), recording.Value)

	// Verify tags include workflow details (caller comes from context headers, which defaults to _unknown_)
	tags := recording.Tags
	assert.Equal(t, "_unknown_", tags[metrics.NamespaceTag("").Key])
	assert.Equal(t, "test-workflow-id", tags["workflow_id"])
	assert.Equal(t, "", tags["run_id"]) // No run ID in GetCurrentExecutionRequest
}

func TestExecutionPersistenceClient_DataLossMetrics_WithEmptyWorkflowDetails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	// Enable data loss metrics
	enableDataLossMetrics := func() bool { return true }

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		enableDataLossMetrics,
	)

	ctx := context.Background()
	request := &ListConcreteExecutionsRequest{
		ShardID:  1,
		PageSize: 10,
	}

	// Mock the underlying call to return DataLoss error
	dataLossErr := serviceerror.NewDataLoss("test data loss error")
	mockExecutionManager.EXPECT().
		ListConcreteExecutions(ctx, request).
		Return(nil, dataLossErr)

	// Start capturing metrics
	capture := metricsHandler.StartCapture()

	// Call the method
	_, err := client.ListConcreteExecutions(ctx, request)

	// Verify error is returned
	require.Error(t, err)

	// Verify data loss metrics are emitted even without workflow details
	snapshot := capture.Snapshot()
	dataLossMetrics := snapshot[metrics.DataLossCounter.Name()]
	require.Len(t, dataLossMetrics, 1)

	recording := dataLossMetrics[0]
	assert.Equal(t, int64(1), recording.Value)

	// Verify tags are set with empty workflow details
	tags := recording.Tags
	assert.Equal(t, "", tags["workflow_id"])
	assert.Equal(t, "", tags["run_id"])
	assert.Equal(t, metrics.PersistenceListConcreteExecutionsScope, tags[metrics.OperationTag("").Key])
}
