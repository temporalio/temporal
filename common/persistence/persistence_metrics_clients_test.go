package persistence

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
		func() bool { return false }, // enableCurrentRecordMissingMetric
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
		func() bool { return false }, // enableCurrentRecordMissingMetric
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
		func() bool { return false }, // enableCurrentRecordMissingMetric
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
		func() bool { return false }, // enableCurrentRecordMissingMetric
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
		func() bool { return false }, // enableCurrentRecordMissingMetric
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

func TestExecutionPersistenceClient_CurrentRecordMissingMetric_EmittedOnMissingRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		func() bool { return false }, // enableDataLossMetrics
		func() bool { return true },  // enableCurrentRecordMissingMetric
	)

	ctx := context.Background()
	request := &UpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: WorkflowMutation{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{WorkflowId: "test-workflow-id"},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "test-run-id"},
		},
	}

	// CurrentWorkflowConditionFailedError with empty RunID = missing current record (corruption)
	missingRecordErr := &CurrentWorkflowConditionFailedError{
		Msg:   "current execution record not found",
		RunID: "", // empty RunID signals missing record
	}
	mockExecutionManager.EXPECT().
		UpdateWorkflowExecution(ctx, request).
		Return(nil, missingRecordErr)

	capture := metricsHandler.StartCapture()

	_, err := client.UpdateWorkflowExecution(ctx, request)
	require.Error(t, err)

	snapshot := capture.Snapshot()
	missingRecordMetrics := snapshot[metrics.CurrentRecordMissingCounter.Name()]
	require.Len(t, missingRecordMetrics, 1)

	recording := missingRecordMetrics[0]
	assert.Equal(t, int64(1), recording.Value)

	tags := recording.Tags
	assert.Equal(t, "test-workflow-id", tags["workflow_id"])
	assert.Equal(t, "test-run-id", tags["run_id"])
	assert.Equal(t, metrics.PersistenceUpdateWorkflowExecutionScope, tags[metrics.OperationTag("").Key])
}

func TestExecutionPersistenceClient_CurrentRecordMissingMetric_NotEmittedForNormalConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		func() bool { return false }, // enableDataLossMetrics
		func() bool { return true },  // enableCurrentRecordMissingMetric - enabled but should NOT fire
	)

	ctx := context.Background()
	request := &UpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: WorkflowMutation{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{WorkflowId: "test-workflow-id"},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "test-run-id"},
		},
	}

	// CurrentWorkflowConditionFailedError with non-empty RunID = normal conflict, NOT corruption
	normalConflictErr := &CurrentWorkflowConditionFailedError{
		Msg:   "current workflow conflict",
		RunID: "other-run-id", // non-empty RunID means another run owns the current record
	}
	mockExecutionManager.EXPECT().
		UpdateWorkflowExecution(ctx, request).
		Return(nil, normalConflictErr)

	capture := metricsHandler.StartCapture()

	_, err := client.UpdateWorkflowExecution(ctx, request)
	require.Error(t, err)

	// Metric should NOT be emitted for normal conflict
	snapshot := capture.Snapshot()
	missingRecordMetrics := snapshot[metrics.CurrentRecordMissingCounter.Name()]
	assert.Empty(t, missingRecordMetrics)
}

func TestExecutionPersistenceClient_CurrentRecordMissingMetric_NotEmittedWhenDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		func() bool { return false }, // enableDataLossMetrics
		func() bool { return false }, // enableCurrentRecordMissingMetric - disabled
	)

	ctx := context.Background()
	request := &UpdateWorkflowExecutionRequest{
		ShardID: 1,
		UpdateWorkflowMutation: WorkflowMutation{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{WorkflowId: "test-workflow-id"},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "test-run-id"},
		},
	}

	// Missing current record error - but metric is disabled
	missingRecordErr := &CurrentWorkflowConditionFailedError{
		Msg:   "current execution record not found",
		RunID: "", // empty RunID = corruption signal
	}
	mockExecutionManager.EXPECT().
		UpdateWorkflowExecution(ctx, request).
		Return(nil, missingRecordErr)

	capture := metricsHandler.StartCapture()

	_, err := client.UpdateWorkflowExecution(ctx, request)
	require.Error(t, err)

	// Metric should NOT be emitted when disabled
	snapshot := capture.Snapshot()
	missingRecordMetrics := snapshot[metrics.CurrentRecordMissingCounter.Name()]
	assert.Empty(t, missingRecordMetrics)
}

func TestExecutionPersistenceClient_CurrentRecordMissingMetric_ConflictResolvePath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewNoopLogger()
	mockExecutionManager := NewMockExecutionManager(ctrl)
	healthSignals := NoopHealthSignalAggregator

	client := NewExecutionPersistenceMetricsClient(
		mockExecutionManager,
		metricsHandler,
		healthSignals,
		logger,
		func() bool { return false }, // enableDataLossMetrics
		func() bool { return true },  // enableCurrentRecordMissingMetric
	)

	ctx := context.Background()
	request := &ConflictResolveWorkflowExecutionRequest{
		ShardID: 1,
		ResetWorkflowSnapshot: WorkflowSnapshot{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{WorkflowId: "resolve-workflow-id"},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "resolve-run-id"},
		},
	}

	missingRecordErr := &CurrentWorkflowConditionFailedError{
		Msg:   "current execution record not found",
		RunID: "",
	}
	mockExecutionManager.EXPECT().
		ConflictResolveWorkflowExecution(ctx, request).
		Return(nil, missingRecordErr)

	capture := metricsHandler.StartCapture()

	_, err := client.ConflictResolveWorkflowExecution(ctx, request)
	require.Error(t, err)

	snapshot := capture.Snapshot()
	missingRecordMetrics := snapshot[metrics.CurrentRecordMissingCounter.Name()]
	require.Len(t, missingRecordMetrics, 1)

	recording := missingRecordMetrics[0]
	assert.Equal(t, int64(1), recording.Value)

	tags := recording.Tags
	assert.Equal(t, "resolve-workflow-id", tags["workflow_id"])
	assert.Equal(t, "resolve-run-id", tags["run_id"])
	assert.Equal(t, metrics.PersistenceConflictResolveWorkflowExecutionScope, tags[metrics.OperationTag("").Key])
}

func TestIsMissingCurrentRecordError(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		want   bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "unrelated error",
			err:  errors.New("something else"),
			want: false,
		},
		{
			name: "ConditionFailedError is not a match",
			err:  &ConditionFailedError{Msg: "generic condition failed"},
			want: false,
		},
		{
			name: "CurrentWorkflowConditionFailedError with non-empty RunID",
			err:  &CurrentWorkflowConditionFailedError{Msg: "conflict", RunID: "some-run-id"},
			want: false,
		},
		{
			name: "CurrentWorkflowConditionFailedError with empty RunID",
			err:  &CurrentWorkflowConditionFailedError{Msg: "missing", RunID: ""},
			want: true,
		},
		{
			name: "wrapped CurrentWorkflowConditionFailedError with empty RunID",
			err:  fmt.Errorf("outer: %w", &CurrentWorkflowConditionFailedError{Msg: "missing", RunID: ""}),
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsMissingCurrentRecordError(tc.err)
			require.Equal(t, tc.want, got)
		})
	}
}
