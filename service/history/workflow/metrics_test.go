package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEmitWorkflowCompletionStats_WorkflowDuration(t *testing.T) {
	logger := log.NewTestLogger()
	testHandler, _ := metricstest.NewHandler(logger, metrics.ClientConfig{})
	testNamespace := namespace.Name("test-namespace")
	config := &configs.Config{
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}

	completionMetric := completionMetric{
		initialized:      true,
		isWorkflow:       true,
		taskQueue:        "test-task-queue",
		namespaceState:   "active",
		workflowTypeName: "test-workflow",
		status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		startTime:        timestamppb.New(time.Unix(100, 0)),
		closeTime:        timestamppb.New(time.Unix(130, 0)),
	}

	emitWorkflowCompletionStats(testHandler, testNamespace, completionMetric, config)

	snapshot, err := testHandler.Snapshot()
	require.NoError(t, err)
	buckets, err := snapshot.Histogram("workflow_duration_milliseconds",

		metrics.StringTag("namespace", "test-namespace"),
		metrics.StringTag("namespace_state", "active"),
		metrics.StringTag("workflowType", "test-workflow"),
		metrics.StringTag("operation", "CompletionStats"),
		metrics.StringTag("taskqueue", "test-task-queue"),
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	)
	require.NoError(t, err)
	require.NotEmpty(t, buckets)
}

func TestEmitWorkflowCompletionStats_SkipNonWorkflow(t *testing.T) {
	logger := log.NewTestLogger()
	testHandler, _ := metricstest.NewHandler(logger, metrics.ClientConfig{})
	testNamespace := namespace.Name("test-namespace")
	completionMetric := completionMetric{isWorkflow: false}
	emitWorkflowCompletionStats(testHandler, testNamespace, completionMetric, nil)
	snapshot, err := testHandler.Snapshot()
	require.NoError(t, err)
	_, err = snapshot.Histogram("workflow_duration_milliseconds")
	require.Error(t, err)
}
