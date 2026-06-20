package nexusoperations_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCompletionHandler_EmitsCallerMetrics(t *testing.T) {
	backend := &hsmtest.NodeBackend{}
	node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
		ScheduleToCloseTimeout: durationpb.New(time.Hour),
	}))

	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	// Completion arrives before the start response: the handler fabricates the started transition
	// (emitting schedule-to-start) and then resolves the operation as succeeded.
	require.NoError(t, nexusoperations.CompletionHandler(
		context.Background(),
		fakeEnv{node},
		hsm.Ref{},
		"",                  // requestID (empty skips the request-id match check)
		"token",             // operationToken
		nil,                 // startTime
		nil,                 // links
		&commonpb.Payload{}, // success result
		nil,                 // opFailedError
		captureHandler,
		chasmnexus.NexusMetricTagConfig{},
	))

	snapshot := capture.Snapshot()

	successCount := snapshot[chasmnexus.NexusOperationSuccessCount.Name()]
	require.Len(t, successCount, 1)
	require.Equal(t, int64(1), successCount[0].Value)
	require.Equal(t, "namespace-name", successCount[0].Tags["namespace"])
	require.Equal(t, "workflow-type", successCount[0].Tags["workflowType"])
	require.Equal(t, "endpoint", successCount[0].Tags["nexus_endpoint"])
	require.Equal(t, "hsm", successCount[0].Tags["impl"])

	// Both the schedule-to-start (from the fabricated start) and schedule-to-close latencies are recorded.
	require.Len(t, snapshot[chasmnexus.NexusOperationScheduleToStartLatency.Name()], 1)
	require.Len(t, snapshot[chasmnexus.NexusOperationScheduleToCloseLatency.Name()], 1)
}
