package nexusoperations_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
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
	// All cases drive a completion that arrives before the start response, so the handler
	// fabricates the started transition (emitting schedule-to-start) and then resolves the
	// operation to the terminal outcome under test.
	allOutcomeCounters := []string{
		chasmnexus.NexusOperationSuccessCount.Name(),
		chasmnexus.NexusOperationFailedCount.Name(),
		chasmnexus.NexusOperationCancelCount.Name(),
	}

	testCases := []struct {
		name        string
		result      *commonpb.Payload
		opErr       *nexus.OperationError
		wantCounter string
	}{
		{
			name:        "succeeded",
			result:      &commonpb.Payload{},
			wantCounter: chasmnexus.NexusOperationSuccessCount.Name(),
		},
		{
			name: "failed",
			opErr: &nexus.OperationError{
				State:           nexus.OperationStateFailed,
				Message:         "operation failed",
				OriginalFailure: &nexus.Failure{Message: "operation failed"},
			},
			wantCounter: chasmnexus.NexusOperationFailedCount.Name(),
		},
		{
			name: "canceled",
			opErr: &nexus.OperationError{
				State:           nexus.OperationStateCanceled,
				Message:         "operation canceled",
				OriginalFailure: &nexus.Failure{Message: "operation canceled"},
			},
			wantCounter: chasmnexus.NexusOperationCancelCount.Name(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &hsmtest.NodeBackend{}
			node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}))

			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)

			completionHandler := nexusoperations.NewCompletionHandler(captureHandler, &nexusoperations.Config{})
			require.NoError(t, completionHandler.Handle(
				context.Background(),
				fakeEnv{node},
				hsm.Ref{},
				"",      // requestID (empty skips the request-id match check)
				"token", // operationToken
				nil,     // startTime
				nil,     // links
				tc.result,
				tc.opErr,
			))

			snapshot := capture.Snapshot()

			counter := snapshot[tc.wantCounter]
			require.Len(t, counter, 1)
			require.Equal(t, int64(1), counter[0].Value)
			require.Equal(t, "namespace-name", counter[0].Tags["namespace"])
			require.Equal(t, "workflow-type", counter[0].Tags["workflowType"])
			require.Equal(t, "endpoint", counter[0].Tags["nexus_endpoint"])

			// Exactly the outcome counter under test is recorded, and no sibling counters leak.
			for _, name := range allOutcomeCounters {
				if name == tc.wantCounter {
					require.Len(t, snapshot[name], 1, "expected counter %s recorded", name)
					continue
				}
				require.Empty(t, snapshot[name], "unexpected counter %s recorded", name)
			}

			// The fabricated start emits schedule-to-start; the terminal close emits schedule-to-close.
			require.Len(t, snapshot[chasmnexus.NexusOperationScheduleToStartLatency.Name()], 1)
			require.Len(t, snapshot[chasmnexus.NexusOperationScheduleToCloseLatency.Name()], 1)
		})
	}
}
