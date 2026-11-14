package nexusoperations_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCompletionHandlerRejectsInvalidRequestID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, requestID string }{
		{"empty", ""},
		{"mismatched", "other-request-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
				RequestId: "request-id", ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}))
			handler := nexusoperations.NewCompletionHandler(metricstest.NewCaptureHandler(), &nexusoperations.Config{})
			err := handler.Handle(context.Background(), fakeEnv{node: node}, hsm.Ref{}, tc.requestID, "token", nil, nil, &commonpb.Payload{}, nil)
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, op.NexusOperationInfo.State)
		})
	}
}

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
		name              string
		result            *commonpb.Payload
		opErr             *nexus.OperationError
		wantCounter       string
		wantReason        string
		includeBackendTag bool
	}{
		{
			name:              "succeeded",
			result:            &commonpb.Payload{},
			wantCounter:       chasmnexus.NexusOperationSuccessCount.Name(),
			includeBackendTag: true,
		},
		{
			name: "failed",
			opErr: &nexus.OperationError{
				State:           nexus.OperationStateFailed,
				Message:         "operation failed",
				OriginalFailure: &nexus.Failure{Message: "operation failed"},
			},
			wantCounter: chasmnexus.NexusOperationFailedCount.Name(),
			wantReason:  "operation_error",
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
				RequestId:              "request-id",
			}))

			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)

			completionHandler := nexusoperations.NewCompletionHandler(captureHandler, &nexusoperations.Config{
				MetricTagConfig: dynamicconfig.GetTypedPropertyFn(chasmnexus.NexusMetricTagConfig{IncludeBackendTag: tc.includeBackendTag}),
			})
			require.NoError(t, completionHandler.Handle(
				context.Background(),
				fakeEnv{node: node},
				hsm.Ref{},
				"request-id", // requestID
				"token",      // operationToken
				nil,          // startTime
				nil,          // links
				tc.result,
				tc.opErr,
			))

			snapshot := capture.Snapshot()
			for metric, recordings := range snapshot {
				for _, recording := range recordings {
					if tc.includeBackendTag {
						require.Equal(t, "hsm", recording.Tags["backend"], metric)
					} else {
						require.NotContains(t, recording.Tags, "backend", metric)
					}
				}
			}

			counter := snapshot[tc.wantCounter]
			require.Len(t, counter, 1)
			require.Equal(t, int64(1), counter[0].Value)
			require.Equal(t, "namespace-name", counter[0].Tags["namespace"])
			require.Equal(t, "workflow-type", counter[0].Tags["workflowType"])
			require.Equal(t, "endpoint", counter[0].Tags["nexus_endpoint"])
			require.Equal(t, tc.wantReason, counter[0].Tags["reason"], "only the failed counter carries a reason")

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

func TestCompletionHandler_AlreadyStartedMetricsUseWorkflowTime(t *testing.T) {
	workflowNow := time.Date(2027, time.January, 1, 12, 0, 0, 0, time.UTC)
	scheduledTime := workflowNow.Add(-time.Minute)
	startedTime := workflowNow.Add(-30 * time.Second)
	wallNow := workflowNow.Add(-time.Hour)

	backend := &hsmtest.NodeBackend{WorkflowTimeFn: func() time.Time { return workflowNow }}
	node := newOperationNode(t, backend, mustNewScheduledEvent(scheduledTime, &historypb.NexusOperationScheduledEventAttributes{
		ScheduleToCloseTimeout: durationpb.New(time.Hour),
		RequestId:              "request-id",
	}))
	require.NoError(t, hsm.MachineTransition(node, func(operation nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionStarted.Apply(operation, nexusoperations.EventStarted{
			Time: startedTime,
			Node: node,
			Attributes: &historypb.NexusOperationStartedEventAttributes{
				OperationToken: "token",
			},
		})
	}))

	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	completionHandler := nexusoperations.NewCompletionHandler(captureHandler, &nexusoperations.Config{})
	require.NoError(t, completionHandler.Handle(
		context.Background(),
		fakeEnv{node: node, nowFn: func() time.Time { return wallNow }},
		hsm.Ref{},
		"request-id", // requestID
		"token",      // operationToken
		nil,          // startTime
		nil,          // links
		&commonpb.Payload{},
		nil,
	))

	snapshot := capture.Snapshot()
	scheduleToClose := snapshot[chasmnexus.NexusOperationScheduleToCloseLatency.Name()]
	require.Len(t, scheduleToClose, 1)
	require.Equal(t, time.Minute, scheduleToClose[0].Value)
	startToClose := snapshot[chasmnexus.NexusOperationStartToCloseLatency.Name()]
	require.Len(t, startToClose, 1)
	require.Equal(t, 30*time.Second, startToClose[0].Value)
	require.Empty(t, snapshot[chasmnexus.NexusOperationScheduleToStartLatency.Name()])
}
