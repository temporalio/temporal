package nexusoperation

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOperationBackendMetricTag(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  NexusMetricTagConfig
		wantTag bool
	}{
		{name: "unset"},
		{name: "disabled", config: NexusMetricTagConfig{IncludeBackendTag: false}},
		{name: "enabled", config: NexusMetricTagConfig{IncludeBackendTag: true}, wantTag: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, workflowOperation := range []bool{false, true} {
				name := "standalone"
				if workflowOperation {
					name = "workflow"
				}
				t.Run(name, func(t *testing.T) {
					handler := metricstest.NewCaptureHandler()
					capture := handler.StartCapture()
					defer handler.StopCapture(capture)
					ctx := &chasm.MockContext{
						HandleNamespaceEntry: func() *namespace.Namespace {
							return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
						},
						HandleMetricsHandler: func() metrics.Handler { return handler },
						GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
							MetricTagConfig: dynamicconfig.GetTypedPropertyFn(tc.config),
						}),
					}
					op := newTestOperation()
					if workflowOperation {
						op.Store = chasm.NewMockParentPtr[OperationStore](&mockStoreComponent{})
					}
					closeTime := defaultTime.Add(2 * time.Second)
					op.emitOnSucceededMetrics(ctx, closeTime)
					op.emitOnFailedMetrics(ctx, closeTime, FailedReasonOperationFailed)
					op.emitOnCanceledMetrics(ctx, closeTime)
					op.emitOnTerminatedMetrics(ctx, closeTime)
					op.emitOnTimedOutMetrics(ctx, closeTime, "SCHEDULE_TO_CLOSE")
					op.StartedTime = timestamppb.New(defaultTime.Add(time.Second))
					op.emitOnSucceededMetrics(ctx, closeTime)
					snapshot := capture.Snapshot()
					require.ElementsMatch(t, []string{
						"nexus_operation_success",
						"nexus_operation_fail",
						"nexus_operation_cancel",
						"nexus_operation_terminate",
						"nexus_operation_timeout",
						"nexus_operation_schedule_to_close_latency",
						"nexus_operation_schedule_to_start_latency",
						"nexus_operation_start_to_close_latency",
					}, slices.Collect(maps.Keys(snapshot)))
					for metric, recordings := range snapshot {
						for _, recording := range recordings {
							if tc.wantTag {
								require.Equal(t, "chasm", recording.Tags["backend"], metric)
							} else {
								require.NotContains(t, recording.Tags, "backend", metric)
							}
						}
					}
				})
			}
		})
	}
}

func TestBackendMetricTagDisabledByDefault(t *testing.T) {
	require.False(t, MetricTagConfiguration.Get(dynamicconfig.NewNoopCollection())().IncludeBackendTag)
}

func TestAttemptFailedReason(t *testing.T) {
	t.Parallel()

	// The failure a start attempt resolves the operation with when the handler rejects it.
	rejected, err := newInvocationResult(nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad input"))
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, rejected)
	handlerFailure := rejected.(invocationResultFail).failure

	// The failure a start attempt resolves the operation with when the handler fails it synchronously.
	failed, err := newInvocationResult(nil, nexus.NewOperationFailedErrorf("insufficient funds"))
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, failed)
	operationFailure := failed.(invocationResultFail).failure

	handlerFailureOfType := func(errType string) *failurepb.Failure {
		return &failurepb.Failure{
			FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
				NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: errType},
			},
		}
	}

	for _, tc := range []struct {
		name    string
		failure *failurepb.Failure
		want    metrics.ReasonString
	}{
		{
			name:    "rejected start attempt",
			failure: handlerFailure,
			want:    FailedReasonHandlerError,
		},
		{
			name:    "handler error type outside the spec",
			failure: handlerFailureOfType("WHATEVER_THE_HANDLER_SAID"),
			want:    FailedReasonHandlerError,
		},
		{
			name:    "synchronous operation failure",
			failure: operationFailure,
			want:    FailedReasonOperationFailed,
		},
		{
			name: "handler error as the cause of an operation failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "OperationError"},
				},
				Cause: handlerFailure,
			},
			want: FailedReasonOperationFailed,
		},
		{
			name: "non-retryable server failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ServerFailureInfo{
					ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: true},
				},
			},
			want: FailedReasonServerError,
		},
		{
			name: "HSM call error",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "CallError", NonRetryable: true},
				},
			},
			want: FailedReasonServerError,
		},
		{
			name:    "nil failure",
			failure: nil,
			want:    FailedReasonOperationFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, AttemptFailedReason(tc.failure))
		})
	}
}
