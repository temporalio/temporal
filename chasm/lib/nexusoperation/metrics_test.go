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
					op.emitOnFailedMetrics(ctx, closeTime, FailedReasonOperationError)
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

func TestOperationFailedReason(t *testing.T) {
	t.Parallel()

	// Use the actual start-result conversion for a rejected request.
	rejected, err := newInvocationResult(nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad input"))
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, rejected)
	handlerFailure := rejected.(invocationResultFail).failure

	// Use the same conversion for a synchronous operation failure.
	failed, err := newInvocationResult(nil, nexus.NewOperationFailedErrorf("insufficient funds"))
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, failed)
	operationFailure := failed.(invocationResultFail).failure

	for _, tc := range []struct {
		name    string
		failure *failurepb.Failure
		want    metrics.ReasonString
	}{
		{
			name:    "handler failure uses handler_error",
			failure: handlerFailure,
			want:    FailedReasonHandlerError,
		},
		{
			name: "unknown handler error type uses handler_error",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "WHATEVER_THE_HANDLER_SAID"},
				},
			},
			want: FailedReasonHandlerError,
		},
		{
			name: "server failure uses caller_error",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ServerFailureInfo{
					ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: true},
				},
			},
			want: FailedReasonCallerError,
		},
		{
			name: "HSM CallError application failure uses caller_error",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "CallError", NonRetryable: true},
				},
			},
			want: FailedReasonCallerError,
		},
		{
			name:    "synchronous operation failure uses operation_error",
			failure: operationFailure,
			want:    FailedReasonOperationError,
		},
		{
			name: "nested handler failure does not change operation_error",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "OperationError"},
				},
				Cause: handlerFailure,
			},
			want: FailedReasonOperationError,
		},
		{
			name:    "nil failure defaults to operation_error",
			failure: nil,
			want:    FailedReasonOperationError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, OperationFailedReason(tc.failure))
		})
	}
}
