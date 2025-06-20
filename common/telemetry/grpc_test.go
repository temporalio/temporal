package telemetry_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func Test_ServerStatsHandler(t *testing.T) {

	makeRequest := func(responseErr error) map[string]attribute.KeyValue {
		t.Helper()

		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
		tmp := propagation.TraceContext{}
		otelStatsHandler := telemetry.NewServerStatsHandler(tp, tmp, nil)

		ctx := otelStatsHandler.TagRPC(context.Background(), &stats.RPCTagInfo{
			FullMethodName: api.WorkflowServicePrefix,
		})
		otelStatsHandler.HandleRPC(ctx, &stats.InPayload{
			Payload: &workflowservice.TerminateWorkflowExecutionRequest{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: "WF-ID",
					RunId:      "RUN-ID",
				},
			},
		})
		if responseErr == nil {
			otelStatsHandler.HandleRPC(ctx, &stats.OutPayload{
				Payload: &workflowservice.TerminateWorkflowExecutionResponse{},
			})
		}
		otelStatsHandler.HandleRPC(ctx, &stats.End{
			Error: responseErr,
		})

		exportedSpans := exporter.GetSpans()
		require.Len(t, exportedSpans, 1)
		attrByKey := map[string]attribute.KeyValue{}
		for _, a := range exportedSpans[0].Attributes {
			attrByKey[string(a.Key)] = a
		}
		return attrByKey
	}

	t.Run("annotate span with workflow tags", func(t *testing.T) {
		spanAttrsByKey := makeRequest(nil)

		require.Equal(t, "WF-ID", spanAttrsByKey["temporalWorkflowID"].Value.AsString())
		require.Equal(t, "RUN-ID", spanAttrsByKey["temporalRunID"].Value.AsString())

		// ensure no debug attributes are present
		require.NotContains(t, spanAttrsByKey, "rpc.request.payload")
		require.NotContains(t, spanAttrsByKey, "rpc.response.payload")
	})

	t.Run("annotate span with request/response payload in debug mode", func(t *testing.T) {
		os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
		defer os.Unsetenv("TEMPORAL_OTEL_DEBUG")

		spanAttrsByKey := makeRequest(nil)

		require.Equal(t,
			`{"workflowExecution":{"workflowId":"WF-ID","runId":"RUN-ID"}}`,
			toStr(t, spanAttrsByKey["rpc.request.payload"].Value))
		require.Equal(t, "{}", spanAttrsByKey["rpc.response.payload"].Value.AsString())
	})

	t.Run("annotate span with response error payload in debug mode", func(t *testing.T) {
		os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
		defer os.Unsetenv("TEMPORAL_OTEL_DEBUG")

		spanAttrsByKey := makeRequest(status.Errorf(codes.Internal, "Something went wrong"))

		require.Equal(t,
			`{"code":13,"message":"Something went wrong"}`,
			toStr(t, spanAttrsByKey["rpc.response.error"].Value))
	})

	t.Run("skip if noop trace provider", func(t *testing.T) {
		tp := telemetry.NoopTracerProvider
		tmp := propagation.TraceContext{}
		otelStatsHandler := telemetry.NewServerStatsHandler(tp, tmp, nil)
		require.Nil(t, otelStatsHandler)
	})
}

func Test_ClientStatsHandler(t *testing.T) {

	t.Run("skip if noop trace provider", func(t *testing.T) {
		tp := telemetry.NoopTracerProvider
		tmp := propagation.TraceContext{}
		otelStatsHandler := telemetry.NewClientStatsHandler(tp, tmp)
		require.Nil(t, otelStatsHandler)
	})
}

func toStr(t *testing.T, v attribute.Value) string {
	t.Helper()
	var payload map[string]json.RawMessage
	payloadStr := v.AsString()
	// protobuf adds random whitespaces when encoding output;
	// therefore we need to unmarshal and marshal again to get a consistent result
	require.NoError(t, json.Unmarshal([]byte(payloadStr), &payload))
	m, err := json.Marshal(payload)
	require.NoError(t, err)
	return string(m)
}
