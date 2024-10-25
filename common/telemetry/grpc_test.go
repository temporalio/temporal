package telemetry_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/telemetry"
	"google.golang.org/grpc/stats"
)

func Test_ServerStatsHandler(t *testing.T) {

	makeRequest := func() map[string]attribute.KeyValue {
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
				WorkflowExecution: &common.WorkflowExecution{
					WorkflowId: "WF-ID",
					RunId:      "RUN-ID",
				},
			},
		})
		otelStatsHandler.HandleRPC(ctx, &stats.OutPayload{
			Payload: &workflowservice.TerminateWorkflowExecutionResponse{},
		})
		otelStatsHandler.HandleRPC(ctx, &stats.End{})

		exportedSpans := exporter.GetSpans()
		require.Len(t, exportedSpans, 2) // first one is the "check-span"
		attrByKey := map[string]attribute.KeyValue{}
		for _, a := range exportedSpans[1].Attributes {
			attrByKey[string(a.Key)] = a
		}
		return attrByKey
	}

	t.Run("annotate span with workflow tags", func(t *testing.T) {
		spanAttrsByKey := makeRequest()

		require.Equal(t, "WF-ID", spanAttrsByKey["temporalWorkflowID"].Value.AsString())
		require.Equal(t, "RUN-ID", spanAttrsByKey["temporalRunID"].Value.AsString())

		// ensure no debug attributes are present
		require.NotContains(t, spanAttrsByKey, "rpc.request.payload")
		require.NotContains(t, spanAttrsByKey, "rpc.response.payload")
	})

	t.Run("annotate span with request/response payload in debug mode", func(t *testing.T) {
		os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
		defer os.Unsetenv("TEMPORAL_OTEL_DEBUG")

		spanAttrsByKey := makeRequest()

		require.Equal(t,
			`{"workflowExecution":{"workflowId":"WF-ID","runId":"RUN-ID"}}`,
			spanAttrsByKey["rpc.request.payload"].Value.AsString())
		require.Equal(t, "{}", spanAttrsByKey["rpc.response.payload"].Value.AsString())
	})

	t.Run("skip if noop trace provider", func(t *testing.T) {
		tp := noop.NewTracerProvider()
		tmp := propagation.TraceContext{}
		otelStatsHandler := telemetry.NewServerStatsHandler(tp, tmp, nil)
		require.Nil(t, otelStatsHandler)
	})
}

func Test_ClientStatsHandler(t *testing.T) {

	t.Run("skip if noop trace provider", func(t *testing.T) {
		tp := noop.NewTracerProvider()
		tmp := propagation.TraceContext{}
		otelStatsHandler := telemetry.NewClientStatsHandler(tp, tmp)
		require.Nil(t, otelStatsHandler)
	})
}
