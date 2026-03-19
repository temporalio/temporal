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

	type spanResult struct {
		mainSpanAttrs    map[string]attribute.KeyValue
		requestSpanAttrs map[string]attribute.KeyValue
		requestSpanName  string
	}

	makeRequest := func(responseErr error) spanResult {
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
		require.Len(t, exportedSpans, 2, "expected 2 spans: main RPC span and request span")

		// Find the request span (ends with /request) and main span
		var mainSpanAttrs, requestSpanAttrs map[string]attribute.KeyValue
		var requestSpanName string
		for _, span := range exportedSpans {
			attrByKey := map[string]attribute.KeyValue{}
			for _, a := range span.Attributes {
				attrByKey[string(a.Key)] = a
			}
			if span.Name == api.WorkflowServicePrefix+"/request" {
				requestSpanAttrs = attrByKey
				requestSpanName = span.Name
			} else {
				mainSpanAttrs = attrByKey
			}
		}
		require.NotNil(t, requestSpanAttrs, "request span not found")
		require.NotNil(t, mainSpanAttrs, "main span not found")

		return spanResult{
			mainSpanAttrs:    mainSpanAttrs,
			requestSpanAttrs: requestSpanAttrs,
			requestSpanName:  requestSpanName,
		}
	}

	// Helper to get main span attributes (for backward compatibility with existing tests)
	getMainSpanAttrs := func(responseErr error) map[string]attribute.KeyValue {
		return makeRequest(responseErr).mainSpanAttrs
	}

	t.Run("annotate span with workflow tags", func(t *testing.T) {
		// Request attributes are now only on the request span, not the main span
		result := makeRequest(nil)

		// Main span should NOT have workflow tags (they're on the request span)
		require.NotContains(t, result.mainSpanAttrs, "temporalWorkflowID")
		require.NotContains(t, result.mainSpanAttrs, "temporalRunID")

		// Request span should have workflow tags
		require.Equal(t, "WF-ID", result.requestSpanAttrs["temporalWorkflowID"].Value.AsString())
		require.Equal(t, "RUN-ID", result.requestSpanAttrs["temporalRunID"].Value.AsString())
	})

	t.Run("annotate span with request/response payload in debug mode", func(t *testing.T) {
		os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
		defer os.Unsetenv("TEMPORAL_OTEL_DEBUG")

		result := makeRequest(nil)

		// Request payload is on the request span, response payload is on the main span
		require.Equal(t,
			`{"workflowExecution":{"workflowId":"WF-ID","runId":"RUN-ID"}}`,
			toStr(t, result.requestSpanAttrs["rpc.request.payload"].Value))
		require.Equal(t, "{}", result.mainSpanAttrs["rpc.response.payload"].Value.AsString())
	})

	t.Run("annotate span with response error payload in debug mode", func(t *testing.T) {
		os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
		defer os.Unsetenv("TEMPORAL_OTEL_DEBUG")

		spanAttrsByKey := getMainSpanAttrs(status.Errorf(codes.Internal, "Something went wrong"))

		require.Equal(t,
			`{"code":13,"message":"Something went wrong"}`,
			toStr(t, spanAttrsByKey["rpc.response.error"].Value))
	})

	t.Run("request span is exported immediately with workflow tags", func(t *testing.T) {
		result := makeRequest(nil)

		// Verify request span has correct name
		require.Equal(t, api.WorkflowServicePrefix+"/request", result.requestSpanName)

		// Verify request span has workflow tags
		require.Equal(t, "WF-ID", result.requestSpanAttrs["temporalWorkflowID"].Value.AsString())
		require.Equal(t, "RUN-ID", result.requestSpanAttrs["temporalRunID"].Value.AsString())
	})

	t.Run("request span contains payload in debug mode", func(t *testing.T) {
		os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
		defer os.Unsetenv("TEMPORAL_OTEL_DEBUG")

		result := makeRequest(nil)

		// Verify request span has payload in debug mode
		require.Equal(t,
			`{"workflowExecution":{"workflowId":"WF-ID","runId":"RUN-ID"}}`,
			toStr(t, result.requestSpanAttrs["rpc.request.payload"].Value))
		require.Equal(t, "TerminateWorkflowExecutionRequest", result.requestSpanAttrs["rpc.request.type"].Value.AsString())
	})

	t.Run("request span is exported even without response", func(t *testing.T) {
		// This test simulates a scenario where the server crashes before sending a response.
		// The request span should still be exported.
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
		tmp := propagation.TraceContext{}
		otelStatsHandler := telemetry.NewServerStatsHandler(tp, tmp, nil)

		ctx := otelStatsHandler.TagRPC(context.Background(), &stats.RPCTagInfo{
			FullMethodName: api.WorkflowServicePrefix,
		})

		// Only send the request, no response or end
		otelStatsHandler.HandleRPC(ctx, &stats.InPayload{
			Payload: &workflowservice.TerminateWorkflowExecutionRequest{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: "WF-ID",
					RunId:      "RUN-ID",
				},
			},
		})

		// The request span should already be exported (without waiting for response)
		exportedSpans := exporter.GetSpans()
		require.Len(t, exportedSpans, 1, "request span should be exported immediately")
		require.Equal(t, api.WorkflowServicePrefix+"/request", exportedSpans[0].Name)

		// Verify it has the workflow tags
		attrByKey := map[string]attribute.KeyValue{}
		for _, a := range exportedSpans[0].Attributes {
			attrByKey[string(a.Key)] = a
		}
		require.Equal(t, "WF-ID", attrByKey["temporalWorkflowID"].Value.AsString())
		require.Equal(t, "RUN-ID", attrByKey["temporalRunID"].Value.AsString())
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
