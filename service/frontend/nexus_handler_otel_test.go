package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/common/telemetry"
)

func startRecordingSpan(t *testing.T) (context.Context, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tp.Tracer("test")
	ctx, _ := tracer.Start(context.Background(), "test-span")
	return ctx, exporter
}

func getSpanAttributes(t *testing.T, exporter *tracetest.InMemoryExporter, ctx context.Context) map[string]attribute.Value {
	t.Helper()
	oteltrace.SpanFromContext(ctx).End()
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	attrs := make(map[string]attribute.Value)
	for _, a := range spans[0].Attributes {
		attrs[string(a.Key)] = a.Value
	}
	return attrs
}

func TestWorkflowIDFromTemporalURL(t *testing.T) {
	t.Run("valid workflow URL", func(t *testing.T) {
		u := "temporal:///namespaces/my-ns/workflows/my-wf/run-1/history"
		require.Equal(t, "my-wf", workflowIDFromTemporalURL(u))
	})

	t.Run("percent-encoded workflow ID", func(t *testing.T) {
		u := "temporal:///namespaces/ns/workflows/example%2FNexusAsync%2Fhandler/run/history"
		require.Equal(t, "example/NexusAsync/handler", workflowIDFromTemporalURL(u))
	})

	t.Run("empty string", func(t *testing.T) {
		require.Equal(t, "", workflowIDFromTemporalURL(""))
	})

	t.Run("non-temporal URL", func(t *testing.T) {
		require.Equal(t, "", workflowIDFromTemporalURL("https://example.com"))
	})

	t.Run("malformed path", func(t *testing.T) {
		require.Equal(t, "", workflowIDFromTemporalURL("temporal:///foo/bar"))
	})
}

func TestExtractWorkflowIDFromProtoLinks(t *testing.T) {
	t.Run("returns first workflow ID", func(t *testing.T) {
		links := []*nexuspb.Link{
			{Url: "temporal:///namespaces/ns/workflows/wf-1/run-1/history", Type: "temporal.api.common.v1.Link.WorkflowEvent"},
			{Url: "temporal:///namespaces/ns/workflows/wf-2/run-2/history", Type: "temporal.api.common.v1.Link.WorkflowEvent"},
		}
		require.Equal(t, "wf-1", extractWorkflowIDFromProtoLinks(links))
	})

	t.Run("skips non-temporal links", func(t *testing.T) {
		links := []*nexuspb.Link{
			{Url: "https://example.com", Type: "other"},
			{Url: "temporal:///namespaces/ns/workflows/wf-id/run/history", Type: "temporal.api.common.v1.Link.WorkflowEvent"},
		}
		require.Equal(t, "wf-id", extractWorkflowIDFromProtoLinks(links))
	})

	t.Run("empty links", func(t *testing.T) {
		require.Equal(t, "", extractWorkflowIDFromProtoLinks(nil))
	})
}

func TestAnnotateNexusSpanResponseLinks(t *testing.T) {
	t.Run("sets handler workflow ID and nexus links", func(t *testing.T) {
		ctx, exporter := startRecordingSpan(t)
		protoLinks := []*nexuspb.Link{
			{Url: "temporal:///namespaces/ns/workflows/handler-wf-id/run-2/history", Type: "temporal.api.common.v1.Link.WorkflowEvent"},
		}

		annotateNexusSpanResponseLinks(ctx, protoLinks)
		attrs := getSpanAttributes(t, exporter, ctx)

		require.Equal(t, "handler-wf-id", attrs[telemetry.NexusHandlerWorkflowIDKey].AsString())
		linksJSON := attrs[telemetry.NexusLinksKey].AsString()
		require.Contains(t, linksJSON, "handler-wf-id")
	})

	t.Run("empty response links does not set attributes", func(t *testing.T) {
		ctx, exporter := startRecordingSpan(t)
		annotateNexusSpanResponseLinks(ctx, nil)
		attrs := getSpanAttributes(t, exporter, ctx)
		_, hasHandlerWF := attrs[telemetry.NexusHandlerWorkflowIDKey]
		_, hasLinks := attrs[telemetry.NexusLinksKey]
		require.False(t, hasHandlerWF)
		require.False(t, hasLinks)
	})
}

func TestAnnotateNexusSpanHTTPPayload(t *testing.T) {
	t.Run("request payload", func(t *testing.T) {
		ctx, exporter := startRecordingSpan(t)
		payload := &commonpb.Payload{Data: []byte(`"Hello"`)}
		annotateNexusSpanHTTPRequestPayload(ctx, payload)
		attrs := getSpanAttributes(t, exporter, ctx)
		require.Equal(t, `"Hello"`, attrs["http.request.payload"].AsString())
	})

	t.Run("nil payload does not set attribute", func(t *testing.T) {
		ctx, exporter := startRecordingSpan(t)
		annotateNexusSpanHTTPRequestPayload(ctx, nil)
		attrs := getSpanAttributes(t, exporter, ctx)
		_, has := attrs["http.request.payload"]
		require.False(t, has)
	})

	t.Run("sync response payload", func(t *testing.T) {
		ctx, exporter := startRecordingSpan(t)
		payload := &commonpb.Payload{Data: []byte(`{"result":"ok"}`)}
		annotateNexusSpanHTTPResponsePayload(ctx, payload)
		attrs := getSpanAttributes(t, exporter, ctx)
		require.Equal(t, `{"result":"ok"}`, attrs["http.response.payload"].AsString())
	})

	t.Run("async response payload", func(t *testing.T) {
		ctx, exporter := startRecordingSpan(t)
		annotateNexusSpanHTTPResponseAsync(ctx, "my-token")
		attrs := getSpanAttributes(t, exporter, ctx)
		resp := attrs["http.response.payload"].AsString()
		require.Contains(t, resp, "my-token")
		require.Contains(t, resp, "running")
	})
}
