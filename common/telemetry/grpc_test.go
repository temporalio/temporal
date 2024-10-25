package telemetry_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/telemetry"
	"google.golang.org/grpc"
)

func Test_AnnotateSpanWithWorkflowTags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	exporter := tracetest.NewInMemoryExporter()
	tp := oteltrace.NewTracerProvider(oteltrace.WithSyncer(exporter))
	tmp := propagation.TraceContext{}
	otelInterceptor := telemetry.NewServerTraceInterceptor(tp, tmp, nil)

	_, _ = otelInterceptor(
		ctx,
		&workflowservice.TerminateWorkflowExecutionRequest{
			WorkflowExecution: &common.WorkflowExecution{
				WorkflowId: "WF-ID",
				RunId:      "RUN-ID",
			},
		},
		&grpc.UnaryServerInfo{
			FullMethod: api.WorkflowServicePrefix,
		},
		func(ctx context.Context, req any) (any, error) {
			return nil, nil
		})

	exportedSpans := exporter.GetSpans()
	require.Len(t, exportedSpans, 1)
	exportedSpan := exportedSpans[0]
	attrByKey := map[string]attribute.KeyValue{}
	for _, a := range exportedSpan.Attributes {
		attrByKey[string(a.Key)] = a
	}
	require.Equal(t, attrByKey["temporalWorkflowID"].Value.AsString(), "WF-ID")
	require.Equal(t, attrByKey["temporalRunID"].Value.AsString(), "RUN-ID")
}
