package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

func TestFactDecoderImportSpanAssignsEventTime(t *testing.T) {
	eventTime := time.Date(2026, time.August, 12, 11, 0, 0, 0, time.UTC)
	span := tracetest.SpanStub{Events: []sdktrace.Event{{
		Name: telemetry.EventWorkflowExecutionStarted,
		Time: eventTime,
		Attributes: []attribute.KeyValue{
			telemetry.AttrWorkflowID.String("workflow-id"),
			telemetry.AttrRunID.String("run-id"),
			telemetry.AttrNamespaceID.String("namespace-id"),
		},
	}}}.Snapshot()

	facts := NewFactDecoder().ImportSpan(span)

	require.Len(t, facts, 1)
	started, ok := facts[0].(*fact.WorkflowRunStarted)
	require.True(t, ok)
	require.Equal(t, eventTime, started.EventTime())
}
