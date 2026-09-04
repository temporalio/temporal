package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/telemetry"
)

func executionSpanStartOption(request any) trace.SpanStartOption {
	return trace.WithAttributes(executionSpanAttributes(request)...)
}

type executionIdentityProvider interface {
	ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey)
}

func executionSpanAttributes(request any) []attribute.KeyValue {
	provider, ok := request.(executionIdentityProvider)
	if !ok {
		return nil
	}
	archetypeID, executionKey := provider.ExecutionIdentity()
	if archetypeID == 0 {
		return nil
	}

	attrs := make([]attribute.KeyValue, 0, 4)
	if executionKey.NamespaceID != "" {
		attrs = append(attrs, attribute.String(telemetry.NamespaceIDKey, executionKey.NamespaceID))
	}
	if executionKey.BusinessID != "" {
		attrs = append(attrs, attribute.String(telemetry.BusinessIDKey, executionKey.BusinessID))
	}
	if executionKey.RunID != "" {
		attrs = append(attrs, attribute.String(telemetry.RunIDKey, executionKey.RunID))
	}
	return append(attrs, attribute.Int64(telemetry.ChasmArchetypeIDKey, int64(archetypeID)))
}
