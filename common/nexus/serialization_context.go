package nexus

import (
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/common/metrics"
)

const (
	serializationContextSame            metrics.ReasonString = "same_nexus_context"
	serializationContextDifferent       metrics.ReasonString = "different_nexus_context"
	serializationContextExistingMissing metrics.ReasonString = "existing_nexus_context_missing"
	serializationContextIncomingMissing metrics.ReasonString = "incoming_nexus_context_missing"
)

// SerializationContextMatch classifies Nexus contexts for USE_EXISTING callback attachment metrics.
// An empty reason means neither caller has a Nexus context.
func SerializationContextMatch(existing, incoming *nexuspb.PropagatedSerializationContext) metrics.ReasonString {
	switch {
	case existing == nil && incoming == nil:
		return ""
	case existing == nil:
		return serializationContextExistingMissing
	case incoming == nil:
		return serializationContextIncomingMissing
	case existing.GetEndpoint() == incoming.GetEndpoint() &&
		existing.GetService() == incoming.GetService() &&
		existing.GetOperation() == incoming.GetOperation():
		return serializationContextSame
	default:
		return serializationContextDifferent
	}
}
