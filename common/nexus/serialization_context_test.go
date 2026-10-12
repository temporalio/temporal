package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/common/metrics"
)

func TestSerializationContextMatch(t *testing.T) {
	t.Parallel()

	serializationContext := &nexuspb.PropagatedSerializationContext{
		Endpoint:  "endpoint",
		Service:   "service",
		Operation: "operation",
	}
	for _, tc := range []struct {
		name     string
		existing *nexuspb.PropagatedSerializationContext
		incoming *nexuspb.PropagatedSerializationContext
		expected metrics.ReasonString
	}{
		{
			name:     "same context",
			existing: serializationContext,
			incoming: serializationContext,
			expected: "same_nexus_context",
		},
		{
			name:     "different endpoint",
			existing: serializationContext,
			incoming: &nexuspb.PropagatedSerializationContext{Endpoint: "other-endpoint", Service: "service", Operation: "operation"},
			expected: "different_nexus_context",
		},
		{
			name:     "existing context missing",
			incoming: serializationContext,
			expected: "existing_nexus_context_missing",
		},
		{
			name:     "incoming context missing",
			existing: serializationContext,
			expected: "incoming_nexus_context_missing",
		},
		{
			name: "both contexts missing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, SerializationContextMatch(tc.existing, tc.incoming))
		})
	}
}
