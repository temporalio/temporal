package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

func TestValidatePropagatedSerializationContext(t *testing.T) {
	t.Parallel()

	serializationContext := &commonpb.PropagatedNexusSerializationContext{
		Endpoint:  "endpoint",
		Service:   "service",
		Operation: "operation",
	}

	testCases := []struct {
		name      string
		existing  *commonpb.PropagatedNexusSerializationContext
		requested *commonpb.PropagatedNexusSerializationContext
		wantErr   bool
	}{
		{
			name: "both nil",
		},
		{
			name:      "equal contexts",
			existing:  serializationContext,
			requested: serializationContext,
		},
		{
			name:     "missing requested context",
			existing: serializationContext,
			wantErr:  true,
		},
		{
			name:     "different contexts",
			existing: serializationContext,
			requested: &commonpb.PropagatedNexusSerializationContext{
				Endpoint:  serializationContext.Endpoint,
				Service:   serializationContext.Service,
				Operation: "different-operation",
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePropagatedSerializationContext(tc.existing, tc.requested, "workflow")
			if tc.wantErr {
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
				require.Equal(t, "propagated nexus serialization context must match the existing workflow", err.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}
