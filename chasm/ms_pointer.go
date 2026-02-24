package chasm

import (
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// MSPointer is a special CHASM type which components can use to access their Node's underlying backend (i.e. mutable
// state). It is used to expose methods needed from the mutable state without polluting the chasm.Context interface.
// When deserializing components with fields of this type, the CHASM engine will set the value to its NodeBackend.
// This should only be used by the Workflow component.
type MSPointer struct {
	backend NodeBackend
}

// NewMSPointer creates a new MSPointer instance.
func NewMSPointer(backend NodeBackend) MSPointer {
	return MSPointer{
		backend: backend,
	}
}

// GetNexusCompletion retrieves the Nexus operation completion data for the given request ID from the underlying mutable state.
func (m MSPointer) GetNexusCompletion(ctx Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return m.backend.GetNexusCompletion(ctx.goContext(), requestID)
}
