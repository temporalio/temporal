package chasm

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// MSPointer is a special CHASM type which components can use to access their Node's underlying backend (i.e. mutable
// state). It is used to expose methods needed from the mutable state without polluting the chasm.Context interface.
// When deserializing components with fields of this type, the CHASM engine will set the value to its NodeBackend.
type MSPointer interface {
	// TODO: Add methods needed from MutableState / NodeBackend here.
	GetNexusCompletion(
		ctx context.Context,
		requestID string,
	) (nexus.OperationCompletion, error)
}
