package chasm

import (
	"google.golang.org/protobuf/proto"
)

// DescribableComponent is implemented by components that can render their own state as a proto
// message, for a reader holding persisted CHASM nodes but not the execution they came from.
// Such a reader rehydrates the tree (see NewDetachedTree) and calls DescribeComponent on the
// root component, rather than reconstructing the projection itself.
//
// Implementations must be read only, and must not consult library config or task handlers:
// these readers register libraries with nil handlers. The returned message is an external
// contract, since a reader may keep it long after the execution is gone, so evolve it like a
// public API proto. Omit anything only meaningful against a live execution, such as tokens.
type DescribableComponent interface {
	Component

	// DescribeComponent returns this component's state as a proto message.
	DescribeComponent(Context) (proto.Message, error)
}
