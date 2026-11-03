package nsreplication

import (
	"context"

	"go.temporal.io/api/serviceerror"
)

type operatorRepairContextKey struct{}

// WithOperatorRepair preserves receiver-local fields during direct operator
// writes and enforces identity before applying an existing namespace snapshot.
func WithOperatorRepair(ctx context.Context) context.Context {
	return context.WithValue(ctx, operatorRepairContextKey{}, true)
}

func isOperatorRepair(ctx context.Context) bool {
	return ctx.Value(operatorRepairContextKey{}) == true
}

// ValidatePeerSet validates peer names at both frontend and history RPC boundaries.
func ValidatePeerSet(field string, peers []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			return nil, serviceerror.NewInvalidArgument(field + " contains an empty peer")
		}
		if _, duplicate := result[peer]; duplicate {
			return nil, serviceerror.NewInvalidArgument(field + " contains duplicate peer " + peer)
		}
		result[peer] = struct{}{}
	}
	return result, nil
}
