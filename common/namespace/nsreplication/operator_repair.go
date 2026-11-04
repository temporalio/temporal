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

func validatePeerSet(field string, peers []string) (map[string]struct{}, error) {
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

// ValidatePeerSets validates repair membership at frontend and history boundaries.
func ValidatePeerSets(authoritative, verified []string) (authoritativePeers, verifiedPeers map[string]struct{}, err error) {
	authoritativePeers, err = validatePeerSet("authoritative peers", authoritative)
	if err != nil {
		return nil, nil, err
	}
	verifiedPeers, err = validatePeerSet("verified peers", verified)
	if err != nil {
		return nil, nil, err
	}
	for peer := range verifiedPeers {
		if _, ok := authoritativePeers[peer]; !ok {
			return nil, nil, serviceerror.NewInvalidArgument("verified peers must be a subset of authoritative peers")
		}
	}
	return authoritativePeers, verifiedPeers, nil
}
