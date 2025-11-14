package chasm

import (
	"google.golang.org/protobuf/proto"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/persistence/transitionhistory"
)

// HasStateAdvanced returns (entityRef, true, nil) if entity state has advanced beyond the state
// encoded in stateToken.
func HasStateAdvanced(c Component, ctx Context, stateToken []byte) ([]byte, bool, error) {
	refBytes, err := ctx.Ref(c)
	if err != nil {
		return nil, false, err
	}
	ref, err := DeserializeComponentRef(refBytes)
	if err != nil {
		return nil, false, err
	}

	token, err := decodeStateToken(stateToken)
	if err != nil {
		return nil, false, err
	}

	switch transitionhistory.Compare(token.VersionedTransition, ref.entityLastUpdateVT) {
	case -1:
		// State has advanced beyond stateToken
		return refBytes, true, nil
	case 0:
		// stateToken matches current state
		return nil, false, nil
	case 1:
		// StateToken is ahead of current state
		// TODO(dan): consts.ErrStaleState
		return nil, false, serviceerror.NewFailedPrecondition("long-poll token represents a state beyond current")
	default:
		// Impossible: Compare only returns -1, 0, or 1
		return nil, false, serviceerror.NewInternal("unexpected transition history comparison result")
	}

}

func EncodeStateToken(refBytes []byte) ([]byte, error) {
	ref, err := DeserializeComponentRef(refBytes)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&activitypb.StateToken{
		Version:             1,
		VersionedTransition: ref.entityLastUpdateVT,
	})
}

func decodeStateToken(stateToken []byte) (*activitypb.StateToken, error) {
	if len(stateToken) == 0 {
		return &activitypb.StateToken{}, nil
	}
	var token activitypb.StateToken
	if err := proto.Unmarshal(stateToken, &token); err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid state token")
	}
	return &token, nil
}
