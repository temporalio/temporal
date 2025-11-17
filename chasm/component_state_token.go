package chasm

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/consts"
	"google.golang.org/protobuf/proto"
)

// ComponentStateChanged returns a ref for the component if component state has advanced beyond the state
// encoded in stateToken.
func ComponentStateChanged(c Component, ctx Context, stateToken []byte) ([]byte, bool, error) {
	refBytes, err := ctx.Ref(c)
	if err != nil {
		return nil, false, err
	}
	ref, err := DeserializeComponentRef(refBytes)
	if err != nil {
		return nil, false, err
	}

	token, err := decodeComponentStateToken(stateToken)
	if err != nil {
		return nil, false, err
	}

	switch transitionhistory.Compare(token.VersionedTransition, ref.componentVT) {
	case -1:
		// State has advanced beyond stateToken
		return refBytes, true, nil
	case 0:
		// stateToken matches current state
		return nil, false, nil
	case 1:
		// StateToken is ahead of current state
		return nil, false, consts.ErrStaleState
	default:
		// Impossible: Compare only returns -1, 0, or 1
		return nil, false, serviceerror.NewInternal("unexpected transition history comparison result")
	}

}

func EncodeComponentStateToken(refBytes []byte) ([]byte, error) {
	ref, err := DeserializeComponentRef(refBytes)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&activitypb.ComponentStateToken{
		Version:             1,
		VersionedTransition: ref.componentVT,
	})
}

func decodeComponentStateToken(tokenBytes []byte) (*activitypb.ComponentStateToken, error) {
	if len(tokenBytes) == 0 {
		return &activitypb.ComponentStateToken{}, nil
	}
	var token activitypb.ComponentStateToken
	if err := proto.Unmarshal(tokenBytes, &token); err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid state token")
	}
	return &token, nil
}
