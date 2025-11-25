package chasm

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/consts"
)

// ErrMalformedComponentRef is returned when the provided component ref cannot be deserialized.
var ErrMalformedComponentRef = serviceerror.NewInternal("malformed component ref")

// ErrInvalidComponentRef is returned when the provided component ref does not match the target execution.
var ErrInvalidComponentRef = serviceerror.NewInvalidArgument("invalid component ref")

// ExecutionStateChanged returns true if execution state has advanced beyond the state encoded in
// refBytes.
func ExecutionStateChanged(c Component, ctx Context, refBytes []byte) (bool, error) {
	ref, err := DeserializeComponentRef(refBytes)
	if err != nil {
		return false, ErrMalformedComponentRef
	}
	currentRefBytes, err := ctx.Ref(c)
	if err != nil {
		return false, err
	}
	currentRef, err := DeserializeComponentRef(currentRefBytes)
	if err != nil {
		return false, err
	}

	if ref.EntityKey != currentRef.EntityKey {
		return false, ErrInvalidComponentRef
	}

	switch transitionhistory.Compare(ref.entityLastUpdateVT, currentRef.entityLastUpdateVT) {
	case -1:
		// Execution state has advanced beyond submitted ref
		return true, nil
	case 0:
		// Execution state has not advanced beyond submitted ref
		return false, nil
	case 1:
		// Execution state is behind submitted ref
		return false, consts.ErrStaleState
	}
	panic("unexpected result from transitionhistory.Compare")
}
