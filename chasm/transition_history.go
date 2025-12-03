package chasm

import (
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/consts"
)

// ExecutionStateChanged returns true if execution state has advanced beyond the state encoded in
// refBytes. It may return ErrInvalidComponentRef or ErrMalformedComponentRef. Callers should
// consider converting these to serviceerror.NewInvalidArgument.
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
