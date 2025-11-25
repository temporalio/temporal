package chasm

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/consts"
)

// ErrInvalidComponentRefBytes is returned when the provided component ref cannot be deserialized.
var ErrInvalidComponentRefBytes = serviceerror.NewInternal("invalid component ref bytes")

// ExecutionStateChanged returns a ref for the component if execution state has advanced beyond the state
// encoded in refBytes.
func ExecutionStateChanged(c Component, ctx Context, refBytes []byte) ([]byte, bool, error) {
	ref, err := DeserializeComponentRef(refBytes)
	if err != nil {
		return nil, false, ErrInvalidComponentRefBytes
	}
	currentRefBytes, err := ctx.Ref(c)
	if err != nil {
		return nil, false, err
	}
	currentRef, err := DeserializeComponentRef(currentRefBytes)
	if err != nil {
		return nil, false, err
	}

	if len(refBytes) > 0 && ref.EntityKey != currentRef.EntityKey {
		return nil, false, serviceerror.NewInternalf(
			"ref execution key (%v) does not match component execution (%v)", ref.EntityKey, currentRef.EntityKey)
	}

	switch transitionhistory.Compare(ref.entityLastUpdateVT, currentRef.entityLastUpdateVT) {
	case -1:
		// Execution state has advanced beyond submitted ref
		return currentRefBytes, true, nil
	case 0:
		// Execution state has not advanced beyond submitted ref
		return nil, false, nil
	case 1:
		// Execution state is behind submitted ref
		return nil, false, consts.ErrStaleState
	}
	panic("unexpected result from transitionhistory.Compare")
}
