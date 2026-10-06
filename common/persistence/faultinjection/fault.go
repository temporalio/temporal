package faultinjection

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

type (
	fault struct {
		err error
		// execOp indicates whether the operation should be executed before returning the error.
		execOp bool
	}
)

// newFault returns an error based on the provided name. If the name is not recognized, then this method will
// panic.
func newFault(errName string, errRate float64, methodName string) *fault {
	header := fmt.Sprintf("fault injection error at %s with %.2f rate", methodName, errRate)
	switch errName {
	case "ShardOwnershipLost":
		return &fault{err: &persistence.ShardOwnershipLostError{Msg: fmt.Sprintf("%s: persistence.ShardOwnershipLostError", header)}}
	case "DeadlineExceeded":
		// Real persistence store never returns context.DeadlineExceeded error. It returns persistence.TimeoutError instead.
		// Therefor "DeadlineExceeded" shouldn't be used with fault injection. Use "Timeout" instead.
		return &fault{err: fmt.Errorf("%s: %w", header, context.DeadlineExceeded)}
	case "Timeout":
		return &fault{err: &persistence.TimeoutError{Msg: fmt.Sprintf("%s: persistence.TimeoutError", header)}}
	case "ExecuteAndTimeout":
		// Special error which emulates case, when caller got a Timeout error,
		// but operation actually reached persistence and was executed successfully.
		return &fault{
			err:    &persistence.TimeoutError{Msg: fmt.Sprintf("%s: persistence.TimeoutError", header)},
			execOp: true,
		}
	case "ResourceExhausted":
		return &fault{err: &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: fmt.Sprintf("%s: serviceerror.ResourceExhausted", header),
		}}
	case "Unavailable":
		return &fault{err: serviceerror.NewUnavailablef("%s: serviceerror.Unavailable", header)}
	default:
		panic(fmt.Sprintf("unsupported error type: %v", errName))
	}
}

func (f *fault) inject(op func() error) error {
	if f == nil {
		return op()
	}
	if f.execOp {
		err := op()
		if err != nil {
			return err
		}
	}
	return f.err
}
