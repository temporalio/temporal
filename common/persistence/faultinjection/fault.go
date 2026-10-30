package faultinjection

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/faultinjection"
	"go.temporal.io/server/common/persistence"
)

// newFault returns an error based on the provided name. If the name is not recognized, then this method will
// panic.
func newFault(errName string, errRate float64, methodName string) *faultinjection.Outcome[struct{}] {
	header := fmt.Sprintf("fault injection error at %s with %.2f rate", methodName, errRate)
	switch errName {
	case "ShardOwnershipLost":
		return &faultinjection.Outcome[struct{}]{Error: &persistence.ShardOwnershipLostError{Msg: fmt.Sprintf("%s: persistence.ShardOwnershipLostError", header)}}
	case "DeadlineExceeded":
		// Real persistence store never returns context.DeadlineExceeded error. It returns persistence.TimeoutError instead.
		// Therefor "DeadlineExceeded" shouldn't be used with fault injection. Use "Timeout" instead.
		return &faultinjection.Outcome[struct{}]{Error: fmt.Errorf("%s: %w", header, context.DeadlineExceeded)}
	case "Timeout":
		return &faultinjection.Outcome[struct{}]{Error: &persistence.TimeoutError{Msg: fmt.Sprintf("%s: persistence.TimeoutError", header)}}
	case "ExecuteAndTimeout":
		// Special error which emulates case, when caller got a Timeout error,
		// but operation actually reached persistence and was executed successfully.
		f := &faultinjection.Outcome[struct{}]{Error: &persistence.TimeoutError{Msg: fmt.Sprintf("%s: persistence.TimeoutError", header)}}
		f.AfterSuccess = true
		return f
	case "ResourceExhausted":
		return &faultinjection.Outcome[struct{}]{Error: &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: fmt.Sprintf("%s: serviceerror.ResourceExhausted", header),
		}}
	case "Unavailable":
		return &faultinjection.Outcome[struct{}]{Error: serviceerror.NewUnavailablef("%s: serviceerror.Unavailable", header)}
	default:
		panic(fmt.Sprintf("unsupported error type: %v", errName))
	}
}
