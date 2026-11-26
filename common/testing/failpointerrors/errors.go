// Package failpointerrors registers the service errors in the failpoint error catalog, so that
// failpoints configured from outside the process can return them.
package failpointerrors

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/testx/failpoint"
)

func init() {
	failpoint.RegisterError("Unavailable", serviceerror.NewUnavailable)
	failpoint.RegisterError("ResourceExhausted", func(msg string) error {
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, msg)
	})
	failpoint.RegisterError("Internal", serviceerror.NewInternal)
	failpoint.RegisterError("DeadlineExceeded", serviceerror.NewDeadlineExceeded)
	failpoint.RegisterError("Canceled", serviceerror.NewCanceled)
	failpoint.RegisterError("NotFound", serviceerror.NewNotFound)
	failpoint.RegisterError("InvalidArgument", serviceerror.NewInvalidArgument)
}
