//go:build test_dep

// Package failpointerrors registers the server's errors in the failpoint error catalog, so that
// failpoints configured from outside the process can return them.
package failpointerrors

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
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
	failpoint.RegisterError("ShardOwnershipLost", func(msg string) error {
		return &persistence.ShardOwnershipLostError{Msg: msg}
	})
	failpoint.RegisterError("Timeout", func(msg string) error {
		return &persistence.TimeoutError{Msg: msg}
	})
	failpoint.RegisterError("ConditionFailed", func(msg string) error {
		return &persistence.ConditionFailedError{Msg: msg}
	})
}
