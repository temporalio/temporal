package softassert

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
)

// FailedAssertion represents a failed assertion that can be converted to a serviceerror.
type FailedAssertion struct {
	message      string
	errorDetails string
}

func (f *FailedAssertion) Error() string {
	message := f.message
	if f.errorDetails != "" {
		message = fmt.Sprintf("%s: %s", message, f.errorDetails)
	}
	return message
}

func (f *FailedAssertion) InternalServiceErr() error {
	return serviceerror.NewInternal(f.Error())
}
