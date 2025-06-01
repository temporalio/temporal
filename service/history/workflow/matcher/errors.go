package matcher

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
)

var (
	malformedSqlQueryErrMessage = "malformed SQL query"
	notSupportedErrMessage      = "operation is not supported"
	invalidExpressionErrMessage = "invalid expression"
)

func NewMatcherError(format string, a ...any) error {
	message := fmt.Sprintf(format, a...)
	return serviceerror.NewInvalidArgument(message)
}
