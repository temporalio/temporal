package workers

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
)

var (
	malformedSqlQueryErrMessage = "malformed query"
	notSupportedErrMessage      = "operation is not supported"
	invalidExpressionErrMessage = "invalid expression"
)

func NewQueryError(format string, a ...any) error {
	message := fmt.Sprintf(format, a...)
	return serviceerror.NewInvalidArgument(message)
}
