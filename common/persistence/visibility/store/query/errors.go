package query

import (
	"errors"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

type (
	ConverterError struct {
		message string
	}
)

var (
	MalformedSqlQueryErrMessage = "malformed SQL query"
	NotSupportedErrMessage      = "operation is not supported"
	InvalidExpressionErrMessage = "invalid expression"
)

func NewConverterError(format string, a ...interface{}) error {
	message := fmt.Sprintf(format, a...)
	return &ConverterError{message: message}
}

func (c *ConverterError) Error() string {
	return c.message
}

func (c *ConverterError) ToInvalidArgument() error {
	return serviceerror.NewInvalidArgumentf("invalid query: %v", c)
}

func NewOperatorNotSupportedError(
	saName string,
	saType enumspb.IndexedValueType,
	operator string,
) error {
	return NewConverterError(
		"%s: operator '%s' not supported for %s type search attribute '%s'",
		NotSupportedErrMessage,
		strings.ToUpper(operator),
		saType.String(),
		saName,
	)
}

func wrapConverterError(message string, err error) error {
	var converterErr *ConverterError
	if errors.As(err, &converterErr) {
		return NewConverterError("%s: %v", message, converterErr)
	}
	return err
}
