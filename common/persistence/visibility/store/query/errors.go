package query

import (
	"errors"
	"fmt"

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

func wrapConverterError(message string, err error) error {
	var converterErr *ConverterError
	if errors.As(err, &converterErr) {
		return NewConverterError("%s: %v", message, converterErr)
	}
	return err
}
