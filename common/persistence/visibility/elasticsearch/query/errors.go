package query

import (
	"errors"
)

var (
	MalformedSqlQueryErr = errors.New("malformed SQL query")
	NotSupportedErr      = errors.New("operation is not supported")
	InvalidExpressionErr = errors.New("invalid expression")
)
