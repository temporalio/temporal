package sadefs

import "errors"

var (
	ErrInvalidName   = errors.New("invalid search attribute name")
	ErrInvalidType   = errors.New("invalid search attribute type")
	ErrInvalidString = errors.New("SearchAttribute value is not a valid UTF-8 string")
)
