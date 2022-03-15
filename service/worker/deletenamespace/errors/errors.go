package errors

import (
	"errors"
)

var (
	ErrUnableToExecuteActivity      = errors.New("unable to execute activity")
	ErrUnableToExecuteChildWorkflow = errors.New("unable to execute child workflow")
)
