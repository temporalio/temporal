// Copyright (c) 2020 Temporal Technologies, Inc.

package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/persistence"
)

type (
	// FieldNotFoundError is an error type returned when an untyped query return does not contain the expected fields.
	FieldNotFoundError struct {
		Msg string
	}
)

func (f FieldNotFoundError) Error() string {
	return f.Msg
}

func newFieldNotFoundError(fieldName string, payload map[string]interface{}) *FieldNotFoundError {
	return &FieldNotFoundError{Msg: fmt.Sprintf("Unable to find field '%s' in payload - '%v'", fieldName, payload)}
}

type (
	// PersistedTypeMismatchError is an error type returned when a persisted cassandra value does not match the expected type.
	PersistedTypeMismatchError struct {
		Msg string
	}
)

func (f PersistedTypeMismatchError) Error() string {
	return f.Msg
}

func newPersistedTypeMismatchError(
	fieldName string,
	expectedType interface{},
	received interface{},
	payload map[string]interface{},
) *PersistedTypeMismatchError {
	return &PersistedTypeMismatchError{
		Msg: fmt.Sprintf("Field '%s' is of type '%T' but expected type '%T' in payload - '%v'",
			fieldName, received, expectedType, payload)}
}

func convertCommonErrors(
	operation string,
	err error,
) error {
	if err == gocql.ErrNotFound {
		return serviceerror.NewNotFound(fmt.Sprintf("%v failed. Error: %v ", operation, err))
	} else if isTimeoutError(err) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("%v timed out. Error: %v", operation, err)}
	} else if isThrottlingError(err) {
		return serviceerror.NewResourceExhausted(fmt.Sprintf("%v operation failed. Error: %v", operation, err))
	}
	return serviceerror.NewInternal(fmt.Sprintf("%v operation failed. Error: %v", operation, err))
}
