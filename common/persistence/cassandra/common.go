package cassandra

import (
	"fmt"
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

func newFieldNotFoundError(fieldName string, payload map[string]interface{}) error {
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

// Returns a correctly typed value for fieldName retrieved from a row populated by a MapScan operation.
// Returns the zero value for the provided type and an appropriate error if the field is not found in
// the row or if the value cannot be cast to the provided type.
func getTypedFieldFromRow[T any](fieldName string, row map[string]interface{}) (T, error) {
	var zeroVal T // used as a placeholder for zero value of type T since we can't directly return nil

	raw, ok := row[fieldName]
	if !ok {
		return zeroVal, newFieldNotFoundError(fieldName, row)
	}

	typed, ok := raw.(T)
	if !ok {
		return zeroVal, newPersistedTypeMismatchError(fieldName, typed, raw, row)
	}

	return typed, nil
}
