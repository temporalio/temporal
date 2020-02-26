// Copyright (c) 2020 Temporal Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
