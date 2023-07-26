// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"
	"encoding/gob"
	"fmt"

	"go.temporal.io/api/serviceerror"
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

func gobSerialize(x interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(x)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Error in serialization: %v", err))
	}
	return b.Bytes(), nil
}

func gobDeserialize(a []byte, x interface{}) error {
	b := bytes.NewBuffer(a)
	d := gob.NewDecoder(b)
	err := d.Decode(x)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Error in deserialization: %v", err))
	}
	return nil
}
