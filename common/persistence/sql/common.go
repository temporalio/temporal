// Copyright (c) 2018 Uber Technologies, Inc.
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

package sql

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

func gobSerialize(x interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(x)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Error in serialization: %v", err),
		}
	}
	return b.Bytes(), nil
}

func gobDeserialize(a []byte, x interface{}) error {
	b := bytes.NewBuffer(a)
	d := gob.NewDecoder(b)
	err := d.Decode(x)

	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Error in deserialization: %v", err),
		}
	}
	return nil
}

const (
	dataSourceName = "%s:%s@tcp(%s:%d)/%s?multiStatements=true&tx_isolation=%%27READ-COMMITTED%%27&parseTime=true&clientFoundRows=true"
)

var maximumExpiryTs = time.Unix(1<<63-62135596801, 999999999)

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func int64ToBool(i int64) bool {
	if i == 0 {
		return false
	}
	return true
}

func takeAddressIfNotNil(a []byte) *[]byte {
	if a != nil {
		return &a
	}
	return nil
}

func dereferenceIfNotNil(a *[]byte) []byte {
	if a != nil {
		return *a
	}
	return nil
}
