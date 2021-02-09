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

package searchattribute

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

var (
	ErrUnableToUnmarshalJson = errors.New("unable to unmarshal JSON")
)

// Stringify converts map of search attribute Payloads to map of JSON strings.
// In case of error, messages is stored as value for corresponding search attribute.
func Stringify(searchAttributes map[string]*commonpb.Payload) map[string]string {
	if len(searchAttributes) == 0 {
		return nil
	}

	result := make(map[string]string, len(searchAttributes))

	for searchAttrName, searchAttrPayload := range searchAttributes {
		searchAttrValue, err := DecodeValue(searchAttrPayload)
		if err != nil {
			// In case of error just use raw data (which is JSON anyway).
			result[searchAttrName] = string(searchAttrPayload.GetData())
			continue
		}
		searchAttrString, err := json.Marshal(searchAttrValue)
		if err != nil {
			// Should never happen.
			// strconv.Quote to fit JSON string encoding rules.
			result[searchAttrName] = strconv.Quote(err.Error())
			continue
		}
		result[searchAttrName] = string(searchAttrString)
	}

	return result
}

// Parse converts maps of search attribute JSON strings to map of search attribute Payloads.
func Parse(searchAttributesString map[string]string, validSearchAttributes map[string]enumspb.IndexedValueType) (map[string]*commonpb.Payload, error) {
	searchAttributesObject := make(map[string]interface{}, len(searchAttributesString))
	var lastErr error
	for saName, saValString := range searchAttributesString {
		var saValObj interface{}
		err := json.Unmarshal([]byte(saValString), &searchAttributesObject)
		if err != nil {
			lastErr = fmt.Errorf("%w: %v", ErrUnableToUnmarshalJson, err)
			continue
		}
		searchAttributesObject[saName] = saValObj
	}

	searchAttributes, err := Encode(searchAttributesObject, validSearchAttributes)
	if err != nil {
		lastErr = err
	}

	return searchAttributes, lastErr
}
