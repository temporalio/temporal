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
	"fmt"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

// Stringify converts map of search attribute Payloads to map of strings.
// In case of error, messages is stored as value for corresponding search attribute and last error is returned.
func Stringify(searchAttributes *commonpb.SearchAttributes) (map[string]string, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil, nil
	}

	result := make(map[string]string, len(searchAttributes.GetIndexedFields()))
	var lastErr error

	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		saValue, err := DecodeValue(saPayload)
		if err != nil {
			result[saName] = err.Error()
			lastErr = err
			continue
		}

		switch saTypedValue := saValue.(type) {
		case string:
			result[saName] = saTypedValue
		case int64:
			result[saName] = strconv.FormatInt(saTypedValue, 10)
		case float64:
			result[saName] = strconv.FormatFloat(saTypedValue, 'f', -1, 64)
		case bool:
			result[saName] = strconv.FormatBool(saTypedValue)
		case time.Time:
			result[saName] = saTypedValue.Format(time.RFC3339)
		default:
			result[saName] = fmt.Sprintf("%v", saTypedValue)
		}
	}

	return result, lastErr
}

// Parse converts maps of search attribute JSON strings to map of search attribute Payloads.
func Parse(searchAttributesString map[string]string, validSearchAttributes map[string]enumspb.IndexedValueType) (*commonpb.SearchAttributes, error) {
	if len(searchAttributesString) == 0 {
		return nil, nil
	}

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: make(map[string]*commonpb.Payload, len(searchAttributesString)),
	}
	var lastErr error

	for saName, saValString := range searchAttributesString {
		// TODO: support nil validSearchAttributes ??
		ivt, isValidSearchAttribute := validSearchAttributes[saName]
		if !isValidSearchAttribute {
			ivt = enumspb.INDEXED_VALUE_TYPE_STRING
			lastErr = fmt.Errorf("%w: %s", ErrInvalidName, saName)
		}
		saValPayload, err := parseValue(saValString, ivt)
		if err != nil {
			lastErr = err
		}
		searchAttributes.IndexedFields[saName] = saValPayload
	}

	return searchAttributes, lastErr
}

func parseValue(val string, valueType enumspb.IndexedValueType) (*commonpb.Payload, error) {
	var valObject interface{}
	var err error
	switch valueType {
	// TODO: Array support!!!
	case enumspb.INDEXED_VALUE_TYPE_STRING, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		valObject = val
	case enumspb.INDEXED_VALUE_TYPE_INT:
		valObject, err = strconv.ParseInt(val, 10, 64)
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		valObject, err = strconv.ParseFloat(val, 64)
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		valObject, err = strconv.ParseBool(val)
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		valObject, err = time.Parse(time.RFC3339, val)
	default:
		err = fmt.Errorf("%w: %v", ErrInvalidMetadataIndexValueType, valueType)
	}

	if err != nil {
		return nil, err
	}

	valPayload, err := payload.Encode(valObject)
	if err != nil {
		return nil, err
	}

	setPayloadType(valPayload, valueType)
	return valPayload, nil
}
