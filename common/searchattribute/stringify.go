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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

// Stringify converts search attributes to map of strings using (in order):
// 1. type from MetadataType field,
// 2. type from typeMap (can be nil).
// In case of error, it will continue to next search attribute and return last error.
// Single values are converted using strconv, arrays are converted using json.Marshal.
// Search attributes with `nil` values are skipped.
func Stringify(searchAttributes *commonpb.SearchAttributes, typeMap *NameTypeMap) (map[string]string, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil, nil
	}

	result := make(map[string]string, len(searchAttributes.GetIndexedFields()))
	var lastErr error

	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		saType := enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
		if typeMap != nil {
			saType, _ = typeMap.getType(saName, customCategory|predefinedCategory)
		}
		saValue, err := DecodeValue(saPayload, saType)
		if err != nil {
			// If DecodeValue failed, save error and use raw JSON from Data field.
			result[saName] = string(saPayload.GetData())
			lastErr = err
			continue
		}

		if saValue == nil {
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
			result[saName] = saTypedValue.Format(time.RFC3339Nano)
		default:
			switch reflect.TypeOf(saValue).Kind() {
			case reflect.Slice, reflect.Array:
				valBytes, err := json.Marshal(saValue)
				if err != nil {
					result[saName] = string(saPayload.GetData())
					lastErr = err
					continue
				}
				result[saName] = string(valBytes)
			default:
				result[saName] = fmt.Sprintf("%v", saTypedValue)
			}
		}
	}

	return result, lastErr
}

// Parse converts maps of search attribute strings to search attributes.
// typeMap can be nil (values will be parsed with strconv and MetadataType field won't be set).
// In case of error, it will continue to next search attribute and return last error.
// Single values are parsed using strconv, arrays are parsed using json.Unmarshal.
func Parse(searchAttributesStr map[string]string, typeMap *NameTypeMap) (*commonpb.SearchAttributes, error) {
	if len(searchAttributesStr) == 0 {
		return nil, nil
	}

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: make(map[string]*commonpb.Payload, len(searchAttributesStr)),
	}
	var lastErr error

	for saName, saValStr := range searchAttributesStr {
		saType := enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
		if typeMap != nil {
			saType, _ = typeMap.getType(saName, customCategory|predefinedCategory)
		}
		saValPayload, err := parseValueOrArray(saValStr, saType)
		if err != nil {
			lastErr = err
		}
		searchAttributes.IndexedFields[saName] = saValPayload
	}

	return searchAttributes, lastErr
}

func parseValueOrArray(valStr string, t enumspb.IndexedValueType) (*commonpb.Payload, error) {
	var val interface{}

	if isJsonArray(valStr) {
		var err error
		val, err = parseJsonArray(valStr, t)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		val, err = parseValueTyped(valStr, t)
		if err != nil {
			return nil, err
		}
	}

	valPayload, err := payload.Encode(val)
	if err != nil {
		return nil, err
	}

	setMetadataType(valPayload, t)
	return valPayload, nil
}

func parseValueTyped(valStr string, t enumspb.IndexedValueType) (interface{}, error) {
	var val interface{}
	var err error

	switch t {
	case enumspb.INDEXED_VALUE_TYPE_TEXT, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		val = valStr
	case enumspb.INDEXED_VALUE_TYPE_INT:
		val, err = strconv.ParseInt(valStr, 10, 64)
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		val, err = strconv.ParseFloat(valStr, 64)
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		val, err = strconv.ParseBool(valStr)
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		val, err = time.Parse(time.RFC3339Nano, valStr)
	case enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED:
		val = parseValueUnspecified(valStr)
	default:
		err = fmt.Errorf("%w: %v", ErrInvalidType, t)
	}

	return val, err
}

func parseValueUnspecified(valStr string) interface{} {
	var val interface{}
	var err error

	if val, err = strconv.ParseInt(valStr, 10, 64); err == nil {
	} else if val, err = strconv.ParseBool(valStr); err == nil {
	} else if val, err = strconv.ParseFloat(valStr, 64); err == nil {
	} else if val, err = time.Parse(time.RFC3339Nano, valStr); err == nil {
	} else if isJsonArray(valStr) {
		arr, err := parseJsonArray(valStr, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED)
		if err != nil {
			val = valStr
		} else {
			val = arr
		}
	} else {
		val = valStr
	}

	return val
}

func isJsonArray(str string) bool {
	str = strings.TrimSpace(str)
	return strings.HasPrefix(str, "[") && strings.HasSuffix(str, "]")
}

func parseJsonArray(str string, t enumspb.IndexedValueType) (interface{}, error) {
	switch t {
	case enumspb.INDEXED_VALUE_TYPE_TEXT, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		var result []string
		err := json.Unmarshal([]byte(str), &result)
		return result, err
	case enumspb.INDEXED_VALUE_TYPE_INT:
		var result []int64
		err := json.Unmarshal([]byte(str), &result)
		return result, err
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		var result []float64
		err := json.Unmarshal([]byte(str), &result)
		return result, err
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		var result []bool
		err := json.Unmarshal([]byte(str), &result)
		return result, err
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		var result []time.Time
		err := json.Unmarshal([]byte(str), &result)
		return result, err
	case enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED:
		var result []interface{}
		err := json.Unmarshal([]byte(str), &result)
		return result, err
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidType, t)
	}
}
