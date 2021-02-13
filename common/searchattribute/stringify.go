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
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

// Stringify converts map of search attribute Payloads to map of strings.
// In case of error, messages is stored as value for corresponding search attribute and last error is returned.
// validSearchAttributes can be nil (MetadataType field will be used).
func Stringify(searchAttributes *commonpb.SearchAttributes, validSearchAttributes map[string]enumspb.IndexedValueType) (map[string]string, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil, nil
	}

	result := make(map[string]string, len(searchAttributes.GetIndexedFields()))
	var lastErr error

	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		saValue, err := DecodeValue(saPayload, getType(saName, validSearchAttributes))
		if err != nil {
			result[saName] = string(saPayload.GetData())
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
			switch reflect.TypeOf(saValue).Kind() {
			case reflect.Slice, reflect.Array:
				var b strings.Builder
				b.WriteRune('[')
				rv := reflect.ValueOf(saValue)
				for i := 0; i < rv.Len(); i++ {
					_, _ = fmt.Fprint(&b, rv.Index(i).Interface())
					!!!!!! just use json for arrays!
					b.WriteRune(',')
				}
				b.WriteRune(']')
			default:
				result[saName] = fmt.Sprintf("%v", saTypedValue)
			}
		}
	}

	return result, lastErr
}

// Parse converts maps of search attribute JSON strings to map of search attribute Payloads.
// validSearchAttributes can be nil (values will be parsed with strconv).
func Parse(searchAttributesString map[string]string, validSearchAttributes map[string]enumspb.IndexedValueType) (*commonpb.SearchAttributes, error) {
	if len(searchAttributesString) == 0 {
		return nil, nil
	}

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: make(map[string]*commonpb.Payload, len(searchAttributesString)),
	}
	var lastErr error

	for saName, saValString := range searchAttributesString {
		saType := enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
		if len(validSearchAttributes) > 0 {
			ivt, isValidName := validSearchAttributes[saName]
			if isValidName {
				saType = ivt
			}
		}
		saValPayload, err := parseValue(saValString, saType)
		if err != nil {
			lastErr = err
		}
		searchAttributes.IndexedFields[saName] = saValPayload
	}

	return searchAttributes, lastErr
}

func parseValue(valStr string, t enumspb.IndexedValueType) (*commonpb.Payload, error) {
	var val interface{}

	if isJsonArray(valStr) {
		arr, err := parseJsonArray(valStr)
		if err != nil {
			return nil, err
		}
		parsedArr := make([]interface{}, len(arr))
		for i, str := range arr {
			parsedArr[i], err = parseValueType(str, t)
			if err != nil {
				return nil, err
			}
		}
		val = parsedArr
	} else {
		var err error
		val, err = parseValueType(valStr, t)
		if err != nil {
			return nil, err
		}
	}

	valPayload, err := payload.Encode(val)
	if err != nil {
		return nil, err
	}

	if t != enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		setMetadataType(valPayload, t)
	}
	return valPayload, nil
}

func parseValueType(valStr string, t enumspb.IndexedValueType) (interface{}, error) {
	var val interface{}
	var err error

	switch t {
	case enumspb.INDEXED_VALUE_TYPE_STRING, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		val = valStr
	case enumspb.INDEXED_VALUE_TYPE_INT:
		val, err = strconv.ParseInt(valStr, 10, 64)
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		val, err = strconv.ParseFloat(valStr, 64)
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		val, err = strconv.ParseBool(valStr)
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		val, err = time.Parse(time.RFC3339, valStr)
	case enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED:
		val = parseValueUnspecified(valStr)
	default:
		err = fmt.Errorf("%w: %v", ErrInvalidType, t)
	}

	return val, err
}

func parseValueUnspecified(valStr string) interface{} {
	var parsedVal interface{}
	var err error

	if parsedVal, err = strconv.ParseInt(valStr, 10, 64); err == nil {
	} else if parsedVal, err = strconv.ParseBool(valStr); err == nil {
	} else if parsedVal, err = strconv.ParseFloat(valStr, 64); err == nil {
	} else if parsedVal, err = time.Parse(time.RFC3339, valStr); err == nil {
	} else if isJsonArray(valStr) {
		arr, err := parseJsonArray(valStr)
		if err != nil {
			parsedVal = valStr
		} else {
			parsedArr := make([]interface{}, len(arr))
			for i, str := range arr {
				parsedArr[i] = parseValueUnspecified(str)
			}
			parsedVal = parsedArr
		}

	} else {
		parsedVal = valStr
	}

	return parsedVal
}

func isJsonArray(json string) bool {
	json = strings.TrimSpace(json)
	return len(json) > 0 && json[0] == '[' && json[len(json)-1] == ']'
}

func parseJsonArray(json string) ([]string, error) {
	parsedValues, err := fastjson.Parse(json)
	if err != nil {
		return nil, err
	}
	arr, err := parsedValues.Array()
	if err != nil {
		return nil, err
	}
	result := make([]string, len(arr))
	for i, item := range arr {
		result[i] = string(item.MarshalTo(nil))
	}
	return result, nil
}
