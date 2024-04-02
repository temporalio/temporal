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
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

var ErrInvalidString = errors.New("SearchAttribute value is not a valid UTF-8 string")

// EncodeValue encodes search attribute value and IndexedValueType to Payload.
func EncodeValue(val interface{}, t enumspb.IndexedValueType) (*commonpb.Payload, error) {
	valPayload, err := payload.Encode(val)
	if err != nil {
		return nil, err
	}

	setMetadataType(valPayload, t)
	return valPayload, nil
}

// DecodeValue decodes search attribute value from Payload using (in order):
// 1. passed type t.
// 2. type from MetadataType field, if t is not specified.
// allowList allows list of values when it's not keyword list type.
func DecodeValue(
	value *commonpb.Payload,
	t enumspb.IndexedValueType,
	allowList bool,
) (any, error) {
	if t == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		var err error
		t, err = enumspb.IndexedValueTypeFromString(string(value.Metadata[MetadataType]))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidType, t)
		}
	}

	switch t {
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		return decodeValueTyped[bool](value, allowList)
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		return decodeValueTyped[time.Time](value, allowList)
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		return decodeValueTyped[float64](value, allowList)
	case enumspb.INDEXED_VALUE_TYPE_INT:
		return decodeValueTyped[int64](value, allowList)
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		return validateStrings(decodeValueTyped[string](value, allowList))
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		return validateStrings(decodeValueTyped[string](value, allowList))
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		return validateStrings(decodeValueTyped[[]string](value, false))
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidType, t)
	}
}

func validateStrings(anyValue any, err error) (any, error) {
	if err != nil {
		return anyValue, err
	}

	// validate strings
	switch value := anyValue.(type) {
	case string:
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidString, value)
		}
	case []string:
		for _, item := range value {
			if !utf8.ValidString(item) {
				return nil, fmt.Errorf("%w: %s", ErrInvalidString, item)
			}
		}
	}
	return anyValue, err
}

// decodeValueTyped tries to decode to the given type.
// If the input is a list and allowList is false, then it will return only the first element.
// If the input is a list and allowList is true, then it will return the decoded list.
//
//nolint:revive // allowList is a control flag
func decodeValueTyped[T any](value *commonpb.Payload, allowList bool) (any, error) {
	// At first, it tries to decode to pointer of actual type (i.e. `*string` for `string`).
	// This is to ensure that `nil` values are decoded back as `nil` using `NilPayloadConverter`.
	// If value is not `nil` but some value of expected type, the code relies on the fact that
	// search attributes are always encoded with `JsonPayloadConverter`, which uses standard
	// `json.Unmarshal` function, which works fine with pointer types when decoding values.
	// If decoding to pointer type fails, it tries to decode to array of the same type because
	// search attributes support polymorphism: field of specific type may also have an array of that type.
	// If resulting slice has zero length, it gets substitute with `nil` to treat nils and empty slices equally.
	// If allowList is true, it returns the list as it is. If allowList is false and the list has
	// only one element, then return it. Otherwise, return an error.
	// If search attribute value is `nil`, it means that search attribute needs to be removed from the document.
	var val *T
	if err := payload.Decode(value, &val); err != nil {
		var listVal []T
		if err := payload.Decode(value, &listVal); err != nil {
			return nil, err
		}
		if len(listVal) == 0 {
			return nil, nil
		}
		if allowList {
			return listVal, nil
		}
		if len(listVal) == 1 {
			return listVal[0], nil
		}
		return nil, fmt.Errorf("list of values not allowed for type %T", listVal[0])
	}
	if val == nil {
		return nil, nil
	}
	return *val, nil
}
