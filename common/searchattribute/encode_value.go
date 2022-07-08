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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

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
func DecodeValue(value *commonpb.Payload, t enumspb.IndexedValueType) (interface{}, error) {
	if t == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		t = enumspb.IndexedValueType(enumspb.IndexedValueType_value[string(value.Metadata[MetadataType])])
	}

	// Here are similar code sections for all types.
	// At first, it tries to decode to pointer of actual type (i.e. `*string` for `string`).
	// This is to ensure that `nil` values are decoded back as `nil` using `NilPayloadConverter`.
	// If value is not `nil` but some value of expected type, the code relies on the fact that
	// search attributes are always encoded with `JsonPayloadConverter`, which uses standard
	// `json.Unmarshal` function, which works fine with pointer types when decoding values.
	// If decoding to pointer type fails, it tries to decode to array of the same type because
	// search attributes support polymorphism: field of specific type may also have an array of that type.
	// If resulting slice has zero length, it gets substitute with `nil` to treat nils and empty slices equally.
	// If search attribute value is `nil`, it means that search attribute needs to be removed from the document.

	switch t {
	case enumspb.INDEXED_VALUE_TYPE_TEXT, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		var val *string
		if err := payload.Decode(value, &val); err != nil {
			var listVal []string
			err = payload.Decode(value, &listVal)
			if len(listVal) == 0 {
				return nil, err
			}
			return listVal, err
		}
		if val == nil {
			return nil, nil
		}
		return *val, nil
	case enumspb.INDEXED_VALUE_TYPE_INT:
		var val *int64
		if err := payload.Decode(value, &val); err != nil {
			var listVal []int64
			err = payload.Decode(value, &listVal)
			if len(listVal) == 0 {
				return nil, err
			}
			return listVal, err
		}
		if val == nil {
			return nil, nil
		}
		return *val, nil
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		var val *float64
		if err := payload.Decode(value, &val); err != nil {
			var listVal []float64
			err = payload.Decode(value, &listVal)
			if len(listVal) == 0 {
				return nil, err
			}
			return listVal, err
		}
		if val == nil {
			return nil, nil
		}
		return *val, nil
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		var val *bool
		if err := payload.Decode(value, &val); err != nil {
			var listVal []bool
			err = payload.Decode(value, &listVal)
			if len(listVal) == 0 {
				return nil, err
			}
			return listVal, err
		}
		if val == nil {
			return nil, nil
		}
		return *val, nil
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		var val *time.Time
		if err := payload.Decode(value, &val); err != nil {
			var listVal []time.Time
			err = payload.Decode(value, &listVal)
			if len(listVal) == 0 {
				return nil, err
			}
			return listVal, err
		}
		if val == nil {
			return nil, nil
		}
		return *val, nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidType, t)
	}
}
