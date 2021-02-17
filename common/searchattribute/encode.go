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

// DecodeValue decodes search attribute value from Payload using (in order):
// 1. type from MetadataType field,
// 2. passed type t.
func DecodeValue(value *commonpb.Payload, t enumspb.IndexedValueType) (interface{}, error) {
	valueTypeMetadata, metadataHasValueType := value.Metadata[MetadataType]
	if metadataHasValueType {
		ivt, err := getIndexedValueType(string(valueTypeMetadata))
		if err == nil {
			// MetadataType field has priority over passed type.
			t = ivt
		}
	}

	switch t {
	case enumspb.INDEXED_VALUE_TYPE_STRING, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		var val string
		if err := payload.Decode(value, &val); err != nil {
			var listVal []string
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_INT:
		var val int64
		if err := payload.Decode(value, &val); err != nil {
			var listVal []int64
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		var val float64
		if err := payload.Decode(value, &val); err != nil {
			var listVal []float64
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		var val bool
		if err := payload.Decode(value, &val); err != nil {
			var listVal []bool
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		var val time.Time
		if err := payload.Decode(value, &val); err != nil {
			var listVal []time.Time
			err = payload.Decode(value, &listVal)
			return listVal, err
		}
		return val, nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidType, t)
	}
}

// EncodeValue encodes search attribute value and IndexedValueType to Payload.
func EncodeValue(val interface{}, t enumspb.IndexedValueType) (*commonpb.Payload, error) {
	valPayload, err := payload.Encode(val)
	if err != nil {
		return nil, err
	}

	setMetadataType(valPayload, t)
	return valPayload, nil
}

// Encode encodes map of search attribute values to map of Payloads.
func Encode(searchAttributes map[string]interface{}, validSearchAttributes map[string]enumspb.IndexedValueType) (*commonpb.SearchAttributes, error) {
	if len(searchAttributes) == 0 {
		return nil, nil
	}

	indexedFields := make(map[string]*commonpb.Payload, len(searchAttributes))
	var lastErr error
	for saName, saValue := range searchAttributes {
		valPayload, err := payload.Encode(saValue)
		if err != nil {
			lastErr = err
			indexedFields[saName] = nil
			continue
		}

		indexedFields[saName] = valPayload

		if len(validSearchAttributes) == 0 {
			lastErr = ErrValidMapIsEmpty
			continue
		}

		ivt, ok := validSearchAttributes[saName]
		if !ok {
			lastErr = fmt.Errorf("%w: %s", ErrInvalidName, saName)
		}
		setMetadataType(valPayload, ivt)
	}
	return &commonpb.SearchAttributes{IndexedFields: indexedFields}, lastErr
}
