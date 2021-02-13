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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/service/dynamicconfig"
)

const (
	MetadataType = "type"
)

var (
	ErrMissingMetadataType           = errors.New("search attribute metadata doesn't have value type")
	ErrInvalidMetadataType           = errors.New("search attribute metadata has invalid value type")
	ErrInvalidMetadataIndexValueType = errors.New("search attribute metadata has invalid index value type")
	ErrInvalidName                   = errors.New("invalid search attribute name")
	ErrInvalidType                   = errors.New("invalid search attribute type")
	ErrValidMapIsEmpty               = errors.New("valid search attributes map is empty")
	ErrValueNotArray                 = errors.New("value is not an array")
)

// GetTypeMap converts search attributes types from dynamic config map to typed map.
func GetTypeMap(validSearchAttributesFn dynamicconfig.MapPropertyFn) (map[string]enumspb.IndexedValueType, error) {
	if validSearchAttributesFn == nil {
		return nil, nil
	}
	validSearchAttributes := validSearchAttributesFn()
	if len(validSearchAttributes) == 0 {
		return nil, nil
	}

	result := make(map[string]enumspb.IndexedValueType, len(validSearchAttributes))
	for saName, saType := range validSearchAttributes {
		var err error
		result[saName], err = getIndexedValueType(saType)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// GetType returns type of search attribute from dynamic config map.
func GetType(name string, validSearchAttributesFn dynamicconfig.MapPropertyFn) (enumspb.IndexedValueType, error) {
	if validSearchAttributesFn == nil {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ErrValidMapIsEmpty
	}

	validSearchAttributes := validSearchAttributesFn()
	if len(validSearchAttributes) == 0 {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ErrValidMapIsEmpty
	}

	saType, isValidName := validSearchAttributes[name]
	if !isValidName {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("%w: %s", ErrInvalidName, name)
	}
	return getIndexedValueType(saType)
}

// DecodeValue decodes search attribute value from Payload with it's type in metadata "type" field.
func DecodeValue(value *commonpb.Payload, t enumspb.IndexedValueType) (interface{}, error) {
	valueTypeMetadata, metadataHasValueType := value.Metadata[MetadataType]
	if metadataHasValueType {
		ivt, err := getIndexedValueType(string(valueTypeMetadata))
		if err != nil {
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

// SetTypes set types for all valid search attributes which don't have it.
// It doesn't do any validation and just skip invalid or already set fields.
func SetTypes(searchAttributes *commonpb.SearchAttributes, validSearchAttributes map[string]enumspb.IndexedValueType) {
	if len(validSearchAttributes) == 0 {
		return
	}

	for searchAttributeName, searchAttributePayload := range searchAttributes.GetIndexedFields() {
		_, metadataHasValueType := searchAttributePayload.Metadata[MetadataType]
		if metadataHasValueType {
			continue
		}
		valueType, isValidSearchAttribute := validSearchAttributes[searchAttributeName]
		if !isValidSearchAttribute {
			continue
		}
		setMetadataType(searchAttributePayload, valueType)
	}
}

// getIndexedValueType takes dynamicConfigType as interface{} and convert to IndexedValueType.
// This func is because different implementation of dynamic config client may have different type of dynamicConfigType
// and to support backward compatibility.
func getIndexedValueType(dynamicConfigType interface{}) (enumspb.IndexedValueType, error) {
	switch t := dynamicConfigType.(type) {
	case float64:
		ivt := enumspb.IndexedValueType(t)
		if _, isValid := enumspb.IndexedValueType_name[int32(ivt)]; isValid {
			return ivt, nil
		}
	case int:
		ivt := enumspb.IndexedValueType(t)
		if _, isValid := enumspb.IndexedValueType_name[int32(ivt)]; isValid {
			return ivt, nil
		}
	case string:
		if ivt, ok := enumspb.IndexedValueType_value[t]; ok {
			return enumspb.IndexedValueType(ivt), nil
		}
	case enumspb.IndexedValueType:
		if _, isValid := enumspb.IndexedValueType_name[int32(t)]; isValid {
			return t, nil
		}
	}

	return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("%w: %v of type %T", ErrInvalidType, dynamicConfigType, dynamicConfigType)
}

func setMetadataType(p *commonpb.Payload, t enumspb.IndexedValueType) {
	tString, isValidT := enumspb.IndexedValueType_name[int32(t)]
	if !isValidT {
		panic(fmt.Sprintf("unknown index value type %v", t))
	}
	p.Metadata[MetadataType] = []byte(tString)
}

func getType(name string, validSearchAttributes map[string]enumspb.IndexedValueType) enumspb.IndexedValueType {
	if len(validSearchAttributes) == 0 {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}

	saType, isValidName := validSearchAttributes[name]
	if !isValidName {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}
	return saType
}
