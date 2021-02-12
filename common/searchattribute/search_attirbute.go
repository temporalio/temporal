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
	ErrUnableToSetMetadataType       = errors.New("unable to set search attribute metadata type")
	ErrInvalidName                   = errors.New("invalid search attribute name")
)

// ConvertDynamicConfigTypeToIndexedValueType takes dynamicConfigType as interface{} and convert to IndexedValueType.
// This func is because different implementation of dynamic config client may have different type of dynamicConfigType
// and to support backward compatibility.
func ConvertDynamicConfigTypeToIndexedValueType(dynamicConfigType interface{}) enumspb.IndexedValueType {
	switch t := dynamicConfigType.(type) {
	case float64:
		return enumspb.IndexedValueType(t)
	case int:
		return enumspb.IndexedValueType(t)
	case string:
		if ivt, ok := enumspb.IndexedValueType_value[t]; ok {
			return enumspb.IndexedValueType(ivt)
		}
	case enumspb.IndexedValueType:
		return t
	}

	// Unknown dynamicConfigType, please make sure dynamic config return correct value type.
	// panic will be captured by logger.
	panic(fmt.Sprintf("unknown index value type %v of type %T", dynamicConfigType, dynamicConfigType))
}

// ConvertDynamicConfigToIndexedValueTypes converts search attributes from dynamic config to typed map.
func ConvertDynamicConfigToIndexedValueTypes(validSearchAttributesFn dynamicconfig.MapPropertyFn) map[string]enumspb.IndexedValueType {
	if validSearchAttributesFn == nil {
		return nil
	}

	validSearchAttributes := validSearchAttributesFn()
	if validSearchAttributes == nil {
		return nil
	}

	result := make(map[string]enumspb.IndexedValueType, len(validSearchAttributes))
	for searchAttributeName, searchAttributeType := range validSearchAttributes {
		result[searchAttributeName] = ConvertDynamicConfigTypeToIndexedValueType(searchAttributeType)
	}
	return result
}

// DecodeValue decodes search attribute value from Payload with it's type in metadata "type" field.
func DecodeValue(value *commonpb.Payload) (interface{}, error) {
	valueTypeMetadata, metadataHasValueType := value.Metadata[MetadataType]
	if !metadataHasValueType {
		return nil, ErrMissingMetadataType
	}

	valueType, isValidValueType := enumspb.IndexedValueType_value[string(valueTypeMetadata)]
	if !isValidValueType {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMetadataType, string(valueTypeMetadata))
	}

	switch enumspb.IndexedValueType(valueType) {
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
		return nil, fmt.Errorf("%w: %v", ErrInvalidMetadataIndexValueType, valueType)
	}
}

// EncodeValue encodes search attribute value and IndexedValueType to Payload.
func EncodeValue(val interface{}, ivt enumspb.IndexedValueType) (*commonpb.Payload, error) {
	valPayload, err := payload.Encode(val)
	if err != nil {
		return nil, err
	}

	setPayloadType(valPayload, ivt)
	return valPayload, nil
}

// Encode encodes map of search attribute values to map of Payloads.
func Encode(searchAttributes map[string]interface{}, validSearchAttributes map[string]enumspb.IndexedValueType) (map[string]*commonpb.Payload, error) {
	indexedFields := make(map[string]*commonpb.Payload, len(searchAttributes))
	var lastErr error
	for attrName, attrValue := range searchAttributes {
		valPayload, err := payload.Encode(attrValue)
		if err != nil {
			lastErr = err
			continue
		}

		if validSearchAttributes == nil {
			lastErr = fmt.Errorf("%w: %s", ErrUnableToSetMetadataType, attrName)
			continue
		}

		ivt, ok := validSearchAttributes[attrName]
		if !ok {
			lastErr = fmt.Errorf("%w: %s", ErrUnableToSetMetadataType, attrName)
			continue
		}

		setPayloadType(valPayload, ivt)
		indexedFields[attrName] = valPayload
	}
	return indexedFields, lastErr
}

// SetTypes set types for all valid search attributes which don't have it.
// It doesn't do any validation and just skip invalid or already set fields.
func SetTypes(searchAttributes *commonpb.SearchAttributes, validSearchAttributes map[string]enumspb.IndexedValueType) {
	if validSearchAttributes == nil {
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
		setPayloadType(searchAttributePayload, valueType)
	}
}

func setPayloadType(p *commonpb.Payload, t enumspb.IndexedValueType) {
	tString, isValidT := enumspb.IndexedValueType_name[int32(t)]
	if !isValidT {
		panic(fmt.Sprintf("unknown index value type %v", t))
	}
	p.Metadata[MetadataType] = []byte(tString)
}
