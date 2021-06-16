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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination search_attribute_mock.go

package searchattribute

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/dynamicconfig"
)

const (
	MetadataType = "type"
)

type (
	Provider interface {
		GetSearchAttributes(indexName string, forceRefreshCache bool) (NameTypeMap, error)
	}

	Manager interface {
		Provider
		SaveSearchAttributes(indexName string, newCustomSearchAttributes map[string]enumspb.IndexedValueType) error
	}
)

var (
	ErrInvalidName = errors.New("invalid search attribute name")
	ErrInvalidType = errors.New("invalid search attribute type")
)

// BuildTypeMap converts search attributes types from dynamic config map to type map.
// TODO: Remove after 1.10.0 release
func BuildTypeMap(validSearchAttributesFn dynamicconfig.MapPropertyFn) (map[string]enumspb.IndexedValueType, error) {
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
		result[saName], err = convertDynamicConfigType(saType)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ApplyTypeMap set type for all valid search attributes which don't have it.
// It doesn't do any validation and just skip invalid or already set search attributes.
func ApplyTypeMap(searchAttributes *commonpb.SearchAttributes, typeMap NameTypeMap) {
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		_, metadataHasValueType := saPayload.Metadata[MetadataType]
		if metadataHasValueType {
			continue
		}
		valueType, err := typeMap.GetType(saName)
		if err != nil {
			continue
		}
		setMetadataType(saPayload, valueType)
	}
}

// convertDynamicConfigType takes dynamicConfigType as interface{} and convert to IndexedValueType.
// This func is needed because different implementation of dynamic config client may have different type of dynamicConfigType
// and to support backward compatibility.
// TODO: remove after 1.10.0 release.
func convertDynamicConfigType(dynamicConfigType interface{}) (enumspb.IndexedValueType, error) {
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
	if t == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		return
	}

	tString, isValidT := enumspb.IndexedValueType_name[int32(t)]
	if !isValidT {
		panic(fmt.Sprintf("unknown index value type %v", t))
	}
	p.Metadata[MetadataType] = []byte(tString)
}
