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
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
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
		SaveSearchAttributes(ctx context.Context, indexName string, newCustomSearchAttributes map[string]enumspb.IndexedValueType) error
	}
)

var (
	ErrInvalidName = errors.New("invalid search attribute name")
	ErrInvalidType = errors.New("invalid search attribute type")
)

// ApplyTypeMap set type for all valid search attributes which don't have it.
// It doesn't do any validation and just skip invalid or already set search attributes.
func ApplyTypeMap(searchAttributes *commonpb.SearchAttributes, typeMap NameTypeMap) {
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		_, metadataHasValueType := saPayload.Metadata[MetadataType]
		if metadataHasValueType {
			continue
		}
		valueType, err := typeMap.getType(saName, customCategory|predefinedCategory)
		if err != nil {
			continue
		}
		setMetadataType(saPayload, valueType)
	}
}

func setMetadataType(p *commonpb.Payload, t enumspb.IndexedValueType) {
	if t == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		return
	}

	_, isValidT := enumspb.IndexedValueType_name[int32(t)]
	if !isValidT {
		panic(fmt.Sprintf("unknown index value type %v", t))
	}
	p.Metadata[MetadataType] = []byte(enumspb.IndexedValueType(t).String())
}

func getMetadataType(p *commonpb.Payload) (enumspb.IndexedValueType, error) {
	metadataType := string(p.Metadata[MetadataType])
	if len(metadataType) == 0 {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, nil
	}
	t, err := enumspb.IndexedValueTypeFromString(string(p.Metadata[MetadataType]))
	if err != nil {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("%w: %v", ErrInvalidType, metadataType)
	}
	return t, nil
}

// This may mutate saPtr and *saPtr
func AddSearchAttribute(saPtr **commonpb.SearchAttributes, key string, value *commonpb.Payload) {
	if *saPtr == nil {
		*saPtr = &commonpb.SearchAttributes{}
	}
	if (*saPtr).IndexedFields == nil {
		(*saPtr).IndexedFields = make(map[string]*commonpb.Payload)
	}
	(*saPtr).IndexedFields[key] = value
}

func GetSearchAttributeKeyFromPayloadMetadata(
	name string,
	payload *commonpb.Payload,
) (temporal.SearchAttributeKey, error) {
	t, err := getMetadataType(payload)
	if err != nil {
		return nil, err
	}
	switch t {
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		return temporal.NewSearchAttributeKeyBool(name), nil
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		return temporal.NewSearchAttributeKeyTime(name), nil
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		return temporal.NewSearchAttributeKeyFloat64(name), nil
	case enumspb.INDEXED_VALUE_TYPE_INT:
		return temporal.NewSearchAttributeKeyInt64(name), nil
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		return temporal.NewSearchAttributeKeyKeyword(name), nil
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		return temporal.NewSearchAttributeKeyString(name), nil
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		return temporal.NewSearchAttributeKeyKeywordList(name), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidType, t)
	}
}
