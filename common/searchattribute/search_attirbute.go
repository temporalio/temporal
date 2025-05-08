//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination search_attribute_mock.go

package searchattribute

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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
