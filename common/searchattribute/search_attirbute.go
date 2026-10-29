//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination search_attribute_mock.go

package searchattribute

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/searchattribute/sadefs"
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
		sadefs.SetMetadataType(saPayload, valueType)
	}
}

// This may mutate saPtr and *saPtr
func AddSearchAttributes(saPtr **commonpb.SearchAttributes, sas ...chasm.SearchAttributeKeyValue) {
	if *saPtr == nil {
		*saPtr = &commonpb.SearchAttributes{}
	}
	if (*saPtr).IndexedFields == nil {
		(*saPtr).IndexedFields = make(map[string]*commonpb.Payload)
	}
	for _, sa := range sas {
		(*saPtr).IndexedFields[sa.Field] = sa.Value.MustEncode()
	}
}
