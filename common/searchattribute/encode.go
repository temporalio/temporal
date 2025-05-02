package searchattribute

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/payload"
)

// Encode encodes map of search attribute values to search attributes.
// typeMap can be nil (then MetadataType field won't be set).
// In case of error, it will continue to next search attribute and return last error.
func Encode(searchAttributes map[string]interface{}, typeMap *NameTypeMap) (*commonpb.SearchAttributes, error) {
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
		saType := enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
		if typeMap != nil {
			saType, err = typeMap.getType(saName, customCategory|predefinedCategory)
			if err != nil {
				lastErr = err
				continue
			}
			setMetadataType(valPayload, saType)
		}
	}
	return &commonpb.SearchAttributes{IndexedFields: indexedFields}, lastErr
}

// Decode decodes search attributes to the map of search attribute values using (in order):
// 1. type from typeMap,
// 2. if typeMap is nil, type from MetadataType field is used.
// In case of error, it will continue to next search attribute and return last error.
func Decode(
	searchAttributes *commonpb.SearchAttributes,
	typeMap *NameTypeMap,
	allowList bool,
) (map[string]interface{}, error) {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil, nil
	}

	result := make(map[string]interface{}, len(searchAttributes.GetIndexedFields()))
	var lastErr error
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		saType := enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
		if typeMap != nil {
			var err error
			saType, err = typeMap.getType(saName, customCategory|predefinedCategory)
			if err != nil {
				lastErr = err
			}
		}

		searchAttributeValue, err := DecodeValue(saPayload, saType, allowList)
		if err != nil {
			lastErr = err
			result[saName] = nil
			continue
		}
		result[saName] = searchAttributeValue
	}

	return result, lastErr
}
