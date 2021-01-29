package searchattribute

import (
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

const (
	MetadataType = "type"
)

var (
	ErrMissingMetadataType           = errors.New("search attribute metadata doesn't have value type")
	ErrInvalidMetadataType           = errors.New("search attribute metadata has invalid value type")
	ErrInvalidMetadataIndexValueType = errors.New("search attribute metadata has invalid index value type")
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

func ConvertDynamicConfigToIndexedValueTypes(validSearchAttributes map[string]interface{}) map[string]enumspb.IndexedValueType {
	result := make(map[string]enumspb.IndexedValueType, len(validSearchAttributes))
	for searchAttributeName, searchAttributeType := range validSearchAttributes {
		result[searchAttributeName] = ConvertDynamicConfigTypeToIndexedValueType(searchAttributeType)
	}
	return result
}

// Decode takes payload with search attribute value and it's type in
// metadata "type" field and decode the value into a concrete type.
func Decode(value *commonpb.Payload) (interface{}, error) {
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

// SetTypes set types for all valid search attributes which don't have it.
// It doesn't do any validation.
func SetType(searchAttributes *commonpb.SearchAttributes, validSearchAttributes map[string]enumspb.IndexedValueType) {
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
		valueTypeString, isValidValueType := enumspb.IndexedValueType_name[int32(valueType)]
		if !isValidValueType {
			continue
		}
		searchAttributePayload.Metadata[MetadataType] = []byte(valueTypeString)
	}
}
