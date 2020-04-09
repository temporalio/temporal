package definition

import (
	commonpb "go.temporal.io/temporal-proto/common"
)

// valid indexed fields on ES
const (
	NamespaceID     = "NamespaceId"
	WorkflowID      = "WorkflowId"
	RunID           = "RunId"
	WorkflowType    = "WorkflowType"
	StartTime       = "StartTime"
	ExecutionTime   = "ExecutionTime"
	CloseTime       = "CloseTime"
	ExecutionStatus = "ExecutionStatus"
	HistoryLength   = "HistoryLength"
	Encoding        = "Encoding"
	KafkaKey        = "KafkaKey"
	BinaryChecksums = "BinaryChecksums"

	CustomStringField     = "CustomStringField"
	CustomKeywordField    = "CustomKeywordField"
	CustomIntField        = "CustomIntField"
	CustomBoolField       = "CustomBoolField"
	CustomDoubleField     = "CustomDoubleField"
	CustomDatetimeField   = "CustomDatetimeField"
	TemporalChangeVersion = "TemporalChangeVersion"
)

// valid non-indexed fields on ES
const (
	Memo = "Memo"
)

// Attr is prefix of custom search attributes
const Attr = "Attr"

// defaultIndexedKeys defines all searchable keys
var defaultIndexedKeys = createDefaultIndexedKeys()

func createDefaultIndexedKeys() map[string]interface{} {
	defaultIndexedKeys := map[string]interface{}{
		CustomStringField:     commonpb.IndexedValueType_String,
		CustomKeywordField:    commonpb.IndexedValueType_Keyword,
		CustomIntField:        commonpb.IndexedValueType_Int,
		CustomBoolField:       commonpb.IndexedValueType_Bool,
		CustomDoubleField:     commonpb.IndexedValueType_Double,
		CustomDatetimeField:   commonpb.IndexedValueType_Datetime,
		TemporalChangeVersion: commonpb.IndexedValueType_Keyword,
		BinaryChecksums:       commonpb.IndexedValueType_Keyword,
	}
	for k, v := range systemIndexedKeys {
		defaultIndexedKeys[k] = v
	}
	return defaultIndexedKeys
}

// GetDefaultIndexedKeys return default valid indexed keys
func GetDefaultIndexedKeys() map[string]interface{} {
	return defaultIndexedKeys
}

// systemIndexedKeys is Temporal created visibility keys
var systemIndexedKeys = map[string]interface{}{
	NamespaceID:     commonpb.IndexedValueType_Keyword,
	WorkflowID:      commonpb.IndexedValueType_Keyword,
	RunID:           commonpb.IndexedValueType_Keyword,
	WorkflowType:    commonpb.IndexedValueType_Keyword,
	StartTime:       commonpb.IndexedValueType_Int,
	ExecutionTime:   commonpb.IndexedValueType_Int,
	CloseTime:       commonpb.IndexedValueType_Int,
	ExecutionStatus: commonpb.IndexedValueType_Int,
	HistoryLength:   commonpb.IndexedValueType_Int,
}

// IsSystemIndexedKey return true is key is system added
func IsSystemIndexedKey(key string) bool {
	_, ok := systemIndexedKeys[key]
	return ok
}
