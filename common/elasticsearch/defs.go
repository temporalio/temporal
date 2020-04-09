package elasticsearch

import (
	indexergenpb "github.com/temporalio/temporal/.gen/proto/indexer"
)

// All legal fields allowed in elastic search index
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
	Memo            = "Memo"
	Encoding        = "Encoding"

	KafkaKey = "KafkaKey"
)

// Supported field types
var (
	FieldTypeString = indexergenpb.FieldType_String
	FieldTypeInt    = indexergenpb.FieldType_Int
	FieldTypeBool   = indexergenpb.FieldType_Bool
	FieldTypeBinary = indexergenpb.FieldType_Binary
)
