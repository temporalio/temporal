// Copyright (c) 2017 Uber Technologies, Inc.
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

package definition

import (
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
		CustomStringField:     commonpb.IndexedValueTypeString,
		CustomKeywordField:    commonpb.IndexedValueTypeKeyword,
		CustomIntField:        commonpb.IndexedValueTypeInt,
		CustomBoolField:       commonpb.IndexedValueTypeBool,
		CustomDoubleField:     commonpb.IndexedValueTypeDouble,
		CustomDatetimeField:   commonpb.IndexedValueTypeDatetime,
		TemporalChangeVersion: commonpb.IndexedValueTypeKeyword,
		BinaryChecksums:       commonpb.IndexedValueTypeKeyword,
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
	NamespaceID:     commonpb.IndexedValueTypeKeyword,
	WorkflowID:      commonpb.IndexedValueTypeKeyword,
	RunID:           commonpb.IndexedValueTypeKeyword,
	WorkflowType:    commonpb.IndexedValueTypeKeyword,
	StartTime:       commonpb.IndexedValueTypeInt,
	ExecutionTime:   commonpb.IndexedValueTypeInt,
	CloseTime:       commonpb.IndexedValueTypeInt,
	ExecutionStatus: commonpb.IndexedValueTypeInt,
	HistoryLength:   commonpb.IndexedValueTypeInt,
}

// IsSystemIndexedKey return true is key is system added
func IsSystemIndexedKey(key string) bool {
	_, ok := systemIndexedKeys[key]
	return ok
}
