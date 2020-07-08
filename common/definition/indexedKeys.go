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

package definition

import (
	enumspb "go.temporal.io/api/enums/v1"
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
	TaskQueue       = "TaskQueue"

	CustomStringField     = "CustomStringField"
	CustomKeywordField    = "CustomKeywordField"
	CustomIntField        = "CustomIntField"
	CustomDoubleField     = "CustomDoubleField"
	CustomBoolField       = "CustomBoolField"
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
		CustomStringField:     enumspb.INDEXED_VALUE_TYPE_STRING,
		CustomKeywordField:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		CustomIntField:        enumspb.INDEXED_VALUE_TYPE_INT,
		CustomDoubleField:     enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		CustomBoolField:       enumspb.INDEXED_VALUE_TYPE_BOOL,
		CustomDatetimeField:   enumspb.INDEXED_VALUE_TYPE_DATETIME,
		TemporalChangeVersion: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BinaryChecksums:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
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
	NamespaceID:     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	WorkflowID:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	RunID:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	WorkflowType:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	StartTime:       enumspb.INDEXED_VALUE_TYPE_INT,
	ExecutionTime:   enumspb.INDEXED_VALUE_TYPE_INT,
	CloseTime:       enumspb.INDEXED_VALUE_TYPE_INT,
	ExecutionStatus: enumspb.INDEXED_VALUE_TYPE_INT,
	HistoryLength:   enumspb.INDEXED_VALUE_TYPE_INT,
	TaskQueue:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
}

// IsSystemIndexedKey return true is key is system added
func IsSystemIndexedKey(key string) bool {
	_, ok := systemIndexedKeys[key]
	return ok
}
