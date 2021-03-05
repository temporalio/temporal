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

package searchattribute

import (
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	// Indexed fields on ES.
	NamespaceID     = "NamespaceId"
	WorkflowID      = "WorkflowId"
	RunID           = "RunId"
	WorkflowType    = "WorkflowType"
	StartTime       = "StartTime"
	ExecutionTime   = "ExecutionTime"
	CloseTime       = "CloseTime"
	ExecutionStatus = "ExecutionStatus"

	// TODO (alex): move these 3 to non-indexed.
	HistoryLength = "HistoryLength"
	TaskQueue     = "TaskQueue"
	Encoding      = "Encoding"

	// Valid non-indexed fields on ES.
	Memo              = "Memo"
	VisibilityTaskKey = "VisibilityTaskKey"

	// Attr is prefix of custom search attributes.
	Attr = "Attr"
	// Custom search attributes.
	CustomStringField     = "CustomStringField"
	CustomKeywordField    = "CustomKeywordField"
	CustomIntField        = "CustomIntField"
	CustomDoubleField     = "CustomDoubleField"
	CustomBoolField       = "CustomBoolField"
	CustomDatetimeField   = "CustomDatetimeField"
	TemporalChangeVersion = "TemporalChangeVersion"
	BinaryChecksums       = "BinaryChecksums"
	CustomNamespace       = "CustomNamespace"
	Operator              = "Operator"
)

var (
	// systemTypeMap is internal system search attributes.
	systemTypeMap = map[string]enumspb.IndexedValueType{
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
		Encoding:        enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}

	// defaultTypeMap defines all search attributes.
	defaultTypeMap = newDefaultTypeMap()
)

func newDefaultTypeMap() map[string]interface{} {
	result := map[string]interface{}{
		CustomStringField:     enumspb.INDEXED_VALUE_TYPE_STRING,
		CustomKeywordField:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		CustomIntField:        enumspb.INDEXED_VALUE_TYPE_INT,
		CustomDoubleField:     enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		CustomBoolField:       enumspb.INDEXED_VALUE_TYPE_BOOL,
		CustomDatetimeField:   enumspb.INDEXED_VALUE_TYPE_DATETIME,
		TemporalChangeVersion: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BinaryChecksums:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		CustomNamespace:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		Operator:              enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
	for k, v := range systemTypeMap {
		result[k] = v
	}
	return result
}

// GetDefaultTypeMap return default valid search attributes.
func GetDefaultTypeMap() map[string]interface{} {
	return defaultTypeMap
}

// IsSystem return true if search attribute is system.
func IsSystem(saName string) bool {
	_, ok := systemTypeMap[saName]
	return ok
}
