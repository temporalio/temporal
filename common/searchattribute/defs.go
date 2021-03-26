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
	TaskQueue       = "TaskQueue"

	// Valid non-indexed fields on ES.
	HistoryLength     = "HistoryLength"
	Encoding          = "Encoding"
	Memo              = "Memo"
	VisibilityTaskKey = "VisibilityTaskKey"

	// Attr is prefix for search attributes.
	Attr = "Attr"
	// System search attributes.
	TemporalChangeVersion = "TemporalChangeVersion"
	BinaryChecksums       = "BinaryChecksums"
	CustomNamespace       = "CustomNamespace"
	Operator              = "Operator"

	// Default custom search attributes.
	CustomStringField   = "CustomStringField"
	CustomKeywordField  = "CustomKeywordField"
	CustomIntField      = "CustomIntField"
	CustomDoubleField   = "CustomDoubleField"
	CustomBoolField     = "CustomBoolField"
	CustomDatetimeField = "CustomDatetimeField"
)

var (
	// reservedFields are internal field names that can't be used as search attribute names.
	reservedFields = map[string]struct{}{
		NamespaceID:     {},
		WorkflowID:      {},
		RunID:           {},
		WorkflowType:    {},
		StartTime:       {},
		ExecutionTime:   {},
		CloseTime:       {},
		ExecutionStatus: {},
		TaskQueue:       {},

		HistoryLength:     {},
		Encoding:          {},
		Memo:              {},
		VisibilityTaskKey: {},
	}

	// systemSearchAttributes are internal system search attributes which don't require Attr prefix.
	systemSearchAttributes = map[string]struct{}{
		NamespaceID:     {},
		WorkflowID:      {},
		RunID:           {},
		WorkflowType:    {},
		StartTime:       {},
		ExecutionTime:   {},
		CloseTime:       {},
		ExecutionStatus: {},
		TaskQueue:       {},
	}

	// builtInSearchAttributes are built-in internal search attributes.
	builtInSearchAttributes = map[string]struct{}{
		TemporalChangeVersion: {},
		BinaryChecksums:       {},
		CustomNamespace:       {},
		Operator:              {},
	}

	// defaultTypeMap defines default search attributes and their types.
	defaultTypeMap = map[string]interface{}{
		NamespaceID:     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		WorkflowID:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		RunID:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		WorkflowType:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		StartTime:       enumspb.INDEXED_VALUE_TYPE_INT,
		ExecutionTime:   enumspb.INDEXED_VALUE_TYPE_INT,
		CloseTime:       enumspb.INDEXED_VALUE_TYPE_INT,
		ExecutionStatus: enumspb.INDEXED_VALUE_TYPE_INT,
		TaskQueue:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,

		CustomStringField:   enumspb.INDEXED_VALUE_TYPE_STRING,
		CustomKeywordField:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		CustomIntField:      enumspb.INDEXED_VALUE_TYPE_INT,
		CustomDoubleField:   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		CustomBoolField:     enumspb.INDEXED_VALUE_TYPE_BOOL,
		CustomDatetimeField: enumspb.INDEXED_VALUE_TYPE_DATETIME,

		TemporalChangeVersion: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BinaryChecksums:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		CustomNamespace:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		Operator:              enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
)

// IsReservedField return true if field name is system reserved.
func IsReservedField(fieldName string) bool {
	_, ok := reservedFields[fieldName]
	return ok
}

// IsBuiltIn return true if search attribute is system built-in.
func IsBuiltIn(saName string) bool {
	_, ok := builtInSearchAttributes[saName]
	return ok
}

// IsSystem return true if search attribute is system and doesn't require Attr prefix.
func IsSystem(saName string) bool {
	_, ok := systemSearchAttributes[saName]
	return ok
}

// GetDefaultTypeMap return default valid search attributes.
func GetDefaultTypeMap() map[string]interface{} {
	return defaultTypeMap
}
