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
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
)

const (
	NamespaceID          = "NamespaceId"
	WorkflowID           = "WorkflowId"
	RunID                = "RunId"
	WorkflowType         = "WorkflowType"
	StartTime            = "StartTime"
	ExecutionTime        = "ExecutionTime"
	CloseTime            = "CloseTime"
	ExecutionStatus      = "ExecutionStatus"
	TaskQueue            = "TaskQueue"
	HistoryLength        = "HistoryLength"
	ExecutionDuration    = "ExecutionDuration"
	StateTransitionCount = "StateTransitionCount"

	TemporalChangeVersion = "TemporalChangeVersion"
	BinaryChecksums       = "BinaryChecksums"
	BatcherNamespace      = "BatcherNamespace"
	BatcherUser           = "BatcherUser"

	// These fields are not in Elasticsearch mappings definition and therefore are not indexed.
	MemoEncoding      = "MemoEncoding"
	Memo              = "Memo"
	VisibilityTaskKey = "VisibilityTaskKey"

	// Added to workflows started by a schedule.
	TemporalScheduledStartTime = "TemporalScheduledStartTime"
	TemporalScheduledById      = "TemporalScheduledById"

	// Used by scheduler workflow.
	TemporalSchedulePaused = "TemporalSchedulePaused"
	// TemporalScheduleInfoJSON is not in Elasticsearch mappings definition and therefore is not indexed.
	TemporalScheduleInfoJSON = "TemporalScheduleInfoJSON"

	ReservedPrefix = "Temporal"
)

var (
	// system are internal search attributes which are passed and stored as separate fields.
	system = map[string]enumspb.IndexedValueType{
		WorkflowID:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		RunID:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		WorkflowType:         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		StartTime:            enumspb.INDEXED_VALUE_TYPE_DATETIME,
		ExecutionTime:        enumspb.INDEXED_VALUE_TYPE_DATETIME,
		CloseTime:            enumspb.INDEXED_VALUE_TYPE_DATETIME,
		ExecutionStatus:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TaskQueue:            enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		HistoryLength:        enumspb.INDEXED_VALUE_TYPE_INT,
		ExecutionDuration:    enumspb.INDEXED_VALUE_TYPE_INT,
		StateTransitionCount: enumspb.INDEXED_VALUE_TYPE_INT,
	}

	// predefined are internal search attributes which are passed and stored in SearchAttributes object together with custom search attributes.
	predefined = map[string]enumspb.IndexedValueType{
		TemporalChangeVersion:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BinaryChecksums:            enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BatcherNamespace:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BatcherUser:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalScheduledStartTime: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		TemporalScheduledById:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalSchedulePaused:     enumspb.INDEXED_VALUE_TYPE_BOOL,
		TemporalScheduleInfoJSON:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}

	// reserved are internal field names that can't be used as search attribute names.
	reserved = map[string]struct{}{
		NamespaceID:       {},
		MemoEncoding:      {},
		Memo:              {},
		VisibilityTaskKey: {},
	}
)

// IsReserved returns true if name is system reserved and can't be used as custom search attribute name.
func IsReserved(name string) bool {
	if _, ok := system[name]; ok {
		return true
	}
	if _, ok := predefined[name]; ok {
		return true
	}
	if _, ok := reserved[name]; ok {
		return true
	}
	return strings.HasPrefix(name, ReservedPrefix)
}

// IsMappable returns true if name can have be mapped tho the alias.
func IsMappable(name string) bool {
	if _, ok := system[name]; ok {
		return false
	}
	if _, ok := predefined[name]; ok {
		return false
	}
	return true
}
