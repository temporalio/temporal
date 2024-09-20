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

// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package enums

import (
	"fmt"
)

var (
	ReplicationTaskType_shorthandValue = map[string]int32{
		"Unspecified":                   0,
		"NamespaceTask":                 1,
		"HistoryTask":                   2,
		"SyncShardStatusTask":           3,
		"SyncActivityTask":              4,
		"HistoryMetadataTask":           5,
		"HistoryV2Task":                 6,
		"SyncWorkflowStateTask":         7,
		"TaskQueueUserData":             8,
		"SyncHsmTask":                   9,
		"BackfillHistoryTask":           10,
		"VerifyVersionedTransitionTask": 11,
		"SyncVersionedTransitionTask":   12,
	}
)

// ReplicationTaskTypeFromString parses a ReplicationTaskType value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to ReplicationTaskType
func ReplicationTaskTypeFromString(s string) (ReplicationTaskType, error) {
	if v, ok := ReplicationTaskType_value[s]; ok {
		return ReplicationTaskType(v), nil
	} else if v, ok := ReplicationTaskType_shorthandValue[s]; ok {
		return ReplicationTaskType(v), nil
	}
	return ReplicationTaskType(0), fmt.Errorf("%s is not a valid ReplicationTaskType", s)
}

var (
	NamespaceOperation_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Create":      1,
		"Update":      2,
	}
)

// NamespaceOperationFromString parses a NamespaceOperation value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to NamespaceOperation
func NamespaceOperationFromString(s string) (NamespaceOperation, error) {
	if v, ok := NamespaceOperation_value[s]; ok {
		return NamespaceOperation(v), nil
	} else if v, ok := NamespaceOperation_shorthandValue[s]; ok {
		return NamespaceOperation(v), nil
	}
	return NamespaceOperation(0), fmt.Errorf("%s is not a valid NamespaceOperation", s)
}

var (
	ReplicationFlowControlCommand_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Resume":      1,
		"Pause":       2,
	}
)

// ReplicationFlowControlCommandFromString parses a ReplicationFlowControlCommand value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to ReplicationFlowControlCommand
func ReplicationFlowControlCommandFromString(s string) (ReplicationFlowControlCommand, error) {
	if v, ok := ReplicationFlowControlCommand_value[s]; ok {
		return ReplicationFlowControlCommand(v), nil
	} else if v, ok := ReplicationFlowControlCommand_shorthandValue[s]; ok {
		return ReplicationFlowControlCommand(v), nil
	}
	return ReplicationFlowControlCommand(0), fmt.Errorf("%s is not a valid ReplicationFlowControlCommand", s)
}
