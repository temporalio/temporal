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

package enums

import (
	"fmt"
)

var (
	TaskSource_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"History":     1,
		"DbBacklog":   2,
	}
)

// TaskSourceFromString parses a TaskSource value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to TaskSource
func TaskSourceFromString(s string) (TaskSource, error) {
	if v, ok := TaskSource_value[s]; ok {
		return TaskSource(v), nil
	} else if v, ok := TaskSource_shorthandValue[s]; ok {
		return TaskSource(v), nil
	}
	return TaskSource(0), fmt.Errorf("%s is not a valid TaskSource", s)
}

var (
	TaskType_shorthandValue = map[string]int32{
		"Unspecified":                  0,
		"ReplicationHistory":           1,
		"ReplicationSyncActivity":      2,
		"TransferWorkflowTask":         3,
		"TransferActivityTask":         4,
		"TransferCloseExecution":       5,
		"TransferCancelExecution":      6,
		"TransferStartChildExecution":  7,
		"TransferSignalExecution":      8,
		"TransferResetWorkflow":        10,
		"WorkflowTaskTimeout":          12,
		"ActivityTimeout":              13,
		"UserTimer":                    14,
		"WorkflowRunTimeout":           15,
		"DeleteHistoryEvent":           16,
		"ActivityRetryTimer":           17,
		"WorkflowBackoffTimer":         18,
		"VisibilityStartExecution":     19,
		"VisibilityUpsertExecution":    20,
		"VisibilityCloseExecution":     21,
		"VisibilityDeleteExecution":    22,
		"TransferDeleteExecution":      24,
		"ReplicationSyncWorkflowState": 25,
		"ArchivalArchiveExecution":     26,
	}
)

// TaskTypeFromString parses a TaskType value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to TaskType
func TaskTypeFromString(s string) (TaskType, error) {
	if v, ok := TaskType_value[s]; ok {
		return TaskType(v), nil
	} else if v, ok := TaskType_shorthandValue[s]; ok {
		return TaskType(v), nil
	}
	return TaskType(0), fmt.Errorf("%s is not a valid TaskType", s)
}
