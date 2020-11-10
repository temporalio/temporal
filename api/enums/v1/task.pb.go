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

// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/enums/v1/task.proto

package enums

import (
	fmt "fmt"
	math "math"
	strconv "strconv"

	proto "github.com/gogo/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// TaskSource is the source from which a task was produced.
type TaskSource int32

const (
	TASK_SOURCE_UNSPECIFIED TaskSource = 0
	// Task produced by history service.
	TASK_SOURCE_HISTORY TaskSource = 1
	// Task produced from matching db backlog.
	TASK_SOURCE_DB_BACKLOG TaskSource = 2
)

var TaskSource_name = map[int32]string{
	0: "Unspecified",
	1: "History",
	2: "DbBacklog",
}

var TaskSource_value = map[string]int32{
	"Unspecified": 0,
	"History":     1,
	"DbBacklog":   2,
}

func (TaskSource) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_36a3d3674ca3cfa6, []int{0}
}

type TaskCategory int32

const (
	TASK_CATEGORY_UNSPECIFIED TaskCategory = 0
	// Transfer is the task type for transfer task.
	TASK_CATEGORY_TRANSFER TaskCategory = 1
	// Timer is the task type for timer task.
	TASK_CATEGORY_TIMER TaskCategory = 2
	// Replication is the task type for replication task.
	TASK_CATEGORY_REPLICATION TaskCategory = 3
	// Visibility is the task type for visibility task.
	TASK_CATEGORY_VISIBILITY TaskCategory = 4
)

var TaskCategory_name = map[int32]string{
	0: "Unspecified",
	1: "Transfer",
	2: "Timer",
	3: "Replication",
	4: "Visibility",
}

var TaskCategory_value = map[string]int32{
	"Unspecified": 0,
	"Transfer":    1,
	"Timer":       2,
	"Replication": 3,
	"Visibility":  4,
}

func (TaskCategory) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_36a3d3674ca3cfa6, []int{1}
}

type TaskType int32

const (
	TASK_TYPE_UNSPECIFIED                                  TaskType = 0
	TASK_TYPE_REPLICATION_HISTORY                          TaskType = 1
	TASK_TYPE_REPLICATION_SYNC_ACTIVITY                    TaskType = 2
	TASK_TYPE_TRANSFER_WORKFLOW_TASK                       TaskType = 3
	TASK_TYPE_TRANSFER_ACTIVITY_TASK                       TaskType = 4
	TASK_TYPE_TRANSFER_CLOSE_EXECUTION                     TaskType = 5 // Deprecated: Do not use.
	TASK_TYPE_TRANSFER_CANCEL_EXECUTION                    TaskType = 6
	TASK_TYPE_TRANSFER_START_CHILD_EXECUTION               TaskType = 7
	TASK_TYPE_TRANSFER_SIGNAL_EXECUTION                    TaskType = 8
	TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED             TaskType = 9 // Deprecated: Do not use.
	TASK_TYPE_TRANSFER_RESET_WORKFLOW                      TaskType = 10
	TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES   TaskType = 11 // Deprecated: Do not use.
	TASK_TYPE_WORKFLOW_TASK_TIMEOUT                        TaskType = 12
	TASK_TYPE_ACTIVITY_TIMEOUT                             TaskType = 13
	TASK_TYPE_USER_TIMER                                   TaskType = 14
	TASK_TYPE_WORKFLOW_RUN_TIMEOUT                         TaskType = 15
	TASK_TYPE_DELETE_HISTORY_EVENT                         TaskType = 16
	TASK_TYPE_ACTIVITY_RETRY_TIMER                         TaskType = 17
	TASK_TYPE_WORKFLOW_BACKOFF_TIMER                       TaskType = 18
	TASK_TYPE_VISIBILITY_RECORD_WORKFLOW_STARTED           TaskType = 19
	TASK_TYPE_VISIBILITY_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES TaskType = 20
	TASK_TYPE_VISIBILITY_CLOSE_EXECUTION                   TaskType = 21
	TASK_TYPE_VISIBILITY_DELETE_EXECUTION                  TaskType = 22
)

var TaskType_name = map[int32]string{
	0:  "Unspecified",
	1:  "ReplicationHistory",
	2:  "ReplicationSyncActivity",
	3:  "TransferWorkflowTask",
	4:  "TransferActivityTask",
	5:  "TransferCloseExecution",
	6:  "TransferCancelExecution",
	7:  "TransferStartChildExecution",
	8:  "TransferSignalExecution",
	9:  "TransferRecordWorkflowStarted",
	10: "TransferResetWorkflow",
	11: "TransferUpsertWorkflowSearchAttributes",
	12: "WorkflowTaskTimeout",
	13: "ActivityTimeout",
	14: "UserTimer",
	15: "WorkflowRunTimeout",
	16: "DeleteHistoryEvent",
	17: "ActivityRetryTimer",
	18: "WorkflowBackoffTimer",
	19: "VisibilityRecordWorkflowStarted",
	20: "VisibilityUpsertWorkflowSearchAttributes",
	21: "VisibilityCloseExecution",
	22: "VisibilityDeleteExecution",
}

var TaskType_value = map[string]int32{
	"Unspecified":                              0,
	"ReplicationHistory":                       1,
	"ReplicationSyncActivity":                  2,
	"TransferWorkflowTask":                     3,
	"TransferActivityTask":                     4,
	"TransferCloseExecution":                   5,
	"TransferCancelExecution":                  6,
	"TransferStartChildExecution":              7,
	"TransferSignalExecution":                  8,
	"TransferRecordWorkflowStarted":            9,
	"TransferResetWorkflow":                    10,
	"TransferUpsertWorkflowSearchAttributes":   11,
	"WorkflowTaskTimeout":                      12,
	"ActivityTimeout":                          13,
	"UserTimer":                                14,
	"WorkflowRunTimeout":                       15,
	"DeleteHistoryEvent":                       16,
	"ActivityRetryTimer":                       17,
	"WorkflowBackoffTimer":                     18,
	"VisibilityRecordWorkflowStarted":          19,
	"VisibilityUpsertWorkflowSearchAttributes": 20,
	"VisibilityCloseExecution":                 21,
	"VisibilityDeleteExecution":                22,
}

func (TaskType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_36a3d3674ca3cfa6, []int{2}
}

func init() {
	proto.RegisterEnum("temporal.server.api.enums.v1.TaskSource", TaskSource_name, TaskSource_value)
	proto.RegisterEnum("temporal.server.api.enums.v1.TaskCategory", TaskCategory_name, TaskCategory_value)
	proto.RegisterEnum("temporal.server.api.enums.v1.TaskType", TaskType_name, TaskType_value)
}

func init() {
	proto.RegisterFile("temporal/server/api/enums/v1/task.proto", fileDescriptor_36a3d3674ca3cfa6)
}

var fileDescriptor_36a3d3674ca3cfa6 = []byte{
	// 638 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x94, 0xcf, 0x6e, 0xd3, 0x4e,
	0x10, 0xc7, 0xed, 0xb4, 0xbf, 0xb6, 0xbf, 0xa1, 0xc0, 0xb2, 0xfd, 0x5f, 0xda, 0x85, 0xfe, 0x53,
	0x4b, 0x54, 0x25, 0x14, 0x10, 0x07, 0xe0, 0xe2, 0x6c, 0x36, 0xed, 0xaa, 0xc6, 0x8e, 0x76, 0xd7,
	0x2d, 0xe1, 0x50, 0x2b, 0x20, 0xab, 0xaa, 0x4a, 0x71, 0xe4, 0xa4, 0x95, 0x7a, 0xe3, 0x11, 0x78,
	0x03, 0xae, 0x3c, 0x01, 0xcf, 0xc0, 0xb1, 0xc7, 0x1e, 0xa9, 0x7b, 0xe1, 0xd8, 0x47, 0x40, 0x71,
	0x13, 0xdb, 0x09, 0x8e, 0xb8, 0xad, 0xf4, 0xfd, 0xcc, 0x7c, 0x67, 0x67, 0x67, 0x16, 0xd6, 0x5b,
	0xde, 0x49, 0xc3, 0x0f, 0xea, 0x9f, 0x8a, 0x4d, 0x2f, 0x38, 0xf3, 0x82, 0x62, 0xbd, 0x71, 0x54,
	0xf4, 0x3e, 0x9f, 0x9e, 0x34, 0x8b, 0x67, 0x5b, 0xc5, 0x56, 0xbd, 0x79, 0x5c, 0x68, 0x04, 0x7e,
	0xcb, 0xc7, 0x0b, 0x5d, 0xb0, 0x70, 0x0b, 0x16, 0xea, 0x8d, 0xa3, 0x42, 0x04, 0x16, 0xce, 0xb6,
	0xf2, 0x07, 0x00, 0xaa, 0xde, 0x3c, 0x96, 0xfe, 0x69, 0xf0, 0xd1, 0xc3, 0x0f, 0x61, 0x46, 0x19,
	0x72, 0xd7, 0x95, 0xb6, 0x23, 0x28, 0x73, 0x1d, 0x4b, 0x56, 0x19, 0xe5, 0x15, 0xce, 0xca, 0x48,
	0xc3, 0x33, 0x30, 0x91, 0x16, 0x77, 0xb8, 0x54, 0xb6, 0xa8, 0x21, 0x1d, 0xcf, 0xc3, 0x74, 0x5a,
	0x28, 0x97, 0xdc, 0x92, 0x41, 0x77, 0x4d, 0x7b, 0x1b, 0xe5, 0xf2, 0xdf, 0x74, 0x18, 0x6f, 0x1b,
	0xd0, 0x7a, 0xcb, 0x3b, 0xf4, 0x83, 0x73, 0xbc, 0x08, 0x73, 0x11, 0x4c, 0x0d, 0xc5, 0xb6, 0x6d,
	0x51, 0xeb, 0x33, 0xe9, 0xe6, 0x8a, 0x65, 0x25, 0x0c, 0x4b, 0x56, 0x98, 0x40, 0x7a, 0x5c, 0x40,
	0xa2, 0xf1, 0xb7, 0x4c, 0xa0, 0xdc, 0xdf, 0x39, 0x05, 0xab, 0x9a, 0x9c, 0x1a, 0x8a, 0xdb, 0x16,
	0x1a, 0xc2, 0x0b, 0x30, 0xdb, 0x2b, 0xef, 0x71, 0xc9, 0x4b, 0xdc, 0xe4, 0xaa, 0x86, 0x86, 0xf3,
	0x3f, 0x46, 0x61, 0xac, 0x5d, 0xa1, 0x3a, 0x6f, 0x78, 0x78, 0x0e, 0xa6, 0x22, 0x54, 0xd5, 0xaa,
	0xfd, 0xd7, 0x5f, 0x82, 0xc5, 0x44, 0x4a, 0x19, 0xa4, 0x1a, 0xb1, 0x0e, 0x2b, 0xd9, 0x88, 0xac,
	0x59, 0xd4, 0x35, 0xa8, 0xe2, 0x7b, 0x6d, 0xcf, 0x1c, 0x5e, 0x85, 0xc7, 0x09, 0xd8, 0xbd, 0xa1,
	0xbb, 0x6f, 0x8b, 0xdd, 0x8a, 0x69, 0xef, 0xbb, 0x6d, 0x0d, 0x0d, 0x0d, 0xa0, 0xba, 0x69, 0x6e,
	0xa9, 0x61, 0x9c, 0x87, 0xe5, 0x0c, 0x8a, 0x9a, 0xb6, 0x64, 0x2e, 0x7b, 0xc7, 0xa8, 0x13, 0x75,
	0xe1, 0xbf, 0xf9, 0xdc, 0x58, 0x5f, 0x81, 0x09, 0x6b, 0x58, 0x94, 0x99, 0x29, 0x78, 0x04, 0x6f,
	0xc2, 0x46, 0x06, 0x28, 0x95, 0x21, 0x94, 0x4b, 0x77, 0xb8, 0x59, 0x4e, 0xd1, 0xa3, 0x03, 0xd2,
	0x4a, 0xbe, 0x6d, 0x19, 0xe9, 0xb4, 0x63, 0xf8, 0x19, 0xe4, 0x33, 0x40, 0xc1, 0xa8, 0x2d, 0xca,
	0xc9, 0xf5, 0x23, 0x1b, 0x56, 0x46, 0xff, 0x47, 0x35, 0xaf, 0xc1, 0x52, 0x66, 0x8c, 0x64, 0x2a,
	0x0e, 0x41, 0x80, 0xdf, 0xc0, 0x8b, 0x0c, 0xcc, 0xa9, 0x4a, 0x26, 0x54, 0x2a, 0x35, 0x33, 0x04,
	0xdd, 0x71, 0x0d, 0xa5, 0x04, 0x2f, 0x39, 0x8a, 0x49, 0x74, 0x27, 0x32, 0x59, 0x81, 0x47, 0x49,
	0x74, 0xcf, 0x3b, 0x44, 0x43, 0x66, 0x3b, 0x0a, 0x8d, 0x63, 0x02, 0xf3, 0x09, 0x94, 0x3c, 0x43,
	0x47, 0xbf, 0x8b, 0x67, 0x61, 0x32, 0x35, 0x3c, 0x92, 0x89, 0xce, 0x80, 0xde, 0xc3, 0xcb, 0x40,
	0x32, 0xd2, 0x0b, 0xc7, 0x8a, 0xa3, 0xef, 0xf7, 0x32, 0x65, 0x66, 0x32, 0x15, 0xef, 0x98, 0xcb,
	0xf6, 0x98, 0xa5, 0x10, 0xea, 0x65, 0xe2, 0x0a, 0x04, 0x53, 0xf1, 0x32, 0x3c, 0xe8, 0x9d, 0x9a,
	0xd8, 0xab, 0xbd, 0x91, 0x76, 0xa5, 0xd2, 0xa1, 0x30, 0x7e, 0x0a, 0x9b, 0x09, 0x95, 0xec, 0xc3,
	0xc0, 0xb7, 0x98, 0xc0, 0xaf, 0xe0, 0x65, 0x66, 0xc4, 0xbf, 0x5b, 0x3c, 0x89, 0x37, 0x60, 0x35,
	0x33, 0xb6, 0x7f, 0x4a, 0xa7, 0xf0, 0x13, 0x58, 0xcb, 0x24, 0x3b, 0x0d, 0x49, 0xd0, 0xe9, 0xd2,
	0xc1, 0xc5, 0x15, 0xd1, 0x2e, 0xaf, 0x88, 0x76, 0x73, 0x45, 0xf4, 0x2f, 0x21, 0xd1, 0xbf, 0x87,
	0x44, 0xff, 0x19, 0x12, 0xfd, 0x22, 0x24, 0xfa, 0xaf, 0x90, 0xe8, 0xbf, 0x43, 0xa2, 0xdd, 0x84,
	0x44, 0xff, 0x7a, 0x4d, 0xb4, 0x8b, 0x6b, 0xa2, 0x5d, 0x5e, 0x13, 0xed, 0xfd, 0xc6, 0xa1, 0x5f,
	0x88, 0x7f, 0xc4, 0x23, 0x3f, 0xeb, 0xf7, 0x7c, 0x1d, 0x1d, 0x3e, 0x8c, 0x44, 0xff, 0xe7, 0xf3,
	0x3f, 0x01, 0x00, 0x00, 0xff, 0xff, 0xfd, 0x63, 0x13, 0xe7, 0x6a, 0x05, 0x00, 0x00,
}

func (x TaskSource) String() string {
	s, ok := TaskSource_name[int32(x)]
	if ok {
		return s
	}
	return strconv.Itoa(int(x))
}
func (x TaskCategory) String() string {
	s, ok := TaskCategory_name[int32(x)]
	if ok {
		return s
	}
	return strconv.Itoa(int(x))
}
func (x TaskType) String() string {
	s, ok := TaskType_name[int32(x)]
	if ok {
		return s
	}
	return strconv.Itoa(int(x))
}
