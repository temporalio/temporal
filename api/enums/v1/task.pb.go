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
	// Archival is the task type for workflow archival tasks.
	TASK_CATEGORY_ARCHIVAL TaskCategory = 5
)

var TaskCategory_name = map[int32]string{
	0: "Unspecified",
	1: "Transfer",
	2: "Timer",
	3: "Replication",
	4: "Visibility",
	5: "Archival",
}

var TaskCategory_value = map[string]int32{
	"Unspecified": 0,
	"Transfer":    1,
	"Timer":       2,
	"Replication": 3,
	"Visibility":  4,
	"Archival":    5,
}

func (TaskCategory) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_36a3d3674ca3cfa6, []int{1}
}

type TaskType int32

const (
	TASK_TYPE_UNSPECIFIED                     TaskType = 0
	TASK_TYPE_REPLICATION_HISTORY             TaskType = 1
	TASK_TYPE_REPLICATION_SYNC_ACTIVITY       TaskType = 2
	TASK_TYPE_TRANSFER_WORKFLOW_TASK          TaskType = 3
	TASK_TYPE_TRANSFER_ACTIVITY_TASK          TaskType = 4
	TASK_TYPE_TRANSFER_CLOSE_EXECUTION        TaskType = 5
	TASK_TYPE_TRANSFER_CANCEL_EXECUTION       TaskType = 6
	TASK_TYPE_TRANSFER_START_CHILD_EXECUTION  TaskType = 7
	TASK_TYPE_TRANSFER_SIGNAL_EXECUTION       TaskType = 8
	TASK_TYPE_TRANSFER_RESET_WORKFLOW         TaskType = 10
	TASK_TYPE_WORKFLOW_TASK_TIMEOUT           TaskType = 12
	TASK_TYPE_ACTIVITY_TIMEOUT                TaskType = 13
	TASK_TYPE_USER_TIMER                      TaskType = 14
	TASK_TYPE_WORKFLOW_RUN_TIMEOUT            TaskType = 15
	TASK_TYPE_DELETE_HISTORY_EVENT            TaskType = 16
	TASK_TYPE_ACTIVITY_RETRY_TIMER            TaskType = 17
	TASK_TYPE_WORKFLOW_BACKOFF_TIMER          TaskType = 18
	TASK_TYPE_VISIBILITY_START_EXECUTION      TaskType = 19
	TASK_TYPE_VISIBILITY_UPSERT_EXECUTION     TaskType = 20
	TASK_TYPE_VISIBILITY_CLOSE_EXECUTION      TaskType = 21
	TASK_TYPE_VISIBILITY_DELETE_EXECUTION     TaskType = 22
	TASK_TYPE_TRANSFER_DELETE_EXECUTION       TaskType = 24
	TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE TaskType = 25
	TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION      TaskType = 26
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
	10: "TransferResetWorkflow",
	12: "WorkflowTaskTimeout",
	13: "ActivityTimeout",
	14: "UserTimer",
	15: "WorkflowRunTimeout",
	16: "DeleteHistoryEvent",
	17: "ActivityRetryTimer",
	18: "WorkflowBackoffTimer",
	19: "VisibilityStartExecution",
	20: "VisibilityUpsertExecution",
	21: "VisibilityCloseExecution",
	22: "VisibilityDeleteExecution",
	24: "TransferDeleteExecution",
	25: "ReplicationSyncWorkflowState",
	26: "ArchivalArchiveExecution",
}

var TaskType_value = map[string]int32{
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
	// 640 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x94, 0xcb, 0x4e, 0xdb, 0x4e,
	0x14, 0xc6, 0x6d, 0x08, 0x10, 0x0e, 0xfc, 0xff, 0x9d, 0x0e, 0x97, 0x00, 0x85, 0x69, 0x09, 0x50,
	0x2e, 0x6a, 0x13, 0xa1, 0x2e, 0xbb, 0x72, 0x26, 0x13, 0x18, 0xe1, 0xda, 0xd1, 0xcc, 0x24, 0x34,
	0x5d, 0x60, 0xa5, 0x95, 0x85, 0x10, 0xa5, 0x8e, 0x92, 0x80, 0xc4, 0xae, 0x8f, 0xd0, 0xc7, 0xe8,
	0x3b, 0xf0, 0x02, 0x5d, 0xb2, 0x64, 0x59, 0xcc, 0xa6, 0x4b, 0x1e, 0xa1, 0x8a, 0x49, 0x7c, 0x01,
	0xa7, 0x3b, 0x4b, 0xdf, 0xcf, 0xdf, 0x39, 0xf3, 0xcd, 0x39, 0x03, 0x9b, 0x5d, 0xf7, 0xac, 0xe5,
	0xb5, 0x9b, 0x5f, 0x8b, 0x1d, 0xb7, 0x7d, 0xe1, 0xb6, 0x8b, 0xcd, 0xd6, 0x49, 0xd1, 0xfd, 0x76,
	0x7e, 0xd6, 0x29, 0x5e, 0xec, 0x16, 0xbb, 0xcd, 0xce, 0x69, 0xa1, 0xd5, 0xf6, 0xba, 0x1e, 0x5e,
	0x1e, 0x80, 0x85, 0x07, 0xb0, 0xd0, 0x6c, 0x9d, 0x14, 0x02, 0xb0, 0x70, 0xb1, 0xbb, 0x73, 0x04,
	0xa0, 0x9a, 0x9d, 0x53, 0xe9, 0x9d, 0xb7, 0xbf, 0xb8, 0xf8, 0x05, 0xe4, 0x94, 0x21, 0x0f, 0x1c,
	0x69, 0xd7, 0x04, 0x65, 0x4e, 0xcd, 0x92, 0x55, 0x46, 0x79, 0x85, 0xb3, 0x32, 0xd2, 0x70, 0x0e,
	0x66, 0xe2, 0xe2, 0x3e, 0x97, 0xca, 0x16, 0x0d, 0xa4, 0xe3, 0x25, 0x98, 0x8f, 0x0b, 0xe5, 0x92,
	0x53, 0x32, 0xe8, 0x81, 0x69, 0xef, 0xa1, 0x91, 0x9d, 0x2b, 0x1d, 0xa6, 0x7b, 0x05, 0x68, 0xb3,
	0xeb, 0x1e, 0x7b, 0xed, 0x4b, 0xbc, 0x02, 0x8b, 0x01, 0x4c, 0x0d, 0xc5, 0xf6, 0x6c, 0xd1, 0x78,
	0x54, 0x64, 0xe0, 0x15, 0xca, 0x4a, 0x18, 0x96, 0xac, 0x30, 0x81, 0xf4, 0xb0, 0x81, 0x48, 0xe3,
	0x1f, 0x98, 0x40, 0x23, 0x4f, 0x3d, 0x05, 0xab, 0x9a, 0x9c, 0x1a, 0x8a, 0xdb, 0x16, 0x1a, 0xc5,
	0xcb, 0xb0, 0x90, 0x94, 0xeb, 0x5c, 0xf2, 0x12, 0x37, 0xb9, 0x6a, 0xa0, 0xcc, 0xd3, 0x8a, 0x86,
	0xa0, 0xfb, 0xbc, 0x6e, 0x98, 0x68, 0x6c, 0xe7, 0x6a, 0x02, 0xb2, 0xbd, 0xee, 0xd5, 0x65, 0xcb,
	0xc5, 0x8b, 0x30, 0x17, 0x80, 0xaa, 0x51, 0x7d, 0x1c, 0xcd, 0x2a, 0xac, 0x44, 0x52, 0xac, 0x78,
	0x2c, 0xa4, 0x4d, 0x58, 0x4b, 0x47, 0x64, 0xc3, 0xa2, 0x8e, 0x41, 0x15, 0xaf, 0xf7, 0xfa, 0x19,
	0xc1, 0xeb, 0xf0, 0x2a, 0x02, 0x07, 0xa7, 0x77, 0x0e, 0x6d, 0x71, 0x50, 0x31, 0xed, 0x43, 0xa7,
	0xa7, 0xa1, 0xd1, 0x21, 0xd4, 0xc0, 0xe6, 0x81, 0xca, 0xe0, 0xd7, 0x90, 0x4f, 0xa1, 0xa8, 0x69,
	0x4b, 0xe6, 0xb0, 0x8f, 0x8c, 0xd6, 0x82, 0x84, 0xc6, 0x92, 0xcd, 0x45, 0x9c, 0x61, 0x51, 0x66,
	0xc6, 0xc0, 0x71, 0xfc, 0x06, 0xb6, 0x52, 0x40, 0xa9, 0x0c, 0xa1, 0x1c, 0xba, 0xcf, 0xcd, 0x72,
	0x8c, 0x9e, 0x18, 0x62, 0x2b, 0xf9, 0x9e, 0x65, 0xc4, 0x6d, 0xb3, 0x78, 0x03, 0x56, 0x53, 0x40,
	0xc1, 0x24, 0x53, 0xe1, 0xc9, 0x11, 0xe0, 0x35, 0x78, 0x19, 0x61, 0x89, 0x44, 0x82, 0x51, 0xb0,
	0x6b, 0x0a, 0x4d, 0x63, 0x02, 0x4b, 0x11, 0x14, 0x05, 0xd2, 0xd7, 0xff, 0xc3, 0x0b, 0x30, 0x1b,
	0xbb, 0x46, 0xc9, 0x44, 0x7f, 0x8c, 0xfe, 0xc7, 0x79, 0x20, 0x29, 0xf6, 0xa2, 0x66, 0x85, 0x7f,
	0x3f, 0x4b, 0x32, 0x65, 0x66, 0x32, 0x15, 0x6e, 0x82, 0xc3, 0xea, 0xcc, 0x52, 0x08, 0x25, 0x99,
	0xb0, 0x03, 0xc1, 0x54, 0x38, 0xb2, 0xcf, 0x93, 0xf7, 0x17, 0xd6, 0xea, 0xed, 0x8d, 0x5d, 0xa9,
	0xf4, 0x29, 0x8c, 0xb7, 0x60, 0x3d, 0xa2, 0xa2, 0xa9, 0xed, 0x07, 0x1e, 0x25, 0x38, 0x83, 0xb7,
	0x61, 0x23, 0x95, 0xac, 0x55, 0x25, 0x4b, 0xa0, 0xb3, 0x43, 0x4d, 0x1f, 0x8f, 0xc5, 0xdc, 0x50,
	0xd3, 0xfe, 0xb9, 0x23, 0x74, 0x7e, 0xc8, 0x55, 0x3f, 0x01, 0x17, 0xf0, 0x5b, 0xd8, 0xfe, 0xc7,
	0x1e, 0x84, 0x49, 0x48, 0x65, 0x28, 0x86, 0x16, 0x93, 0xcd, 0x0e, 0x36, 0xb3, 0xff, 0x11, 0x37,
	0x5e, 0xca, 0x67, 0xb2, 0x93, 0x68, 0x32, 0x9f, 0xc9, 0x4e, 0xa1, 0xa9, 0x7c, 0x26, 0x9b, 0x43,
	0xb9, 0xd2, 0xd1, 0xf5, 0x2d, 0xd1, 0x6e, 0x6e, 0x89, 0x76, 0x7f, 0x4b, 0xf4, 0xef, 0x3e, 0xd1,
	0x7f, 0xfa, 0x44, 0xff, 0xe5, 0x13, 0xfd, 0xda, 0x27, 0xfa, 0x6f, 0x9f, 0xe8, 0x7f, 0x7c, 0xa2,
	0xdd, 0xfb, 0x44, 0xff, 0x71, 0x47, 0xb4, 0xeb, 0x3b, 0xa2, 0xdd, 0xdc, 0x11, 0xed, 0xd3, 0xd6,
	0xb1, 0x57, 0x08, 0x9f, 0xcc, 0x13, 0x2f, 0xed, 0x79, 0x7d, 0x1f, 0x7c, 0x7c, 0x1e, 0x0f, 0x1e,
	0xd8, 0x77, 0x7f, 0x03, 0x00, 0x00, 0xff, 0xff, 0x20, 0x76, 0x6f, 0xef, 0x8b, 0x05, 0x00, 0x00,
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
