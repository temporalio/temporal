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
)

var TaskCategory_name = map[int32]string{
	0: "Unspecified",
	1: "Transfer",
	2: "Timer",
	3: "Replication",
}

var TaskCategory_value = map[string]int32{
	"Unspecified": 0,
	"Transfer":    1,
	"Timer":       2,
	"Replication": 3,
}

func (TaskCategory) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_36a3d3674ca3cfa6, []int{1}
}

type TaskType int32

const (
	TASK_TYPE_UNSPECIFIED                                TaskType = 0
	TASK_TYPE_REPLICATION_HISTORY                        TaskType = 1
	TASK_TYPE_REPLICATION_SYNC_ACTIVITY                  TaskType = 2
	TASK_TYPE_TRANSFER_WORKFLOW_TASK                     TaskType = 3
	TASK_TYPE_TRANSFER_ACTIVITY_TASK                     TaskType = 4
	TASK_TYPE_TRANSFER_CLOSE_EXECUTION                   TaskType = 5
	TASK_TYPE_TRANSFER_CANCEL_EXECUTION                  TaskType = 6
	TASK_TYPE_TRANSFER_START_CHILD_EXECUTION             TaskType = 7
	TASK_TYPE_TRANSFER_SIGNAL_EXECUTION                  TaskType = 8
	TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED           TaskType = 9
	TASK_TYPE_TRANSFER_RESET_WORKFLOW                    TaskType = 10
	TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES TaskType = 11
	TASK_TYPE_WORKFLOW_TASK_TIMEOUT                      TaskType = 12
	TASK_TYPE_ACTIVITY_TASK_TIMEOUT                      TaskType = 13
	TASK_TYPE_USER_TIMER                                 TaskType = 14
	TASK_TYPE_WORKFLOW_RUN_TIMEOUT                       TaskType = 15
	TASK_TYPE_DELETE_HISTORY_EVENT                       TaskType = 16
	TASK_TYPE_ACTIVITY_RETRY_TIMER                       TaskType = 17
	TASK_TYPE_WORKFLOW_BACKOFF_TIMER                     TaskType = 18
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
}

var TaskType_value = map[string]int32{
	"Unspecified":                            0,
	"ReplicationHistory":                     1,
	"ReplicationSyncActivity":                2,
	"TransferWorkflowTask":                   3,
	"TransferActivityTask":                   4,
	"TransferCloseExecution":                 5,
	"TransferCancelExecution":                6,
	"TransferStartChildExecution":            7,
	"TransferSignalExecution":                8,
	"TransferRecordWorkflowStarted":          9,
	"TransferResetWorkflow":                  10,
	"TransferUpsertWorkflowSearchAttributes": 11,
	"WorkflowTaskTimeout":                    12,
	"ActivityTimeout":                        13,
	"UserTimer":                              14,
	"WorkflowRunTimeout":                     15,
	"DeleteHistoryEvent":                     16,
	"ActivityRetryTimer":                     17,
	"WorkflowBackoffTimer":                   18,
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
	// 575 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x94, 0x4f, 0x4f, 0xd4, 0x4e,
	0x1c, 0xc6, 0x5b, 0xf8, 0xc1, 0x0f, 0x47, 0xd4, 0x71, 0xfc, 0x83, 0xa0, 0x4c, 0x04, 0xff, 0x40,
	0x36, 0xa6, 0x1b, 0xa2, 0x07, 0x13, 0x4f, 0xdd, 0xe9, 0x77, 0x61, 0x42, 0x6d, 0x37, 0x33, 0x53,
	0x70, 0x3d, 0xd0, 0x54, 0xd3, 0x10, 0x82, 0xd8, 0x4d, 0x77, 0xd9, 0x84, 0x9b, 0xbe, 0x03, 0x5f,
	0x86, 0x2f, 0xc5, 0xe3, 0x1e, 0x39, 0xba, 0xdd, 0x8b, 0x47, 0x5e, 0x82, 0x69, 0xd9, 0x6d, 0xbb,
	0x58, 0x6e, 0x4d, 0x9e, 0x4f, 0x9f, 0xef, 0xf7, 0x69, 0x9f, 0x19, 0xb4, 0xd1, 0x0b, 0x4f, 0x3a,
	0x51, 0x1c, 0x7c, 0xa9, 0x77, 0xc3, 0xb8, 0x1f, 0xc6, 0xf5, 0xa0, 0x73, 0x54, 0x0f, 0xbf, 0x9e,
	0x9e, 0x74, 0xeb, 0xfd, 0xad, 0x7a, 0x2f, 0xe8, 0x1e, 0x1b, 0x9d, 0x38, 0xea, 0x45, 0xe4, 0xc9,
	0x04, 0x34, 0x2e, 0x41, 0x23, 0xe8, 0x1c, 0x19, 0x19, 0x68, 0xf4, 0xb7, 0x6a, 0x07, 0x08, 0xa9,
	0xa0, 0x7b, 0x2c, 0xa3, 0xd3, 0xf8, 0x73, 0x48, 0x1e, 0xa3, 0x25, 0x65, 0xca, 0x5d, 0x5f, 0xba,
	0x9e, 0x60, 0xe0, 0x7b, 0x8e, 0x6c, 0x01, 0xe3, 0x4d, 0x0e, 0x16, 0xd6, 0xc8, 0x12, 0xba, 0x57,
	0x16, 0x77, 0xb8, 0x54, 0xae, 0x68, 0x63, 0x9d, 0xac, 0xa0, 0x87, 0x65, 0xc1, 0x6a, 0xf8, 0x0d,
	0x93, 0xed, 0xda, 0xee, 0x36, 0x9e, 0xa9, 0x7d, 0xd7, 0xd1, 0x62, 0x3a, 0x80, 0x05, 0xbd, 0xf0,
	0x30, 0x8a, 0xcf, 0xc8, 0x2a, 0x5a, 0xce, 0x60, 0x66, 0x2a, 0xd8, 0x76, 0x45, 0xfb, 0xca, 0x90,
	0x89, 0x57, 0x2e, 0x2b, 0x61, 0x3a, 0xb2, 0x09, 0x02, 0xeb, 0xf9, 0x02, 0x85, 0xc6, 0xdf, 0x83,
	0xc0, 0x33, 0xff, 0x7a, 0x0a, 0x68, 0xd9, 0x9c, 0x99, 0x8a, 0xbb, 0x0e, 0x9e, 0xad, 0x25, 0x73,
	0x68, 0x21, 0xdd, 0x41, 0x9d, 0x75, 0x42, 0xb2, 0x8c, 0x1e, 0x64, 0xac, 0x6a, 0xb7, 0xae, 0x06,
	0x5c, 0x43, 0xab, 0x85, 0x54, 0xb2, 0x28, 0x45, 0xdd, 0x40, 0xcf, 0xaa, 0x11, 0xd9, 0x76, 0x98,
	0x6f, 0x32, 0xc5, 0xf7, 0xb8, 0x6a, 0xe3, 0x19, 0xf2, 0x1c, 0x3d, 0x2d, 0xc0, 0x49, 0x06, 0x7f,
	0xdf, 0x15, 0xbb, 0x4d, 0xdb, 0xdd, 0xf7, 0x53, 0x0d, 0xcf, 0x5e, 0x43, 0x4d, 0x6c, 0x2e, 0xa9,
	0xff, 0xc8, 0x4b, 0xb4, 0x5e, 0x41, 0x31, 0xdb, 0x95, 0xe0, 0xc3, 0x07, 0x60, 0x5e, 0x96, 0x73,
	0x6e, 0x7a, 0xb9, 0x82, 0x33, 0x1d, 0x06, 0x76, 0x09, 0x9c, 0x27, 0xaf, 0xd0, 0x66, 0x05, 0x28,
	0x95, 0x29, 0x94, 0xcf, 0x76, 0xb8, 0x6d, 0x95, 0xe8, 0xff, 0xaf, 0xb1, 0x95, 0x7c, 0xdb, 0x31,
	0xcb, 0xb6, 0x0b, 0xc4, 0x40, 0xb5, 0x0a, 0x50, 0x00, 0x73, 0x85, 0x55, 0x44, 0xcf, 0xc6, 0x80,
	0x85, 0x6f, 0x90, 0x17, 0x68, 0xad, 0x92, 0x97, 0xa0, 0x72, 0x1c, 0x23, 0xf2, 0x16, 0xbd, 0xa9,
	0xc0, 0xbc, 0x96, 0x04, 0xa1, 0x4a, 0xb6, 0x60, 0x0a, 0xb6, 0xe3, 0x9b, 0x4a, 0x09, 0xde, 0xf0,
	0x14, 0x48, 0x7c, 0x93, 0x50, 0xb4, 0x52, 0xbc, 0x69, 0x01, 0xe3, 0x32, 0xfd, 0x55, 0x69, 0x6b,
	0x5c, 0x4f, 0xe1, 0xc5, 0x69, 0xbd, 0xf8, 0xea, 0x63, 0xfd, 0x16, 0x79, 0x84, 0xee, 0x97, 0xba,
	0x22, 0x41, 0x8c, 0x1b, 0x77, 0x9b, 0xac, 0x23, 0x5a, 0x28, 0xf9, 0x0e, 0xc2, 0x2b, 0xdc, 0xef,
	0x4c, 0x33, 0x16, 0xd8, 0xa0, 0xf2, 0x43, 0xe3, 0xc3, 0x1e, 0x38, 0x0a, 0xe3, 0x69, 0x26, 0xdf,
	0x40, 0x80, 0xca, 0xdb, 0x7d, 0x77, 0xba, 0x24, 0xf9, 0xac, 0xf4, 0x88, 0xb9, 0xcd, 0xe6, 0x98,
	0x22, 0x8d, 0x83, 0xc1, 0x90, 0x6a, 0xe7, 0x43, 0xaa, 0x5d, 0x0c, 0xa9, 0xfe, 0x2d, 0xa1, 0xfa,
	0xcf, 0x84, 0xea, 0xbf, 0x12, 0xaa, 0x0f, 0x12, 0xaa, 0xff, 0x4e, 0xa8, 0xfe, 0x27, 0xa1, 0xda,
	0x45, 0x42, 0xf5, 0x1f, 0x23, 0xaa, 0x0d, 0x46, 0x54, 0x3b, 0x1f, 0x51, 0xed, 0xe3, 0xe6, 0x61,
	0x64, 0xe4, 0xf7, 0xc3, 0x51, 0x54, 0x75, 0x97, 0xbc, 0xcb, 0x1e, 0x3e, 0xcd, 0x67, 0xb7, 0xc9,
	0xeb, 0xbf, 0x01, 0x00, 0x00, 0xff, 0xff, 0x10, 0xc2, 0xe5, 0xe9, 0x78, 0x04, 0x00, 0x00,
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
