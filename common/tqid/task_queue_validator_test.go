package tqid

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/primitives"
)

func TestNormalizeAndValidate(t *testing.T) {
	tests := []struct {
		name             string
		taskQueue        *taskqueuepb.TaskQueue
		defaultVal       string
		maxIDLengthLimit int
		expectedError    string
		expectedKind     enumspb.TaskQueueKind
		validAsPartition bool
	}{
		{
			name:             "Nil task queue",
			taskQueue:        nil,
			defaultVal:       "default",
			maxIDLengthLimit: 100,
			expectedError:    "taskQueue is not set",
			expectedKind:     enumspb.TASK_QUEUE_KIND_UNSPECIFIED,
		},
		{
			name:             "Empty name, no default",
			taskQueue:        &taskqueuepb.TaskQueue{},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "missing task queue name",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Empty name, with default",
			taskQueue:        &taskqueuepb.TaskQueue{},
			defaultVal:       "default",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Valid name",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "valid-name"},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Name exactly at max length",
			taskQueue:        &taskqueuepb.TaskQueue{Name: strings.Repeat("a", 100)},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Name one character over max length",
			taskQueue:        &taskqueuepb.TaskQueue{Name: strings.Repeat("a", 101)},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "taskQueue length exceeds limit",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Reserved prefix",
			taskQueue:        &taskqueuepb.TaskQueue{Name: reservedTaskQueuePrefix + "name"},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "task queue name cannot start with reserved prefix /_sys/",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
			validAsPartition: true,
		},
		{
			name:             "Sticky queue with valid normal name",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "sticky", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "normal"},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_STICKY,
		},
		{
			name:             "Non-sticky queue with normal name set",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "normal", Kind: enumspb.TASK_QUEUE_KIND_NORMAL, NormalName: "should-be-ignored"},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Task queue with unspecified kind",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "unspecified", Kind: enumspb.TASK_QUEUE_KIND_UNSPECIFIED},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalTaskQueue := tt.taskQueue
			var originalKind enumspb.TaskQueueKind
			if tt.taskQueue != nil {
				originalKind = tt.taskQueue.GetKind()
			}

			err := NormalizeAndValidate(tt.taskQueue, tt.defaultVal, tt.maxIDLengthLimit)

			if tt.expectedError == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			}

			err = NormalizeAndValidatePartition(tt.taskQueue, tt.defaultVal, tt.maxIDLengthLimit)

			if tt.expectedError == "" || tt.validAsPartition {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			}

			if originalTaskQueue == nil {
				assert.Nil(t, tt.taskQueue)
			} else {
				if originalKind == enumspb.TASK_QUEUE_KIND_UNSPECIFIED {
					assert.Equal(t, tt.expectedKind, tt.taskQueue.GetKind(), "Kind should be set to NORMAL if it was UNSPECIFIED")
				} else {
					assert.Equal(t, originalKind, tt.taskQueue.GetKind(), "Kind should not change if it wasn't UNSPECIFIED")
				}
			}

			if tt.taskQueue != nil && tt.taskQueue.GetName() == "" && tt.defaultVal != "" {
				assert.Equal(t, tt.defaultVal, tt.taskQueue.GetName())
			}
		})
	}
}

func TestNormalizeAndValidateUserDefined(t *testing.T) {
	tests := []struct {
		name             string
		taskQueue        *taskqueuepb.TaskQueue
		maxIDLengthLimit int
		expectError      bool
		expectedKind     enumspb.TaskQueueKind
	}{
		{
			name:             "User defined task queue - valid",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "my-custom-task-queue"},
			maxIDLengthLimit: 100,
			expectError:      false,
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Internal task queue - PerNSWorkerTaskQueue should be blocked",
			taskQueue:        &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			maxIDLengthLimit: 100,
			expectError:      true,
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Other internal task queues should be allowed",
			taskQueue:        &taskqueuepb.TaskQueue{Name: primitives.DefaultWorkerTaskQueue},
			maxIDLengthLimit: 100,
			expectError:      false,
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Nil task queue",
			taskQueue:        nil,
			maxIDLengthLimit: 100,
			expectError:      true,
			expectedKind:     enumspb.TASK_QUEUE_KIND_UNSPECIFIED,
		},
		{
			name:             "Empty name without default",
			taskQueue:        &taskqueuepb.TaskQueue{},
			maxIDLengthLimit: 100,
			expectError:      true,
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Name exceeds max length",
			taskQueue:        &taskqueuepb.TaskQueue{Name: strings.Repeat("a", 101)},
			maxIDLengthLimit: 100,
			expectError:      true,
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Reserved prefix is allowed for partitions",
			taskQueue:        &taskqueuepb.TaskQueue{Name: reservedTaskQueuePrefix + "name"},
			maxIDLengthLimit: 100,
			expectError:      false,
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Valid sticky queue",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "sticky", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "normal"},
			maxIDLengthLimit: 100,
			expectError:      false,
			expectedKind:     enumspb.TASK_QUEUE_KIND_STICKY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NormalizeAndValidateUserDefined(tt.taskQueue, "" /* defaultVal */, tt.maxIDLengthLimit)

			if tt.expectError {
				require.Error(t, err)
				var invalidArgument *serviceerror.InvalidArgument
				assert.ErrorAs(t, err, &invalidArgument)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedKind, tt.taskQueue.GetKind())
			}
		})
	}
}
