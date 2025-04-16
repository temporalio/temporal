// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package tqid

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
			name:             "Valid UTF-8 name",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "válid-nàmé"},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		{
			name:             "Invalid UTF-8 name",
			taskQueue:        &taskqueuepb.TaskQueue{Name: string([]byte{0xff, 0xfe, 0xfd})},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "taskQueue \"\\xff\\xfe\\xfd\" is not a valid UTF-8 string",
			expectedKind:     enumspb.TASK_QUEUE_KIND_NORMAL,
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
			name:             "Sticky queue with invalid UTF-8 normal name",
			taskQueue:        &taskqueuepb.TaskQueue{Name: "sticky", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: string([]byte{0xff, 0xfe, 0xfd})},
			defaultVal:       "",
			maxIDLengthLimit: 100,
			expectedError:    "taskQueue \"\\xff\\xfe\\xfd\" is not a valid UTF-8 string",
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
