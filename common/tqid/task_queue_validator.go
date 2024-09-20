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
	"fmt"
	"strings"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/enums"
)

const (
	reservedTaskQueuePrefix = "/_sys/"
)

// NormalizeAndValidate validates a TaskQueue object and normalizes its fields.
// It checks the TaskQueue's name for emptiness, length, UTF-8 validity, and whitespace.
// For sticky queues, it also validates the NormalName.
// If the name is empty and defaultVal is provided, it sets the name to defaultVal.
// If the Kind is unspecified, it sets it to NORMAL.
//
// Parameters:
//   - taskQueue: The TaskQueue to validate and normalize. If nil, returns an error.
//   - defaultName: Default name to use if taskQueue name is empty.
//   - maxIDLengthLimit: Maximum allowed length for the TaskQueue name.
//
// Returns an error if validation fails, nil otherwise.
func NormalizeAndValidate(
	taskQueue *taskqueue.TaskQueue,
	defaultName string,
	maxIDLengthLimit int,
) error {
	if taskQueue == nil {
		return serviceerror.NewInvalidArgument("taskQueue is not set")
	}

	enums.SetDefaultTaskQueueKind(&taskQueue.Kind)

	if taskQueue.GetName() == "" {
		if defaultName == "" {
			return serviceerror.NewInvalidArgument("missing task queue name")
		}
		taskQueue.Name = defaultName
	}

	if err := Validate(taskQueue.GetName(), maxIDLengthLimit); err != nil {
		return err
	}

	if taskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		normalName := taskQueue.GetNormalName()
		if normalName != "" {
			if err := Validate(normalName, maxIDLengthLimit); err != nil {
				return err
			}
		}
	}

	return nil
}

// Validate checks if a given task queue name is valid.
// It verifies the name is not empty, does not exceed the maximum length,
// and is a valid UTF-8 string.
//
// Parameters:
//   - name: The task queue name to validate.
//   - maxLength: The maximum allowed length for the name.
//
// Returns an error if the name is invalid, nil otherwise.
func Validate(taskQueueName string, maxLength int) error {
	if taskQueueName == "" {
		return serviceerror.NewInvalidArgument("taskQueue is not set")
	}
	if len(taskQueueName) > maxLength {
		return serviceerror.NewInvalidArgument("taskQueue length exceeds limit")
	}

	if !utf8.ValidString(taskQueueName) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("taskQueue %q is not a valid UTF-8 string", taskQueueName))
	}

	if strings.HasPrefix(taskQueueName, reservedTaskQueuePrefix) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("task queue name cannot start with reserved prefix %v", reservedTaskQueuePrefix))
	}

	return nil
}
