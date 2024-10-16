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

// NormalizeAndValidatePartition validates a TaskQueue proto object as a task queue partition,
// and normalizes its fields.
// Note that a TaskQueue proto holds a task queue partition in the general case, not necessarily
// a high-level task queue.
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
func NormalizeAndValidatePartition(
	partition *taskqueue.TaskQueue,
	defaultName string,
	maxIDLengthLimit int,
) error {
	return normalizeAndValidate(partition, defaultName, maxIDLengthLimit, false)
}

// NormalizeAndValidate validates a TaskQueue proto object as a top-level task queue or
// a sticky queue and normalizes its fields.
// Note that a TaskQueue proto holds a task queue partition in the general case, not necessarily
// a top-level task queue.
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
	return normalizeAndValidate(taskQueue, defaultName, maxIDLengthLimit, true)
}

func normalizeAndValidate(
	taskQueue *taskqueue.TaskQueue,
	defaultName string,
	maxIDLengthLimit int,
	expectRootPartition bool,
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

	if err := validate(taskQueue.GetName(), maxIDLengthLimit, expectRootPartition); err != nil {
		return err
	}

	if taskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		normalName := taskQueue.GetNormalName()
		// Old SDKs might not send the normal name, so we accept empty normal names for the time being.
		if normalName != "" {
			if err := validate(normalName, maxIDLengthLimit, false); err != nil {
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
	return validate(taskQueueName, maxLength, true)
}

func validate(taskQueueName string, maxLength int, expectRootPartition bool) error {
	if taskQueueName == "" {
		return serviceerror.NewInvalidArgument("taskQueue is not set")
	}
	if len(taskQueueName) > maxLength {
		return serviceerror.NewInvalidArgument("taskQueue length exceeds limit")
	}

	if !utf8.ValidString(taskQueueName) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("taskQueue %q is not a valid UTF-8 string", taskQueueName))
	}

	if expectRootPartition && strings.HasPrefix(taskQueueName, reservedTaskQueuePrefix) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("task queue name cannot start with reserved prefix %v", reservedTaskQueuePrefix))
	}

	return nil
}
