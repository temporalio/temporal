package tqid

import (
	"fmt"
	"strings"
	"unicode/utf8"

	enums2 "go.temporal.io/api/enums/v1"
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

	if err := ValidateTaskQueueName(taskQueue.GetName(), maxIDLengthLimit); err != nil {
		return err
	}

	if taskQueue.GetKind() == enums2.TASK_QUEUE_KIND_STICKY {
		if err := ValidateTaskQueueName(taskQueue.GetNormalName(), maxIDLengthLimit); err != nil {
			return err
		}
	}

	return nil
}

// ValidateTaskQueueName checks if a given task queue name is valid.
// It verifies the name is not empty, does not exceed the maximum length,
// and is a valid UTF-8 string.
//
// Parameters:
//   - name: The task queue name to validate.
//   - maxLength: The maximum allowed length for the name.
//
// Returns an error if the name is invalid, nil otherwise.
func ValidateTaskQueueName(name string, maxLength int) error {
	if name == "" {
		return serviceerror.NewInvalidArgument("taskQueue is not set")
	}
	if len(name) > maxLength {
		return serviceerror.NewInvalidArgument("taskQueue length exceeds limit")
	}

	if !utf8.ValidString(name) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("taskQueue %q is not a valid UTF-8 string", name))
	}

	if strings.HasPrefix(name, reservedTaskQueuePrefix) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("task queue name cannot start with reserved prefix %v", reservedTaskQueuePrefix))
	}

	return nil
}
