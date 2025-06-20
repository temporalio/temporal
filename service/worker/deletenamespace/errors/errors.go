package errors

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

const (
	InvalidArgumentErrType                = "InvalidArgument"
	FailedPreconditionErrType             = "FailedPrecondition"
	ExecutionsStillExistErrType           = "ExecutionsStillExist"
	NoProgressErrType                     = "NoProgress"
	NotDeletedExecutionsStillExistErrType = "NotDeletedExecutionsStillExist"
)

func NewInvalidArgument(message string, cause error) error {
	return temporal.NewNonRetryableApplicationError(message, InvalidArgumentErrType, cause, nil)
}

func NewFailedPrecondition(message string, cause error) error {
	return temporal.NewNonRetryableApplicationError(message, FailedPreconditionErrType, cause, nil)
}

func NewExecutionsStillExist(count int) error {
	return temporal.NewApplicationError(fmt.Sprintf("%d executions are still exist", count), ExecutionsStillExistErrType, count)
}

func NewNoProgress(count int) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("no progress was made: %d executions are still exist", count), NoProgressErrType, nil, count)
}

func NewNotDeletedExecutionsStillExist(count int) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("%d not deleted executions are still exist", count), NotDeletedExecutionsStillExistErrType, nil, count)
}

func ToServiceError(err error, workflowID, runID string) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case InvalidArgumentErrType:
			return serviceerror.NewInvalidArgument(appErr.Message())
		case FailedPreconditionErrType:
			return serviceerror.NewFailedPrecondition(appErr.Message())
		}
	}
	return serviceerror.NewSystemWorkflow(
		&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		err,
	)
}
