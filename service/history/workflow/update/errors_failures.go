package update

import (
	"errors"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
)

var (
	registryClearedErr          = errors.New("update registry was cleared")
	AbortedByServerErr          = serviceerror.NewUnavailable("workflow update was aborted")
	AbortedByWorkflowClosingErr = serviceerror.NewNotFound("workflow update was aborted by closing workflow")
	workflowTaskFailErr         = serviceerror.NewWorkflowNotReady("Unable to perform workflow execution update due to unexpected workflow task failure.")
)

var (
	unprocessedUpdateFailure = failurepb.Failure_builder{
		Message: "Workflow Update is rejected because it wasn't processed by worker. Probably, Workflow Update is not supported by the worker.",
		Source:  "Server",
		ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
			Type:         "UnprocessedUpdate",
			NonRetryable: true,
		}.Build(),
	}.Build()

	acceptedUpdateCompletedWorkflowFailure = failurepb.Failure_builder{
		Message: "Workflow Update failed because the Workflow completed before the Update completed.",
		Source:  "Server",
		ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
			Type:         "AcceptedUpdateCompletedWorkflow",
			NonRetryable: true,
		}.Build(),
	}.Build()
)
