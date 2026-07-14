package frontend

import (
	apivalidate "go.temporal.io/server/api/protohelpers/validate"
)

// startWorkflowRequestRules exhaustively classifies every field of a
// StartWorkflowExecutionRequest as required or optional. Because the generated
// validator requires a rule for every field, adding a new proto field forces a
// decision here instead of silently skipping validation. It is a thin
// presence check run before the detailed validation in
// prepareStartWorkflowRequest; format, length, and semantic checks remain
// there.
//
// It is deliberately non-preempting: fields that already have a dedicated,
// specific error downstream (workflow_id, namespace, and the empty-name cases
// of workflow_type/task_queue) are Optional here so those errors are preserved.
// Required is used only where the presence guarantee is unambiguous.
var startWorkflowRequestRules = apivalidate.StartWorkflowExecutionRequest{
	WorkflowType: apivalidate.Required(), // must be a non-nil message
	TaskQueue:    apivalidate.Required(), // must be a non-nil message

	// Optional: defaulted, server-managed, or validated with a specific error
	// downstream in prepareStartWorkflowRequest.
	Namespace:                    apivalidate.Optional(), // resolved (and rejected) by the namespace registry
	WorkflowId:                   apivalidate.Optional(), // specific error: workflow.ErrWorkflowIDNotSet
	Input:                        apivalidate.Optional(),
	WorkflowExecutionTimeout:     apivalidate.Optional(),
	WorkflowRunTimeout:           apivalidate.Optional(),
	WorkflowTaskTimeout:          apivalidate.Optional(),
	Identity:                     apivalidate.Optional(),
	RequestId:                    apivalidate.Optional(), // defaulted when empty
	WorkflowIdReusePolicy:        apivalidate.Optional(), // defaulted
	WorkflowIdConflictPolicy:     apivalidate.Optional(), // defaulted
	RetryPolicy:                  apivalidate.Optional(),
	CronSchedule:                 apivalidate.Optional(),
	Memo:                         apivalidate.Optional(),
	SearchAttributes:             apivalidate.Optional(),
	Header:                       apivalidate.Optional(),
	RequestEagerExecution:        apivalidate.Optional(),
	ContinuedFailure:             apivalidate.Optional(),
	LastCompletionResult:         apivalidate.Optional(),
	WorkflowStartDelay:           apivalidate.Optional(),
	CompletionCallbacks:          apivalidate.Optional(),
	UserMetadata:                 apivalidate.Optional(),
	Links:                        apivalidate.Optional(),
	VersioningOverride:           apivalidate.Optional(),
	OnConflictOptions:            apivalidate.Optional(),
	Priority:                     apivalidate.Optional(),
	EagerWorkerDeploymentOptions: apivalidate.Optional(),
	TimeSkippingConfig:           apivalidate.Optional(),
}
