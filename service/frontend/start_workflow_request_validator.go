package frontend

import (
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/protohelpers/validate"
	"go.temporal.io/server/api/protohelpers/validation"
	"google.golang.org/protobuf/types/known/durationpb"
)

type startWorkflowRequest = workflowservice.StartWorkflowExecutionRequest

// requiredMessage rejects a nil message field.
func requiredMessage[V comparable]() validation.FieldValidator[startWorkflowRequest, V] {
	var zero V
	return validation.Field[startWorkflowRequest](func(fieldName string, v V) error {
		if v == zero {
			return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
		}
		return nil
	})
}

// startWorkflowRequestValidator is an exhaustive, typed validator for
// StartWorkflowExecutionRequest run before the detailed validation in
// prepareStartWorkflowRequest. Every field is assigned a validator, so a new
// proto field forces a decision here.
//
// It is deliberately non-preempting: fields with a dedicated, specific error
// downstream (workflow_id, namespace, and the empty-name cases of
// workflow_type/task_queue) are Optional so those errors are preserved. Only
// unambiguous nil-message presence is enforced here. Config-aware, normalizing,
// or cross-field rules can be added per field following PR #10200's pattern.
var startWorkflowRequestValidator = validate.StartWorkflowExecutionRequestFieldValidators{
	WorkflowType: requiredMessage[*commonpb.WorkflowType](),
	TaskQueue:    requiredMessage[*taskqueuepb.TaskQueue](),

	Namespace:                    validation.Optional[startWorkflowRequest, string](),
	WorkflowId:                   validation.Optional[startWorkflowRequest, string](),
	Input:                        validation.Optional[startWorkflowRequest, *commonpb.Payloads](),
	WorkflowExecutionTimeout:     validation.Optional[startWorkflowRequest, *durationpb.Duration](),
	WorkflowRunTimeout:           validation.Optional[startWorkflowRequest, *durationpb.Duration](),
	WorkflowTaskTimeout:          validation.Optional[startWorkflowRequest, *durationpb.Duration](),
	Identity:                     validation.Optional[startWorkflowRequest, string](),
	RequestId:                    validation.Optional[startWorkflowRequest, string](),
	WorkflowIdReusePolicy:        validation.Optional[startWorkflowRequest, enumspb.WorkflowIdReusePolicy](),
	WorkflowIdConflictPolicy:     validation.Optional[startWorkflowRequest, enumspb.WorkflowIdConflictPolicy](),
	RetryPolicy:                  validation.Optional[startWorkflowRequest, *commonpb.RetryPolicy](),
	CronSchedule:                 validation.Optional[startWorkflowRequest, string](),
	Memo:                         validation.Optional[startWorkflowRequest, *commonpb.Memo](),
	SearchAttributes:             validation.Optional[startWorkflowRequest, *commonpb.SearchAttributes](),
	Header:                       validation.Optional[startWorkflowRequest, *commonpb.Header](),
	RequestEagerExecution:        validation.Optional[startWorkflowRequest, bool](),
	ContinuedFailure:             validation.Optional[startWorkflowRequest, *failurepb.Failure](),
	LastCompletionResult:         validation.Optional[startWorkflowRequest, *commonpb.Payloads](),
	WorkflowStartDelay:           validation.Optional[startWorkflowRequest, *durationpb.Duration](),
	CompletionCallbacks:          validation.Optional[startWorkflowRequest, []*commonpb.Callback](),
	UserMetadata:                 validation.Optional[startWorkflowRequest, *sdkpb.UserMetadata](),
	Links:                        validation.Optional[startWorkflowRequest, []*commonpb.Link](),
	VersioningOverride:           validation.Optional[startWorkflowRequest, *workflowpb.VersioningOverride](),
	OnConflictOptions:            validation.Optional[startWorkflowRequest, *workflowpb.OnConflictOptions](),
	Priority:                     validation.Optional[startWorkflowRequest, *commonpb.Priority](),
	EagerWorkerDeploymentOptions: validation.Optional[startWorkflowRequest, *deploymentpb.WorkerDeploymentOptions](),
	TimeSkippingConfig:           validation.Optional[startWorkflowRequest, *commonpb.TimeSkippingConfig](),
}
