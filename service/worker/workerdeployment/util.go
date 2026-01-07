package workerdeployment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	update2 "go.temporal.io/server/service/history/workflow/update"
)

const (
	// Workflow types
	WorkerDeploymentVersionWorkflowType = "temporal-sys-worker-deployment-version-workflow"
	WorkerDeploymentWorkflowType        = "temporal-sys-worker-deployment-workflow"

	// Namespace division
	WorkerDeploymentNamespaceDivision = "TemporalWorkerDeployment"

	// Updates
	RegisterWorkerInDeploymentVersion = "register-task-queue-worker"    // for Worker Deployment Version wf
	SyncVersionState                  = "sync-version-state"            // for Worker Deployment Version wfs
	UpdateVersionMetadata             = "update-version-metadata"       // for Worker Deployment Version wfs
	RegisterWorkerInWorkerDeployment  = "register-worker-in-deployment" // for Worker Deployment wfs
	SetCurrentVersion                 = "set-current-version"           // for Worker Deployment wfs
	SetRampingVersion                 = "set-ramping-version"           // for Worker Deployment wfs
	DeleteVersion                     = "delete-version"                // for WorkerDeployment wfs
	DeleteDeployment                  = "delete-deployment"             // for WorkerDeployment wfs
	SetManagerIdentity                = "set-manager-identity"          // for WorkerDeployment wfs
	serverDeleteVersionIdentity       = "try-delete-for-add-version"    // identity of the worker-deployment workflow when it tries to delete a version on the event that the addition
	// of a version exceeds the max number of versions allowed in a worker-deployment (defaultMaxVersions)

	// Signals
	ForceCANSignalName        = "force-continue-as-new" // for Worker Deployment Version _and_ Worker Deployment wfs
	SyncDrainageSignalName    = "sync-drainage-status"
	TerminateDrainageSignal   = "terminate-drainage"
	SyncVersionSummarySignal  = "sync-version-summary"
	PropagationCompleteSignal = "propagation-complete"

	// Queries
	QueryDescribeVersion    = "describe-version"    // for Worker Deployment Version wf
	QueryDescribeDeployment = "describe-deployment" // for Worker Deployment wf

	// Memos
	WorkerDeploymentMemoField = "WorkerDeploymentMemo" // for Worker Deployment wf

	// Application error names for rejected updates
	errNoChangeType               = "errNoChange"
	errTooManyVersions            = "errTooManyVersions"
	errTooManyDeployments         = "errTooManyDeployments"
	errVersionAlreadyExistsType   = "errVersionAlreadyExists"
	errMaxTaskQueuesInVersionType = "errMaxTaskQueuesInVersion"
	errVersionNotFound            = "Version not found in deployment"
	errDeploymentDeleted          = "worker deployment deleted"         // returned in the race condition that the deployment is deleted but the workflow is not yet closed.
	errVersionDeleted             = "worker deployment version deleted" // returned in the race condition that the deployment version is deleted but the workflow is not yet closed.
	errLongHistory                = "errLongHistory"                    // update is not accepted until CaN happens. client should retry
	errVersionIsDraining          = "errVersionIsDraining"
	errVersionHasPollers          = "errVersionHasPollersSuffix"

	errFailedPrecondition = "FailedPrecondition"

	ErrVersionIsDraining         = "version '%s' cannot be deleted since it is draining"
	ErrVersionHasPollers         = "version '%s' cannot be deleted since it has active pollers"
	ErrVersionIsCurrentOrRamping = "version '%s' cannot be deleted since it is current or ramping"

	ErrRampingVersionDoesNotHaveAllTaskQueues = "proposed ramping version '%s' is missing active task queues from the current version; these would become unversioned if it is set as the ramping version"
	ErrCurrentVersionDoesNotHaveAllTaskQueues = "proposed current version '%s' is missing active task queues from the current version; these would become unversioned if it is set as the current version"
	ErrManagerIdentityMismatch                = "ManagerIdentity '%s' is set and does not match user identity '%s'; to proceed, set your own identity as the ManagerIdentity, remove the ManagerIdentity, or wait for the other client to do so"
	ErrWorkerDeploymentNotFound               = "no Worker Deployment found with name '%s'; does your Worker Deployment have pollers?"
	ErrWorkerDeploymentVersionNotFound        = "build ID '%s' not found in Worker Deployment '%s'"
	ErrTooManyRequests                        = "too many requests issued to Worker Deployment '%s'. Please try again later"
)

var (
	WorkerDeploymentVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		sadefs.WorkflowType,
		WorkerDeploymentWorkflowType,
		sadefs.TemporalNamespaceDivision,
		WorkerDeploymentNamespaceDivision,
		sadefs.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)
)

var (
	defaultActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 100 * time.Millisecond,
			MaximumAttempts: 5,
		},
	}
	propagationActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 100 * time.Millisecond,
			// unlimited attempts
		},
	}
)

// validateVersionWfParams is a helper that verifies if the fields used for generating
// Worker Deployment Version related workflowID's are valid
func validateVersionWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	return worker_versioning.ValidateDeploymentVersionFields(fieldName, field, maxIDLengthLimit)
}

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment workflows
func GenerateDeploymentWorkflowID(deploymentName string) string {
	return worker_versioning.WorkerDeploymentWorkflowIDPrefix + worker_versioning.WorkerDeploymentVersionDelimiter + deploymentName
}

func GetDeploymentNameFromWorkflowID(workflowID string) string {
	_, deploymentName, _ := strings.Cut(workflowID, worker_versioning.WorkerDeploymentVersionDelimiter)
	return deploymentName
}

// GenerateVersionWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment Version workflows
func GenerateVersionWorkflowID(deploymentName string, buildID string) string {
	versionString := worker_versioning.ExternalWorkerDeploymentVersionToString(&deploymentpb.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	})
	return worker_versioning.WorkerDeploymentVersionWorkflowIDPrefix + worker_versioning.WorkerDeploymentVersionDelimiter + versionString
}

func DecodeWorkerDeploymentMemo(memo *commonpb.Memo) *deploymentspb.WorkerDeploymentWorkflowMemo {
	var workerDeploymentWorkflowMemo deploymentspb.WorkerDeploymentWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[WorkerDeploymentMemoField], &workerDeploymentWorkflowMemo)
	if err != nil {
		return nil
	}
	return &workerDeploymentWorkflowMemo
}

func getSafeDurationConfig(ctx workflow.Context, id string, unsafeGetter func() time.Duration, defaultValue time.Duration) (time.Duration, error) {
	get := func(_ workflow.Context) interface{} {
		return unsafeGetter()
	}
	var value time.Duration
	if err := workflow.MutableSideEffect(ctx, id, get, durationEq).Get(&value); err != nil {
		return defaultValue, err
	}
	return value, nil
}

func durationEq(a, b any) bool {
	return a == b
}

func isFailedPreconditionOrNotFound(err error) bool {
	var failedPreconditionError *serviceerror.FailedPrecondition
	var notFound *serviceerror.NotFound
	return errors.As(err, &failedPreconditionError) || errors.As(err, &notFound)
}

// update updates an already existing deployment version/deployment workflow.
func updateWorkflow(
	ctx context.Context,
	historyClient historyservice.HistoryServiceClient,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	updateRequest *updatepb.Request,
) (*updatepb.Outcome, error) {
	updateReq := &historyservice.UpdateWorkflowExecutionRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request: &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: namespaceEntry.Name().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			Request:    updateRequest,
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
		},
	}

	var outcome *updatepb.Outcome
	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		// historyClient retries internally on retryable rpc errors, we just have to retry on
		// successful but un-completed responses.
		res, err := historyClient.UpdateWorkflowExecution(ctx, updateReq)
		if err != nil {
			return err
		}

		if err := convertUpdateFailure(res.GetResponse()); err != nil {
			return err
		}

		outcome = res.GetResponse().GetOutcome()
		return nil
	}, retryPolicy, isRetryableUpdateError)

	return outcome, err
}

// extractApplicationErrorOrInternal extract application error from update failure preserving error type and retriability.
// If the failure is no-nil but not an application error, it returns an internal error.
func extractApplicationErrorOrInternal(failure *failurepb.Failure) error {
	if failure != nil {
		if af := failure.GetApplicationFailureInfo(); af != nil {
			if af.GetNonRetryable() {
				return temporal.NewNonRetryableApplicationError(failure.GetMessage(), af.GetType(), nil)
			}
			return temporal.NewApplicationError(failure.GetMessage(), af.GetType(), nil)
		}
		return serviceerror.NewInternal(failure.Message)
	}
	return nil
}

func updateWorkflowWithStart(
	ctx context.Context,
	historyClient historyservice.HistoryServiceClient,
	namespaceEntry *namespace.Namespace,
	workflowType string,
	workflowID string,
	memo *commonpb.Memo,
	input *commonpb.Payloads,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	// Start workflow execution, if it hasn't already
	startReq := makeStartRequest(requestID, workflowID, identity, workflowType, namespaceEntry, memo, input)

	updateReq := &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: namespaceEntry.Name().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
		Request:    updateRequest,
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
	}

	// This is an atomic operation; if one operation fails, both will.
	multiOpReq := &historyservice.ExecuteMultiOperationRequest{
		NamespaceId: namespaceEntry.ID().String(),
		WorkflowId:  workflowID,
		Operations: []*historyservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &historyservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: &historyservice.StartWorkflowExecutionRequest{
						NamespaceId:  namespaceEntry.ID().String(),
						StartRequest: startReq,
					},
				},
			},
			{
				Operation: &historyservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: &historyservice.UpdateWorkflowExecutionRequest{
						NamespaceId: namespaceEntry.ID().String(),
						Request:     updateReq,
					},
				},
			},
		},
	}

	var outcome *updatepb.Outcome

	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		// historyClient retries internally on retryable rpc errors, we just have to retry on
		// successful but un-completed responses.
		res, err := historyClient.ExecuteMultiOperation(ctx, multiOpReq)
		if err != nil {
			return err
		}

		// we should get exactly one of each of these
		var startRes *historyservice.StartWorkflowExecutionResponse
		var updateRes *workflowservice.UpdateWorkflowExecutionResponse
		for _, response := range res.Responses {
			if sr := response.GetStartWorkflow(); sr != nil {
				startRes = sr
			} else if ur := response.GetUpdateWorkflow().GetResponse(); ur != nil {
				updateRes = ur
			}
		}
		if startRes == nil {
			return serviceerror.NewInternal("failed to start deployment workflow")
		}

		if err := convertUpdateFailure(updateRes); err != nil {
			return err
		}

		outcome = updateRes.GetOutcome()
		return nil
	}, retryPolicy, isRetryableUpdateError)

	return outcome, err
}

func convertUpdateFailure(updateRes *workflowservice.UpdateWorkflowExecutionResponse) error {
	if updateRes == nil {
		return serviceerror.NewInternal("failed to update deployment workflow")
	}

	if updateRes.Stage != enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED {
		// update not completed, try again
		return errUpdateInProgress
	}

	if failure := updateRes.GetOutcome().GetFailure(); failure != nil {
		if failure.GetApplicationFailureInfo().GetType() == errLongHistory {
			// Retryable
			return errWorkflowHistoryTooLong
		} else if failure.GetApplicationFailureInfo().GetType() == errVersionDeleted {
			// Non-retryable
			return serviceerror.NewNotFoundf("Worker Deployment Version not found")
		} else if failure.GetApplicationFailureInfo().GetType() == errDeploymentDeleted {
			// Non-retryable
			return serviceerror.NewNotFoundf("Worker Deployment not found")
		} else if failure.GetApplicationFailureInfo().GetType() == errFailedPrecondition {
			return serviceerror.NewFailedPrecondition(failure.GetMessage())
		}

		// we let caller handle other update failures
	} else if updateRes.GetOutcome().GetSuccess() == nil {
		return serviceerror.NewInternal("outcome missing success and failure")
	}
	return nil
}

func isRetryableQueryError(err error) bool {
	var internalErr *serviceerror.Internal
	return api.IsRetryableError(err) && !errors.As(err, &internalErr)
}

func isRetryableUpdateError(err error) bool {
	if errors.Is(err, errUpdateInProgress) || errors.Is(err, errWorkflowHistoryTooLong) ||
		err.Error() == consts.ErrWorkflowClosing.Error() || err.Error() == update2.AbortedByServerErr.Error() {
		return true
	}

	var errResourceExhausted *serviceerror.ResourceExhausted
	if errors.As(err, &errResourceExhausted) &&
		(errResourceExhausted.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT ||
			errResourceExhausted.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW) {
		// We're hitting the max concurrent update limit for the wf. Retrying will eventually succeed.
		return true
	}

	var errWfNotReady *serviceerror.WorkflowNotReady
	if errors.As(err, &errWfNotReady) {
		// Update edge cases, can retry.
		return true
	}

	// All updates that are admitted as the workflow is closing due to CaN are considered retryable.
	// The ErrWorkflowClosing and ResourceExhausted could be nested.
	var errMultiOps *serviceerror.MultiOperationExecution
	if errors.As(err, &errMultiOps) {
		for _, e := range errMultiOps.OperationErrors() {
			if errors.As(e, &errResourceExhausted) &&
				(errResourceExhausted.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT ||
					errResourceExhausted.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW) {
				// We're hitting the max concurrent update limit for the wf. Retrying will eventually succeed.
				return true
			}
			if e.Error() == consts.ErrWorkflowClosing.Error() || e.Error() == update2.AbortedByServerErr.Error() {
				return true
			}
		}
	}
	return false
}

func makeStartRequest(
	requestID, workflowID, identity, workflowType string,
	namespaceEntry *namespace.Namespace,
	memo *commonpb.Memo,
	input *commonpb.Payloads,
) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                requestID,
		Namespace:                namespaceEntry.Name().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    input,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		SearchAttributes:         buildSearchAttributes(),
		Memo:                     memo,
		Identity:                 identity,
	}
}

func buildSearchAttributes() *commonpb.SearchAttributes {
	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, sadefs.TemporalNamespaceDivision, payload.EncodeString(WorkerDeploymentNamespaceDivision))
	return sa
}
