package workerdeployment

import (
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
)

const (
	// Workflow types
	WorkerDeploymentVersionWorkflowType = "temporal-sys-worker-deployment-version-workflow"
	WorkerDeploymentWorkflowType        = "temporal-sys-worker-deployment-workflow"

	// Namespace division
	WorkerDeploymentNamespaceDivision = "TemporalWorkerDeployment"

	// Updates
	RegisterWorkerInDeploymentVersion = "register-task-queue-worker"       // for Worker Deployment Version wf
	SyncVersionState                  = "sync-version-state"               // for Worker Deployment Version wfs
	UpdateVersionMetadata             = "update-version-metadata"          // for Worker Deployment Version wfs
	RegisterWorkerInWorkerDeployment  = "register-worker-in-deployment"    // for Worker Deployment wfs
	SetCurrentVersion                 = "set-current-version"              // for Worker Deployment wfs
	SetRampingVersion                 = "set-ramping-version"              // for Worker Deployment wfs
	AddVersionToWorkerDeployment      = "add-version-to-worker-deployment" // for Worker Deployment wfs
	DeleteVersion                     = "delete-version"                   // for WorkerDeployment wfs
	DeleteDeployment                  = "delete-deployment"                // for WorkerDeployment wfs
	SetManagerIdentity                = "set-manager-identity"             // for WorkerDeployment wfs

	// Signals
	ForceCANSignalName       = "force-continue-as-new" // for Worker Deployment Version _and_ Worker Deployment wfs
	SyncDrainageSignalName   = "sync-drainage-status"
	TerminateDrainageSignal  = "terminate-drainage"
	SyncVersionSummarySignal = "sync-version-summary"

	// Queries
	QueryDescribeVersion    = "describe-version"    // for Worker Deployment Version wf
	QueryDescribeDeployment = "describe-deployment" // for Worker Deployment wf

	// Memos
	WorkerDeploymentMemoField = "WorkerDeploymentMemo" // for Worker Deployment wf

	// Prefixes, Delimeters and Keys
	WorkerDeploymentVersionWorkflowIDPrefix      = "temporal-sys-worker-deployment-version"
	WorkerDeploymentVersionWorkflowIDDelimeter   = ":"
	WorkerDeploymentVersionWorkflowIDInitialSize = len(WorkerDeploymentVersionWorkflowIDDelimeter) + len(WorkerDeploymentVersionWorkflowIDPrefix)
	WorkerDeploymentNameFieldName                = "WorkerDeploymentName"
	WorkerDeploymentBuildIDFieldName             = "BuildID"

	// Application error names for rejected updates
	errNoChangeType               = "errNoChange"
	errTooManyVersions            = "errTooManyVersions"
	errTooManyDeployments         = "errTooManyDeployments"
	errVersionAlreadyExistsType   = "errVersionAlreadyExists"
	errMaxTaskQueuesInVersionType = "errMaxTaskQueuesInVersion"
	errVersionNotFound            = "Version not found in deployment"

	errConflictTokenMismatchType = "errConflictTokenMismatch"
	errFailedPrecondition        = "FailedPrecondition"

	ErrVersionIsDraining         = "Version cannot be deleted since it is draining."
	ErrVersionHasPollers         = "Version cannot be deleted since it has active pollers."
	ErrVersionIsCurrentOrRamping = "Version cannot be deleted since it is current or ramping."

	ErrRampingVersionDoesNotHaveAllTaskQueues = "proposed ramping version is missing active task queues from the current version; these would become unversioned if it is set as the ramping version"
	ErrCurrentVersionDoesNotHaveAllTaskQueues = "proposed current version is missing active task queues from the current version; these would become unversioned if it is set as the current version"
	ErrManagerIdentityMismatch                = "ManagerIdentity '%s' is set and does not match user identity '%s'; to proceed, set your own identity as the ManagerIdentity, remove the ManagerIdentity, or wait for the other client to do so"
	ErrWorkerDeploymentNotFound               = "no Worker Deployment found with name %s; does your Worker Deployment have pollers?"
)

var (
	WorkerDeploymentVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		WorkerDeploymentWorkflowType,
		searchattribute.TemporalNamespaceDivision,
		WorkerDeploymentNamespaceDivision,
		searchattribute.ExecutionStatus,
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
)

// validateVersionWfParams is a helper that verifies if the fields used for generating
// Worker Deployment Version related workflowID's are valid
func validateVersionWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgumentf("%v cannot be empty", fieldName)
	}

	// Length of each field should be: (MaxIDLengthLimit - (prefix + delimeter length)) / 2
	if len(field) > (maxIDLengthLimit-WorkerDeploymentVersionWorkflowIDInitialSize)/2 {
		return serviceerror.NewInvalidArgumentf("size of %v larger than the maximum allowed", fieldName)
	}

	// deploymentName cannot have "."
	// TODO: remove this restriction once the old version strings are completely cleaned from external and internal API
	if fieldName == WorkerDeploymentNameFieldName && strings.Contains(field, worker_versioning.WorkerDeploymentVersionIdDelimiterV31) {
		return serviceerror.NewInvalidArgumentf("worker deployment name cannot contain '%s'", worker_versioning.WorkerDeploymentVersionIdDelimiterV31)
	}
	// deploymentName cannot have ":"
	if fieldName == WorkerDeploymentNameFieldName && strings.Contains(field, worker_versioning.WorkerDeploymentVersionIdDelimiter) {
		return serviceerror.NewInvalidArgumentf("worker deployment name cannot contain '%s'", worker_versioning.WorkerDeploymentVersionIdDelimiter)
	}

	// buildID or deployment name cannot start with "__"
	if strings.HasPrefix(field, "__") {
		return serviceerror.NewInvalidArgumentf("%v cannot start with '__'", fieldName)
	}

	return nil
}

func DecodeWorkerDeploymentMemo(memo *commonpb.Memo) *deploymentspb.WorkerDeploymentWorkflowMemo {
	var workerDeploymentWorkflowMemo deploymentspb.WorkerDeploymentWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[WorkerDeploymentMemoField], &workerDeploymentWorkflowMemo)
	if err != nil {
		return nil
	}
	return &workerDeploymentWorkflowMemo
}

func getSafeDurationConfig(ctx workflow.Context, id string, unsafeGetter func() any, defaultValue time.Duration) (time.Duration, error) {
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

// isFailedPrecondition checks if the error is a FailedPrecondition error. It also checks if the FailedPrecondition error is wrapped in an ApplicationError.
func isFailedPrecondition(err error) bool {
	var failedPreconditionError *serviceerror.FailedPrecondition
	var applicationError *temporal.ApplicationError
	return errors.As(err, &failedPreconditionError) || (errors.As(err, &applicationError) && applicationError.Type() == errFailedPrecondition)
}
