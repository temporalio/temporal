package workerdeployment

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client interface {
	RegisterTaskQueueWorker(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, buildId string,
		taskQueueName string,
		taskQueueType enumspb.TaskQueueType,
		identity string,
	) error

	DescribeVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		version string,
		reportTaskQueueStats bool,
	) (*deploymentpb.WorkerDeploymentVersionInfo, []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue, error)

	DescribeWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
	) (*deploymentpb.WorkerDeploymentInfo, []byte, error)

	SetCurrentVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		identity string,
		ignoreMissingTaskQueues bool,
		conflictToken []byte,
		allowNoPollers bool,
	) (*deploymentspb.SetCurrentVersionResponse, error)

	ListWorkerDeployments(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		pageSize int,
		nextPageToken []byte,
	) ([]*deploymentspb.WorkerDeploymentSummary, []byte, error)

	DeleteWorkerDeploymentVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		version string,
		skipDrainage bool,
		identity string,
	) error

	DeleteWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		identity string,
	) error

	SetRampingVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		percentage float32,
		identity string,
		ignoreMissingTaskQueues bool,
		conflictToken []byte,
		allowNoPollers bool,
	) (*deploymentspb.SetRampingVersionResponse, error)

	UpdateVersionMetadata(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		version *deploymentpb.WorkerDeploymentVersion,
		upsertEntries map[string]*commonpb.Payload,
		removeEntries []string,
		identity string,
	) (*deploymentpb.VersionMetadata, error)

	SetManager(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		request *workflowservice.SetWorkerDeploymentManagerRequest,
	) (*workflowservice.SetWorkerDeploymentManagerResponse, error)

	// Used internally by the Worker Deployment Version workflow in its StartWorkerDeployment Activity
	// Deprecated.
	StartWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		identity string,
		requestID string,
	) error

	// Used internally by the Worker Deployment workflow in its StartWorkerDeploymentVersion Activity
	StartWorkerDeploymentVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, buildID string,
		identity string,
		requestID string,
	) error

	// Used internally by the Worker Deployment workflow in its SyncWorkerDeploymentVersion Activity
	SyncVersionWorkflowFromWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, version string,
		args *deploymentspb.SyncVersionStateUpdateArgs,
		identity string,
		requestID string,
	) (*deploymentspb.SyncVersionStateResponse, error)

	// Used internally by the Drainage workflow (child of Worker Deployment Version workflow)
	// in its GetVersionDrainageStatus Activity
	GetVersionDrainageStatus(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		version string) (enumspb.VersionDrainageStatus, error)

	// Used internally by the Worker Deployment workflow in its IsVersionMissingTaskQueues Activity
	// to verify if there are missing task queues in the new current/ramping version.
	IsVersionMissingTaskQueues(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		prevCurrentVersion, newVersion string,
	) (bool, error)

	// Used internally by the Worker Deployment workflow in its RegisterWorkerInVersion Activity
	// to register a task-queue worker in a version.
	RegisterWorkerInVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		args *deploymentspb.RegisterWorkerInVersionArgs,
		identity string,
	) error

	// SignalVersionReactivation sends a reactivation signal to a version workflow.
	// Used when workflows are pinned to a potentially DRAINED/INACTIVE version.
	// This is a fire-and-forget operation - errors are logged but returned for caller handling.
	SignalVersionReactivation(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, buildID string,
	) error
}

type ErrRegister struct{ error }

var retryPolicy = backoff.NewExponentialRetryPolicy(100 * time.Millisecond).WithExpirationInterval(1 * time.Minute)

// ClientImpl implements Client
type ClientImpl struct {
	logger                           log.Logger
	historyClient                    historyservice.HistoryServiceClient
	visibilityManager                manager.VisibilityManager
	matchingClient                   resource.MatchingClient
	maxIDLengthLimit                 dynamicconfig.IntPropertyFn
	visibilityMaxPageSize            dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxTaskQueuesInDeploymentVersion dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxDeployments                   dynamicconfig.IntPropertyFnWithNamespaceFilter
	testHooks                        testhooks.TestHooks
	metricsHandler                   metrics.Handler
}

func (d *ClientImpl) SetManager(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	request *workflowservice.SetWorkerDeploymentManagerRequest,
) (_ *workflowservice.SetWorkerDeploymentManagerResponse, retErr error) {
	var newManagerID string
	if request.GetSelf() {
		newManagerID = request.GetIdentity()
	} else {
		newManagerID = request.GetManagerIdentity()
	}
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("SetManager", request.GetDeploymentName(), &retErr, newManagerID, request.GetIdentity())()

	// validating params
	err := validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, request.GetDeploymentName(), d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	requestID := uuid.NewString()
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetManagerIdentityArgs{
		Identity:        request.GetIdentity(),
		ManagerIdentity: newManagerID,
		ConflictToken:   request.GetConflictToken(),
	})
	if err != nil {
		return nil, err
	}

	outcome, err := updateWorkflow(
		ctx,
		d.historyClient,
		namespaceEntry,
		GenerateDeploymentWorkflowID(request.GetDeploymentName()),
		&updatepb.Request{
			Input: &updatepb.Input{Name: SetManagerIdentity, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: request.GetIdentity()},
		},
	)
	if err != nil {
		return nil, err
	}

	var res deploymentspb.SetManagerIdentityResponse
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		res.PreviousManagerIdentity = newManagerID
		// Returning the latest conflict token
		details := failure.GetApplicationFailureInfo().GetDetails().GetPayloads()
		if len(details) > 0 {
			res.ConflictToken = details[0].GetData()
		}
		return &workflowservice.SetWorkerDeploymentManagerResponse{
			ConflictToken:           res.GetConflictToken(),
			PreviousManagerIdentity: res.GetPreviousManagerIdentity(),
		}, nil
	} else if failure.GetApplicationFailureInfo().GetType() == errFailedPrecondition {
		return nil, serviceerror.NewFailedPrecondition(failure.Message)
	} else if failure != nil {
		return nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}
	return &workflowservice.SetWorkerDeploymentManagerResponse{
		ConflictToken:           res.GetConflictToken(),
		PreviousManagerIdentity: res.GetPreviousManagerIdentity(),
	}, nil
}

var _ Client = (*ClientImpl)(nil)

var errUpdateInProgress = errors.New("update in progress")
var errWorkflowHistoryTooLong = errors.New("workflow history too long")

func (d *ClientImpl) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildId string,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	identity string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("RegisterTaskQueueWorker", deploymentName, &retErr, taskQueueName, taskQueueType, identity)()

	// Creating request ID out of build ID + TQ name + TQ type. Many updates may come from multiple
	// matching partitions, we do not want them to create new update requests.
	requestID := fmt.Sprintf("reg-ver-%v-%v-%d", farm.Fingerprint64([]byte(buildId)), farm.Fingerprint64([]byte(taskQueueName)), taskQueueType)

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.RegisterWorkerInWorkerDeploymentArgs{
		TaskQueueName: taskQueueName,
		TaskQueueType: taskQueueType,
		MaxTaskQueues: int32(d.maxTaskQueuesInDeploymentVersion(namespaceEntry.Name().String())),
		Version: &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        buildId,
		},
	})
	if err != nil {
		return err
	}

	// starting and updating the deployment version workflow, which in turn starts a deployment workflow.
	outcome, err := d.updateWithStartWorkerDeployment(ctx, namespaceEntry, deploymentName, buildId, &updatepb.Request{
		Input: &updatepb.Input{Name: RegisterWorkerInWorkerDeployment, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
	}, identity, requestID, d.getSyncBatchSize())
	if err != nil {
		return err
	}
	return d.handleRegisterVersionFailures(outcome)
}

func (d *ClientImpl) handleRegisterVersionFailures(outcome *updatepb.Outcome) error {
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errMaxTaskQueuesInVersionType ||
		failure.GetApplicationFailureInfo().GetType() == errTooManyVersions {
		return newResourceExhaustedError(failure.GetMessage())
	} else if failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		return nil
	} else if failure != nil {
		return ErrRegister{error: errors.New(failure.Message)}
	}
	return nil
}

func newResourceExhaustedError(message string) *serviceerror.ResourceExhausted {
	return &serviceerror.ResourceExhausted{
		Message: message,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS,
	}
}

func (d *ClientImpl) handleUpdateVersionFailures(outcome *updatepb.Outcome, deploymentName, buildID string) error {
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errVersionNotFound {
		return serviceerror.NewNotFoundf(ErrWorkerDeploymentVersionNotFound, buildID, deploymentName)
	} else if failure.GetApplicationFailureInfo().GetType() == errFailedPrecondition {
		return serviceerror.NewFailedPrecondition(failure.Message)
	} else if failure != nil {
		return serviceerror.NewInternal(failure.Message)
	}
	return nil
}

func (d *ClientImpl) DescribeVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	version string,
	reportTaskQueueStats bool,
) (
	_ *deploymentpb.WorkerDeploymentVersionInfo,
	_ []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue,
	retErr error,
) {
	v, err := worker_versioning.WorkerDeploymentVersionFromStringV31(version)
	if err != nil {
		return nil, nil, serviceerror.NewInvalidArgumentf("invalid version string %q, expected format is \"<deployment_name>.<build_id>\"", version)
	}
	deploymentName := v.GetDeploymentName()
	buildID := v.GetBuildId()

	//revive:disable-next-line:defer
	defer d.convertAndRecordError("DescribeVersion", deploymentName, &retErr, buildID)()

	// validate deployment name
	if err = validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit()); err != nil {
		return nil, nil, err
	}

	// validate buildID
	if err = validateVersionWfParams(worker_versioning.WorkerDeploymentBuildIDFieldName, buildID, d.maxIDLengthLimit()); err != nil {
		return nil, nil, err
	}

	workflowID := GenerateVersionWorkflowID(deploymentName, buildID)

	req := &historyservice.QueryWorkflowRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Namespace: namespaceEntry.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			Query: &querypb.WorkflowQuery{QueryType: QueryDescribeVersion},
		},
	}

	res, err := d.queryWorkflowWithRetry(ctx, req)

	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, nil, serviceerror.NewNotFound("Worker Deployment Version not found")
		}
		var queryFailed *serviceerror.QueryFailed
		if errors.As(err, &queryFailed) && queryFailed.Error() == errVersionDeleted {
			return nil, nil, serviceerror.NewNotFoundf(ErrWorkerDeploymentVersionNotFound, buildID, deploymentName)
		}
		return nil, nil, err
	}

	if rej := res.GetResponse().GetQueryRejected(); rej != nil {
		// This should not happen
		return nil, nil, serviceerror.NewInternalf("describe deployment query rejected with status %s", rej.GetStatus())
	}

	if res.GetResponse().GetQueryResult() == nil {
		return nil, nil, serviceerror.NewInternal("Did not receive deployment info")
	}

	var queryResponse deploymentspb.QueryDescribeVersionResponse
	err = sdk.PreferProtoDataConverter.FromPayloads(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, nil, err
	}

	tqInfos, err := d.getTaskQueueDetails(ctx, namespaceEntry.ID(), queryResponse.VersionState, reportTaskQueueStats)
	if err != nil {
		return nil, nil, err
	}

	versionInfo := versionStateToVersionInfo(queryResponse.VersionState, tqInfos)
	return versionInfo, tqInfos, nil
}

func (d *ClientImpl) queryWorkflowWithRetry(ctx context.Context, req *historyservice.QueryWorkflowRequest) (*historyservice.QueryWorkflowResponse, error) {
	var res *historyservice.QueryWorkflowResponse
	var err error
	err = backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		res, err = d.historyClient.QueryWorkflow(ctx, req)
		return err
	}, retryPolicy, isRetryableQueryError)
	return res, err
}

func (d *ClientImpl) UpdateVersionMetadata(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	version *deploymentpb.WorkerDeploymentVersion,
	upsertEntries map[string]*commonpb.Payload,
	removeEntries []string,
	identity string,
) (_ *deploymentpb.VersionMetadata, retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("UpdateVersionMetadata", version.GetDeploymentName(), &retErr, namespaceEntry.Name(), version.GetBuildId(), upsertEntries, removeEntries, identity)()
	requestID := uuid.NewString()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.UpdateVersionMetadataArgs{
		UpsertEntries: upsertEntries,
		RemoveEntries: removeEntries,
		Identity:      identity,
	})
	if err != nil {
		return nil, err
	}

	workflowID := GenerateVersionWorkflowID(version.GetDeploymentName(), version.GetBuildId())
	outcome, err := updateWorkflow(ctx, d.historyClient, namespaceEntry, workflowID, &updatepb.Request{
		Input: &updatepb.Input{Name: UpdateVersionMetadata, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
	})
	if err != nil {
		return nil, err
	}

	if failure := outcome.GetFailure(); failure != nil {
		return nil, serviceerror.NewInternal(failure.Message)
	}

	var res deploymentspb.UpdateVersionMetadataResponse
	if err := sdk.PreferProtoDataConverter.FromPayloads(outcome.GetSuccess(), &res); err != nil {
		return nil, err
	}

	return res.Metadata, nil
}

func (d *ClientImpl) DescribeWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
) (_ *deploymentpb.WorkerDeploymentInfo, conflictToken []byte, retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("DescribeWorkerDeployment", deploymentName, &retErr)()

	// validating params
	err := validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, nil, err
	}

	deploymentWorkflowID := GenerateDeploymentWorkflowID(deploymentName)

	req := &historyservice.QueryWorkflowRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Namespace: namespaceEntry.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: deploymentWorkflowID,
			},
			Query: &querypb.WorkflowQuery{QueryType: QueryDescribeDeployment},
		},
	}

	res, err := d.queryWorkflowWithRetry(ctx, req)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, nil, serviceerror.NewNotFoundf(ErrWorkerDeploymentNotFound, deploymentName)
		}
		var queryFailed *serviceerror.QueryFailed
		if errors.As(err, &queryFailed) && queryFailed.Error() == errDeploymentDeleted {
			return nil, nil, serviceerror.NewNotFoundf(ErrWorkerDeploymentNotFound, deploymentName)
		}
		return nil, nil, err
	}

	if rej := res.GetResponse().GetQueryRejected(); rej != nil {
		// This should not happen
		return nil, nil, serviceerror.NewInternalf("describe deployment query rejected with status %s", rej.GetStatus())
	}

	if res.GetResponse().GetQueryResult() == nil {
		return nil, nil, serviceerror.NewInternal("Did not receive deployment info")
	}

	var queryResponse deploymentspb.QueryDescribeWorkerDeploymentResponse
	err = sdk.PreferProtoDataConverter.FromPayloads(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, nil, err
	}

	dInfo, err := d.deploymentStateToDeploymentInfo(deploymentName, queryResponse.State)
	if err != nil {
		return nil, nil, err
	}
	return dInfo, queryResponse.GetState().GetConflictToken(), nil
}

func (d *ClientImpl) workerDeploymentExists(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
) (bool, error) {
	deploymentWorkflowID := GenerateDeploymentWorkflowID(deploymentName)

	res, err := d.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespaceEntry.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: deploymentWorkflowID,
			},
		},
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}

	// Deployment exists only if the entity wf is running
	return res.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}

func (d *ClientImpl) ListWorkerDeployments(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	pageSize int,
	nextPageToken []byte,
) (_ []*deploymentspb.WorkerDeploymentSummary, _ []byte, retError error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("ListWorkerDeployments", "", &retError)()

	query := WorkerDeploymentVisibilityBaseListQuery

	if pageSize == 0 {
		pageSize = d.visibilityMaxPageSize(namespaceEntry.Name().String())
	}

	persistenceResp, err := d.visibilityManager.ListWorkflowExecutions(
		ctx,
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceEntry.ID(),
			Namespace:     namespaceEntry.Name(),
			PageSize:      pageSize,
			NextPageToken: nextPageToken,
			Query:         query,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	workerDeploymentSummaries := make([]*deploymentspb.WorkerDeploymentSummary, 0, len(persistenceResp.Executions))
	for _, ex := range persistenceResp.Executions {
		var workerDeploymentInfo *deploymentspb.WorkerDeploymentWorkflowMemo
		if ex.GetMemo() != nil {
			workerDeploymentInfo, err = DecodeWorkerDeploymentMemo(ex.GetMemo())
			if err != nil {
				d.logger.Error("unable to decode worker deployment memo", tag.Error(err), tag.WorkflowNamespace(namespaceEntry.Name().String()), tag.WorkflowID(ex.GetExecution().GetWorkflowId()))
				continue
			}
		} else {
			// There is a race condition where the Deployment workflow exists, but has not yet
			// upserted the memo. If that is the case, we handle it here.
			workerDeploymentInfo = &deploymentspb.WorkerDeploymentWorkflowMemo{
				DeploymentName: GetDeploymentNameFromWorkflowID(ex.GetExecution().GetWorkflowId()),
				CreateTime:     ex.GetStartTime(),
				RoutingConfig:  &deploymentpb.RoutingConfig{CurrentVersion: worker_versioning.UnversionedVersionId},
			}
		}

		workerDeploymentSummaries = append(workerDeploymentSummaries, &deploymentspb.WorkerDeploymentSummary{
			Name:                  workerDeploymentInfo.DeploymentName,
			CreateTime:            workerDeploymentInfo.CreateTime,
			RoutingConfig:         workerDeploymentInfo.RoutingConfig,
			LatestVersionSummary:  workerDeploymentInfo.LatestVersionSummary,
			RampingVersionSummary: workerDeploymentInfo.RampingVersionSummary,
			CurrentVersionSummary: workerDeploymentInfo.CurrentVersionSummary,
		})
	}

	return workerDeploymentSummaries, persistenceResp.NextPageToken, nil
}

func (d *ClientImpl) SetCurrentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	version string,
	identity string,
	ignoreMissingTaskQueues bool,
	conflictToken []byte,
	allowNoPollers bool,
) (_ *deploymentspb.SetCurrentVersionResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("SetCurrentVersion", deploymentName, &retErr, namespaceEntry.Name(), version, identity)()

	versionObj, err := worker_versioning.WorkerDeploymentVersionFromStringV31(version)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid version string: " + err.Error())
	}
	if versionObj.GetDeploymentName() != "" && versionObj.GetDeploymentName() != deploymentName {
		return nil, serviceerror.NewInvalidArgumentf("invalid version string '%s' does not match deployment name '%s'", version, deploymentName)
	}

	err = validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetCurrentVersionArgs{
		Identity:                identity,
		Version:                 version,
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
		ConflictToken:           conflictToken,
		AllowNoPollers:          allowNoPollers,
	})
	if err != nil {
		return nil, err
	}

	// Generating a new updateID and requestID for each request. No-ops are handled by the worker-deployment workflow.
	updateID := uuid.NewString()
	requestID := uuid.NewString()

	var outcome *updatepb.Outcome
	if allowNoPollers {
		// we want to start the Worker Deployment workflow if it hasn't been started by a poller
		outcome, err = d.updateWithStartWorkerDeployment(
			ctx,
			namespaceEntry,
			deploymentName,
			versionObj.GetBuildId(),
			&updatepb.Request{
				Input: &updatepb.Input{Name: SetCurrentVersion, Args: updatePayload},
				Meta:  &updatepb.Meta{UpdateId: updateID, Identity: identity},
			},
			identity,
			requestID,
			d.getSyncBatchSize(),
		)
		if err != nil {
			return nil, err
		}
	} else {
		// we *don't* want to start the Worker Deployment workflow; it should be started by a poller
		outcome, err = updateWorkflow(
			ctx,
			d.historyClient,
			namespaceEntry,
			GenerateDeploymentWorkflowID(deploymentName),
			&updatepb.Request{
				Input: &updatepb.Input{Name: SetCurrentVersion, Args: updatePayload},
				Meta:  &updatepb.Meta{UpdateId: updateID, Identity: identity},
			},
		)
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				return nil, serviceerror.NewNotFoundf(ErrWorkerDeploymentNotFound, deploymentName)
			}
			return nil, err
		}
	}

	var res deploymentspb.SetCurrentVersionResponse
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		res.PreviousVersion = version
		// Returning the latest conflict token
		details := failure.GetApplicationFailureInfo().GetDetails().GetPayloads()
		if len(details) > 0 {
			res.ConflictToken = details[0].GetData()
		}
		return &res, nil
	} else if updateErr := d.handleUpdateVersionFailures(outcome, deploymentName, versionObj.GetBuildId()); updateErr != nil {
		return nil, updateErr
	} else if registerErr := d.handleRegisterVersionFailures(outcome); registerErr != nil {
		return nil, registerErr
	}

	success := outcome.GetSuccess()
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (d *ClientImpl) SetRampingVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	version string,
	percentage float32,
	identity string,
	ignoreMissingTaskQueues bool,
	conflictToken []byte,
	allowNoPollers bool,
) (_ *deploymentspb.SetRampingVersionResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("SetRampingVersion", deploymentName, &retErr, namespaceEntry.Name(), version, percentage, identity)()

	var err error
	var versionObj *deploymentspb.WorkerDeploymentVersion
	if version != "" {
		versionObj, err = worker_versioning.WorkerDeploymentVersionFromStringV31(version)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument("invalid version string: " + err.Error())
		}
		if versionObj.GetDeploymentName() != "" && versionObj.GetDeploymentName() != deploymentName {
			return nil, serviceerror.NewInvalidArgumentf("invalid version string '%s' does not match deployment name '%s'", version, deploymentName)
		}
	}

	err = validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateDeploymentWorkflowID(deploymentName)

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetRampingVersionArgs{
		Identity:                identity,
		Version:                 version,
		Percentage:              percentage,
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
		ConflictToken:           conflictToken,
		AllowNoPollers:          allowNoPollers,
	})
	if err != nil {
		return nil, err
	}

	// Generating a new updateID for each request. No-ops are handled by the worker-deployment workflow.
	updateID := uuid.NewString()
	requestID := uuid.NewString()

	var outcome *updatepb.Outcome
	if allowNoPollers {
		// we want to start the Worker Deployment workflow if it hasn't been started by a poller
		outcome, err = d.updateWithStartWorkerDeployment(
			ctx,
			namespaceEntry,
			deploymentName,
			versionObj.GetBuildId(),
			&updatepb.Request{
				Input: &updatepb.Input{Name: SetRampingVersion, Args: updatePayload},
				Meta:  &updatepb.Meta{UpdateId: updateID, Identity: identity},
			},
			identity,
			requestID,
			d.getSyncBatchSize(),
		)
		if err != nil {
			return nil, err
		}
	} else {
		outcome, err = updateWorkflow(
			ctx,
			d.historyClient,
			namespaceEntry,
			workflowID,
			&updatepb.Request{
				Input: &updatepb.Input{Name: SetRampingVersion, Args: updatePayload},
				Meta:  &updatepb.Meta{UpdateId: updateID, Identity: identity},
			},
		)
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				return nil, serviceerror.NewNotFoundf(ErrWorkerDeploymentNotFound, deploymentName)
			}
			return nil, err
		}
	}

	var res deploymentspb.SetRampingVersionResponse
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		res.PreviousVersion = version
		res.PreviousPercentage = percentage

		// Returning the latest conflict token
		details := failure.GetApplicationFailureInfo().GetDetails().GetPayloads()
		if len(details) > 0 {
			res.ConflictToken = details[0].GetData()
		}

		return &res, nil
	} else if updateErr := d.handleUpdateVersionFailures(outcome, deploymentName, versionObj.GetBuildId()); updateErr != nil {
		return nil, updateErr
	} else if registerErr := d.handleRegisterVersionFailures(outcome); registerErr != nil {
		return nil, registerErr
	}

	success := outcome.GetSuccess()
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (d *ClientImpl) DeleteWorkerDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	version string,
	skipDrainage bool,
	identity string,
) (retErr error) {
	v, err := worker_versioning.WorkerDeploymentVersionFromStringV31(version)
	if err != nil {
		return serviceerror.NewInvalidArgumentf("invalid version string %q, expected format is \"<deployment_name>.<build_id>\"", version)
	}
	deploymentName := v.GetDeploymentName()
	buildId := v.GetBuildId()

	//revive:disable-next-line:defer
	defer d.convertAndRecordError("DeleteWorkerDeploymentVersion", deploymentName, &retErr, namespaceEntry.Name(), buildId)()
	requestID := uuid.NewString()

	if identity == "" {
		identity = requestID
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeleteVersionArgs{
		Identity: identity,
		Version: worker_versioning.WorkerDeploymentVersionToStringV31(&deploymentspb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        buildId,
		}),
		SkipDrainage: skipDrainage,
	})
	if err != nil {
		return err
	}

	err = validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return err
	}

	workflowID := GenerateDeploymentWorkflowID(deploymentName)

	outcome, err := updateWorkflow(
		ctx,
		d.historyClient,
		namespaceEntry,
		workflowID,
		&updatepb.Request{
			Input: &updatepb.Input{Name: DeleteVersion, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
	)
	if err != nil {
		return err
	}

	if failure := outcome.GetFailure(); failure != nil {
		if failure.GetApplicationFailureInfo().GetType() == errVersionNotFound {
			return nil
		} else if failure.GetApplicationFailureInfo().GetType() == errFailedPrecondition {
			return serviceerror.NewFailedPrecondition(failure.GetMessage()) // non-retryable error to stop multiple activity attempts
		} else if failure.GetCause().GetApplicationFailureInfo().GetType() == errFailedPrecondition {
			return serviceerror.NewFailedPrecondition(failure.GetCause().GetMessage())
		}
		return serviceerror.NewInternal(failure.Message)
	}
	return nil
}

func (d *ClientImpl) DeleteWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	identity string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("DeleteWorkerDeployment", deploymentName, &retErr, namespaceEntry.Name(), identity)()

	// validating params
	err := validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return err
	}

	requestID := uuid.NewString()
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeleteDeploymentArgs{
		Identity: identity,
	})
	if err != nil {
		return err
	}

	err = validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return err
	}
	workflowID := GenerateDeploymentWorkflowID(deploymentName)

	outcome, err := updateWorkflow(
		ctx,
		d.historyClient,
		namespaceEntry,
		workflowID,
		&updatepb.Request{
			Input: &updatepb.Input{Name: DeleteDeployment, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
	)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return err
	}

	if failure := outcome.GetFailure(); failure != nil {
		return serviceerror.NewInternal(failure.Message)
	}
	return nil
}

func (d *ClientImpl) StartWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	identity string,
	requestID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("StartWorkerDeployment", deploymentName, &retErr, namespaceEntry.Name(), identity)()

	workflowID := GenerateDeploymentWorkflowID(deploymentName)

	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  namespaceEntry.Name().String(),
		NamespaceId:    namespaceEntry.ID().String(),
		DeploymentName: deploymentName,
	})
	if err != nil {
		return err
	}

	startReq := makeStartRequest(requestID, workflowID, identity, WorkerDeploymentWorkflowType, namespaceEntry, nil, input)

	historyStartReq := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:  namespaceEntry.ID().String(),
		StartRequest: startReq,
	}

	_, err = d.historyClient.StartWorkflowExecution(ctx, historyStartReq)
	return err
}

func (d *ClientImpl) StartWorkerDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
	identity string,
	requestID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("StartWorkerDeploymentVersion", deploymentName, &retErr, namespaceEntry.Name(), identity)()

	err := validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return err
	}
	err = validateVersionWfParams(worker_versioning.WorkerDeploymentBuildIDFieldName, buildID, d.maxIDLengthLimit())
	if err != nil {
		return err
	}

	workflowID := GenerateVersionWorkflowID(deploymentName, buildID)
	input, err := sdk.PreferProtoDataConverter.ToPayloads(d.makeVersionWorkflowArgs(deploymentName, buildID, namespaceEntry))
	if err != nil {
		return err
	}
	startReq := makeStartRequest(requestID, workflowID, identity, WorkerDeploymentVersionWorkflowType, namespaceEntry, nil, input)

	historyStartReq := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:  namespaceEntry.ID().String(),
		StartRequest: startReq,
	}

	_, err = d.historyClient.StartWorkflowExecution(ctx, historyStartReq)
	return err
}

func (d *ClientImpl) SyncVersionWorkflowFromWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, version string,
	args *deploymentspb.SyncVersionStateUpdateArgs,
	identity string,
	requestID string,
) (_ *deploymentspb.SyncVersionStateResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("SyncVersionWorkflowFromWorkerDeployment", deploymentName, &retErr, namespaceEntry.Name(), version, args, identity)()

	versionObj, err := worker_versioning.WorkerDeploymentVersionFromStringV31(version)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid version string: " + err.Error())
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(args)
	if err != nil {
		return nil, err
	}

	workflowID := GenerateVersionWorkflowID(deploymentName, versionObj.GetBuildId())

	// updates an already existing deployment version workflow.
	outcome, err := updateWorkflow(
		ctx,
		d.historyClient,
		namespaceEntry,
		workflowID,
		&updatepb.Request{
			Input: &updatepb.Input{Name: SyncVersionState, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
	)
	if err != nil {
		return nil, err
	}

	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		// pretend this is a success
		outcome = &updatepb.Outcome{
			Value: &updatepb.Outcome_Success{
				Success: failure.GetApplicationFailureInfo().GetDetails(),
			},
		}
	} else if failure != nil {
		// TODO: is there an easy way to recover the original type here?
		return nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	var res deploymentspb.SyncVersionStateResponse
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (d *ClientImpl) updateWithStartWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
	syncBatchSize int32,
) (*updatepb.Outcome, error) {
	err := validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = validateVersionWfParams(worker_versioning.WorkerDeploymentBuildIDFieldName, buildID, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateDeploymentWorkflowID(deploymentName)

	exists, err := d.workerDeploymentExists(ctx, namespaceEntry, deploymentName)
	if err != nil {
		return nil, err
	}
	if !exists {
		// New deployment, make sure we're not exceeding the limit
		count, err := d.countWorkerDeployments(ctx, namespaceEntry)
		if err != nil {
			return nil, err
		}
		limit := d.maxDeployments(namespaceEntry.Name().String())
		if count >= int64(limit) {
			return nil, newResourceExhaustedError(fmt.Sprintf("reached maximum deployments in namespace (%d)", limit))
		}
	}

	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  namespaceEntry.Name().String(),
		NamespaceId:    namespaceEntry.ID().String(),
		DeploymentName: deploymentName,
		State: &deploymentspb.WorkerDeploymentLocalState{
			SyncBatchSize: syncBatchSize,
		},
	})
	if err != nil {
		return nil, err
	}

	return updateWorkflowWithStart(
		ctx,
		d.historyClient,
		namespaceEntry,
		WorkerDeploymentWorkflowType,
		workflowID,
		nil,
		input,
		updateRequest,
		identity,
		requestID,
	)
}

func (d *ClientImpl) countWorkerDeployments(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
) (count int64, retError error) {
	query := WorkerDeploymentVisibilityBaseListQuery

	persistenceResp, err := d.visibilityManager.CountWorkflowExecutions(
		ctx,
		&manager.CountWorkflowExecutionsRequest{
			NamespaceID: namespaceEntry.ID(),
			Namespace:   namespaceEntry.Name(),
			Query:       query,
		},
	)
	metrics.WorkerDeploymentVersionVisibilityQueryCount.With(d.metricsHandler).Record(1, metrics.OperationTag("countWorkerDeployments"))
	if err != nil {
		return 0, err
	}
	return persistenceResp.Count, nil
}

func (d *ClientImpl) updateWithStartWorkerDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	err := validateVersionWfParams(worker_versioning.WorkerDeploymentNameFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = validateVersionWfParams(worker_versioning.WorkerDeploymentBuildIDFieldName, buildID, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateVersionWorkflowID(deploymentName, buildID)
	input, err := sdk.PreferProtoDataConverter.ToPayloads(d.makeVersionWorkflowArgs(deploymentName, buildID, namespaceEntry))
	if err != nil {
		return nil, err
	}

	return updateWorkflowWithStart(
		ctx,
		d.historyClient,
		namespaceEntry,
		WorkerDeploymentVersionWorkflowType,
		workflowID,
		nil,
		input,
		updateRequest,
		identity,
		requestID,
	)
}

func (d *ClientImpl) convertAndRecordError(operation string, deploymentName string, retErr *error, args ...any) func() {
	start := time.Now()
	return func() {
		elapsed := time.Since(start)

		// TODO: add metrics recording here

		if *retErr != nil {
			if isFailedPreconditionOrNotFound(*retErr) {
				d.logger.Debug("deployment client failure due to a failed precondition or not found error",
					tag.Error(*retErr),
					tag.Operation(operation),
					tag.Deployment(deploymentName),
					tag.Duration("elapsed", elapsed),
					tag.Any("args", args),
				)
			} else {
				if isRetryableUpdateError(*retErr) || isRetryableQueryError(*retErr) {
					d.logger.Debug("deployment client throttling due to retryable error",
						tag.Error(*retErr),
						tag.Operation(operation),
						tag.Deployment(deploymentName),
						tag.Duration("elapsed", elapsed),
						tag.Any("args", args),
					)
					var errResourceExhausted *serviceerror.ResourceExhausted
					if !errors.As(*retErr, &errResourceExhausted) || errResourceExhausted.Cause != enumspb.RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS {
						// if it's not a deployment limits error, we don't want to expose the underlying cause to the user
						*retErr = &serviceerror.ResourceExhausted{
							Message: fmt.Sprintf(ErrTooManyRequests, deploymentName),
							Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
							// These errors are caused by workflow throughput limits, so BUSY_WORKFLOW is the most appropriate cause.
							// This cause is not sent back to the user.
							Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
						}
					}
				} else if errors.Is(*retErr, context.DeadlineExceeded) ||
					errors.Is(*retErr, context.Canceled) ||
					common.IsContextDeadlineExceededErr(*retErr) ||
					common.IsContextCanceledErr(*retErr) {
					d.logger.Debug("deployment client timeout or cancellation",
						tag.Error(*retErr),
						tag.Operation(operation),
						tag.Deployment(deploymentName),
						tag.Duration("elapsed", elapsed),
						tag.Any("args", args),
					)
				} else {
					d.logger.Error("deployment client unexpected error",
						tag.Error(*retErr),
						tag.Operation(operation),
						tag.Deployment(deploymentName),
						tag.Duration("elapsed", elapsed),
						tag.Any("args", args),
					)
				}
			}
		} else {
			d.logger.Debug("deployment client success",
				tag.Operation(operation),
				tag.Deployment(deploymentName),
				tag.Duration("elapsed", elapsed),
				tag.Any("args", args),
			)
		}
	}
}

//nolint:staticcheck
func versionStateToVersionInfo(
	state *deploymentspb.VersionLocalState,
	taskQueueInfos []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue,
) *deploymentpb.WorkerDeploymentVersionInfo {
	if state == nil {
		return nil
	}

	infos := make([]*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo, 0, len(taskQueueInfos))
	for _, taskQueueInfo := range taskQueueInfos {
		infos = append(infos, &deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{
			Name: taskQueueInfo.Name,
			Type: taskQueueInfo.Type,
		})
	}

	// never return empty drainage info
	drainageInfo := state.GetDrainageInfo()
	if drainageInfo.GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED {
		drainageInfo = nil
	}

	return &deploymentpb.WorkerDeploymentVersionInfo{
		Version:            worker_versioning.WorkerDeploymentVersionToStringV31(state.Version),
		DeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromVersion(state.Version),
		Status:             state.Status,
		CreateTime:         state.CreateTime,
		RoutingChangedTime: state.RoutingUpdateTime,
		CurrentSinceTime:   state.CurrentSinceTime,
		RampingSinceTime:   state.RampingSinceTime,
		RampPercentage:     state.RampPercentage,
		TaskQueueInfos:     infos,
		DrainageInfo:       drainageInfo,
		Metadata:           state.Metadata,
	}
}

func (d *ClientImpl) getTaskQueueDetails(
	ctx context.Context,
	namespaceID namespace.ID,
	state *deploymentspb.VersionLocalState,
	reportTaskQueueStats bool,
) ([]*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue, error) {
	if state == nil {
		return nil, nil
	}

	tqOutputs := []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{}
	for tqName, taskQueueFamilyInfo := range state.TaskQueueFamilies {
		for tqType := range taskQueueFamilyInfo.TaskQueues {
			tqOutputs = append(tqOutputs, &workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
				Name: tqName,
				Type: enumspb.TaskQueueType(tqType),
			})
		}
	}
	if len(tqOutputs) == 0 {
		return nil, nil
	}

	// Only query the matching service for task queue stats if requested since it's an expensive operation.
	if reportTaskQueueStats {
		tqInputs := []*matchingservice.DescribeVersionedTaskQueuesRequest_VersionTaskQueue{}
		for _, tq := range tqOutputs {
			vtq := &matchingservice.DescribeVersionedTaskQueuesRequest_VersionTaskQueue{
				Name: tq.Name,
				Type: tq.Type,
			}
			tqInputs = append(tqInputs, vtq)
		}

		// Sort the task queues by name and type to ensure that the task queue we query is deterministic.
		// This ensures we'll hit the cache on the same task queue partition.
		sort.Slice(tqInputs, func(i, j int) bool {
			if tqInputs[i].Name != tqInputs[j].Name {
				return tqInputs[i].Name < tqInputs[j].Name
			}
			return tqInputs[i].Type < tqInputs[j].Type
		})
		routeTQ := tqInputs[0]

		tqResp, err := d.matchingClient.DescribeVersionedTaskQueues(ctx,
			&matchingservice.DescribeVersionedTaskQueuesRequest{
				NamespaceId:       namespaceID.String(),
				TaskQueue:         &taskqueuepb.TaskQueue{Name: routeTQ.Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				TaskQueueType:     routeTQ.Type,
				Version:           state.Version,
				VersionTaskQueues: tqInputs,
			})
		if err != nil {
			return nil, err
		}

		tqKey := func(tqName string, tqType enumspb.TaskQueueType) string { return fmt.Sprintf("%s-%d", tqName, tqType) }
		tqRespMap := make(map[string]*matchingservice.DescribeVersionedTaskQueuesResponse_VersionTaskQueue)
		for _, tq := range tqResp.GetVersionTaskQueues() {
			tqRespMap[tqKey(tq.Name, tq.Type)] = tq
		}

		// Update stats for existing entries
		for i, tq := range tqOutputs {
			if tqRespTQ, ok := tqRespMap[tqKey(tq.Name, tq.Type)]; ok {
				tqOutputs[i].Stats = tqRespTQ.Stats
				tqOutputs[i].StatsByPriorityKey = tqRespTQ.StatsByPriorityKey
				continue
			}
			// This *should* never happen, but in case it does, we should error instead of returning partial results.
			return nil, serviceerror.NewNotFoundf("task queue %s of type %s not found in this version", tq.Name, tq.Type)
		}
	}

	return tqOutputs, nil
}

func (d *ClientImpl) deploymentStateToDeploymentInfo(deploymentName string, state *deploymentspb.WorkerDeploymentLocalState) (*deploymentpb.WorkerDeploymentInfo, error) {
	if state == nil {
		return nil, nil
	}

	var workerDeploymentInfo deploymentpb.WorkerDeploymentInfo

	workerDeploymentInfo.Name = deploymentName
	workerDeploymentInfo.CreateTime = state.CreateTime
	workerDeploymentInfo.RoutingConfig = state.RoutingConfig
	workerDeploymentInfo.LastModifierIdentity = state.LastModifierIdentity
	workerDeploymentInfo.ManagerIdentity = state.ManagerIdentity
	if len(state.PropagatingRevisions) > 0 {
		workerDeploymentInfo.RoutingConfigUpdateState = enumspb.ROUTING_CONFIG_UPDATE_STATE_IN_PROGRESS
	} else {
		workerDeploymentInfo.RoutingConfigUpdateState = enumspb.ROUTING_CONFIG_UPDATE_STATE_COMPLETED
	}

	for _, v := range state.Versions {
		workerDeploymentInfo.VersionSummaries = append(workerDeploymentInfo.VersionSummaries, &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
			Version:              v.GetVersion(),
			DeploymentVersion:    worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(v.Version),
			CreateTime:           v.GetCreateTime(),
			DrainageStatus:       v.GetDrainageInfo().GetStatus(), // deprecated.
			DrainageInfo:         v.GetDrainageInfo(),
			RoutingUpdateTime:    v.GetRoutingUpdateTime(),
			CurrentSinceTime:     v.GetCurrentSinceTime(),
			RampingSinceTime:     v.GetRampingSinceTime(),
			FirstActivationTime:  v.GetFirstActivationTime(),
			LastCurrentTime:      v.GetLastCurrentTime(),
			LastDeactivationTime: v.GetLastDeactivationTime(),
			Status:               v.GetStatus(),
		})
	}

	// Sort by create time, with the latest version first.
	sort.Slice(workerDeploymentInfo.VersionSummaries, func(i, j int) bool {
		return workerDeploymentInfo.VersionSummaries[i].CreateTime.AsTime().After(workerDeploymentInfo.VersionSummaries[j].CreateTime.AsTime())
	})

	return &workerDeploymentInfo, nil
}

func (d *ClientImpl) GetVersionDrainageStatus(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	version string) (enumspb.VersionDrainageStatus, error) {
	countRequest := manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespaceEntry.ID(),
		Namespace:   namespaceEntry.Name(),
		Query:       makeDeploymentQuery(worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(version))),
	}
	countResponse, err := d.visibilityManager.CountWorkflowExecutions(ctx, &countRequest)
	metrics.WorkerDeploymentVersionVisibilityQueryCount.With(d.metricsHandler).Record(1, metrics.OperationTag("GetVersionDrainageStatus"))
	if err != nil {
		return enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED, err
	}
	if countResponse.Count == 0 {
		return enumspb.VERSION_DRAINAGE_STATUS_DRAINED, nil
	}
	return enumspb.VERSION_DRAINAGE_STATUS_DRAINING, nil
}

func makeDeploymentQuery(version string) string {
	deploymentFilter := fmt.Sprintf("= '%s'", version)
	statusFilter := "= 'Running'"
	behaviorFilter := "= 'Pinned'"
	return fmt.Sprintf("%s %s AND %s %s AND %s %s",
		sadefs.TemporalWorkerDeploymentVersion, deploymentFilter,
		sadefs.TemporalWorkflowVersioningBehavior, behaviorFilter,
		sadefs.ExecutionStatus, statusFilter,
	)
}

func (d *ClientImpl) IsVersionMissingTaskQueues(ctx context.Context, namespaceEntry *namespace.Namespace, prevCurrentVersion, newVersion string) (bool, error) {
	// Check if all the task-queues in the prevCurrentVersion are present in the newCurrentVersion (newVersion is either the new ramping version or the new current version)
	prevCurrentVersionInfo, _, err := d.DescribeVersion(ctx, namespaceEntry, prevCurrentVersion, false)
	if err != nil {
		return false, serviceerror.NewFailedPreconditionf("Version %s not found in deployment with error: %v", prevCurrentVersion, err)
	}

	newVersionInfo, _, err := d.DescribeVersion(ctx, namespaceEntry, newVersion, false)
	if err != nil {
		return false, serviceerror.NewFailedPreconditionf("Version %s not found in deployment with error: %v", newVersion, err)
	}

	missingTaskQueues, err := d.checkForMissingTaskQueues(prevCurrentVersionInfo, newVersionInfo)
	if err != nil {
		return false, err
	}

	if len(missingTaskQueues) == 0 {
		return false, nil
	}

	// Verify that all the missing task-queues have been added to another deployment or do not have backlogged tasks/add-rate > 0
	for _, missingTaskQueue := range missingTaskQueues {
		isExpectedInNewVersion, err := d.isTaskQueueExpectedInNewVersion(ctx, namespaceEntry, missingTaskQueue, prevCurrentVersionInfo)
		if err != nil {
			return false, err
		}
		if isExpectedInNewVersion {
			// one of the missing task queues is expected in the new version
			return true, nil
		}
	}

	// all expected task queues are present in the new version
	return false, nil
}

// isTaskQueueExpectedInNewVersion checks if a task queue is expected in the new version. A task queue is expected in the new version if:
// 1. It is not assigned to a deployment different from the deployment's current version.
// 2. It has backlogged tasks or add-rate > 0.
func (d *ClientImpl) isTaskQueueExpectedInNewVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	taskQueue *deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo,
	prevCurrentVersionInfo *deploymentpb.WorkerDeploymentVersionInfo,
) (bool, error) {
	// First check if task queue is assigned to another deployment
	response, err := d.matchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceEntry.ID().String(),
		DescRequest: &workflowservice.DescribeTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue.Name,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			TaskQueueType: taskQueue.Type,
		},
	})
	if err != nil {
		return false, err
	}

	// Task Queue has been moved to another Worker Deployment
	if response.DescResponse.VersioningInfo != nil &&
		response.DescResponse.VersioningInfo.GetCurrentVersion() != prevCurrentVersionInfo.GetVersion() {
		return false, nil
	}

	versionStr := worker_versioning.ExternalWorkerDeploymentVersionToString(prevCurrentVersionInfo.GetDeploymentVersion())

	// Check if task queue has backlogged tasks or add-rate > 0
	req := &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceEntry.ID().String(),
		DescRequest: &workflowservice.DescribeTaskQueueRequest{
			ApiMode: enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue.Name,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			TaskQueueTypes: []enumspb.TaskQueueType{taskQueue.Type},
			Versions: &taskqueuepb.TaskQueueVersionSelection{
				BuildIds: []string{versionStr}, // pretending the version string is a build id
			},
			// Since request doesn't pass through frontend, this field is not automatically populated.
			// Moreover, DescribeTaskQueueEnhanced requires this field to be set to WORKFLOW type.
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ReportStats:   true,
		},
	}
	response, err = d.matchingClient.DescribeTaskQueue(ctx, req)
	if err != nil {
		d.logger.Error("error fetching AddRate for task-queue", tag.Error(err))
		return false, err
	}

	typesInfo := response.GetDescResponse().GetVersionsInfo()[versionStr].GetTypesInfo()
	if typesInfo != nil {
		typeStats := typesInfo[int32(taskQueue.Type)]
		if typeStats != nil && typeStats.GetStats() != nil &&
			(typeStats.GetStats().GetTasksAddRate() != 0 || typeStats.GetStats().GetApproximateBacklogCount() != 0) {
			return true, nil
		}
	}

	return false, nil
}

// checkForMissingTaskQueues checks if all the task-queues in the previous version are present in the new version
func (d *ClientImpl) checkForMissingTaskQueues(prevCurrentVersionInfo, newCurrentVersionInfo *deploymentpb.WorkerDeploymentVersionInfo) ([]*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo, error) {
	prevCurrentVersionTaskQueues := prevCurrentVersionInfo.GetTaskQueueInfos()
	newCurrentVersionTaskQueues := newCurrentVersionInfo.GetTaskQueueInfos()

	missingTaskQueues := []*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{}
	for _, prevTaskQueue := range prevCurrentVersionTaskQueues {
		found := false
		for _, newTaskQueue := range newCurrentVersionTaskQueues {
			if prevTaskQueue.GetName() == newTaskQueue.GetName() && prevTaskQueue.GetType() == newTaskQueue.GetType() {
				found = true
				break
			}
		}
		if !found {
			missingTaskQueues = append(missingTaskQueues, prevTaskQueue)
		}
	}

	return missingTaskQueues, nil
}

func (d *ClientImpl) RegisterWorkerInVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	args *deploymentspb.RegisterWorkerInVersionArgs,
	identity string,
) error {
	versionObj, err := worker_versioning.WorkerDeploymentVersionFromStringV31(args.Version)
	if err != nil {
		return serviceerror.NewInvalidArgument("invalid version string: " + err.Error())
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(args)
	if err != nil {
		return err
	}

	requestID := uuid.NewString()
	outcome, err := d.updateWithStartWorkerDeploymentVersion(ctx, namespaceEntry, versionObj.DeploymentName, versionObj.BuildId, &updatepb.Request{
		Input: &updatepb.Input{Name: RegisterWorkerInDeploymentVersion, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
	}, identity, requestID)
	if err != nil {
		return err
	}

	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errMaxTaskQueuesInVersionType {
		// translate to a non-retryable error
		return temporal.NewNonRetryableApplicationError(failure.Message, errMaxTaskQueuesInVersionType, serviceerror.NewFailedPrecondition(failure.Message))
	} else if failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		return nil
	} else if failure != nil {
		return ErrRegister{error: errors.New(failure.Message)}
	}

	return nil
}

func (d *ClientImpl) SignalVersionReactivation(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.convertAndRecordError("SignalVersionReactivation", deploymentName, &retErr, buildID)()

	workflowID := GenerateVersionWorkflowID(deploymentName, buildID)

	signalRequest := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: namespaceEntry.ID().String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: namespaceEntry.Name().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			SignalName: ReactivateVersionSignalName,
			Input:      nil,
			Identity:   "history-service",
		},
	}

	_, err := d.historyClient.SignalWorkflowExecution(ctx, signalRequest)
	return err
}

func (d *ClientImpl) getSyncBatchSize() int32 {
	syncBatchSize := int32(25)
	if n, ok := testhooks.Get(d.testHooks, testhooks.TaskQueuesInDeploymentSyncBatchSize, testhooks.GlobalScope); ok && n > 0 {
		// In production, the testhook would be set to 0 and never reach here!
		syncBatchSize = int32(n)
	}
	return syncBatchSize
}

func (d *ClientImpl) makeVersionWorkflowArgs(
	deploymentName, buildID string,
	namespaceEntry *namespace.Namespace,
) *deploymentspb.WorkerDeploymentVersionWorkflowArgs {
	return &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        buildID,
			},
			CreateTime:        timestamppb.Now(),
			RoutingUpdateTime: nil,
			CurrentSinceTime:  nil,                                 // not current
			RampingSinceTime:  nil,                                 // not ramping
			RampPercentage:    0,                                   // not ramping
			DrainageInfo:      &deploymentpb.VersionDrainageInfo{}, // not draining or drained
			Metadata:          nil,
			SyncBatchSize:     d.getSyncBatchSize(),
		},
	}
}
