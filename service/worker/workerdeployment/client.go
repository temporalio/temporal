// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
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

package workerdeployment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
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
		firstPoll time.Time,
		identity string,
		requestID string,
	) error

	DescribeVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		version string,
	) (*deploymentpb.WorkerDeploymentVersionInfo, error)

	DescribeWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
	) (*workflowservice.DescribeWorkerDeploymentResponse, error)

	SetCurrentVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		identity string,
		conflictToken []byte,
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
	) error

	SetRampingVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		percentage float32,
		identity string,
		conflictToken []byte,
	) (*deploymentspb.SetRampingVersionResponse, error)

	// Used internally by the Worker Deployment workflow in its StartWorkerDeployment Activity
	StartWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
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

	// Used internally by the Worker Deployment workflow in its DeleteVersion Activity
	DeleteVersionFromWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, version string,
		identity string,
		requestID string,
	) error

	// Used internally by the Worker Deployment Version workflow in its AddVersionToWorkerDeployment Activity
	AddVersionToWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		identity string,
		requestID string,
	) (*deploymentspb.AddVersionToWorkerDeploymentResponse, error)

	// Used internally by the Drainage workflow (child of Worker Deployment Version workflow)
	// in its GetVersionDrainageStatus Activity
	GetVersionDrainageStatus(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, buildID string) (enumspb.VersionDrainageStatus, error)
}

type ErrMaxTaskQueuesInDeployment struct{ error }

type ErrRegister struct{ error }

// ClientImpl implements Client
type ClientImpl struct {
	logger                    log.Logger
	historyClient             historyservice.HistoryServiceClient
	visibilityManager         manager.VisibilityManager
	maxIDLengthLimit          dynamicconfig.IntPropertyFn
	visibilityMaxPageSize     dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxTaskQueuesInDeployment dynamicconfig.IntPropertyFnWithNamespaceFilter
}

var _ Client = (*ClientImpl)(nil)

var errRetry = errors.New("retry update")

func (d *ClientImpl) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildId string,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	firstPoll time.Time,
	identity string,
	requestID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.record("RegisterTaskQueueWorker", &retErr, taskQueueName, taskQueueType, identity)()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.RegisterWorkerInVersionArgs{
		TaskQueueName:   taskQueueName,
		TaskQueueType:   taskQueueType,
		FirstPollerTime: timestamppb.New(firstPoll),
		MaxTaskQueues:   int32(d.maxTaskQueuesInDeployment(namespaceEntry.Name().String())),
	})
	if err != nil {
		return err
	}

	outcome, err := d.updateWithStartWorkerDeploymentVersion(ctx, namespaceEntry, deploymentName, buildId, &updatepb.Request{
		Input: &updatepb.Input{Name: RegisterWorkerInDeployment, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
	}, identity, requestID)
	if err != nil {
		return err
	}

	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errMaxTaskQueuesInVersionType {
		// translate to client-side error type
		return ErrMaxTaskQueuesInDeployment{error: errors.New(failure.Message)}
	} else if failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		return nil
	} else if failure != nil {
		return ErrRegister{error: errors.New(failure.Message)}
	}

	return nil
}

func (d *ClientImpl) DescribeVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	version string,
) (_ *deploymentpb.WorkerDeploymentVersionInfo, retErr error) {
	v, err := worker_versioning.WorkerDeploymentVersionFromString(version)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid version string %q, expected format is \"<deployment_name>/<build_id>\"", version))
	}
	deploymentName := v.GetDeploymentName()
	buildID := v.GetBuildId()

	//revive:disable-next-line:defer
	defer d.record("DescribeVersion", &retErr, deploymentName, buildID)()

	// validate deployment name
	err = validateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	// validate buildID
	err = validateVersionWfParams(WorkerDeploymentBuildIDFieldName, buildID, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := worker_versioning.GenerateVersionWorkflowID(deploymentName, buildID)

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

	res, err := d.historyClient.QueryWorkflow(ctx, req)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, serviceerror.NewNotFound("Deployment Version not found")
		}
		return nil, err
	}

	var queryResponse deploymentspb.QueryDescribeVersionResponse
	err = sdk.PreferProtoDataConverter.FromPayloads(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, err
	}

	return versionStateToVersionInfo(queryResponse.VersionState), nil
}

func (d *ClientImpl) DescribeWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
) (_ *workflowservice.DescribeWorkerDeploymentResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("DescribeWorkerDeployment", &retErr, deploymentName)()

	// validating params
	err := validateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	deploymentWorkflowID := worker_versioning.GenerateDeploymentWorkflowID(deploymentName)

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

	res, err := d.historyClient.QueryWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}

	var queryResponse deploymentspb.QueryDescribeWorkerDeploymentResponse
	err = sdk.PreferProtoDataConverter.FromPayloads(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, err
	}

	dInfo, err := d.deploymentStateToDeploymentInfo(ctx, namespaceEntry, deploymentName, queryResponse.State)
	if err != nil {
		return nil, err
	}

	return &workflowservice.DescribeWorkerDeploymentResponse{
		ConflictToken:        queryResponse.State.ConflictToken,
		WorkerDeploymentInfo: dInfo,
	}, nil
}

func (d *ClientImpl) ListWorkerDeployments(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	pageSize int,
	nextPageToken []byte,
) (_ []*deploymentspb.WorkerDeploymentSummary, _ []byte, retError error) {
	//revive:disable-next-line:defer
	defer d.record("ListWorkerDeployments", &retError)()

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

	workerDeploymentSummaries := make([]*deploymentspb.WorkerDeploymentSummary, len(persistenceResp.Executions))
	for i, ex := range persistenceResp.Executions {
		workerDeploymentInfo := DecodeWorkerDeploymentMemo(ex.GetMemo())
		workerDeploymentSummaries[i] = &deploymentspb.WorkerDeploymentSummary{
			Name:          workerDeploymentInfo.DeploymentName,
			CreateTime:    workerDeploymentInfo.CreateTime,
			RoutingConfig: workerDeploymentInfo.RoutingConfig,
		}
	}

	return workerDeploymentSummaries, persistenceResp.NextPageToken, nil
}

func (d *ClientImpl) SetCurrentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	version string,
	identity string,
	conflictToken []byte,
) (_ *deploymentspb.SetCurrentVersionResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("SetCurrentVersion", &retErr, namespaceEntry.Name(), version, identity)()
	requestID := uuid.New()
	versionObj, err := worker_versioning.WorkerDeploymentVersionFromString(version)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid version string: " + err.Error())
	}
	if versionObj.GetDeploymentName() != deploymentName {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid version string '%s' does not match deployment name '%s'", version, deploymentName))
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetCurrentVersionArgs{
		Identity:      identity,
		Version:       version,
		ConflictToken: conflictToken,
	})
	if err != nil {
		return nil, err
	}

	outcome, err := d.updateWithStartWorkerDeployment(
		ctx,
		namespaceEntry,
		versionObj.DeploymentName,
		&updatepb.Request{
			Input: &updatepb.Input{Name: SetCurrentVersion, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
		identity,
		requestID,
	)
	if err != nil {
		return nil, err
	}

	var res deploymentspb.SetCurrentVersionResponse
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		res.PreviousVersion = version
		return &res, nil
	} else if failure != nil {
		// TODO: is there an easy way to recover the original type here?
		return nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return nil, serviceerror.NewInternal("outcome missing success and failure")
	}

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
	conflictToken []byte,
) (_ *deploymentspb.SetRampingVersionResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("SetRampingVersion", &retErr, namespaceEntry.Name(), version, percentage, identity)()
	requestID := uuid.New()
	var versionObj *deploymentspb.WorkerDeploymentVersion
	var err error
	if version == "" {
		versionObj = &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        "",
		}
	} else {
		versionObj, err = worker_versioning.WorkerDeploymentVersionFromString(version)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument("invalid version string: " + err.Error())
		}
	}
	if versionObj.GetDeploymentName() != deploymentName {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid version string '%s' does not match deployment name '%s'", version, deploymentName))
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetRampingVersionArgs{
		Identity:      identity,
		Version:       version,
		Percentage:    percentage,
		ConflictToken: conflictToken,
	})
	if err != nil {
		return nil, err
	}
	outcome, err := d.updateWithStartWorkerDeployment(
		ctx,
		namespaceEntry,
		versionObj.GetDeploymentName(),
		&updatepb.Request{
			Input: &updatepb.Input{Name: SetRampingVersion, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
		identity,
		requestID,
	)
	if err != nil {
		return nil, err
	}

	var res deploymentspb.SetRampingVersionResponse
	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		res.PreviousVersion = version
		res.PreviousPercentage = percentage
		return &res, nil
	} else if failure.GetApplicationFailureInfo().GetType() == errVersionAlreadyCurrentType {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("Ramping version %v is already current", version))
	} else if failure != nil {
		// TODO: is there an easy way to recover the original type here?
		return nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return nil, serviceerror.NewInternal("outcome missing success and failure")
	}

	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (d *ClientImpl) DeleteWorkerDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	version string,
) (retErr error) {
	v, err := worker_versioning.WorkerDeploymentVersionFromString(version)
	if err != nil {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("invalid version string %q, expected format is \"<deployment_name>/<build_id>\"", version))
	}
	deploymentName := v.GetDeploymentName()
	buildId := v.GetBuildId()

	// if version.drained and !version.has_pollers, delete
	//revive:disable-next-line:defer
	defer d.record("DeleteWorkerDeploymentVersion", &retErr, namespaceEntry.Name(), deploymentName, buildId)()
	requestID := uuid.New()
	identity := requestID

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeleteVersionArgs{
		Identity: identity,
		Version: worker_versioning.WorkerDeploymentVersionToString(&deploymentspb.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildId:        buildId,
		}),
	})
	if err != nil {
		return err
	}
	outcome, err := d.updateWithStartWorkerDeployment(
		ctx,
		namespaceEntry,
		deploymentName,
		&updatepb.Request{
			Input: &updatepb.Input{Name: DeleteVersion, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
		identity,
		requestID,
	)
	if err != nil {
		return err
	}

	if failure := outcome.GetFailure(); failure != nil {
		return serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return serviceerror.NewInternal("outcome missing success and failure")
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
	defer d.record("StartWorkerDeployment", &retErr, namespaceEntry.Name(), deploymentName, identity)()

	workflowID := worker_versioning.GenerateDeploymentWorkflowID(deploymentName)

	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  namespaceEntry.Name().String(),
		NamespaceId:    namespaceEntry.ID().String(),
		DeploymentName: deploymentName,
	})
	if err != nil {
		return err
	}

	memo, err := d.buildInitialMemo(deploymentName)
	if err != nil {
		return err
	}

	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                requestID,
		Namespace:                namespaceEntry.Name().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: WorkerDeploymentWorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    input,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		SearchAttributes:         d.buildSearchAttributes(),
		Memo:                     memo,
	}

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
	defer d.record("SyncVersionWorkflowFromWorkerDeployment", &retErr, namespaceEntry.Name(), deploymentName, version, args, identity)()

	versionObj, err := worker_versioning.WorkerDeploymentVersionFromString(version)
	if err != nil {
		return nil, fmt.Errorf("invalid version string: " + err.Error())
	}

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(args)
	if err != nil {
		return nil, err
	}
	outcome, err := d.updateWithStartWorkerDeploymentVersion(
		ctx,
		namespaceEntry,
		deploymentName,
		versionObj.BuildId,
		&updatepb.Request{
			Input: &updatepb.Input{Name: SyncVersionState, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
		identity,
		requestID,
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
	if success == nil {
		return nil, serviceerror.NewInternal("outcome missing success and failure")
	}

	var res deploymentspb.SyncVersionStateResponse
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (d *ClientImpl) DeleteVersionFromWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, version string,
	identity string,
	requestID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.record("DeleteVersionFromWorkerDeployment", &retErr, namespaceEntry.Name(), deploymentName, version, identity)()

	versionObj, err := worker_versioning.WorkerDeploymentVersionFromString(version)
	if err != nil {
		return err
	}

	outcome, err := d.updateWithStartWorkerDeploymentVersion(
		ctx,
		namespaceEntry,
		deploymentName,
		versionObj.BuildId,
		&updatepb.Request{
			Input: &updatepb.Input{Name: DeleteVersion, Args: nil},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
		identity,
		requestID,
	)
	if err != nil {
		return err
	}

	if failure := outcome.GetFailure(); failure != nil {
		return serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return serviceerror.NewInternal("outcome missing success and failure")
	}
	return nil
}

func (d *ClientImpl) updateWithStartWorkerDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	err := validateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = validateVersionWfParams(WorkerDeploymentBuildIDFieldName, buildID, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := worker_versioning.GenerateVersionWorkflowID(deploymentName, buildID)

	now := timestamppb.Now()
	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			Version: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        buildID,
			},
			CreateTime:        now,
			RoutingUpdateTime: now,
			CurrentSinceTime:  nil, // not current
			RampingSinceTime:  nil, // not ramping
			RampPercentage:    0,   // not ramping
			DrainageInfo:      nil, // not draining or drained
			Metadata:          nil, // todo
		},
	})
	if err != nil {
		return nil, err
	}

	memo, err := d.buildInitialVersionMemo(deploymentName, buildID)
	if err != nil {
		return nil, err
	}

	return d.updateWithStart(
		ctx,
		namespaceEntry,
		WorkerDeploymentVersionWorkflowType,
		workflowID,
		memo,
		input,
		updateRequest,
		identity,
		requestID,
	)
}

func (d *ClientImpl) updateWithStartWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	// validate params which are used for building workflowIDs
	err := validateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := worker_versioning.GenerateDeploymentWorkflowID(deploymentName)
	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  namespaceEntry.Name().String(),
		NamespaceId:    namespaceEntry.ID().String(),
		DeploymentName: deploymentName,
	})
	if err != nil {
		return nil, err
	}

	memo, err := d.buildInitialMemo(deploymentName)
	if err != nil {
		return nil, err
	}

	return d.updateWithStart(
		ctx,
		namespaceEntry,
		WorkerDeploymentWorkflowType,
		workflowID,
		memo,
		input,
		updateRequest,
		identity,
		requestID,
	)
}

func (d *ClientImpl) AddVersionToWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	version string,
	identity string,
	requestID string,
) (*deploymentspb.AddVersionToWorkerDeploymentResponse, error) {
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(version)
	if err != nil {
		return nil, err
	}

	updateRequest := &updatepb.Request{
		Input: &updatepb.Input{Name: AddVersionToWorkerDeployment, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
	}

	workflowID := worker_versioning.GenerateDeploymentWorkflowID(deploymentName)

	outcome, err := d.updateWithStart(
		ctx,
		namespaceEntry,
		WorkerDeploymentWorkflowType,
		workflowID,
		nil,
		nil,
		updateRequest,
		identity,
		requestID,
	)
	if err != nil {
		return nil, err
	}

	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errVersionAlreadyExistsType {
		// pretend this is a success
		return &deploymentspb.AddVersionToWorkerDeploymentResponse{}, nil
	} else if failure != nil {
		// TODO: is there an easy way to recover the original type here?
		return nil, serviceerror.NewInternal(fmt.Sprintf("failed to add version %v to worker deployment %v with error %v", version, deploymentName, failure.Message))
	}

	success := outcome.GetSuccess()
	if success == nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("outcome missing success and failure while adding version %v to worker deployment %v", version, deploymentName))
	}

	return &deploymentspb.AddVersionToWorkerDeploymentResponse{}, nil
}

func (d *ClientImpl) updateWithStart(
	ctx context.Context,
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
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                requestID,
		Namespace:                namespaceEntry.Name().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    input,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		SearchAttributes:         d.buildSearchAttributes(),
		Memo:                     memo,
		Identity:                 identity,
	}

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

	policy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	isRetryable := func(err error) bool {
		return errors.Is(err, errRetry)
	}
	var outcome *updatepb.Outcome

	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		// historyClient retries internally on retryable rpc errors, we just have to retry on
		// successful but un-completed responses.
		res, err := d.historyClient.ExecuteMultiOperation(ctx, multiOpReq)
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
		} else if updateRes == nil {
			return serviceerror.NewInternal("failed to update deployment workflow")
		}

		if updateRes.Stage != enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED {
			// update not completed, try again
			return errRetry
		}

		outcome = updateRes.GetOutcome()
		return nil
	}, policy, isRetryable)

	return outcome, err
}

// TODO (Shivam): Verify if memo needs changes.
func (d *ClientImpl) buildInitialVersionMemo(deploymentName, buildID string) (*commonpb.Memo, error) {
	pl, err := sdk.PreferProtoDataConverter.ToPayload(&deploymentspb.VersionWorkflowMemo{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	})
	if err != nil {
		return nil, err
	}

	return &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			WorkerDeploymentMemoField: pl,
		},
	}, nil
}

func (d *ClientImpl) buildInitialMemo(deploymentName string) (*commonpb.Memo, error) {
	pl, err := sdk.PreferProtoDataConverter.ToPayload(&deploymentspb.WorkerDeploymentWorkflowMemo{
		DeploymentName: deploymentName,
		CreateTime:     timestamppb.Now(),
		RoutingConfig:  &deploymentpb.RoutingConfig{},
	})
	if err != nil {
		return nil, err
	}

	return &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			WorkerDeploymentMemoField: pl,
		},
	}, nil
}

func (d *ClientImpl) buildSearchAttributes() *commonpb.SearchAttributes {
	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(WorkerDeploymentNamespaceDivision))
	return sa
}

func (d *ClientImpl) record(operation string, retErr *error, args ...any) func() {
	start := time.Now()
	return func() {
		elapsed := time.Since(start)

		// TODO: add metrics recording here

		if *retErr != nil {
			d.logger.Error("deployment client error",
				tag.Error(*retErr),
				tag.Operation(operation),
				tag.NewDurationTag("elapsed", elapsed),
				tag.NewAnyTag("args", args),
			)
		} else {
			d.logger.Debug("deployment client success",
				tag.Operation(operation),
				tag.NewDurationTag("elapsed", elapsed),
				tag.NewAnyTag("args", args),
			)
		}
	}
}

//nolint:staticcheck
func versionStateToVersionInfo(state *deploymentspb.VersionLocalState) *deploymentpb.WorkerDeploymentVersionInfo {
	if state == nil {
		return nil
	}

	taskQueues := make([]*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo, 0, len(state.TaskQueueFamilies)*2)
	for taskQueueName, taskQueueFamilyInfo := range state.TaskQueueFamilies {
		for taskQueueType := range taskQueueFamilyInfo.TaskQueues {
			element := &deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{
				Name: taskQueueName,
				Type: enumspb.TaskQueueType(taskQueueType),
				// TODO (Shivam): Add fields here as needed.
			}
			taskQueues = append(taskQueues, element)
		}
	}

	// TODO (Shivam): Add metadata and aggregated pollers status
	return &deploymentpb.WorkerDeploymentVersionInfo{
		Version:            worker_versioning.WorkerDeploymentVersionToString(state.Version),
		CreateTime:         state.CreateTime,
		RoutingChangedTime: state.RoutingUpdateTime,
		CurrentSinceTime:   state.CurrentSinceTime,
		RampingSinceTime:   state.RampingSinceTime,
		RampPercentage:     state.RampPercentage,
		TaskQueueInfos:     taskQueues,
		DrainageInfo:       state.DrainageInfo,
		Metadata:           state.Metadata,
	}
}

func (d *ClientImpl) deploymentStateToDeploymentInfo(ctx context.Context, namespaceEntry *namespace.Namespace,
	deploymentName string, state *deploymentspb.WorkerDeploymentLocalState) (*deploymentpb.WorkerDeploymentInfo, error) {
	if state == nil {
		return nil, nil
	}

	var workerDeploymentInfo deploymentpb.WorkerDeploymentInfo
	workerDeploymentInfo.Name = deploymentName
	workerDeploymentInfo.CreateTime = state.CreateTime

	workerDeploymentInfo.RoutingConfig = state.RoutingConfig

	for _, version := range state.Versions {
		versionInfo, err := d.DescribeVersion(ctx, namespaceEntry, version)
		if err != nil {
			return nil, err
		}
		workerDeploymentInfo.VersionSummaries = append(workerDeploymentInfo.VersionSummaries, &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
			Version:        versionInfo.Version,
			CreateTime:     versionInfo.CreateTime,
			DrainageStatus: versionInfo.GetDrainageInfo().GetStatus(),
		})
	}

	return &workerDeploymentInfo, nil
}

func (d *ClientImpl) GetVersionDrainageStatus(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string) (enumspb.VersionDrainageStatus, error) {
	countRequest := manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespaceEntry.ID(),
		Namespace:   namespaceEntry.Name(),
		Query:       makeDeploymentQuery(deploymentName, buildID),
	}
	countResponse, err := d.visibilityManager.CountWorkflowExecutions(ctx, &countRequest)
	if err != nil {
		return enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED, err
	}
	if countResponse.Count == 0 {
		return enumspb.VERSION_DRAINAGE_STATUS_DRAINED, nil
	}
	return enumspb.VERSION_DRAINAGE_STATUS_DRAINING, nil
}

func makeDeploymentQuery(deploymentName, buildID string) string {
	var statusFilter string
	deploymentFilter := fmt.Sprintf("= '%s'", worker_versioning.PinnedBuildIdSearchAttribute(&deploymentpb.Deployment{
		SeriesName: deploymentName,
		BuildId:    buildID,
	}))
	statusFilter = "= 'Running'"
	return fmt.Sprintf("%s %s AND %s %s", searchattribute.BuildIds, deploymentFilter, searchattribute.ExecutionStatus, statusFilter)
}
