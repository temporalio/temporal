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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client interface {
	RegisterTaskQueueWorker(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, version string,
		taskQueueName string,
		taskQueueType enumspb.TaskQueueType,
		firstPoll time.Time,
		identity string,
		requestID string,
	) error

	// TODO (Shivam) -
	// Add ListWorkerDeployment + DescribeWorkerDeployment + DeleteWorkerDeployment + DeleteDeploymentVersion

	DescribeVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		version string,
	) (*deploymentpb.WorkerDeploymentVersionInfo, error)

	DescribeWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
	) (*deploymentpb.WorkerDeploymentInfo, error)

	SetCurrentVersion(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		updateMetadata *deploymentpb.UpdateDeploymentMetadata,
		identity string,
		requestID string,
	) (*deploymentpb.WorkerDeploymentVersionInfo, *deploymentpb.WorkerDeploymentVersionInfo, error)

	// Only used internally by Worker Deployment Version workflow:
	StartWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		identity string,
		requestID string,
	) error

	// Only used internally by WorkerDeployment workflow:
	SyncVersionWorkflowFromWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName, version string,
		args *deploymentspb.SyncVersionStateArgs,
		identity string,
		requestID string,
	) (*deploymentspb.SyncVersionStateResponse, error)

	// Only used internally by Worker Deployment Version workflow:
	AddVersionToWorkerDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deploymentName string,
		version string,
		identity string,
		requestID string,
	) (*deploymentspb.AddVersionToWorkerDeploymentResponse, error)
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
	reachabilityCache         reachabilityCache
}

var _ Client = (*ClientImpl)(nil)

var errRetry = errors.New("retry update")

func (d *ClientImpl) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, version string,
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

	outcome, err := d.updateWithStartWorkerDeploymentVersion(ctx, namespaceEntry, deploymentName, version, &updatepb.Request{
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
	//revive:disable-next-line:defer
	defer d.record("DescribeVersion", &retErr, version)()

	// validating params
	err := ValidateVersionWfParams(WorkerDeploymentVersionFieldName, version, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	deploymentWorkflowID := GenerateVersionWorkflowID(version)

	req := &historyservice.QueryWorkflowRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Namespace: namespaceEntry.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: deploymentWorkflowID,
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

	return stateToInfo(queryResponse.VersionState), nil
}

func (d *ClientImpl) DescribeWorkerDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
) (_ *deploymentpb.WorkerDeploymentInfo, retErr error) {
	defer d.record("DescribeWorkerDeployment", &retErr, deploymentName)()

	// validating params
	err := ValidateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	deploymentWorkflowID := GenerateWorkflowID(deploymentName)

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

	// TODO (Shivam) - StatetoInfo type thing here as well?
	var workerDeploymentInfo deploymentpb.WorkerDeploymentInfo
	workerDeploymentInfo.Name = deploymentName
	workerDeploymentInfo.CreateTime = queryResponse.State.CreateTime
	for _, version := range queryResponse.State.Versions {
		versionInfo, err := d.DescribeVersion(ctx, namespaceEntry, version)
		if err != nil {
			return nil, err
		}
		// TODO (Shivam) - Add WorkflowVersioningMode + AcceptsNewExecutions
		workerDeploymentInfo.VersionSummaries = append(workerDeploymentInfo.VersionSummaries, &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
			Version:    versionInfo.Version,
			CreateTime: versionInfo.CreateTime,
		})
	}
	// TODO (Shivam) - LastEditorIdentity will be the latest client that updated the deployment version. Will be implemented after setCurrentVersion is implemented.

	return &workerDeploymentInfo, nil
}

func (d *ClientImpl) GetCurrentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
) (_ *deploymentpb.WorkerDeploymentVersionInfo, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("GetCurrentDeployment", &retErr, deploymentName)()

	// Validating params
	err := ValidateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateWorkflowID(deploymentName)
	resp, err := d.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespaceEntry.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
		},
	})

	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, err
	}

	// Decode value from memo
	val := resp.WorkflowExecutionInfo.GetMemo().GetFields()[WorkerDeploymentMemoField]
	if val == nil {
		// memo missing, series has no set current deployment
		return nil, nil
	}
	var memo deploymentspb.WorkerDeploymentWorkflowMemo
	err = sdk.PreferProtoDataConverter.FromPayload(val, &memo)
	if err != nil {
		return nil, err
	}

	// Series has no set current deployment
	if memo.CurrentVersion == "" {
		return nil, nil
	}

	deploymentInfo, err := d.DescribeVersion(ctx, namespaceEntry, memo.CurrentVersion)
	if err != nil {
		return nil, nil
	}

	return deploymentInfo, nil
}

func (d *ClientImpl) SetCurrentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName string,
	version string,
	updateMetadata *deploymentpb.UpdateDeploymentMetadata,
	identity string,
	requestID string,
) (_ *deploymentpb.WorkerDeploymentVersionInfo, _ *deploymentpb.WorkerDeploymentVersionInfo, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("SetCurrentVersion", &retErr, namespaceEntry.Name(), deploymentName, version, identity)()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetCurrentVersionArgs{
		Identity:  identity,
		Version:   version,
		RequestId: requestID,
	})
	if err != nil {
		return nil, nil, err
	}
	outcome, err := d.updateWithStartWorkerDeployment(
		ctx,
		namespaceEntry,
		deploymentName,
		&updatepb.Request{
			Input: &updatepb.Input{Name: SetCurrentVersion, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
		},
		identity,
		requestID,
	)
	if err != nil {
		return nil, nil, err
	}

	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		return nil, nil, serviceerror.NewAlreadyExist(fmt.Sprintf("Build ID %q is already current for %q",
			version, deploymentName))
	} else if failure != nil {
		// TODO: is there an easy way to recover the original type here?
		return nil, nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return nil, nil, serviceerror.NewInternal("outcome missing success and failure")
	}

	var res deploymentspb.SetCurrentVersionResponse
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, nil, err
	}
	return stateToInfo(nil), stateToInfo(nil), nil
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

	workflowID := GenerateWorkflowID(deploymentName)

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
	args *deploymentspb.SyncVersionStateArgs,
	identity string,
	requestID string,
) (_ *deploymentspb.SyncVersionStateResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("SyncVersionWorkflowFromWorkerDeployment", &retErr, namespaceEntry.Name(), deploymentName, version, args, identity)()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(args)
	if err != nil {
		return nil, err
	}
	outcome, err := d.updateWithStartWorkerDeploymentVersion(
		ctx,
		namespaceEntry,
		deploymentName,
		version,
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

func (d *ClientImpl) updateWithStartWorkerDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, version string,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	// validate params which are used for building workflowIDs
	err := ValidateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = ValidateVersionWfParams(WorkerDeploymentVersionFieldName, version, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateVersionWorkflowID(version)

	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.WorkerDeploymentVersionWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		VersionState: &deploymentspb.VersionLocalState{
			DeploymentName: deploymentName,
			Version:        version,
			CreateTime:     timestamppb.Now(),
		},
	})
	if err != nil {
		return nil, err
	}

	memo, err := d.buildInitialVersionMemo(deploymentName, version)
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
	err := ValidateVersionWfParams(WorkerDeploymentFieldName, deploymentName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateWorkflowID(deploymentName)
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

	workflowID := GenerateWorkflowID(deploymentName)

	outcome, err := d.updateWithStart(
		ctx,
		namespaceEntry,
		WorkerDeploymentVersionWorkflowType,
		workflowID,
		nil, // todo: (Shivam) - add memo
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
		return nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return nil, serviceerror.NewInternal("outcome missing success and failure")
	}

	// todo (Shivam): Do we really need to return this response?
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

func (d *ClientImpl) buildInitialVersionMemo(deploymentName, version string) (*commonpb.Memo, error) {
	pl, err := sdk.PreferProtoDataConverter.ToPayload(&deploymentspb.VersionWorkflowMemo{
		DeploymentName:      deploymentName,
		Version:             version,
		CreateTime:          timestamppb.Now(),
		IsCurrentDeployment: false,
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
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(WorkerDeploymentVersionNamespaceDivision))
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
func stateToInfo(state *deploymentspb.VersionLocalState) *deploymentpb.WorkerDeploymentVersionInfo {
	if state == nil {
		return nil
	}

	taskQueues := make([]*deploymentpb.WorkerDeploymentVersionInfo_DeploymentVersionTaskQueueInfo, 0, len(state.TaskQueueFamilies)*2)
	for taskQueueName, taskQueueFamilyInfo := range state.TaskQueueFamilies {
		for taskQueueType := range taskQueueFamilyInfo.TaskQueues {
			element := &deploymentpb.WorkerDeploymentVersionInfo_DeploymentVersionTaskQueueInfo{
				Name: taskQueueName,
				Type: enumspb.TaskQueueType(taskQueueType),
				// TODO (Shivam): Add fields here as needed.
			}
			taskQueues = append(taskQueues, element)
		}
	}

	// TODO (Shivam): Add metadata and aggregated pollers status
	return &deploymentpb.WorkerDeploymentVersionInfo{
		DeploymentName: state.DeploymentName,
		Version:        state.Version,
		CreateTime:     state.CreateTime,
		TaskQueueInfos: taskQueues,
	}
}
