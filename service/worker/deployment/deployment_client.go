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

package deployment

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

type DeploymentStoreClient interface {
	RegisterTaskQueueWorker(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deployment *deploymentpb.Deployment,
		taskQueueName string,
		taskQueueType enumspb.TaskQueueType,
		firstPoll time.Time,
		identity string,
		requestID string,
	) error

	DescribeDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		buildID string,
	) (*deploymentpb.DeploymentInfo, error)

	GetCurrentDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
	) (*deploymentpb.DeploymentInfo, error)

	ListDeployments(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		nextPageToken []byte,
	) ([]*deploymentpb.DeploymentListInfo, []byte, error)

	GetDeploymentReachability(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		buildID string,
	) (*workflowservice.GetDeploymentReachabilityResponse, error)

	SetCurrentDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deployment *deploymentpb.Deployment,
		updateMetadata *deploymentpb.UpdateDeploymentMetadata,
		identity string,
		requestID string,
	) (*deploymentpb.DeploymentInfo, *deploymentpb.DeploymentInfo, error)

	// Only used internally by deployment workflow:
	StartDeploymentSeries(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		identity string,
		requestID string,
	) error

	// Only used internally by deployment series workflow:
	SyncDeploymentWorkflowFromSeries(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		deployment *deploymentpb.Deployment,
		args *deploymentspb.SyncDeploymentStateArgs,
		identity string,
		requestID string,
	) (*deploymentspb.SyncDeploymentStateResponse, error)
}

type ErrMaxTaskQueuesInDeployment struct{ error }

type ErrRegister struct{ error }

// implements DeploymentStoreClient
type DeploymentClientImpl struct {
	logger                    log.Logger
	historyClient             historyservice.HistoryServiceClient
	visibilityManager         manager.VisibilityManager
	maxIDLengthLimit          dynamicconfig.IntPropertyFn
	visibilityMaxPageSize     dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxTaskQueuesInDeployment dynamicconfig.IntPropertyFnWithNamespaceFilter
	reachabilityCache         reachabilityCache
}

var _ DeploymentStoreClient = (*DeploymentClientImpl)(nil)

var errRetry = errors.New("retry update")

func (d *DeploymentClientImpl) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploymentpb.Deployment,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	firstPoll time.Time,
	identity string,
	requestID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.record("RegisterTaskQueueWorker", &retErr, taskQueueName, taskQueueType, identity)()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.RegisterWorkerInDeploymentArgs{
		TaskQueueName:   taskQueueName,
		TaskQueueType:   taskQueueType,
		FirstPollerTime: timestamppb.New(firstPoll),
		MaxTaskQueues:   int32(d.maxTaskQueuesInDeployment(namespaceEntry.Name().String())),
	})
	if err != nil {
		return err
	}

	outcome, err := d.updateWithStartDeployment(ctx, namespaceEntry, deployment, &updatepb.Request{
		Input: &updatepb.Input{Name: RegisterWorkerInDeployment, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: requestID, Identity: identity},
	}, identity, requestID)
	if err != nil {
		return err
	}

	if failure := outcome.GetFailure(); failure.GetApplicationFailureInfo().GetType() == errMaxTaskQueuesInDeploymentType {
		// translate to client-side error type
		return ErrMaxTaskQueuesInDeployment{error: errors.New(failure.Message)}
	} else if failure.GetApplicationFailureInfo().GetType() == errNoChangeType {
		return nil
	} else if failure != nil {
		return ErrRegister{error: errors.New(failure.Message)}
	}

	return nil
}

func (d *DeploymentClientImpl) DescribeDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
	buildID string,
) (_ *deploymentpb.DeploymentInfo, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("DescribeDeployment", &retErr, seriesName, buildID)()

	// validating params
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, buildID, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	deploymentWorkflowID := GenerateDeploymentWorkflowID(seriesName, buildID)

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
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, serviceerror.NewNotFound("Deployment not found")
		}
		return nil, err
	}

	var queryResponse deploymentspb.QueryDescribeDeploymentResponse
	err = sdk.PreferProtoDataConverter.FromPayloads(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, err
	}

	return stateToInfo(queryResponse.DeploymentLocalState), nil
}

// TODO (carly): pass deployment instead of seriesName + buildId in all these APIs -- separate PR
func (d *DeploymentClientImpl) GetDeploymentReachability(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
	buildID string,
) (_ *workflowservice.GetDeploymentReachabilityResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("GetDeploymentReachability", &retErr, seriesName, buildID)()

	deployInfo, err := d.DescribeDeployment(ctx, namespaceEntry, seriesName, buildID)
	if err != nil {
		return nil, err
	}
	reachability, lastUpdateTime, err := getDeploymentReachability(
		ctx,
		namespaceEntry.ID().String(),
		namespaceEntry.Name().String(),
		seriesName,
		buildID,
		deployInfo.GetIsCurrent(),
		d.reachabilityCache,
	)

	if err != nil {
		return nil, err
	}

	return &workflowservice.GetDeploymentReachabilityResponse{
		DeploymentInfo: deployInfo,
		Reachability:   reachability,
		LastUpdateTime: timestamppb.New(lastUpdateTime),
	}, nil
}

func (d *DeploymentClientImpl) GetCurrentDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
) (_ *deploymentpb.DeploymentInfo, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("GetCurrentDeployment", &retErr, seriesName)()

	// Validating params
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateDeploymentSeriesWorkflowID(seriesName)
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
	val := resp.WorkflowExecutionInfo.GetMemo().GetFields()[DeploymentSeriesMemoField]
	if val == nil {
		// memo missing, series has no set current deployment
		return nil, nil
	}
	var memo deploymentspb.DeploymentSeriesWorkflowMemo
	err = sdk.PreferProtoDataConverter.FromPayload(val, &memo)
	if err != nil {
		return nil, err
	}

	// Series has no set current deployment
	if memo.CurrentBuildId == "" {
		return nil, nil
	}

	deploymentInfo, err := d.DescribeDeployment(ctx, namespaceEntry, seriesName, memo.CurrentBuildId)
	if err != nil {
		return nil, nil
	}

	return deploymentInfo, nil
}

func (d *DeploymentClientImpl) ListDeployments(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
	NextPageToken []byte,
) (_ []*deploymentpb.DeploymentListInfo, _ []byte, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("ListDeployments", &retErr, seriesName)()

	query := ""
	if seriesName != "" {
		query = BuildQueryWithSeriesFilter(seriesName)
	} else {
		query = DeploymentVisibilityBaseListQuery
	}

	persistenceResp, err := d.visibilityManager.ListWorkflowExecutions(
		ctx,
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceEntry.ID(),
			Namespace:     namespaceEntry.Name(),
			PageSize:      d.visibilityMaxPageSize(namespaceEntry.Name().String()),
			NextPageToken: NextPageToken,
			Query:         query,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	deployments := make([]*deploymentpb.DeploymentListInfo, 0)
	for _, ex := range persistenceResp.Executions {
		workflowMemo := DecodeDeploymentMemo(ex.GetMemo())

		deploymentListInfo := &deploymentpb.DeploymentListInfo{
			Deployment: workflowMemo.Deployment,
			CreateTime: workflowMemo.CreateTime,
			IsCurrent:  workflowMemo.IsCurrentDeployment,
		}
		deployments = append(deployments, deploymentListInfo)
	}

	return deployments, NextPageToken, nil

}

func (d *DeploymentClientImpl) SetCurrentDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploymentpb.Deployment,
	updateMetadata *deploymentpb.UpdateDeploymentMetadata,
	identity string,
	requestID string,
) (_ *deploymentpb.DeploymentInfo, _ *deploymentpb.DeploymentInfo, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("SetCurrentDeployment", &retErr, namespaceEntry.Name(), deployment, identity)()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetCurrentDeploymentArgs{
		Identity:       identity,
		BuildId:        deployment.BuildId,
		UpdateMetadata: updateMetadata,
		RequestId:      requestID,
	})
	if err != nil {
		return nil, nil, err
	}
	outcome, err := d.updateWithStartDeploymentSeries(
		ctx,
		namespaceEntry,
		deployment.SeriesName,
		&updatepb.Request{
			Input: &updatepb.Input{Name: SetCurrentDeployment, Args: updatePayload},
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
			deployment.BuildId, deployment.SeriesName))
	} else if failure != nil {
		// TODO: is there an easy way to recover the original type here?
		return nil, nil, serviceerror.NewInternal(failure.Message)
	}

	success := outcome.GetSuccess()
	if success == nil {
		return nil, nil, serviceerror.NewInternal("outcome missing success and failure")
	}

	var res deploymentspb.SetCurrentDeploymentResponse
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, nil, err
	}
	return stateToInfo(res.CurrentDeploymentState), stateToInfo(res.PreviousDeploymentState), nil
}

func (d *DeploymentClientImpl) StartDeploymentSeries(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
	identity string,
	requestID string,
) (retErr error) {
	//revive:disable-next-line:defer
	defer d.record("StartDeploymentSeries", &retErr, namespaceEntry.Name(), seriesName, identity)()

	workflowID := GenerateDeploymentSeriesWorkflowID(seriesName)

	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeploymentSeriesWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		SeriesName:    seriesName,
	})
	if err != nil {
		return err
	}

	memo, err := d.buildInitialDeploymentSeriesMemo(seriesName)
	if err != nil {
		return err
	}

	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                requestID,
		Namespace:                namespaceEntry.Name().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: DeploymentSeriesWorkflowType},
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

func (d *DeploymentClientImpl) SyncDeploymentWorkflowFromSeries(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploymentpb.Deployment,
	args *deploymentspb.SyncDeploymentStateArgs,
	identity string,
	requestID string,
) (_ *deploymentspb.SyncDeploymentStateResponse, retErr error) {
	//revive:disable-next-line:defer
	defer d.record("SyncDeploymentWorkflowFromSeries", &retErr, namespaceEntry.Name(), deployment, args, identity)()

	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(args)
	if err != nil {
		return nil, err
	}
	outcome, err := d.updateWithStartDeployment(
		ctx,
		namespaceEntry,
		deployment,
		&updatepb.Request{
			Input: &updatepb.Input{Name: SyncDeploymentState, Args: updatePayload},
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

	var res deploymentspb.SyncDeploymentStateResponse
	if err := sdk.PreferProtoDataConverter.FromPayloads(success, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (d *DeploymentClientImpl) updateWithStartDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploymentpb.Deployment,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	// validate params which are used for building workflowIDs
	err := ValidateDeploymentWfParams(SeriesFieldName, deployment.SeriesName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, deployment.BuildId, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateDeploymentWorkflowID(deployment.SeriesName, deployment.BuildId)

	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeploymentWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		State: &deploymentspb.DeploymentLocalState{
			Deployment: deployment,
			CreateTime: timestamppb.Now(),
		},
	})
	if err != nil {
		return nil, err
	}

	memo, err := d.buildInitialDeploymentMemo(deployment)
	if err != nil {
		return nil, err
	}

	return d.updateWithStart(
		ctx,
		namespaceEntry,
		DeploymentWorkflowType,
		workflowID,
		memo,
		input,
		updateRequest,
		identity,
		requestID,
	)
}

func (d *DeploymentClientImpl) updateWithStartDeploymentSeries(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
	updateRequest *updatepb.Request,
	identity string,
	requestID string,
) (*updatepb.Outcome, error) {
	// validate params which are used for building workflowIDs
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, d.maxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateDeploymentSeriesWorkflowID(seriesName)
	input, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeploymentSeriesWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		SeriesName:    seriesName,
	})
	if err != nil {
		return nil, err
	}

	memo, err := d.buildInitialDeploymentSeriesMemo(seriesName)
	if err != nil {
		return nil, err
	}

	return d.updateWithStart(
		ctx,
		namespaceEntry,
		DeploymentSeriesWorkflowType,
		workflowID,
		memo,
		input,
		updateRequest,
		identity,
		requestID,
	)
}

func (d *DeploymentClientImpl) updateWithStart(
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

func (d *DeploymentClientImpl) buildInitialDeploymentMemo(deployment *deploymentpb.Deployment) (*commonpb.Memo, error) {
	pl, err := sdk.PreferProtoDataConverter.ToPayload(&deploymentspb.DeploymentWorkflowMemo{
		Deployment:          deployment,
		CreateTime:          timestamppb.Now(),
		IsCurrentDeployment: false,
	})
	if err != nil {
		return nil, err
	}

	return &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			DeploymentMemoField: pl,
		},
	}, nil
}

func (d *DeploymentClientImpl) buildInitialDeploymentSeriesMemo(seriesName string) (*commonpb.Memo, error) {
	pl, err := sdk.PreferProtoDataConverter.ToPayload(&deploymentspb.DeploymentSeriesWorkflowMemo{
		SeriesName: seriesName,
	})
	if err != nil {
		return nil, err
	}

	return &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			DeploymentSeriesMemoField: pl,
		},
	}, nil
}

func (d *DeploymentClientImpl) buildSearchAttributes() *commonpb.SearchAttributes {
	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))
	return sa
}

func (d *DeploymentClientImpl) record(operation string, retErr *error, args ...any) func() {
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

func stateToInfo(state *deploymentspb.DeploymentLocalState) *deploymentpb.DeploymentInfo {
	if state == nil {
		return nil
	}

	taskQueues := make([]*deploymentpb.DeploymentInfo_TaskQueueInfo, 0, len(state.TaskQueueFamilies)*2)
	for taskQueueName, taskQueueFamilyInfo := range state.TaskQueueFamilies {
		for taskQueueType, taskQueueInfo := range taskQueueFamilyInfo.TaskQueues {
			element := &deploymentpb.DeploymentInfo_TaskQueueInfo{
				Name:            taskQueueName,
				Type:            enumspb.TaskQueueType(taskQueueType),
				FirstPollerTime: taskQueueInfo.FirstPollerTime,
			}
			taskQueues = append(taskQueues, element)
		}
	}

	return &deploymentpb.DeploymentInfo{
		Deployment:     state.Deployment,
		CreateTime:     state.CreateTime,
		TaskQueueInfos: taskQueues,
		Metadata:       state.Metadata,
		IsCurrent:      state.IsCurrent,
	}
}
