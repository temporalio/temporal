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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
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
		identity string,
		updateMetadata *deploymentpb.UpdateDeploymentMetadata,
	) (*deploymentpb.DeploymentInfo, *deploymentpb.DeploymentInfo, error)
}

// implements DeploymentStoreClient
type DeploymentClientImpl struct {
	HistoryClient             historyservice.HistoryServiceClient
	VisibilityManager         manager.VisibilityManager
	MaxIDLengthLimit          dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize     dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxTaskQueuesInDeployment dynamicconfig.IntPropertyFnWithNamespaceFilter
	reachabilityCache         reachabilityCache
}

var _ DeploymentStoreClient = (*DeploymentClientImpl)(nil)

func (d *DeploymentClientImpl) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploymentpb.Deployment,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	firstPoll time.Time,
) error {
	updatePayload, err := d.generateRegisterWorkerInDeploymentArgs(namespaceEntry, taskQueueName, taskQueueType, firstPoll)
	if err != nil {
		return err
	}
	_, err = d.updateWithStartDeployment(ctx, namespaceEntry, deployment, &updatepb.Request{
		Input: &updatepb.Input{Name: RegisterWorkerInDeployment, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: uuid.New(), Identity: "deploymentClient"},
	})
	return err
}

func (d *DeploymentClientImpl) DescribeDeployment(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, buildID string) (*deploymentpb.DeploymentInfo, error) {
	// validating params
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, d.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, buildID, d.MaxIDLengthLimit())
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

	res, err := d.HistoryClient.QueryWorkflow(ctx, req)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, serviceerror.NewNotFound("Deployment not found")
		}
		return nil, err
	}

	var queryResponse deploymentspb.QueryDescribeDeploymentResponse
	err = payloads.Decode(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, err
	}

	// build out task-queues for the response object
	var taskQueues []*deploymentpb.DeploymentInfo_TaskQueueInfo
	deploymentLocalState := queryResponse.DeploymentLocalState

	for taskQueueName, taskQueueFamilyInfo := range deploymentLocalState.TaskQueueFamilies {
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
		Deployment:     deploymentLocalState.WorkerDeployment,
		CreateTime:     deploymentLocalState.CreateTime,
		TaskQueueInfos: taskQueues,
		Metadata:       deploymentLocalState.Metadata,
		IsCurrent:      deploymentLocalState.IsCurrent,
	}, nil
}

// TODO (carly): pass deployment instead of seriesName + buildId in all these APIs -- separate PR
func (d *DeploymentClientImpl) GetDeploymentReachability(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	seriesName string,
	buildID string,
) (*workflowservice.GetDeploymentReachabilityResponse, error) {
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

func (d *DeploymentClientImpl) GetCurrentDeployment(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string) (*deploymentpb.DeploymentInfo, error) {
	// Validating params
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, d.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := GenerateDeploymentSeriesWorkflowID(seriesName)
	resp, err := d.HistoryClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
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
	var buildID string
	val := resp.WorkflowExecutionInfo.Memo.Fields[DeploymentSeriesBuildIDMemoField]
	err = sdk.PreferProtoDataConverter.FromPayload(val, &buildID)
	if err != nil {
		return nil, err
	}

	// Series has no set current deployment
	if buildID == "" {
		return nil, nil
	}

	deploymentInfo, err := d.DescribeDeployment(ctx, namespaceEntry, seriesName, buildID)
	if err != nil {
		return nil, nil
	}

	return deploymentInfo, nil
}

func (d *DeploymentClientImpl) ListDeployments(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, NextPageToken []byte) ([]*deploymentpb.DeploymentListInfo, []byte, error) {
	query := ""
	if seriesName != "" {
		query = BuildQueryWithSeriesFilter(seriesName)
	} else {
		query = DeploymentVisibilityBaseListQuery
	}

	persistenceResp, err := d.VisibilityManager.ListWorkflowExecutions(
		ctx,
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceEntry.ID(),
			Namespace:     namespaceEntry.Name(),
			PageSize:      d.VisibilityMaxPageSize(namespaceEntry.Name().String()),
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
	identity string,
	updateMetadata *deploymentpb.UpdateDeploymentMetadata,
) (*deploymentpb.DeploymentInfo, *deploymentpb.DeploymentInfo, error) {
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.SetCurrentDeploymentArgs{
		Identity:       identity,
		UpdateMetadata: updateMetadata,
	})
	outcome, err := d.updateWithStartDeployment(ctx, namespaceEntry, deployment, &updatepb.Request{
		Input: &updatepb.Input{Name: SetCurrentDeployment, Args: updatePayload},
		Meta:  &updatepb.Meta{UpdateId: uuid.New(), Identity: identity},
	})
	if err != nil {
		return nil, nil, err
	}
	if failure := outcome.GetFailure(); failure != nil {
		// FIXME: use the correct error type
		return nil, nil, serviceerror.NewInternal(failure.Message)
	}
	success := outcome.GetSuccess()
	if success == nil {
		return nil, nil, serviceerror.NewInternal("outcome missing success and failure")
	}

	var res deploymentspb.SetCurrentDeploymentResponse
	if err := payloads.Decode(success, &res); err != nil {
		return nil, nil, err
	}
	return res.CurrentDeploymentInfo, res.PreviousDeploymentInfo, nil
}

func (d *DeploymentClientImpl) updateWithStartDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploymentpb.Deployment,
	updateRequest *updatepb.Request,
) (*updatepb.Outcome, error) {
	// validate params which are used for building workflowID's
	err := ValidateDeploymentWfParams(SeriesFieldName, deployment.SeriesName, d.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, deployment.BuildId, d.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	deploymentWorkflowID := GenerateDeploymentWorkflowID(deployment.SeriesName, deployment.BuildId)
	workflowInputPayloads, err := d.generateStartWorkflowPayload(namespaceEntry, deployment)
	if err != nil {
		return nil, err
	}

	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))

	// initial memo fiels
	memo, err := d.buildInitialDeploymentMemo(deployment)
	if err != nil {
		return nil, err
	}

	// Start workflow execution, if it hasn't already
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.New(),
		Namespace:                namespaceEntry.Name().String(),
		WorkflowId:               deploymentWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: DeploymentWorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    workflowInputPayloads,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		SearchAttributes:         sa,
		Memo:                     memo,
	}

	updateReq := &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: namespaceEntry.Name().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: deploymentWorkflowID,
		},
		Request:    updateRequest,
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
	}

	// This is an atomic operation; if one operation fails, both will.
	// FIXME: retry this whole thing until error or update is durable. see
	// https://github.com/temporalio/sdk-go/blob/08d52ce3d7/internal/internal_workflow_client.go#L1800
	res, err := d.HistoryClient.ExecuteMultiOperation(ctx, &historyservice.ExecuteMultiOperationRequest{
		NamespaceId: namespaceEntry.ID().String(),
		WorkflowId:  deploymentWorkflowID,
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
	})
	if err != nil {
		return nil, err
	}

	// FIXME: handle the various places we can get errors
	updateRes := res.Responses[1].GetUpdateWorkflow().GetResponse()
	outcome := updateRes.GetOutcome()
	return outcome, nil

}

// GenerateStartWorkflowPayload generates start workflow execution payload
func (d *DeploymentClientImpl) generateStartWorkflowPayload(namespaceEntry *namespace.Namespace, deployment *deploymentpb.Deployment) (*commonpb.Payloads, error) {
	workflowArgs := &deploymentspb.DeploymentWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		State: &deploymentspb.DeploymentLocalState{
			WorkerDeployment: deployment,
			CreateTime:       timestamppb.Now(),
		},
	}
	return sdk.PreferProtoDataConverter.ToPayloads(workflowArgs)
}

// GenerateUpdateDeploymentPayload generates update workflow payload
func (d *DeploymentClientImpl) generateRegisterWorkerInDeploymentArgs(
	namespaceEntry *namespace.Namespace,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	firstPoll time.Time,
) (*commonpb.Payloads, error) {
	updateArgs := &deploymentspb.RegisterWorkerInDeploymentArgs{
		TaskQueueName:   taskQueueName,
		TaskQueueType:   taskQueueType,
		FirstPollerTime: timestamppb.New(firstPoll),
		MaxTaskQueues:   int32(d.MaxTaskQueuesInDeployment(namespaceEntry.Name().String())),
	}
	return sdk.PreferProtoDataConverter.ToPayloads(updateArgs)
}

func (d *DeploymentClientImpl) buildInitialDeploymentMemo(deployment *deploymentpb.Deployment) (*commonpb.Memo, error) {
	deploymentWorkflowMemo := &deploymentspb.DeploymentWorkflowMemo{
		Deployment:          deployment,
		CreateTime:          timestamppb.Now(),
		IsCurrentDeployment: false,
	}

	memoPayload, err := sdk.PreferProtoDataConverter.ToPayload(deploymentWorkflowMemo)
	if err != nil {
		return nil, err
	}

	return &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			DeploymentMemoField: memoPayload,
		},
	}, nil
}
