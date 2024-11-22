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
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploypb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	deployspb "go.temporal.io/server/api/deployment/v1"
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
		deployment *deploypb.Deployment,
		taskQueueName string,
		taskQueueType enumspb.TaskQueueType,
		pollTimestamp *timestamppb.Timestamp,
	) error

	DescribeDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		buildID string,
	) (*deploypb.DeploymentInfo, error)

	GetCurrentDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
	) (*deploypb.DeploymentInfo, error)

	ListDeployments(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		NextPageToken []byte,
	) ([]*deploypb.DeploymentListInfo, []byte, error)
}

// implements DeploymentStoreClient
type DeploymentClientImpl struct {
	HistoryClient         historyservice.HistoryServiceClient
	VisibilityManager     manager.VisibilityManager
	MaxIDLengthLimit      dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (d *DeploymentClientImpl) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploypb.Deployment,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	pollTimestamp *timestamppb.Timestamp,
) error {
	// validate params which are used for building workflowID's
	err := ValidateDeploymentWfParams(SeriesFieldName, deployment.SeriesName, d.MaxIDLengthLimit())
	if err != nil {
		return err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, deployment.BuildId, d.MaxIDLengthLimit())
	if err != nil {
		return err
	}

	deploymentWorkflowID := GenerateDeploymentWorkflowID(deployment.SeriesName, deployment.BuildId)
	workflowInputPayloads, err := d.generateStartWorkflowPayload(namespaceEntry, deployment)
	if err != nil {
		return err
	}
	updatePayload, err := d.generateRegisterWorkerInDeploymentArgs(taskQueueName, taskQueueType, pollTimestamp)
	if err != nil {
		return err
	}

	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))

	// initial memo fiels
	memo, err := d.addInitialDeploymentMemo()
	if err != nil {
		return err
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
		Request: &updatepb.Request{
			Input: &updatepb.Input{Name: RegisterWorkerInDeployment, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: uuid.New(), Identity: "deploymentClient"},
		},
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
	}

	// This is an atomic operation; if one operation fails, both will.
	_, err = d.HistoryClient.ExecuteMultiOperation(ctx, &historyservice.ExecuteMultiOperationRequest{
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
		return err
	}

	return nil
}

func (d *DeploymentClientImpl) DescribeDeployment(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, buildID string) (*deploypb.DeploymentInfo, error) {
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
		return nil, err
	}

	var queryResponse deployspb.DescribeResponse
	err = payloads.Decode(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, err
	}

	// build out task-queues for the response object
	var taskQueues []*deploypb.DeploymentInfo_TaskQueueInfo
	deploymentLocalState := queryResponse.DeploymentLocalState

	for taskQueueName, taskQueueFamilyInfo := range deploymentLocalState.TaskQueueFamilies {
		for _, taskQueueInfo := range taskQueueFamilyInfo.TaskQueues {
			element := &deploypb.DeploymentInfo_TaskQueueInfo{
				Name:            taskQueueName,
				Type:            enumspb.TaskQueueType(taskQueueInfo.TaskQueueType),
				FirstPollerTime: taskQueueInfo.FirstPollerTime,
			}
			taskQueues = append(taskQueues, element)
		}
	}

	return &deploypb.DeploymentInfo{
		Deployment:     deploymentLocalState.WorkerDeployment,
		CreateTime:     deploymentLocalState.CreateTime,
		TaskQueueInfos: taskQueues,
		Metadata:       deploymentLocalState.Metadata,
		IsCurrent:      deploymentLocalState.IsCurrent,
	}, nil
}

func (d *DeploymentClientImpl) GetCurrentDeployment(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string) (*deploypb.DeploymentInfo, error) {

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
		return nil, fmt.Errorf("error while DescribeWorkflowExecution for workflow: %s", workflowID)
	}
	if resp == nil {
		return nil, fmt.Errorf("empty workflow execution for Deployment Series workflow with workflow ID: %s", workflowID)
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

func (d *DeploymentClientImpl) ListDeployments(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, NextPageToken []byte) ([]*deploypb.DeploymentListInfo, []byte, error) {

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

	if len(persistenceResp.Executions) == 0 {
		fmt.Println("WE ARE soooo wrong")
	}

	deployments := make([]*deploypb.DeploymentListInfo, 0)
	for _, ex := range persistenceResp.Executions {

		seriesName, buildID := ParseDeploymentWorkflowID(ex.Execution.WorkflowId)
		deployment := &deploypb.Deployment{
			SeriesName: seriesName,
			BuildId:    buildID,
		}
		workflowMemo := DecodeDeploymentMemo(ex.GetMemo())

		deploymentListInfo := &deploypb.DeploymentListInfo{
			Deployment: deployment,
			CreateTime: workflowMemo.CreateTime,
			IsCurrent:  workflowMemo.IsCurrentDeployment,
		}
		deployments = append(deployments, deploymentListInfo)
	}

	return deployments, NextPageToken, nil

}

// GenerateStartWorkflowPayload generates start workflow execution payload
func (d *DeploymentClientImpl) generateStartWorkflowPayload(namespaceEntry *namespace.Namespace, deployment *deploypb.Deployment) (*commonpb.Payloads, error) {
	workflowArgs := &deployspb.DeploymentWorkflowArgs{
		NamespaceName: namespaceEntry.Name().String(),
		NamespaceId:   namespaceEntry.ID().String(),
		DeploymentLocalState: &deployspb.DeploymentLocalState{
			WorkerDeployment:  deployment,
			TaskQueueFamilies: nil,
		},
	}
	return sdk.PreferProtoDataConverter.ToPayloads(workflowArgs)
}

// GenerateUpdateDeploymentPayload generates update workflow payload
func (d *DeploymentClientImpl) generateRegisterWorkerInDeploymentArgs(taskQueueName string, taskQueueType enumspb.TaskQueueType,
	pollTimestamp *timestamppb.Timestamp) (*commonpb.Payloads, error) {
	updateArgs := &deployspb.RegisterWorkerInDeploymentArgs{
		TaskQueueName:   taskQueueName,
		TaskQueueType:   taskQueueType,
		FirstPollerTime: nil, // TODO Shivam - come back to this
	}
	return sdk.PreferProtoDataConverter.ToPayloads(updateArgs)
}

func (d *DeploymentClientImpl) addInitialDeploymentMemo() (*commonpb.Memo, error) {
	memo := &commonpb.Memo{}
	memo.Fields = make(map[string]*commonpb.Payload)

	deploymentWorkflowMemo := &deployspb.DeploymentWorkflowMemo{
		CreateTime:          timestamppb.Now(),
		IsCurrentDeployment: false,
	}

	memoPayload, err := sdk.PreferProtoDataConverter.ToPayload(deploymentWorkflowMemo)
	if err != nil {
		return nil, err
	}

	memo.Fields[DeploymentMemoField] = memoPayload
	return memo, nil

}
