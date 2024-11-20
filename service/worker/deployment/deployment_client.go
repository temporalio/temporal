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

	commonpb "go.temporal.io/api/common/v1"
	deploypb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	deployspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
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
		maxIDLengthLimit int,
	) error

	DescribeDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		buildID string,
		maxIDLengthLimit int,
	) (*deploypb.DeploymentInfo, error)

	GetCurrentDeployment(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		maxIDLengthLimit int,
		maxPageSize int,
	) (*deploypb.DeploymentInfo, error)

	ListDeployments(
		ctx context.Context,
		namespaceEntry *namespace.Namespace,
		seriesName string,
		NextPageToken []byte,
		maxPageSize int,
	) ([]*deploypb.DeploymentListInfo, []byte, error)
}

// implements DeploymentClient
type DeploymentClient struct {
	HistoryClient     historyservice.HistoryServiceClient
	VisibilityManager manager.VisibilityManager
}

func (d *DeploymentClient) RegisterTaskQueueWorker(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deployment *deploypb.Deployment,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	pollTimestamp *timestamppb.Timestamp,
	maxIDLengthLimit int,
) error {
	// validate params which are used for building workflowID's
	err := ValidateDeploymentWfParams(SeriesFieldName, deployment.SeriesName, maxIDLengthLimit)
	if err != nil {
		return err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, deployment.BuildId, maxIDLengthLimit)
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

	// initial memo fiels
	memo, err := d.addInitialDeploymentMemo()
	if err != nil {
		return err
	}

	// Start workflow execution, if it hasn't already
	startReq := &workflowservice.StartWorkflowExecutionRequest{
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

func (d *DeploymentClient) DescribeDeployment(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, buildID string, maxIDLengthLimit int) (*deploypb.DeploymentInfo, error) {
	// validating params
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, maxIDLengthLimit)
	if err != nil {
		return nil, err
	}
	err = ValidateDeploymentWfParams(BuildIDFieldName, buildID, maxIDLengthLimit)
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

func (d *DeploymentClient) GetCurrentDeployment(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, maxIDLengthLimit int, maxPageSize int) (*deploypb.DeploymentInfo, error) {

	// Validating params
	err := ValidateDeploymentWfParams(SeriesFieldName, seriesName, maxIDLengthLimit)
	if err != nil {
		return nil, err
	}

	// Fetch the workflow execution to decode it's memo
	query := d.queryWithWorkflowID(seriesName)
	persistenceResp, err := d.VisibilityManager.ListWorkflowExecutions(
		ctx,
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespaceEntry.ID(),
			Namespace:   namespaceEntry.Name(),
			PageSize:    maxPageSize,
			Query:       query,
		},
	)
	if err != nil {
		return nil, err
	}

	if len(persistenceResp.Executions) != 1 {
		return nil, fmt.Errorf("there are more than one deployment series workflow executions")
	}

	// Decode value from memo
	var buildID string
	val := persistenceResp.Executions[0].Memo.Fields[DeploymentSeriesBuildIDMemoField]
	err = sdk.PreferProtoDataConverter.FromPayload(val, &buildID)
	if err != nil {
		return nil, err
	}

	// Series has no set current deployment
	if buildID == "" {
		return nil, nil
	}

	deploymentInfo, err := d.DescribeDeployment(ctx, namespaceEntry, seriesName, buildID, maxIDLengthLimit)
	if err != nil {
		return nil, nil
	}

	return deploymentInfo, nil
}

func (d *DeploymentClient) ListDeployments(ctx context.Context, namespaceEntry *namespace.Namespace, seriesName string, NextPageToken []byte, maxPageSize int) ([]*deploypb.DeploymentListInfo, []byte, error) {

	query := ""
	if seriesName != "" {
		query = d.queryWithWorkflowID(seriesName)
	} else {
		query = DeploymentVisibilityBaseListQuery
	}

	persistenceResp, err := d.VisibilityManager.ListWorkflowExecutions(
		ctx,
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceEntry.ID(),
			Namespace:     namespaceEntry.Name(),
			PageSize:      maxPageSize,
			NextPageToken: NextPageToken,
			Query:         query,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	deployments := make([]*deploypb.DeploymentListInfo, len(persistenceResp.Executions))
	for _, ex := range persistenceResp.Executions {
		deployment := ex.GetVersioningInfo().GetDeployment()
		workflowMemo := d.decodeDeploymentMemo(ex.GetMemo())
		deploymentListInfo := &deploypb.DeploymentListInfo{
			Deployment: deployment,
			CreateTime: workflowMemo.CreateTime,
			IsCurrent:  workflowMemo.IsCurrentDeployment,
		}

		deployments = append(deployments, deploymentListInfo)
	}

	return deployments, NextPageToken, nil

}

func (d *DeploymentClient) decodeDeploymentMemo(memo *commonpb.Memo) *deployspb.DeploymentWorkflowMemo {
	var workflowMemo deployspb.DeploymentWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[DeploymentMemoField], &workflowMemo)
	if err != nil {
		return nil
	}
	return &workflowMemo
}

// queryWithWorkflowID is a helper which generates a query with internally used workflowID's
func (d *DeploymentClient) queryWithWorkflowID(seriesName string) string {
	workflowID := GenerateDeploymentSeriesWorkflowID(seriesName)
	query := fmt.Sprintf("%s AND %s = %s", DeploymentSeriesVisibilityBaseListQuery, searchattribute.WorkflowID, workflowID)
	return query
}

// GenerateStartWorkflowPayload generates start workflow execution payload
func (d *DeploymentClient) generateStartWorkflowPayload(namespaceEntry *namespace.Namespace, deployment *deploypb.Deployment) (*commonpb.Payloads, error) {
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
func (d *DeploymentClient) generateRegisterWorkerInDeploymentArgs(taskQueueName string, taskQueueType enumspb.TaskQueueType,
	pollTimestamp *timestamppb.Timestamp) (*commonpb.Payloads, error) {
	updateArgs := &deployspb.RegisterWorkerInDeploymentArgs{
		TaskQueueName:   taskQueueName,
		TaskQueueType:   taskQueueType,
		FirstPollerTime: nil, // TODO Shivam - come back to this
	}
	return sdk.PreferProtoDataConverter.ToPayloads(updateArgs)
}

func (d *DeploymentClient) addInitialDeploymentMemo() (*commonpb.Memo, error) {
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
