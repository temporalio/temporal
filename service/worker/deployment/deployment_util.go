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
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	deployspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
)

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our deployment workflows
func GenerateDeploymentWorkflowID(deploymentName string, buildID string) string {
	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + deploymentName + DeploymentWorkflowIDDelimeter + buildID
}

// ValidateDeploymentWfParams is a helper that verifies if the fields used for generating
// deployment related workflowID's are valid
func ValidateDeploymentWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot be empty", fieldName))
	}

	// Length of each field should be: (MaxIDLengthLimit - prefix and delimeter length) / 2
	if len(field) > (maxIDLengthLimit-DeploymentWorkflowIDInitialSize)/2 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("size of %v larger than the maximum allowed", fieldName))
	}

	// Invalid character check - Cannot contain the reserved delimeter ("|") since its being used for building deployment workflowID
	if strings.Contains(field, DeploymentWorkflowIDDelimeter) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot contain reserved prefix %v", fieldName, DeploymentWorkflowIDDelimeter))
	}

	// UTF-8 check
	return common.ValidateUTF8String(fieldName, field)
}

// StartAndUpdateDeploymentWorkflow is a helper that validates the input and update workflow parameters
// and uses a multi-operation to start and update a deployment workflow.
func StartAndUpdateDeploymentWorkflow(
	ctx context.Context,
	workflowArgs *deployspb.DeploymentWorkflowArgs,
	updateArgs *deployspb.RegisterWorkerInDeploymentArgs,
	maxIDLengthLimit int,
	historyClient resource.HistoryClient,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))

	// validate params which are used for building workflowID's
	err := ValidateDeploymentWfParams("DeploymentName", workflowArgs.DeploymentName, maxIDLengthLimit)
	if err != nil {
		return nil, err
	}
	err = ValidateDeploymentWfParams("BuildID", workflowArgs.BuildId, maxIDLengthLimit)
	if err != nil {
		return nil, err
	}

	deploymentWorkflowID := GenerateDeploymentWorkflowID(workflowArgs.DeploymentName, workflowArgs.BuildId)

	// workflow input
	workflowInputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(workflowArgs)
	if err != nil {
		return nil, err
	}

	// update input
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(updateArgs)
	if err != nil {
		return nil, err
	}

	// Start workflow execution, if it hasn't already
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                workflowArgs.NamespaceName,
		WorkflowId:               deploymentWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: DeploymentWorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    workflowInputPayloads,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		SearchAttributes:         sa,
	}

	updateReq := &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: workflowArgs.NamespaceName,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: deploymentWorkflowID,
		},
		Request: &updatepb.Request{
			Input: &updatepb.Input{Name: RegisterWorkerInDeployment, Args: updatePayload},
		},
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
	}

	// This is an atomic operation; if one operation fails, both will.
	resp, err := historyClient.ExecuteMultiOperation(ctx, &historyservice.ExecuteMultiOperationRequest{
		NamespaceId: workflowArgs.NamespaceId,
		WorkflowId:  deploymentWorkflowID,
		Operations: []*historyservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &historyservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: &historyservice.StartWorkflowExecutionRequest{
						NamespaceId:  workflowArgs.NamespaceId,
						StartRequest: startReq,
					},
				},
			},
			{
				Operation: &historyservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: &historyservice.UpdateWorkflowExecutionRequest{
						NamespaceId: workflowArgs.NamespaceId,
						Request:     updateReq,
					},
				},
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}
