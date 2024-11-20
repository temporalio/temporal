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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deployspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Updates
	RegisterWorkerInDeployment = "register-task-queue-worker"

	// Signals
	UpdateDeploymentBuildIDSignalName = "update-deployment-build-id"
	ForceCANSignalName                = "force-continue-as-new"

	DeploymentWorkflowIDPrefix      = "temporal-sys-deployment"
	DeploymentNameWorkflowIDPrefix  = "temporal-sys-deployment-name"
	DeploymentWorkflowIDDelimeter   = "|"
	DeploymentWorkflowIDInitialSize = (2 * len(DeploymentWorkflowIDDelimeter)) + len(DeploymentWorkflowIDPrefix)
	BuildIDMemoKey                  = "DefaultBuildID"
)

var (
	defaultActivityOptions = workflow.ActivityOptions{
		ScheduleToCloseTimeout: 1 * time.Hour,
		StartToCloseTimeout:    1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
			MaximumInterval: 60 * time.Second,
		},
	}
)

type DeploymentClient interface {
	RegisterWorker(
		ctx context.Context,
		taskQueueName string,
		taskQueueType enumspb.TaskQueueType,
		pollTimestamp time.Time,
	) error
}

// implements DeploymentClient
type DeploymentWorkflowClient struct {
	namespaceEntry *namespace.Namespace
	deployment     *commonpb.WorkerDeployment
	historyClient  resource.HistoryClient
}

func NewDeploymentWorkflowClient(
	namespaceEntry *namespace.Namespace,
	deployment *commonpb.WorkerDeployment,
	historyClient resource.HistoryClient,
) *DeploymentWorkflowClient {
	return &DeploymentWorkflowClient{
		namespaceEntry: namespaceEntry,
		deployment:     deployment,
		historyClient:  historyClient,
	}
}

func (d *DeploymentWorkflowClient) RegisterTaskQueueWorker(
	ctx context.Context,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	pollTimestamp *timestamppb.Timestamp,
	maxIDLengthLimit int,
) error {
	// validate params which are used for building workflowID's
	err := d.validateDeploymentWfParams("DeploymentName", d.deployment.DeploymentName, maxIDLengthLimit)
	if err != nil {
		return err
	}
	err = d.validateDeploymentWfParams("BuildID", d.deployment.BuildId, maxIDLengthLimit)
	if err != nil {
		return err
	}

	deploymentWorkflowID := d.generateDeploymentWorkflowID()
	workflowInputPayloads, err := d.generateStartWorkflowPayload()
	if err != nil {
		return err
	}
	updatePayload, err := d.generateRegisterWorkerInDeploymentArgs(taskQueueName, taskQueueType, pollTimestamp)
	if err != nil {
		return err
	}

	sa := &commonpb.SearchAttributes{}
	searchattribute.AddSearchAttribute(&sa, searchattribute.TemporalNamespaceDivision, payload.EncodeString(DeploymentNamespaceDivision))

	// Start workflow execution, if it hasn't already
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                d.namespaceEntry.Name().String(),
		WorkflowId:               deploymentWorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: DeploymentWorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    workflowInputPayloads,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		SearchAttributes:         sa,
	}

	updateReq := &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: d.namespaceEntry.Name().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: deploymentWorkflowID,
		},
		Request: &updatepb.Request{
			Input: &updatepb.Input{Name: RegisterWorkerInDeployment, Args: updatePayload},
		},
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
	}

	// This is an atomic operation; if one operation fails, both will.
	_, err = d.historyClient.ExecuteMultiOperation(ctx, &historyservice.ExecuteMultiOperationRequest{
		NamespaceId: d.namespaceEntry.ID().String(),
		WorkflowId:  deploymentWorkflowID,
		Operations: []*historyservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &historyservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: &historyservice.StartWorkflowExecutionRequest{
						NamespaceId:  d.namespaceEntry.ID().String(),
						StartRequest: startReq,
					},
				},
			},
			{
				Operation: &historyservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: &historyservice.UpdateWorkflowExecutionRequest{
						NamespaceId: d.namespaceEntry.ID().String(),
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

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our deployment workflows
func (d *DeploymentWorkflowClient) generateDeploymentWorkflowID() string {
	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedDeploymentName := d.escapeChar(d.deployment.DeploymentName)
	escapedBuildId := d.escapeChar(d.deployment.BuildId)

	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + escapedDeploymentName + DeploymentWorkflowIDDelimeter + escapedBuildId
}

// GenerateStartWorkflowPayload generates start workflow execution payload
func (d *DeploymentWorkflowClient) generateStartWorkflowPayload() (*commonpb.Payloads, error) {
	workflowArgs := &deployspb.DeploymentWorkflowArgs{
		NamespaceName: d.namespaceEntry.Name().String(),
		NamespaceId:   d.namespaceEntry.ID().String(),
		DeploymentLocalState: &deployspb.DeploymentLocalState{
			WorkerDeployment:  d.deployment,
			TaskQueueFamilies: nil,
		},
	}
	return sdk.PreferProtoDataConverter.ToPayloads(workflowArgs)
}

// GenerateUpdateDeploymentPayload generates update workflow payload
func (d *DeploymentWorkflowClient) generateRegisterWorkerInDeploymentArgs(taskQueueName string, taskQueueType enumspb.TaskQueueType,
	pollTimestamp *timestamppb.Timestamp) (*commonpb.Payloads, error) {
	updateArgs := &deployspb.RegisterWorkerInDeploymentArgs{
		TaskQueueName:   taskQueueName,
		TaskQueueType:   taskQueueType,
		FirstPollerTime: nil, // TODO Shivam - come back to this
	}
	return sdk.PreferProtoDataConverter.ToPayloads(updateArgs)
}

// ValidateDeploymentWfParams is a helper that verifies if the fields used for generating
// deployment related workflowID's are valid
func (d *DeploymentWorkflowClient) validateDeploymentWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot be empty", fieldName))
	}

	// Length of each field should be: (MaxIDLengthLimit - prefix and delimeter length) / 2
	if len(field) > (maxIDLengthLimit-DeploymentWorkflowIDInitialSize)/2 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("size of %v larger than the maximum allowed", fieldName))
	}

	// UTF-8 check
	return common.ValidateUTF8String(fieldName, field)
}

func (d *DeploymentWorkflowClient) escapeChar(s string) string {
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, DeploymentWorkflowIDDelimeter, `\`+DeploymentWorkflowIDDelimeter, -1)
	return s
}

func generateDeploymentNameWorkflowID(deploymentName string) string {
	var d *DeploymentWorkflowClient

	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedDeploymentName := d.escapeChar(deploymentName)
	return DeploymentNameWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + escapedDeploymentName
}
