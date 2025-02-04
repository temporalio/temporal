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
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
)

const (
	// Workflow types
	WorkerDeploymentVersionWorkflowType  = "temporal-sys-worker-deployment-version-workflow"
	WorkerDeploymentWorkflowType         = "temporal-sys-worker-deployment-workflow"
	WorkerDeploymentDrainageWorkflowType = "temporal-sys-worker-deployment-drainage-workflow"

	// Namespace division
	WorkerDeploymentNamespaceDivision = "TemporalWorkerDeployment"

	// Updates
	RegisterWorkerInDeployment   = "register-task-queue-worker"       // for Worker Deployment Version wf
	SyncVersionState             = "sync-version-state"               // for Worker Deployment Version wfs
	SetCurrentVersion            = "set-current-version"              // for Worker Deployment wfs
	SetRampingVersion            = "set-ramping-version"              // for Worker Deployment wfs
	AddVersionToWorkerDeployment = "add-version-to-worker-deployment" // for Worker Deployment wfs
	DeleteVersion                = "delete-version"                   // for WorkerDeployment wfs

	// Signals
	ForceCANSignalName      = "force-continue-as-new" // for Worker Deployment Version _and_ Worker Deployment wfs
	SyncDrainageSignalName  = "sync-drainage-status"
	TerminateDrainageSignal = "terminate-drainage"

	// Queries
	QueryDescribeVersion    = "describe-version"    // for Worker Deployment Version wf
	QueryDescribeDeployment = "describe-deployment" // for Worker Deployment wf

	// Memos
	WorkerDeploymentVersionMemoField = "WorkerDeploymentVersionMemo" // for Worker Deployment Version wf
	WorkerDeploymentMemoField        = "WorkerDeploymentMemo"        // for Worker Deployment wf

	// Prefixes, Delimeters and Keys
	WorkerDeploymentVersionWorkflowIDPrefix      = "temporal-sys-worker-deployment-version"
	WorkerDeploymentWorkflowIDPrefix             = "temporal-sys-worker-deployment"
	WorkerDeploymentVersionWorkflowIDDelimeter   = ":"
	WorkerDeploymentVersionWorkflowIDEscape      = "|"
	WorkerDeploymentVersionWorkflowIDInitialSize = len(WorkerDeploymentVersionWorkflowIDDelimeter) + len(WorkerDeploymentVersionWorkflowIDPrefix) // todo (Shivam): Do we need 2 * len(WorkerDeploymentVersionWorkflowIDDelimeter)?
	WorkerDeploymentFieldName                    = "WorkerDeployment"
	WorkerDeploymentBuildIDFieldName             = "BuildID"

	// Application error names for rejected updates
	errNoChangeType               = "errNoChange"
	errVersionAlreadyExistsType   = "errVersionAlreadyExists"
	errMaxTaskQueuesInVersionType = "errMaxTaskQueuesInVersion"
	errVersionAlreadyCurrentType  = "errVersionAlreadyCurrent"
	errConflictTokenMismatchType  = "errConflictTokenMismatch"
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

// validateVersionWfParams is a helper that verifies if the fields used for generating
// Worker Deployment Version related workflowID's are valid
// todo (Shivam): update with latest checks

func validateVersionWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot be empty", fieldName))
	}

	// Length of each field should be: (MaxIDLengthLimit - (prefix + delimeter length)) / 2
	if len(field) > (maxIDLengthLimit-WorkerDeploymentVersionWorkflowIDInitialSize)/2 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("size of %v larger than the maximum allowed", fieldName))
	}

	// deploymentName cannot have "/"
	if fieldName == WorkerDeploymentFieldName && strings.Contains(field, "/") {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot contain '/'", fieldName))
	}

	// buildID cannot start with "__"
	if fieldName == WorkerDeploymentBuildIDFieldName && strings.HasPrefix(field, "__") {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot start with '__'", fieldName))
	}

	// UTF-8 check
	return common.ValidateUTF8String(fieldName, field)
}

func DecodeWorkerDeploymentMemo(memo *commonpb.Memo) *deploymentspb.WorkerDeploymentWorkflowMemo {
	var workerDeploymentWorkflowMemo deploymentspb.WorkerDeploymentWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[WorkerDeploymentMemoField], &workerDeploymentWorkflowMemo)
	if err != nil {
		return nil
	}
	return &workerDeploymentWorkflowMemo
}
