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

	"github.com/temporalio/sqlparser"
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
	WorkerDeploymentVersionWorkflowType = "temporal-sys-worker-deployment-version-workflow"
	WorkerDeploymentWorkflowType        = "temporal-sys-worker-deployment-workflow"

	// Namespace division
	WorkerDeploymentVersionNamespaceDivision = "TemporalWorkerDeploymentVersion"

	// Updates
	RegisterWorkerInDeployment   = "register-task-queue-worker"       // for Worker Deployment Version wf
	SyncVersionState             = "sync-version-state"               // for Worker Deployment Version wfs
	SetCurrentVersion            = "set-current-version"              // for Worker Deployment wfs
	AddVersionToWorkerDeployment = "add-version-to-worker-deployment" // for Worker Deployment wfs

	// Signals
	ForceCANSignalName = "force-continue-as-new" // for Worker Deployment Version _and_ Worker Deployment wfs

	// Queries
	QueryDescribeVersion = "describe-version" // for Worker Deployment Version wf
	QueryCurrentVersion  = "current-version"  // for Worker Deployment wf

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
	WorkerDeploymentVersionFieldName             = "Version"

	// Application error names for rejected updates
	errNoChangeType               = "errNoChange"
	errMaxTaskQueuesInVersionType = "errMaxTaskQueuesInVersion"
)

var (
	DeploymentVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		WorkerDeploymentVersionWorkflowType,
		searchattribute.TemporalNamespaceDivision,
		WorkerDeploymentVersionNamespaceDivision,
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)
)

// ValidateVersionWfParams is a helper that verifies if the fields used for generating
// Worker Deployment Version related workflowID's are valid
func ValidateVersionWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v cannot be empty", fieldName))
	}

	// Length of each field should be: (MaxIDLengthLimit - (prefix + delimeter length))
	if len(field) > (maxIDLengthLimit - WorkerDeploymentVersionWorkflowIDInitialSize) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("size of %v larger than the maximum allowed", fieldName))
	}

	// UTF-8 check
	return common.ValidateUTF8String(fieldName, field)
}

// EscapeChar is a helper which escapes the WorkerDeploymentVersionWorkflowIDDelimeter character
// in the input string
func escapeChar(s string) string {
	s = strings.Replace(s, WorkerDeploymentVersionWorkflowIDEscape, WorkerDeploymentVersionWorkflowIDEscape+WorkerDeploymentVersionWorkflowIDEscape, -1)
	s = strings.Replace(s, WorkerDeploymentVersionWorkflowIDDelimeter, WorkerDeploymentVersionWorkflowIDDelimeter+WorkerDeploymentVersionWorkflowIDDelimeter, -1)
	return s
}

// GenerateWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment workflows
func GenerateWorkflowID(WorkerDeploymentName string) string {
	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedWorkerDeploymentName := escapeChar(WorkerDeploymentName)
	return WorkerDeploymentWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + escapedWorkerDeploymentName
}

// GenerateVersionWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment Version workflows
func GenerateVersionWorkflowID(version string) string {
	escapedVersion := escapeChar(version)

	return WorkerDeploymentVersionWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + escapedVersion
}

func GenerateVersionWorkflowIDForPatternMatching(seriesName string) string {
	escapedSeriesName := escapeChar(seriesName)

	return WorkerDeploymentVersionWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + escapedSeriesName + WorkerDeploymentVersionWorkflowIDDelimeter
}

// BuildQueryWithWorkerDeploymentFilter is a helper which builds a query for pattern matching based on the
// provided workerDeploymentName
func BuildQueryWithWorkerDeploymentFilter(workerDeploymentName string) string {
	workflowID := GenerateVersionWorkflowIDForPatternMatching(workerDeploymentName)
	escapedWorkerDeploymentEntry := sqlparser.String(sqlparser.NewStrVal([]byte(workflowID)))

	query := fmt.Sprintf("%s AND %s STARTS_WITH %s", DeploymentVisibilityBaseListQuery, searchattribute.WorkflowID, escapedWorkerDeploymentEntry)
	return query
}

func DecodeVersionMemo(memo *commonpb.Memo) *deploymentspb.VersionWorkflowMemo {
	var versionWorkflowMemo deploymentspb.VersionWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[WorkerDeploymentVersionMemoField], &versionWorkflowMemo)
	if err != nil {
		return nil
	}
	return &versionWorkflowMemo
}
