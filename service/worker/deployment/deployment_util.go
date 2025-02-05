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
	DeploymentWorkflowType       = "temporal-sys-deployment-workflow"
	DeploymentSeriesWorkflowType = "temporal-sys-deployment-series-workflow"

	// Namespace division
	DeploymentNamespaceDivision = "TemporalDeployment"

	// Updates
	RegisterWorkerInDeployment = "register-task-queue-worker" // for deployment wf
	SyncDeploymentState        = "sync-deployment-state"      // for deployment wfs
	SetCurrentDeployment       = "set-current-deployment"     // for series wfs

	// Signals
	ForceCANSignalName = "force-continue-as-new" // for deployment _and_ series wfs

	// Queries
	QueryDescribeDeployment = "describe-deployment" // for deployment wf
	QueryCurrentDeployment  = "current-deployment"  // for series wf

	// Memos
	DeploymentMemoField       = "DeploymentMemo"       // for deployment wf
	DeploymentSeriesMemoField = "DeploymentSeriesMemo" // for deployment series wf

	// Prefixes, Delimeters and Keys
	DeploymentWorkflowIDPrefix       = "temporal-sys-deployment"
	DeploymentSeriesWorkflowIDPrefix = "temporal-sys-deployment-series"
	DeploymentWorkflowIDDelimeter    = ":"
	DeploymentWorkflowIDEscape       = "|"
	DeploymentWorkflowIDInitialSize  = (2 * len(DeploymentWorkflowIDDelimeter)) + len(DeploymentWorkflowIDPrefix)
	SeriesFieldName                  = "DeploymentSeries"
	BuildIDFieldName                 = "BuildID"

	// Application error names for rejected updates
	errNoChangeType                  = "errNoChange"
	errMaxTaskQueuesInDeploymentType = "errMaxTaskQueuesInDeployment"
)

var (
	DeploymentVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		DeploymentWorkflowType,
		searchattribute.TemporalNamespaceDivision,
		DeploymentNamespaceDivision,
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)
)

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

	// UTF-8 check
	return common.ValidateUTF8String(fieldName, field)
}

// EscapeChar is a helper which escapes the DeploymentWorkflowIDDelimeter character
// in the input string
func escapeChar(s string) string {
	s = strings.Replace(s, DeploymentWorkflowIDEscape, DeploymentWorkflowIDEscape+DeploymentWorkflowIDEscape, -1)
	s = strings.Replace(s, DeploymentWorkflowIDDelimeter, DeploymentWorkflowIDEscape+DeploymentWorkflowIDDelimeter, -1)
	return s
}

func GenerateDeploymentSeriesWorkflowID(deploymentSeriesName string) string {
	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedSeriesName := escapeChar(deploymentSeriesName)
	return DeploymentSeriesWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + escapedSeriesName
}

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our deployment workflows
func GenerateDeploymentWorkflowID(seriesName string, buildID string) string {
	escapedSeriesName := escapeChar(seriesName)
	escapedBuildId := escapeChar(buildID)

	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + escapedSeriesName + DeploymentWorkflowIDDelimeter + escapedBuildId
}

func GenerateDeploymentWorkflowIDForPatternMatching(seriesName string) string {
	escapedSeriesName := escapeChar(seriesName)

	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + escapedSeriesName + DeploymentWorkflowIDDelimeter
}

// BuildQueryWithSeriesFilter is a helper which builds a query for pattern matching based on the
// provided seriesName
func BuildQueryWithSeriesFilter(seriesName string) string {
	workflowID := GenerateDeploymentWorkflowIDForPatternMatching(seriesName)
	escapedSeriesEntry := sqlparser.String(sqlparser.NewStrVal([]byte(workflowID)))

	query := fmt.Sprintf("%s AND %s STARTS_WITH %s", DeploymentVisibilityBaseListQuery, searchattribute.WorkflowID, escapedSeriesEntry)
	return query
}

func DecodeDeploymentMemo(memo *commonpb.Memo) *deploymentspb.DeploymentWorkflowMemo {
	var workflowMemo deploymentspb.DeploymentWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[DeploymentMemoField], &workflowMemo)
	if err != nil {
		return nil
	}
	return &workflowMemo
}
