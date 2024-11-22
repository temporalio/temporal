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
	"time"

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deployspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
)

const (
	// Updates
	RegisterWorkerInDeployment = "register-task-queue-worker"

	// Signals
	UpdateDeploymentBuildIDSignalName = "update-deployment-build-id"
	ForceCANSignalName                = "force-continue-as-new"

	// Queries
	QueryDescribeDeployment = "describe-deployment"
	QueryCurrentDeployment  = "current-deployment"

	// Memos
	DeploymentMemoField              = "DeploymentMemo"
	DeploymentSeriesBuildIDMemoField = "DeploymentSeriesBuildIDMemo"

	// Prefixes, Delimeters and Keys
	DeploymentWorkflowIDPrefix       = "temporal-sys-deployment"
	DeploymentSeriesWorkflowIDPrefix = "temporal-sys-deployment-series"
	DeploymentWorkflowIDDelimeter    = "|"
	DeploymentWorkflowIDInitialSize  = (2 * len(DeploymentWorkflowIDDelimeter)) + len(DeploymentWorkflowIDPrefix)
	SeriesFieldName                  = "DeploymentSeries"
	BuildIDFieldName                 = "BuildID"
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
	s = strings.Replace(s, DeploymentWorkflowIDDelimeter, DeploymentWorkflowIDDelimeter+DeploymentWorkflowIDDelimeter, -1)
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

// ParseDeploymentWorkflowID is a helper which parses out the deployment workflow ID to
// extract the seriesName and buildID
func ParseDeploymentWorkflowID(workflowID string) (string, string) {
	parts := strings.Split(workflowID, DeploymentWorkflowIDDelimeter)
	return parts[1], parts[2]
}

func DecodeDeploymentMemo(memo *commonpb.Memo) *deployspb.DeploymentWorkflowMemo {
	var workflowMemo deployspb.DeploymentWorkflowMemo
	err := sdk.PreferProtoDataConverter.FromPayload(memo.Fields[DeploymentMemoField], &workflowMemo)
	if err != nil {
		return nil
	}
	return &workflowMemo
}
