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

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
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
	DeploymentMemoField = "DeploymentMemo"

	// Prefixes, Delimeters and Keys
	DeploymentWorkflowIDPrefix       = "temporal-sys-deployment"
	DeploymentSeriesWorkflowIDPrefix = "temporal-sys-deployment-series"
	DeploymentWorkflowIDDelimeter    = "|"
	DeploymentWorkflowIDInitialSize  = (2 * len(DeploymentWorkflowIDDelimeter)) + len(DeploymentWorkflowIDPrefix)
	BuildIDMemoKey                   = "DefaultBuildID"
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
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, DeploymentWorkflowIDDelimeter, `\`+DeploymentWorkflowIDDelimeter, -1)
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
	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedSeriesName := escapeChar(seriesName)
	escapedBuildId := escapeChar(buildID)

	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimeter + escapedSeriesName + DeploymentWorkflowIDDelimeter + escapedBuildId
}
