// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package worker_versioning

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	BuildIdSearchAttributePrefixPinned      = "pinned"
	buildIdSearchAttributePrefixAssigned    = "assigned"
	buildIdSearchAttributePrefixVersioned   = "versioned"
	buildIdSearchAttributePrefixUnversioned = "unversioned"
	BuildIdSearchAttributeDelimiter         = ":"
	BuildIdSearchAttributeEscape            = "|"
	// UnversionedSearchAttribute is the sentinel value used to mark all unversioned workflows
	UnversionedSearchAttribute = buildIdSearchAttributePrefixUnversioned
)

// EscapeChar is a helper which escapes the BuildIdSearchAttributeDelimiter character
// in the input string
func escapeChar(s string) string {
	s = strings.Replace(s, BuildIdSearchAttributeEscape, BuildIdSearchAttributeEscape+BuildIdSearchAttributeEscape, -1)
	s = strings.Replace(s, BuildIdSearchAttributeDelimiter, BuildIdSearchAttributeEscape+BuildIdSearchAttributeDelimiter, -1)
	return s
}

// PinnedBuildIdSearchAttribute returns the search attribute value for the currently assigned pinned build ID in the form
// 'pinned:<deployment_series_name>:<deployment_build_id>'. Each workflow execution will have at most one member of the
// BuildIds KeywordList in this format. If the workflow becomes unpinned or unversioned, this entry will be removed from
// that list.
func PinnedBuildIdSearchAttribute(deployment *deploymentpb.Deployment) string {
	return fmt.Sprintf("%s%s%s%s%s",
		BuildIdSearchAttributePrefixPinned,
		BuildIdSearchAttributeDelimiter,
		escapeChar(deployment.GetSeriesName()),
		BuildIdSearchAttributeDelimiter,
		escapeChar(deployment.GetBuildId()),
	)
}

// AssignedBuildIdSearchAttribute returns the search attribute value for the currently assigned build ID
func AssignedBuildIdSearchAttribute(buildId string) string {
	return buildIdSearchAttributePrefixAssigned + BuildIdSearchAttributeDelimiter + buildId
}

// IsUnversionedOrAssignedBuildIdSearchAttribute returns the value is "unversioned" or "assigned:<bld>"
func IsUnversionedOrAssignedBuildIdSearchAttribute(buildId string) bool {
	return buildId == UnversionedSearchAttribute ||
		strings.HasPrefix(buildId, buildIdSearchAttributePrefixAssigned+BuildIdSearchAttributeDelimiter)
}

// VersionedBuildIdSearchAttribute returns the search attribute value for a versioned build ID
func VersionedBuildIdSearchAttribute(buildId string) string {
	return buildIdSearchAttributePrefixVersioned + BuildIdSearchAttributeDelimiter + buildId
}

// UnversionedBuildIdSearchAttribute returns the search attribute value for an unversioned build ID
func UnversionedBuildIdSearchAttribute(buildId string) string {
	return buildIdSearchAttributePrefixUnversioned + BuildIdSearchAttributeDelimiter + buildId
}

// VersionStampToBuildIdSearchAttribute returns the search attribute value for a version stamp
func VersionStampToBuildIdSearchAttribute(stamp *commonpb.WorkerVersionStamp) string {
	if stamp.GetBuildId() == "" {
		return UnversionedSearchAttribute
	}
	if stamp.UseVersioning {
		return VersionedBuildIdSearchAttribute(stamp.BuildId)
	}
	return UnversionedBuildIdSearchAttribute(stamp.BuildId)
}

// FindBuildId finds a build ID in the version data's sets, returning (set index, index within that set).
// Returns -1, -1 if not found.
func FindBuildId(versioningData *persistencespb.VersioningData, buildId string) (setIndex, indexInSet int) {
	versionSets := versioningData.GetVersionSets()
	for sidx, set := range versionSets {
		for bidx, id := range set.GetBuildIds() {
			if buildId == id.Id {
				return sidx, bidx
			}
		}
	}
	return -1, -1
}

func WorkflowsExistForBuildId(ctx context.Context, visibilityManager manager.VisibilityManager, ns *namespace.Namespace, taskQueue, buildId string) (bool, error) {
	escapedTaskQueue := sqlparser.String(sqlparser.NewStrVal([]byte(taskQueue)))
	escapedBuildId := sqlparser.String(sqlparser.NewStrVal([]byte(VersionedBuildIdSearchAttribute(buildId))))
	query := fmt.Sprintf("%s = %s AND %s = %s", searchattribute.TaskQueue, escapedTaskQueue, searchattribute.BuildIds, escapedBuildId)

	response, err := visibilityManager.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: ns.ID(),
		Namespace:   ns.Name(),
		Query:       query,
	})
	if err != nil {
		return false, err
	}
	return response.Count > 0, nil
}

// StampIfUsingVersioning returns the given WorkerVersionStamp if it is using versioning,
// otherwise returns nil.
func StampIfUsingVersioning(stamp *commonpb.WorkerVersionStamp) *commonpb.WorkerVersionStamp {
	if stamp.GetUseVersioning() {
		return stamp
	}
	return nil
}

// BuildIdIfUsingVersioning returns the given WorkerVersionStamp if it is using versioning,
// otherwise returns nil.
func BuildIdIfUsingVersioning(stamp *commonpb.WorkerVersionStamp) string {
	if stamp.GetUseVersioning() {
		return stamp.GetBuildId()
	}
	return ""
}

// DeploymentFromCapabilities returns the deployment if it is using versioning V3, otherwise nil.
func DeploymentFromCapabilities(capabilities *commonpb.WorkerVersionCapabilities) *deploymentpb.Deployment {
	if capabilities.GetUseVersioning() && capabilities.GetDeploymentSeriesName() != "" && capabilities.GetBuildId() != "" {
		return &deploymentpb.Deployment{
			SeriesName: capabilities.GetDeploymentSeriesName(),
			BuildId:    capabilities.GetBuildId(),
		}
	}
	return nil
}

// DeploymentToString is intended to be used for logs and metrics only. Theoretically, it can map
// different deployments to the string.
// DO NOT USE IN SERVER LOGIC.
func DeploymentToString(deployment *deploymentpb.Deployment) string {
	if deployment == nil {
		return "UNVERSIONED"
	}
	return deployment.GetSeriesName() + ":" + deployment.GetBuildId()
}

// MakeDirectiveForWorkflowTask returns a versioning directive based on the following parameters:
// - inheritedBuildId: build ID inherited from a past/previous wf execution (for Child WF or CaN)
// - assignedBuildId: the build ID to which the WF is currently assigned (i.e. mutable state's AssginedBuildId)
// - stamp: the latest versioning stamp of the execution (only needed for old versioning)
// - hasCompletedWorkflowTask: if the wf has completed any WFT
// - behavior: workflow's effective behavior
// - deployment: workflow's effective deployment
func MakeDirectiveForWorkflowTask(
	inheritedBuildId string,
	assignedBuildId string,
	stamp *commonpb.WorkerVersionStamp,
	hasCompletedWorkflowTask bool,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
) *taskqueuespb.TaskVersionDirective {
	if behavior != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		return &taskqueuespb.TaskVersionDirective{Behavior: behavior, Deployment: deployment}
	}
	if id := BuildIdIfUsingVersioning(stamp); id != "" && assignedBuildId == "" {
		// TODO: old versioning only [cleanup-old-wv]
		return MakeBuildIdDirective(id)
	} else if !hasCompletedWorkflowTask && inheritedBuildId == "" {
		// first workflow task (or a retry of) and build ID not inherited. if this is retry we reassign build ID
		// if WF has an inherited build ID, we do not allow usage of assignment rules
		return MakeUseAssignmentRulesDirective()
	} else if assignedBuildId != "" {
		return MakeBuildIdDirective(assignedBuildId)
	}
	// else: unversioned queue
	return nil
}

func MakeUseAssignmentRulesDirective() *taskqueuespb.TaskVersionDirective {
	return &taskqueuespb.TaskVersionDirective{BuildId: &taskqueuespb.TaskVersionDirective_UseAssignmentRules{UseAssignmentRules: &emptypb.Empty{}}}
}

func MakeBuildIdDirective(buildId string) *taskqueuespb.TaskVersionDirective {
	return &taskqueuespb.TaskVersionDirective{BuildId: &taskqueuespb.TaskVersionDirective_AssignedBuildId{AssignedBuildId: buildId}}
}

func StampFromCapabilities(cap *commonpb.WorkerVersionCapabilities) *commonpb.WorkerVersionStamp {
	if cap.GetUseVersioning() && cap.GetDeploymentSeriesName() != "" {
		// Versioning 3, do not return stamp.
		return nil
	}
	// TODO: remove `cap.BuildId != ""` condition after old versioning cleanup. this condition is used to differentiate
	// between old and new versioning in Record*TaskStart calls. [cleanup-old-wv]
	// we don't want to add stamp for task started events in old versioning
	if cap.GetBuildId() != "" {
		return &commonpb.WorkerVersionStamp{UseVersioning: cap.UseVersioning, BuildId: cap.BuildId}
	}
	return nil
}

func StampFromBuildId(buildId string) *commonpb.WorkerVersionStamp {
	return &commonpb.WorkerVersionStamp{UseVersioning: true, BuildId: buildId}
}

// ValidateDeployment returns error if the deployment is nil or it has empty build ID or deployment
// name.
func ValidateDeployment(deployment *deploymentpb.Deployment) error {
	if deployment == nil {
		return serviceerror.NewInvalidArgument("deployment cannot be nil")
	}
	if deployment.GetSeriesName() == "" {
		return serviceerror.NewInvalidArgument("deployment series name cannot be empty")
	}
	if deployment.GetBuildId() == "" {
		return serviceerror.NewInvalidArgument("deployment build ID cannot be empty")
	}
	return nil
}

func ValidateVersioningOverride(override *workflowpb.VersioningOverride) error {
	if override == nil {
		return nil
	}
	switch override.GetBehavior() {
	case enumspb.VERSIONING_BEHAVIOR_PINNED:
		if override.GetDeployment() != nil {
			return ValidateDeployment(override.GetDeployment())
		} else {
			return serviceerror.NewInvalidArgument("must provide deployment if behavior is 'PINNED'")
		}
	case enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE:
		if override.GetDeployment() != nil {
			return serviceerror.NewInvalidArgument("only provide deployment if behavior is 'PINNED'")
		}
	case enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED:
		return serviceerror.NewInvalidArgument("override behavior is required")
	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("override behavior %s not recognized", override.GetBehavior()))
	}
	return nil
}

func FindCurrentDeployment(deployments *persistencespb.DeploymentData) *deploymentpb.Deployment {
	var currentDeployment *deploymentpb.Deployment
	var maxCurrentTime time.Time
	for _, d := range deployments.GetDeployments() {
		if d.Data.LastBecameCurrentTime != nil {
			if t := d.Data.LastBecameCurrentTime.AsTime(); t.After(maxCurrentTime) {
				currentDeployment, maxCurrentTime = d.GetDeployment(), t
			}
		}
	}
	return currentDeployment
}
