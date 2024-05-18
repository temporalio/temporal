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

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/types/known/emptypb"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
)

const (
	buildIdSearchAttributePrefixAssigned    = "assigned"
	buildIdSearchAttributePrefixVersioned   = "versioned"
	buildIdSearchAttributePrefixUnversioned = "unversioned"
	BuildIdSearchAttributeDelimiter         = ":"
	// UnversionedSearchAttribute is the sentinel value used to mark all unversioned workflows
	UnversionedSearchAttribute = buildIdSearchAttributePrefixUnversioned
)

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

// MakeDirectiveForWorkflowTask returns a versioning directive based on the following parameters:
// - inheritedBuildId: build ID inherited from a past/previous wf execution (for Child WF or CaN)
// - assignedBuildId: the build ID to which the WF is currently assigned (i.e. mutable state's AssginedBuildId)
// - stamp: the latest versioning stamp of the execution (only needed for old versioning)
// - hasCompletedWorkflowTask: if the wf has completed any WFT
func MakeDirectiveForWorkflowTask(inheritedBuildId string, assignedBuildId string, stamp *commonpb.WorkerVersionStamp, hasCompletedWorkflowTask bool) *taskqueuespb.TaskVersionDirective {
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
	// TODO: remove `cap.BuildId != ""` condition after old versioning cleanup. this condition is used to differentiate
	// between old and new versioning in Record*TaskStart calls. [cleanup-old-wv]
	// we don't want to add stamp for task started events in old versioning
	if cap != nil && cap.BuildId != "" {
		return &commonpb.WorkerVersionStamp{UseVersioning: cap.UseVersioning, BuildId: cap.BuildId}
	}
	return nil
}
