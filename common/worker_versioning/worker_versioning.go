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

	"github.com/gogo/protobuf/types"
	"github.com/xwb1989/sqlparser"
	commonpb "go.temporal.io/api/common/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
)

const (
	buildIdSearchAttributePrefixVersioned   = "versioned"
	buildIdSearchAttributePrefixUnversioned = "unversioned"
	BuildIdSearchAttributeDelimiter         = ":"
	// UnversionedSearchAttribute is the sentinel value used to mark all unversioned workflows
	UnversionedSearchAttribute = buildIdSearchAttributePrefixUnversioned
)

// VersionedBuildIdSearchAttribute returns the search attribute value for an unversioned build id
func VersionedBuildIdSearchAttribute(buildId string) string {
	return buildIdSearchAttributePrefixVersioned + BuildIdSearchAttributeDelimiter + buildId
}

// VersionedBuildIdSearchAttribute returns the search attribute value for an versioned build id
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

// FindBuildId finds a build id in the version data's sets, returning (set index, index within that set).
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

func MakeDirectiveForWorkflowTask(
	stamp *commonpb.WorkerVersionStamp,
	lastWorkflowTaskStartedEventID int64,
) *taskqueuespb.TaskVersionDirective {
	var directive taskqueuespb.TaskVersionDirective
	if id := StampIfUsingVersioning(stamp).GetBuildId(); id != "" {
		directive.Value = &taskqueuespb.TaskVersionDirective_BuildId{BuildId: id}
	} else if lastWorkflowTaskStartedEventID == common.EmptyEventID {
		// first workflow task
		directive.Value = &taskqueuespb.TaskVersionDirective_UseDefault{UseDefault: &types.Empty{}}
	}
	// else: unversioned queue
	return &directive
}

func MakeDirectiveForActivityTask(
	stamp *commonpb.WorkerVersionStamp,
	useCompatibleVersion bool,
) *taskqueuespb.TaskVersionDirective {
	var directive taskqueuespb.TaskVersionDirective
	if !useCompatibleVersion {
		directive.Value = &taskqueuespb.TaskVersionDirective_UseDefault{UseDefault: &types.Empty{}}
	} else if id := StampIfUsingVersioning(stamp).GetBuildId(); id != "" {
		directive.Value = &taskqueuespb.TaskVersionDirective_BuildId{BuildId: id}
	}
	// else: unversioned queue
	return &directive
}
