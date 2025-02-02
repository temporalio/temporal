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
	"math"
	"math/rand"
	"strings"

	farm "github.com/dgryski/go-farm"
	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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

	// Prefixes, Delimeters and Keys
	WorkerDeploymentVersionIdDelimiter         = "/"
	WorkerDeploymentVersionWorkflowIDPrefix    = "temporal-sys-worker-deployment-version"
	WorkerDeploymentWorkflowIDPrefix           = "temporal-sys-worker-deployment"
	WorkerDeploymentVersionWorkflowIDDelimeter = ":"
	WorkerDeploymentVersionWorkflowIDEscape    = "|"
)

// EscapeChar is a helper which escapes the BuildIdSearchAttributeDelimiter character
// in the input string
func escapeChar(s, escape, delimiter string) string {
	s = strings.Replace(s, escape, escape+escape, -1)
	s = strings.Replace(s, delimiter, escape+delimiter, -1)
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
		escapeChar(deployment.GetSeriesName(), BuildIdSearchAttributeEscape, BuildIdSearchAttributeDelimiter),
		BuildIdSearchAttributeDelimiter,
		escapeChar(deployment.GetBuildId(), BuildIdSearchAttributeEscape, BuildIdSearchAttributeDelimiter),
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
// It returns the deployment from the `options` if present, otherwise, from `capabilities`,
func DeploymentFromCapabilities(capabilities *commonpb.WorkerVersionCapabilities, options *deploymentpb.WorkerDeploymentOptions) *deploymentpb.Deployment {
	if options.GetWorkflowVersioningMode() != enumspb.WORKFLOW_VERSIONING_MODE_UNVERSIONED &&
		options.GetDeploymentName() != "" &&
		options.GetBuildId() != "" {
		return &deploymentpb.Deployment{
			SeriesName: options.GetDeploymentName(),
			BuildId:    options.GetBuildId(),
		}
	}
	if capabilities.GetUseVersioning() && capabilities.GetDeploymentSeriesName() != "" && capabilities.GetBuildId() != "" {
		return &deploymentpb.Deployment{
			SeriesName: capabilities.GetDeploymentSeriesName(),
			BuildId:    capabilities.GetBuildId(),
		}
	}
	return nil
}

// DeploymentOrVersion Temporary helper function to return a Deployment based on passed Deployment
// or WorkerDeploymentVersion objects, if `v` is not nil, it'll take precedence.
func DeploymentOrVersion(d *deploymentpb.Deployment, v *deploymentpb.WorkerDeploymentVersion) *deploymentpb.Deployment {
	if v != nil {
		return DeploymentIfValid(DeploymentFromDeploymentVersion(v))
	}
	return DeploymentIfValid(d)
}

// DeploymentIfValid returns the deployment back if is both of its fields have value.
func DeploymentIfValid(d *deploymentpb.Deployment) *deploymentpb.Deployment {
	if d.GetSeriesName() != "" && d.GetBuildId() != "" {
		return d
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
		return &taskqueuespb.TaskVersionDirective{
			Behavior:          behavior,
			DeploymentVersion: DeploymentVersionFromDeployment(deployment),
		}
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

// DeploymentVersionFromDeployment Temporary helper function to convert Deployment to
// WorkerDeploymentVersion proto until we update code to use the new proto in all places.
func DeploymentVersionFromDeployment(deployment *deploymentpb.Deployment) *deploymentpb.WorkerDeploymentVersion {
	if deployment == nil {
		return nil
	}
	return &deploymentpb.WorkerDeploymentVersion{
		BuildId:        deployment.GetBuildId(),
		DeploymentName: deployment.GetSeriesName(),
	}
}

// DeploymentFromDeploymentVersion Temporary helper function to convert WorkerDeploymentVersion to
// Deployment proto until we update code to use the new proto in all places.
func DeploymentFromDeploymentVersion(dv *deploymentpb.WorkerDeploymentVersion) *deploymentpb.Deployment {
	if dv == nil {
		return nil
	}
	return &deploymentpb.Deployment{
		BuildId:    dv.GetBuildId(),
		SeriesName: dv.GetDeploymentName(),
	}
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

// ValidateDeploymentVersion returns error if the deployment version is nil or it has empty version
// or deployment name.
func ValidateDeploymentVersion(version *deploymentpb.WorkerDeploymentVersion) error {
	if version == nil {
		return serviceerror.NewInvalidArgument("deployment cannot be nil")
	}
	if version.GetDeploymentName() == "" {
		return serviceerror.NewInvalidArgument("deployment name name cannot be empty")
	}
	if version.GetBuildId() == "" {
		return serviceerror.NewInvalidArgument("build id cannot be empty")
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
		} else if override.GetPinnedVersion() != nil {
			return ValidateDeploymentVersion(override.GetPinnedVersion())
		} else {
			return serviceerror.NewInvalidArgument("must provide deployment if behavior is 'PINNED'")
		}
	case enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE:
		if override.GetDeployment() != nil {
			return serviceerror.NewInvalidArgument("only provide deployment if behavior is 'PINNED'")
		}
		if override.GetPinnedVersion() != nil {
			return serviceerror.NewInvalidArgument("only provide pinned version if behavior is 'PINNED'")
		}
	case enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED:
		return serviceerror.NewInvalidArgument("override behavior is required")
	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("override behavior %s not recognized", override.GetBehavior()))
	}
	return nil
}

// FindDeploymentVersionForWorkflowID returns the deployment version that should be used for a
// particular workflow ID based on the versioning info of the task queue. Nil means unversioned.
func FindDeploymentVersionForWorkflowID(versioningInfo *taskqueuepb.TaskQueueVersioningInfo, workflowId string) *deploymentpb.WorkerDeploymentVersion {
	if versioningInfo == nil {
		return nil // unversioned
	}
	ramp := versioningInfo.GetRampingVersionPercentage()
	if ramp <= 0 {
		// No ramp
		return versioningInfo.GetCurrentVersion()
	} else if ramp == 100 {
		return versioningInfo.GetRampingVersion()
	}
	// Partial ramp. Decide based on workflow ID
	wfRampThreshold := calcRampThreshold(workflowId)
	if wfRampThreshold <= float64(ramp) {
		return versioningInfo.GetRampingVersion()
	}
	return versioningInfo.GetCurrentVersion()
}

// calcRampThreshold returns a number in [0, 100) that is deterministically calculated based on the
// passed id. If id is empty, a random threshold is returned.
func calcRampThreshold(id string) float64 {
	if id == "" {
		return rand.Float64()
	}
	h := farm.Fingerprint32([]byte(id))
	return 100 * (float64(h) / (float64(math.MaxUint32) + 1))
}

//revive:disable-next-line:cognitive-complexity
func CalculateTaskQueueVersioningInfo(deployments *persistencespb.DeploymentData) *taskqueuepb.TaskQueueVersioningInfo {
	if deployments == nil {
		return nil
	}

	var current *deploymentspb.DeploymentVersionData
	var ramping *deploymentspb.DeploymentVersionData

	// Find old current
	for _, d := range deployments.GetDeployments() {
		// [cleanup-old-wv]
		if d.Data.LastBecameCurrentTime != nil {
			if t := d.Data.LastBecameCurrentTime.AsTime(); t.After(current.GetRoutingUpdateTime().AsTime()) {
				current = &deploymentspb.DeploymentVersionData{
					Version:           DeploymentVersionFromDeployment(d.Deployment),
					RoutingUpdateTime: d.Data.LastBecameCurrentTime,
				}
			}
		}
	}

	// Find new current and ramping
	for _, v := range deployments.GetVersions() {
		if v.RoutingUpdateTime != nil && v.GetCurrentSinceTime() != nil {
			if t := v.RoutingUpdateTime.AsTime(); t.After(current.GetRoutingUpdateTime().AsTime()) {
				current = v
			}
		}
		if v.RoutingUpdateTime != nil && v.GetRampPercentage() > 0 {
			if t := v.RoutingUpdateTime.AsTime(); t.After(ramping.GetRoutingUpdateTime().AsTime()) {
				ramping = v
			}
		}
	}

	if current == nil && ramping == nil {
		return nil // TODO (Shahab): __unversioned__
	}

	info := &taskqueuepb.TaskQueueVersioningInfo{
		CurrentVersion: current.GetVersion(),
		UpdateTime:     current.GetRoutingUpdateTime(),
	}
	if ramping.GetRampPercentage() > 0 {
		info.RampingVersionPercentage = ramping.GetRampPercentage()
		if ramping.GetVersion().GetBuildId() != "" {
			// If version is "" it means it's ramping to unversioned, so we do not set RampingVersion.
			// todo (carly): handle unversioned
			info.RampingVersion = ramping.GetVersion()
		}
		if info.GetUpdateTime().AsTime().Before(ramping.GetRoutingUpdateTime().AsTime()) {
			info.UpdateTime = ramping.GetRoutingUpdateTime()
		}
	}

	return info
}

func ValidateTaskVersionDirective(
	directive *taskqueuespb.TaskVersionDirective,
	wfBehavior enumspb.VersioningBehavior,
	wfDeployment *deploymentpb.Deployment,
	scheduledDeployment *deploymentpb.Deployment,
) error {
	// Effective behavior and deployment of the workflow when History scheduled the WFT.
	directiveBehavior := directive.GetBehavior()
	if directiveBehavior != wfBehavior &&
		// Verisoning 3 pre-release (v1.26, Dec 2024) is not populating request.VersionDirective so
		// we skip this check until v1.28 if directiveBehavior is unspecified.
		// TODO (shahab): remove this line after v1.27 is released.
		directiveBehavior != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// This must be a task scheduled before the workflow changes behavior. Matching can drop it.
		return serviceerrors.NewObsoleteMatchingTask(fmt.Sprintf(
			"task was scheduled when workflow had versioning behavior %s, now it has versioning behavior %s.",
			directiveBehavior, wfBehavior))
	}

	directiveDeployment := DirectiveDeployment(directive)
	if directiveDeployment == nil {
		// TODO: remove this once the ScheduledDeployment field is removed from proto
		directiveDeployment = scheduledDeployment
	}
	if !directiveDeployment.Equal(wfDeployment) {
		// This must be a task scheduled before the workflow transitions to the current
		// deployment. Matching can drop it.
		return serviceerrors.NewObsoleteMatchingTask(fmt.Sprintf(
			"task was scheduled when workflow was on build %s, now it is on build %s.",
			directiveDeployment.GetBuildId(), wfDeployment.GetBuildId()))
	}
	return nil
}

// DirectiveDeployment Temporary function until Directive proto is removed.
func DirectiveDeployment(directive *taskqueuespb.TaskVersionDirective) *deploymentpb.Deployment {
	if dv := directive.GetDeploymentVersion(); dv != nil {
		return DeploymentFromDeploymentVersion(dv)
	}
	return directive.GetDeployment()
}

func WorkerDeploymentVersionToString(v *deploymentpb.WorkerDeploymentVersion) string {
	if v == nil {
		return "__unversioned__"
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionIdDelimiter + v.GetBuildId()
}
func WorkerDeploymentVersionFromString(s string) (*deploymentpb.WorkerDeploymentVersion, error) {
	if s == "__unversioned__" {
		return nil, nil
	}
	before, after, found := strings.Cut(s, WorkerDeploymentVersionIdDelimiter)
	if !found {
		return nil, fmt.Errorf("expected delimiter %s not found in version string %s", WorkerDeploymentVersionIdDelimiter, s)
	}
	return &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: before,
		BuildId:        after,
	}, nil
}

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment workflows
func GenerateDeploymentWorkflowID(deploymentName string) string {
	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedDeploymentName := escapeChar(deploymentName, WorkerDeploymentVersionWorkflowIDEscape, WorkerDeploymentVersionWorkflowIDDelimeter)
	return WorkerDeploymentWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + escapedDeploymentName
}

// GenerateVersionWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment Version workflows
func GenerateVersionWorkflowID(deploymentName string, buildID string) string {
	escapedVersionString := escapeChar(WorkerDeploymentVersionToString(&deploymentpb.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	}), WorkerDeploymentVersionWorkflowIDEscape, WorkerDeploymentVersionWorkflowIDDelimeter)

	return WorkerDeploymentVersionWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + escapedVersionString
}
