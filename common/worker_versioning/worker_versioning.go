package worker_versioning

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute/sadefs"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/protobuf/proto"
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
	UnversionedVersionId       = "__unversioned__"

	// WorkerDeploymentVersionIdDelimiterV31 will be deleted once we stop supporting v31 version string fields
	// in external and internal APIs. Until then, both delimiters are banned in deployment name. All
	// deprecated version string fields in APIs keep using the old delimiter. Workflow SA uses new delimiter.
	WorkerDeploymentVersionIDDelimiterV31   = "."
	WorkerDeploymentVersionDelimiter        = ":"
	WorkerDeploymentVersionWorkflowIDEscape = "|"

	// Prefixes, Delimeters and Keys that are used in the internal entity workflows backing worker-versioning
	WorkerDeploymentWorkflowIDPrefix             = "temporal-sys-worker-deployment"
	WorkerDeploymentVersionWorkflowIDPrefix      = "temporal-sys-worker-deployment-version"
	WorkerDeploymentVersionWorkflowIDInitialSize = len(WorkerDeploymentVersionWorkflowIDPrefix) + len(WorkerDeploymentVersionDelimiter) // 39
	WorkerDeploymentNameFieldName                = "WorkerDeploymentName"
	WorkerDeploymentBuildIDFieldName             = "BuildID"
)

// PinnedBuildIdSearchAttribute creates the pinned search attribute for the BuildIds list, used as a visibility optimization.
// For pinned workflows using WorkerDeployment APIs (ms.GetEffectiveVersioningBehavior() == PINNED &&
// ms.executionInfo.VersioningInfo.Version != ""), this will be `pinned:<version>`. The version used
// will be the override version if set, or the versioningInfo.Version.
//
// If deprecated Deployment-based APIs are in use and the workflow is pinned, `pinned:<deployment_series_name>:<deployment_build_id>`
// will. The values used will be the override deployment_series and build_id if set, or versioningInfo.Deployment.
//
// If the workflow becomes unpinned or unversioned, this entry will be removed from that list.
func PinnedBuildIdSearchAttribute(version string) string {
	return fmt.Sprintf("%s%s%s",
		BuildIdSearchAttributePrefixPinned,
		BuildIdSearchAttributeDelimiter,
		version,
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
	query := fmt.Sprintf("%s = %s AND %s = %s", sadefs.TaskQueue, escapedTaskQueue, sadefs.BuildIds, escapedBuildId)

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
func DeploymentFromCapabilities(capabilities *commonpb.WorkerVersionCapabilities, options *deploymentpb.WorkerDeploymentOptions) (*deploymentpb.Deployment, error) {
	if options.GetWorkerVersioningMode() == enumspb.WORKER_VERSIONING_MODE_VERSIONED {
		d := options.GetDeploymentName()
		b := options.GetBuildId()
		if d == "" {
			return nil, serviceerror.NewInvalidArgumentf("versioned worker must have deployment name")
		}
		if b == "" {
			return nil, serviceerror.NewInvalidArgumentf("versioned worker must have build id")
		}
		if strings.Contains(d, WorkerDeploymentVersionDelimiter) || strings.Contains(d, WorkerDeploymentVersionIDDelimiterV31) {
			// TODO: allow '.' once we get rid of v31 stuff
			return nil, serviceerror.NewInvalidArgumentf("deployment name cannot contain '%s' or '%s'", WorkerDeploymentVersionDelimiter, WorkerDeploymentVersionIDDelimiterV31)
		}
		return &deploymentpb.Deployment{
			SeriesName: d,
			BuildId:    b,
		}, nil
	}
	if capabilities.GetUseVersioning() && capabilities.GetDeploymentSeriesName() != "" && capabilities.GetBuildId() != "" {
		return &deploymentpb.Deployment{
			SeriesName: capabilities.GetDeploymentSeriesName(),
			BuildId:    capabilities.GetBuildId(),
		}, nil
	}
	return nil, nil
}

func DeploymentNameFromCapabilities(capabilities *commonpb.WorkerVersionCapabilities, options *deploymentpb.WorkerDeploymentOptions) string {
	if d := options.GetDeploymentName(); d != "" {
		return d
	}
	return capabilities.GetDeploymentSeriesName()
}

func BuildIdFromCapabilities(capabilities *commonpb.WorkerVersionCapabilities, options *deploymentpb.WorkerDeploymentOptions) string {
	if d := options.GetBuildId(); d != "" {
		return d
	}
	return capabilities.GetBuildId()
}

func DeploymentVersionFromOptions(options *deploymentpb.WorkerDeploymentOptions) *deploymentspb.WorkerDeploymentVersion {
	if options.GetWorkerVersioningMode() == enumspb.WORKER_VERSIONING_MODE_VERSIONED {
		return &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: options.GetDeploymentName(),
			BuildId:        options.GetBuildId(),
		}
	}
	return nil
}

// DeploymentOrVersion Temporary helper function to return a Deployment based on passed Deployment
// or WorkerDeploymentVersion objects, if `v` is not nil, it'll take precedence.
func DeploymentOrVersion(d *deploymentpb.Deployment, v *deploymentspb.WorkerDeploymentVersion) *deploymentpb.Deployment {
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
	revisionNumber int64,
) *taskqueuespb.TaskVersionDirective {
	if behavior != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		return &taskqueuespb.TaskVersionDirective{
			Behavior:          behavior,
			DeploymentVersion: DeploymentVersionFromDeployment(deployment),
			RevisionNumber:    revisionNumber,
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

type IsWFTaskQueueInVersionDetector = func(ctx context.Context, namespaceID, tq string, version *deploymentpb.WorkerDeploymentVersion) (bool, error)

func GetIsWFTaskQueueInVersionDetector(matchingClient resource.MatchingClient) IsWFTaskQueueInVersionDetector {
	return func(ctx context.Context,
		namespaceID, tq string,
		version *deploymentpb.WorkerDeploymentVersion) (bool, error) {
		resp, err := matchingClient.CheckTaskQueueVersionMembership(ctx, &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   namespaceID,
			TaskQueue:     tq,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			Version:       DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(version)),
		})
		if err != nil {
			return false, err
		}
		return resp.GetIsMember(), nil
	}
}

func FindDeploymentVersion(deployments *persistencespb.DeploymentData, v *deploymentspb.WorkerDeploymentVersion) int {
	for i, vd := range deployments.GetVersions() {
		if proto.Equal(v, vd.GetVersion()) {
			return i
		}
	}
	return -1
}

//nolint:staticcheck
func HasDeploymentVersion(deployments *persistencespb.DeploymentData, v *deploymentspb.WorkerDeploymentVersion) bool {
	// Represents unversioned workers.
	if v == nil {
		return false
	}

	for _, vd := range deployments.GetVersions() {
		if proto.Equal(v, vd.GetVersion()) {
			return true
		}
	}

	// Check for the presence of the version in the new DeploymentData format.
	if deploymentData, ok := deployments.GetDeploymentsData()[v.GetDeploymentName()]; ok {
		vd := deploymentData.GetVersions()[v.GetBuildId()]
		return vd != nil && !vd.GetDeleted()
	}

	return false
}

func CountDeploymentVersions(deployments *persistencespb.DeploymentData) int {
	//nolint:staticcheck // SA1019
	res := len(deployments.GetVersions())

	// Check for the presence of the version in the new DeploymentData format.
	for _, d := range deployments.GetDeploymentsData() {
		for _, vd := range d.GetVersions() {
			if vd != nil && !vd.GetDeleted() {
				res++
			}
		}
	}

	return res
}

// DeploymentVersionFromDeployment Temporary helper function to convert Deployment to
// WorkerDeploymentVersion proto until we update code to use the new proto in all places.
func DeploymentVersionFromDeployment(deployment *deploymentpb.Deployment) *deploymentspb.WorkerDeploymentVersion {
	if deployment == nil {
		return nil
	}
	return &deploymentspb.WorkerDeploymentVersion{
		BuildId:        deployment.GetBuildId(),
		DeploymentName: deployment.GetSeriesName(),
	}
}

// ExternalWorkerDeploymentVersionFromDeployment Temporary helper function to convert Deployment to
// WorkerDeploymentVersion proto until we update code to use the new proto in all places.
func ExternalWorkerDeploymentVersionFromDeployment(deployment *deploymentpb.Deployment) *deploymentpb.WorkerDeploymentVersion {
	if deployment == nil {
		return nil
	}
	return &deploymentpb.WorkerDeploymentVersion{
		BuildId:        deployment.GetBuildId(),
		DeploymentName: deployment.GetSeriesName(),
	}
}

// ExternalWorkerDeploymentVersionFromVersion Temporary helper function to convert internal Worker Deployment to
// WorkerDeploymentVersion proto until we update code to use the new proto in all places.
func ExternalWorkerDeploymentVersionFromVersion(version *deploymentspb.WorkerDeploymentVersion) *deploymentpb.WorkerDeploymentVersion {
	if version == nil {
		return nil
	}
	return &deploymentpb.WorkerDeploymentVersion{
		BuildId:        version.GetBuildId(),
		DeploymentName: version.GetDeploymentName(),
	}
}

// DeploymentFromExternalDeploymentVersion Temporary helper function to convert WorkerDeploymentVersion to
// Deployment proto until we update code to use the new proto in all places.
func DeploymentFromExternalDeploymentVersion(dv *deploymentpb.WorkerDeploymentVersion) *deploymentpb.Deployment {
	if dv == nil {
		return nil
	}
	return &deploymentpb.Deployment{
		BuildId:    dv.GetBuildId(),
		SeriesName: dv.GetDeploymentName(),
	}
}

// DeploymentFromDeploymentVersion Temporary helper function to convert WorkerDeploymentVersion to
// Deployment proto until we update code to use the new proto in all places.
func DeploymentFromDeploymentVersion(dv *deploymentspb.WorkerDeploymentVersion) *deploymentpb.Deployment {
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
		return serviceerror.NewInvalidArgument("deployment name cannot be empty")
	}
	// TODO: remove '.' restriction once the v31 version strings are completely cleaned from external and internal API
	if strings.Contains(deployment.GetSeriesName(), WorkerDeploymentVersionIDDelimiterV31) ||
		strings.Contains(deployment.GetSeriesName(), WorkerDeploymentVersionDelimiter) {
		return serviceerror.NewInvalidArgumentf("deployment name cannot contain '%s' or '%s'", WorkerDeploymentVersionIDDelimiterV31, WorkerDeploymentVersionDelimiter)
	}
	if deployment.GetBuildId() == "" {
		return serviceerror.NewInvalidArgument("deployment build ID cannot be empty")
	}
	return nil
}

// ValidateDeploymentVersion returns error if the deployment version is not a valid entity.
func ValidateDeploymentVersion(version *deploymentspb.WorkerDeploymentVersion, maxIDLengthLimit int) error {
	if version == nil {
		return serviceerror.NewInvalidArgument("deployment version cannot be nil")
	}

	// Validate deployment name
	err := ValidateDeploymentVersionFields(WorkerDeploymentNameFieldName, version.GetDeploymentName(), maxIDLengthLimit)
	if err != nil {
		return err
	}

	// Validate build ID
	err = ValidateDeploymentVersionFields(WorkerDeploymentBuildIDFieldName, version.GetBuildId(), maxIDLengthLimit)
	if err != nil {
		return err
	}

	return nil
}

// ValidateDeploymentVersionFields is a helper that verifies if the fields within a
// Worker Deployment Version are valid
func ValidateDeploymentVersionFields(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgumentf("%v cannot be empty", fieldName)
	}

	// Length of each field should be: (MaxIDLengthLimit - (prefix + delimeter length)) / 2
	// Note: Using the same initial size for both the fields since they are used together to generate the version workflow's ID
	if len(field) > (maxIDLengthLimit-WorkerDeploymentVersionWorkflowIDInitialSize)/2 {
		return serviceerror.NewInvalidArgumentf("size of %v larger than the maximum allowed", fieldName)
	}

	// deploymentName cannot have "."
	// TODO: remove this restriction once the old version strings are completely cleaned from external and internal API
	if fieldName == WorkerDeploymentNameFieldName && strings.Contains(field, WorkerDeploymentVersionIDDelimiterV31) {
		return serviceerror.NewInvalidArgumentf("worker deployment name cannot contain '%s'", WorkerDeploymentVersionIDDelimiterV31)
	}
	// deploymentName cannot have ":"
	if fieldName == WorkerDeploymentNameFieldName && strings.Contains(field, WorkerDeploymentVersionDelimiter) {
		return serviceerror.NewInvalidArgumentf("worker deployment name cannot contain '%s'", WorkerDeploymentVersionDelimiter)
	}

	// buildID or deployment name cannot start with "__"
	if strings.HasPrefix(field, "__") {
		return serviceerror.NewInvalidArgumentf("%v cannot start with '__'", fieldName)
	}

	return nil
}

// ValidateDeploymentVersionStringV31 returns error if the deployment version is nil or it has empty version
// or deployment name.
func ValidateDeploymentVersionStringV31(version string) (*deploymentspb.WorkerDeploymentVersion, error) {
	if version == "" {
		return nil, serviceerror.NewInvalidArgument("version is required")
	}
	v, err := WorkerDeploymentVersionFromStringV31(version)
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("invalid version string %q, expected format is \"<deployment_name>.<build_id>\"", version)
	}
	return v, nil
}

func OverrideIsPinned(override *workflowpb.VersioningOverride) bool {
	//nolint:staticcheck // SA1019: worker versioning v0.31 and v0.30
	return override.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED ||
		override.GetPinned().GetBehavior() == workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED
}

func GetOverridePinnedVersion(override *workflowpb.VersioningOverride) *deploymentpb.WorkerDeploymentVersion {
	if OverrideIsPinned(override) {
		if v := override.GetPinned().GetVersion(); v != nil {
			return v
		} else if v := override.GetPinnedVersion(); v != "" { //nolint:staticcheck // SA1019: worker versioning v0.31
			return ExternalWorkerDeploymentVersionFromStringV31(v)
		}
		return ExternalWorkerDeploymentVersionFromDeployment(override.GetDeployment()) //nolint:staticcheck // SA1019: worker versioning v0.30
	}
	return nil
}
func ExtractVersioningBehaviorFromOverride(override *workflowpb.VersioningOverride) enumspb.VersioningBehavior {
	if override.GetAutoUpgrade() {
		return enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	} else if override.GetPinned() != nil {
		return enumspb.VERSIONING_BEHAVIOR_PINNED
	}

	//nolint:staticcheck // SA1019: worker versioning v0.31
	return override.GetBehavior()
}

func validatePinnedVersionInTaskQueue(ctx context.Context,
	pinnedVersion *deploymentpb.WorkerDeploymentVersion,
	matchingClient resource.MatchingClient,
	versionMembershipCache VersionMembershipCache,
	tq string,
	tqType enumspb.TaskQueueType,
	namespaceID string) error {

	// Check if we have recently queried matching to validate if this version exists in the task queue.
	if isMember, ok := versionMembershipCache.Get(
		namespaceID,
		tq,
		tqType,
		pinnedVersion.DeploymentName,
		pinnedVersion.BuildId,
	); ok {
		if isMember {
			return nil
		}
		return serviceerror.NewFailedPrecondition(
			"Pinned version is not present in the task queue",
		)
	}

	resp, err := matchingClient.CheckTaskQueueVersionMembership(ctx, &matchingservice.CheckTaskQueueVersionMembershipRequest{
		NamespaceId:   namespaceID,
		TaskQueue:     tq,
		TaskQueueType: tqType,
		Version:       DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(pinnedVersion)),
	})
	if err != nil {
		return err
	}

	// Add result to cache
	versionMembershipCache.Put(
		namespaceID,
		tq,
		tqType,
		pinnedVersion.DeploymentName,
		pinnedVersion.BuildId,
		resp.GetIsMember(),
	)
	if !resp.GetIsMember() {
		return serviceerror.NewFailedPrecondition(
			"Pinned version is not present in the task queue",
		)
	}
	return nil
}

func ValidateVersioningOverride(ctx context.Context,
	override *workflowpb.VersioningOverride,
	matchingClient resource.MatchingClient,
	versionMembershipCache VersionMembershipCache,
	tq string,
	tqType enumspb.TaskQueueType,
	namespaceID string) error {
	if override == nil {
		return nil
	}

	if override.GetAutoUpgrade() { // v0.32
		return nil
	} else if p := override.GetPinned(); p != nil {
		if p.GetVersion() == nil {
			return serviceerror.NewInvalidArgument("must provide version if override is pinned.")
		}
		if p.GetBehavior() == workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_UNSPECIFIED {
			return serviceerror.NewInvalidArgument("must specify pinned override behavior if override is pinned.")
		}
		return validatePinnedVersionInTaskQueue(ctx, p.GetVersion(), matchingClient, versionMembershipCache, tq, tqType, namespaceID)
	}

	//nolint:staticcheck // SA1019: worker versioning v0.31
	switch override.GetBehavior() {
	case enumspb.VERSIONING_BEHAVIOR_PINNED:
		if override.GetDeployment() != nil {
			return ValidateDeployment(override.GetDeployment())
		} else if override.GetPinnedVersion() != "" {
			_, err := ValidateDeploymentVersionStringV31(override.GetPinnedVersion())
			if err != nil {
				return err
			}

			return validatePinnedVersionInTaskQueue(ctx, ExternalWorkerDeploymentVersionFromStringV31(override.GetPinnedVersion()), matchingClient, versionMembershipCache, tq, tqType, namespaceID)

		} else {
			return serviceerror.NewInvalidArgument("must provide deployment (deprecated) or pinned version if behavior is 'PINNED'")
		}
	case enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE:
		if override.GetDeployment() != nil {
			return serviceerror.NewInvalidArgument("only provide deployment if behavior is 'PINNED'")
		}
		if override.GetPinnedVersion() != "" {
			return serviceerror.NewInvalidArgument("only provide pinned version if behavior is 'PINNED'")
		}
	case enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED:
		return serviceerror.NewInvalidArgument("override behavior is required")
	default:
		//nolint:staticcheck // SA1019 deprecated stamp will clean up later
		return serviceerror.NewInvalidArgumentf("override behavior %s not recognized", override.GetBehavior())
	}
	return nil
}

// FindTargetDeploymentVersionAndRevisionNumberForWorkflowID returns the deployment version and revision number (if applicable) for
// the particular workflow ID based on the versioning info of the task queue. Nil means unversioned.
func FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(
	current *deploymentspb.WorkerDeploymentVersion,
	currentRevisionNumber int64,
	ramping *deploymentspb.WorkerDeploymentVersion,
	rampingPercentage float32,
	rampingRevisionNumber int64,
	workflowId string,
) (*deploymentspb.WorkerDeploymentVersion, int64) {

	// Apply ramp logic using final values
	if rampingPercentage <= 0 {
		// No ramp
		return current, currentRevisionNumber
	} else if rampingPercentage == 100 {
		return ramping, rampingRevisionNumber
	}
	// Partial ramp. Decide based on workflow ID
	wfRampThreshold := calcRampThreshold(workflowId)
	if wfRampThreshold <= float64(rampingPercentage) {
		return ramping, rampingRevisionNumber
	}
	return current, currentRevisionNumber
}

// PickFinalCurrentAndRamping determines the effective "current" and "ramping" deployment versions
// by comparing timestamps from the legacy deployment data (old format) and the RoutingConfig (new format).
// It returns:
// - final current deployment version and its revision number (0 for old format)
// - final ramping deployment version, its revision number (0 for old format), and ramp percentage
//
//revive:disable-next-line:function-result-limit
func PickFinalCurrentAndRamping(
	current *deploymentspb.DeploymentVersionData,
	ramping *deploymentspb.DeploymentVersionData,
	currentVersionRoutingConfig *deploymentpb.RoutingConfig,
	rampingVersionRoutingConfig *deploymentpb.RoutingConfig,
) (
	finalCurrent *deploymentspb.WorkerDeploymentVersion,
	finalCurrentRev int64,
	finalCurrentUpdateTime time.Time,
	finalRamping *deploymentspb.WorkerDeploymentVersion,
	isRamping bool,
	finalRampPercentage float32,
	finalRampingRev int64,
	finalRampingUpdateTime time.Time,
) {
	// current: choose newer of old vs new format

	oldCurrentTime := current.GetRoutingUpdateTime().AsTime()
	newCurrentTime := currentVersionRoutingConfig.GetCurrentVersionChangedTime().AsTime()

	// Break ties by choosing the newer format
	if newCurrentTime.After(oldCurrentTime) || newCurrentTime.Equal(oldCurrentTime) {
		finalCurrent = DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(currentVersionRoutingConfig.GetCurrentDeploymentVersion()))
		finalCurrentRev = currentVersionRoutingConfig.GetRevisionNumber()
		finalCurrentUpdateTime = newCurrentTime
	} else {
		finalCurrent = current.GetVersion()
		finalCurrentRev = 0
		finalCurrentUpdateTime = oldCurrentTime
	}

	// ramping: choose newer of old vs new format; new format can change either version or percentage

	oldRampingTime := ramping.GetRoutingUpdateTime().AsTime()
	newRampingTime := rampingVersionRoutingConfig.GetRampingVersionPercentageChangedTime().AsTime()

	// Break ties by choosing the newer format

	if newRampingTime.After(oldRampingTime) || newRampingTime.Equal(oldRampingTime) {
		finalRamping = DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(rampingVersionRoutingConfig.GetRampingDeploymentVersion()))
		finalRampingRev = rampingVersionRoutingConfig.GetRevisionNumber()
		finalRampPercentage = rampingVersionRoutingConfig.GetRampingVersionPercentage()
		finalRampingUpdateTime = newRampingTime

		// When using the new deployment format, we do not have access to GetRampingSinceTime. Thus, we need to understand if a version is truly ramping or not.
		// When using the new deployment format, a version is *not ramping* if it has nil ramping version with ramping version percentage set to 0.

		if finalRamping == nil && finalRampPercentage == 0 {
			isRamping = false
		} else {
			isRamping = true
		}

	} else {
		finalRamping = ramping.GetVersion()
		finalRampingRev = 0
		finalRampPercentage = ramping.GetRampPercentage()
		finalRampingUpdateTime = oldRampingTime

		// A version can only be ramping if it has a rampingSinceTime.
		if ramping.GetRampingSinceTime() == nil {
			isRamping = false
		} else {
			isRamping = true
		}
	}

	return finalCurrent, finalCurrentRev, finalCurrentUpdateTime, finalRamping, isRamping, finalRampPercentage, finalRampingRev, finalRampingUpdateTime
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

// CalculateTaskQueueVersioningInfo calculates the current and ramping versioning info for a task queue.
//
//revive:disable-next-line:cognitive-complexity,confusing-results,function-result-limit,cyclomatic
func CalculateTaskQueueVersioningInfo(deployments *persistencespb.DeploymentData) (
	*deploymentspb.WorkerDeploymentVersion, // current version
	int64, // current revision number
	time.Time, // current update time
	*deploymentspb.WorkerDeploymentVersion, // ramping version
	bool, // is ramping (ramping_since_time != nil)
	float32, // ramp percentage
	int64, // ramping revision number
	time.Time, // ramping update time
) {
	if deployments == nil {
		return nil, 0, time.Time{}, nil, false, 0, 0, time.Time{}
	}

	var current *deploymentspb.DeploymentVersionData
	ramping := deployments.GetUnversionedRampData() // nil if there is no unversioned ramp

	// Find current and ramping
	// [cleanup-pp-wv]
	for _, v := range deployments.GetVersions() {
		if v.RoutingUpdateTime != nil && v.GetCurrentSinceTime() != nil {
			if t := v.RoutingUpdateTime.AsTime(); t.After(current.GetRoutingUpdateTime().AsTime()) {
				current = v
			}
		}
		if v.RoutingUpdateTime != nil && v.GetRampingSinceTime() != nil {
			if t := v.RoutingUpdateTime.AsTime(); t.After(ramping.GetRoutingUpdateTime().AsTime()) {
				ramping = v
			}
		}
	}

	// Find new current and ramping and pass information in DeploymentVersionData when returning to the caller to
	// preserve backwards compatibility.
	var routingConfigLatestCurrentVersion *deploymentpb.RoutingConfig
	var routingConfigLatestRampingVersion *deploymentpb.RoutingConfig

	isPartOfSomeCurrentVersion := false
	isPartOfSomeRampingVersion := false

	if deployments.GetDeploymentsData() != nil {

		for _, deploymentInfo := range deployments.GetDeploymentsData() {
			routingConfig := deploymentInfo.GetRoutingConfig()
			if routingConfig == nil {
				continue
			}

			// Only chose those RoutingConfigs which pass the HasDeploymentVersion check due to the following example case:
			// t0: TQ "foo" is in current version A with other TQ's
			// t1: All other TQ's are moved to new version B except for "foo".
			// t2: New version B is set as the current version.
			//
			// When this happens, we sync to "foo" that A is no longer the current version by passing in the new routing config. However,
			// version B should not be considered as the current version for "foo" because the task-queue is not part of version B.
			if t := routingConfig.GetCurrentVersionChangedTime().AsTime(); t.After(routingConfigLatestCurrentVersion.GetCurrentVersionChangedTime().AsTime()) {
				if HasDeploymentVersion(deployments, DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(routingConfig.GetCurrentDeploymentVersion()))) {
					routingConfigLatestCurrentVersion = routingConfig
					isPartOfSomeCurrentVersion = true
				} else if !isPartOfSomeCurrentVersion && routingConfig.GetCurrentDeploymentVersion() == nil {
					routingConfigLatestCurrentVersion = routingConfig
				}
			}

			if t := routingConfig.GetRampingVersionPercentageChangedTime().AsTime(); t.After(routingConfigLatestRampingVersion.GetRampingVersionPercentageChangedTime().AsTime()) {
				if HasDeploymentVersion(deployments, DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(routingConfig.GetRampingDeploymentVersion()))) {
					routingConfigLatestRampingVersion = routingConfig
					isPartOfSomeRampingVersion = true
				} else if !isPartOfSomeRampingVersion && routingConfig.GetRampingDeploymentVersion() == nil {
					routingConfigLatestRampingVersion = routingConfig
				}
			}
		}
	}

	if routingConfigLatestCurrentVersion.GetCurrentDeploymentVersion() == nil && current.GetVersion() != nil {
		// The new current version is not unversioned but belongs to a versioned deployment which synced to the task-queue using the old deployment data format.
		routingConfigLatestCurrentVersion = nil
	}

	if routingConfigLatestRampingVersion.GetRampingDeploymentVersion() == nil && ramping.GetVersion() != nil {
		// The new ramping version is not unversioned but belongs to a versioned deployment which synced to the task-queue using the old deployment data format.
		routingConfigLatestRampingVersion = nil
	}

	// Pick the final current and ramping version amongst the old and new deployment data formats.
	return PickFinalCurrentAndRamping(
		current,
		ramping,
		routingConfigLatestCurrentVersion,
		routingConfigLatestRampingVersion,
	)
}

func ValidateTaskVersionDirective(
	directive *taskqueuespb.TaskVersionDirective,
	wfBehavior enumspb.VersioningBehavior,
	wfDeployment *deploymentpb.Deployment,
	scheduledDeployment *deploymentpb.Deployment,
) error {
	// TODO: consider using activity and wft Stamp for simplifying validation here.

	// Effective behavior and deployment of the workflow when History scheduled the WFT.
	directiveBehavior := directive.GetBehavior()
	if directiveBehavior != wfBehavior &&
		// Verisoning 3 pre-release (v1.26, Dec 2024) is not populating request.VersionDirective so
		// we skip this check until v1.28 if directiveBehavior is unspecified.
		// TODO (shahab): remove this line after v1.27 is released.
		directiveBehavior != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// This must be a task scheduled before the workflow changes behavior. Matching can drop it.
		return serviceerrors.NewObsoleteMatchingTaskf(
			"task was scheduled when workflow had versioning behavior %s, now it has versioning behavior %s.",
			directiveBehavior, wfBehavior)
	}

	directiveDeployment := DirectiveDeployment(directive)
	if directiveDeployment == nil {
		// TODO: remove this once the ScheduledDeployment field is removed from proto
		directiveDeployment = scheduledDeployment
	}
	if !directiveDeployment.Equal(wfDeployment) {
		// This must be a task scheduled before the workflow transitions to the current
		// deployment. Matching can drop it.
		return serviceerrors.NewObsoleteMatchingTaskf(
			"task was scheduled when workflow was on build %s, now it is on build %s.",
			directiveDeployment.GetBuildId(), wfDeployment.GetBuildId())
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

// We store versioning info in the modern v0.32 format, so call this before returning the object to readers
// to mutatively populate the missing fields.
func AddV31VersioningInfoToV32(info *workflowpb.WorkflowExecutionVersioningInfo) *workflowpb.WorkflowExecutionVersioningInfo {
	if info == nil {
		return nil
	}
	//nolint:staticcheck // SA1019: worker versioning v0.31
	if info.Version == "" && info.DeploymentVersion != nil {
		//nolint:staticcheck // SA1019: worker versioning v0.31
		info.Version = ExternalWorkerDeploymentVersionToStringV31(info.DeploymentVersion)
	}
	if t := info.VersionTransition; t != nil {
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if t.Version == "" {
			//nolint:staticcheck // SA1019: worker versioning v0.31
			t.Version = ExternalWorkerDeploymentVersionToStringV31(t.DeploymentVersion)
		}
	}
	if o := info.VersioningOverride; o != nil {
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if o.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
			if o.GetAutoUpgrade() {
				//nolint:staticcheck // SA1019: worker versioning v0.31
				o.Behavior = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			} else if o.GetPinned() != nil {
				//nolint:staticcheck // SA1019: worker versioning v0.31
				o.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED
				//nolint:staticcheck // SA1019: worker versioning v0.31
				o.PinnedVersion = ExternalWorkerDeploymentVersionToStringV31(o.GetPinned().GetVersion())
			}
		}
	}
	return info
}

// ConvertOverrideToV32 reads from deprecated fields and returns a new object with ONLY the equivalent non-deprecated v0.32
// fields. Should be used to replace any passed in override that is stored in persistence.
func ConvertOverrideToV32(override *workflowpb.VersioningOverride) *workflowpb.VersioningOverride {
	if override == nil {
		return nil
	}
	ret := &workflowpb.VersioningOverride{
		Override: override.GetOverride(),
	}
	// populate v0.32 field with deprecated fields
	if ret.Override == nil {
		//nolint:staticcheck // SA1019: worker versioning v0.31
		switch override.GetBehavior() {
		case enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE:
			ret.Override = &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true}
		case enumspb.VERSIONING_BEHAVIOR_PINNED:
			ret.Override = &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				},
			}
			//nolint:staticcheck // SA1019: worker versioning v0.31
			if override.GetPinnedVersion() != "" {
				//nolint:staticcheck // SA1019: worker versioning v0.31
				ret.GetPinned().Version = ExternalWorkerDeploymentVersionFromStringV31(override.GetPinnedVersion())
			} else {
				//nolint:staticcheck // SA1019: worker versioning v0.30
				ret.GetPinned().Version = ExternalWorkerDeploymentVersionFromDeployment(override.GetDeployment())
			}
		case enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED:
			// this won't happen, but if it did, it makes some sense for unspecified behavior to cause a nil override
			return nil
		}
	}
	return ret
}

func WorkerDeploymentVersionToStringV31(v *deploymentspb.WorkerDeploymentVersion) string {
	if v == nil {
		return UnversionedVersionId
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionIDDelimiterV31 + v.GetBuildId()
}

func WorkerDeploymentVersionToStringV32(v *deploymentspb.WorkerDeploymentVersion) string {
	if v == nil {
		return ""
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionDelimiter + v.GetBuildId()
}

func BuildIDToStringV32(deploymentName, buildID string) string {
	return deploymentName + WorkerDeploymentVersionDelimiter + buildID
}

func ExternalWorkerDeploymentVersionToString(v *deploymentpb.WorkerDeploymentVersion) string {
	if v == nil {
		return ""
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionDelimiter + v.GetBuildId()
}

func ExternalWorkerDeploymentVersionToStringV31(v *deploymentpb.WorkerDeploymentVersion) string {
	if v == nil {
		return UnversionedVersionId
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionIDDelimiterV31 + v.GetBuildId()
}

func ExternalWorkerDeploymentVersionFromStringV31(s string) *deploymentpb.WorkerDeploymentVersion {
	if s == "" { // unset ramp is no longer supported in v32, so all empty version strings will be treated as unversioned.
		s = UnversionedVersionId
	}
	v, _ := WorkerDeploymentVersionFromStringV31(s)
	if v == nil {
		return nil
	}
	return &deploymentpb.WorkerDeploymentVersion{
		BuildId:        v.BuildId,
		DeploymentName: v.DeploymentName,
	}
}

func WorkerDeploymentVersionFromStringV31(s string) (*deploymentspb.WorkerDeploymentVersion, error) {
	if s == UnversionedVersionId {
		return nil, nil
	}
	before, after, found := strings.Cut(s, WorkerDeploymentVersionIDDelimiterV31)
	// Also try parsing via the v32 delimiter in case user is using an old CLI/SDK but passing new version strings.
	before32, after32, found32 := strings.Cut(s, WorkerDeploymentVersionDelimiter)
	if !found && !found32 {
		return nil, fmt.Errorf("expected delimiter '%s' or '%s' not found in version string %s", WorkerDeploymentVersionDelimiter, WorkerDeploymentVersionIDDelimiterV31, s)
	}
	if found && found32 && len(before32) < len(before) {
		// choose the values based on the delimiter appeared first to ensure that deployment name does not contain any of the banned delimiters
		before = before32
		after = after32
	}
	if len(before) == 0 {
		return nil, fmt.Errorf("deployment name is empty in version string %s", s)
	}
	if len(after) == 0 {
		return nil, fmt.Errorf("build id is empty in version string %s", s)
	}
	return &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: before,
		BuildId:        after,
	}, nil
}

func WorkerDeploymentVersionFromStringV32(s string) (*deploymentspb.WorkerDeploymentVersion, error) {
	if s == UnversionedVersionId {
		return nil, nil
	}
	before, after, found := strings.Cut(s, WorkerDeploymentVersionDelimiter)
	if !found {
		return nil, fmt.Errorf("expected delimiter '%s' not found in version string %s", WorkerDeploymentVersionDelimiter, s)
	}
	if len(before) == 0 {
		return nil, fmt.Errorf("deployment name is empty in version string %s", s)
	}
	if len(after) == 0 {
		return nil, fmt.Errorf("build id is empty in version string %s", s)
	}
	return &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: before,
		BuildId:        after,
	}, nil
}

// CleanupOldDeletedVersions removes versions deleted more than 7 days ago. Also removes more deleted versions if
// the limit is being exceeded. Never removes undeleted versions.
func CleanupOldDeletedVersions(deploymentData *persistencespb.WorkerDeploymentData, maxVersions int) bool {
	now := time.Now()
	aWeekAgo := now.Add(-time.Hour * 24 * 7)

	// Collect all deleted versions with their metadata
	type deletedVersion struct {
		buildID    string
		updateTime time.Time
	}
	var deletedVersions []deletedVersion
	undeletedCount := 0

	for buildID, versionData := range deploymentData.Versions {
		if versionData.GetDeleted() {
			deletedVersions = append(deletedVersions, deletedVersion{
				buildID:    buildID,
				updateTime: versionData.GetUpdateTime().AsTime(),
			})
		} else {
			undeletedCount++
		}
	}

	// Sort deleted versions by update time (oldest first)
	sort.Slice(deletedVersions, func(i, j int) bool {
		return deletedVersions[i].updateTime.Before(deletedVersions[j].updateTime)
	})

	cleaned := false
	totalCount := undeletedCount + len(deletedVersions)
	for _, dv := range deletedVersions {
		// Stop if:
		// 1. This version is not older than 7 days AND
		// 2. We're not exceeding the limit because of deleted versions
		if !dv.updateTime.Before(aWeekAgo) && totalCount <= maxVersions {
			break
		}

		// Remove this deleted version
		delete(deploymentData.Versions, dv.buildID)
		totalCount--
		cleaned = true
	}

	return cleaned
}
