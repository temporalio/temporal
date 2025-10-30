package worker_versioning

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"

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
	"go.temporal.io/server/common/searchattribute"
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
	WorkerDeploymentVersionIdDelimiterV31      = "."
	WorkerDeploymentVersionIdDelimiter         = ":"
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
		if strings.Contains(d, WorkerDeploymentVersionIdDelimiter) || strings.Contains(d, WorkerDeploymentVersionIdDelimiterV31) {
			// TODO: allow '.' once we get rid of v31 stuff
			return nil, serviceerror.NewInvalidArgumentf("deployment name cannot contain '%s' or '%s'", WorkerDeploymentVersionIdDelimiter, WorkerDeploymentVersionIdDelimiterV31)
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
		resp, err := matchingClient.GetTaskQueueUserData(ctx,
			&matchingservice.GetTaskQueueUserDataRequest{
				NamespaceId:   namespaceID,
				TaskQueue:     tq,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			})
		if err != nil {
			return false, err
		}
		tqData, ok := resp.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)]
		if !ok {
			// The TQ is unversioned
			return false, nil
		}
		return HasDeploymentVersion(tqData.GetDeploymentData(), DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(version))), nil
	}
}

// [cleanup-wv-pre-release]
func FindDeployment(deployments *persistencespb.DeploymentData, deployment *deploymentpb.Deployment) int {
	for i, d := range deployments.GetDeployments() { //nolint:staticcheck // SA1019: worker versioning v0.30
		if d.Deployment.Equal(deployment) {
			return i
		}
	}
	return -1
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
	for _, d := range deployments.GetDeployments() {
		if d.Deployment.Equal(DeploymentFromDeploymentVersion(v)) {
			return true
		}
	}
	for _, vd := range deployments.GetVersions() {
		if proto.Equal(v, vd.GetVersion()) {
			return true
		}
	}
	return false
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
	if strings.Contains(deployment.GetSeriesName(), WorkerDeploymentVersionIdDelimiterV31) ||
		strings.Contains(deployment.GetSeriesName(), WorkerDeploymentVersionIdDelimiter) {
		return serviceerror.NewInvalidArgumentf("deployment name cannot contain '%s' or '%s'", WorkerDeploymentVersionIdDelimiterV31, WorkerDeploymentVersionIdDelimiter)
	}
	if deployment.GetBuildId() == "" {
		return serviceerror.NewInvalidArgument("deployment build ID cannot be empty")
	}
	return nil
}

// ValidateDeploymentVersion returns error if the deployment version is nil or it has empty version
// or deployment name.
func ValidateDeploymentVersion(version *deploymentspb.WorkerDeploymentVersion) error {
	if version == nil {
		return serviceerror.NewInvalidArgument("deployment version cannot be nil")
	}
	if version.GetDeploymentName() == "" {
		return serviceerror.NewInvalidArgument("deployment name cannot be empty")
	}
	if version.GetBuildId() == "" {
		return serviceerror.NewInvalidArgument("build id cannot be empty")
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

func ValidateVersioningOverride(override *workflowpb.VersioningOverride) error {
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
		return nil
	}

	//nolint:staticcheck // SA1019: worker versioning v0.31
	switch override.GetBehavior() {
	case enumspb.VERSIONING_BEHAVIOR_PINNED:
		if override.GetDeployment() != nil {
			return ValidateDeployment(override.GetDeployment())
		} else if override.GetPinnedVersion() != "" {
			_, err := ValidateDeploymentVersionStringV31(override.GetPinnedVersion())
			return err
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

// FindTargetDeploymentVersionAndRevisionNumberForWorkflowID returns the deployment version, revision number and a boolean
// particular workflow ID based on the versioning info of the task queue. Nil means unversioned.
// The boolean is true if the target deployment is new, false if the target deployment is old.
func FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(
	current *deploymentspb.DeploymentVersionData,
	ramping *deploymentspb.DeploymentVersionData,
	currentVersionRoutingConfig *deploymentpb.RoutingConfig,
	rampingVersionRoutingConfig *deploymentpb.RoutingConfig,
	workflowId string,
) (*deploymentspb.WorkerDeploymentVersion, int64, bool) {
	// Find most recent currentDeploymentVersion by comparing old vs new format timestamps
	var finalCurrentDep *deploymentspb.WorkerDeploymentVersion
	var finalCurrentRev int64
	var finalCurrentIsNew, finalRampingIsNew bool

	oldCurrentTime := current.GetRoutingUpdateTime().AsTime()
	newCurrentTime := currentVersionRoutingConfig.GetCurrentVersionChangedTime().AsTime()

	// TODO (Shivam): Discuss this with the team. Here, ties are broken in favor of the new format. What do we do?
	// Example: If activity TQ does not have a version at all, and the workflow TQ did have a version, we would use the new format and use revision number mechanics.
	// This breaks if the workflow TQ did not use revision number mechanics but the activity TQ does.
	if newCurrentTime.After(oldCurrentTime) || newCurrentTime.Equal(oldCurrentTime) {
		// New format current is more recent
		finalCurrentDep = DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(currentVersionRoutingConfig.GetCurrentDeploymentVersion()))
		finalCurrentRev = currentVersionRoutingConfig.GetRevisionNumber()
		finalCurrentIsNew = true
	} else {
		// Old format current is more recent
		finalCurrentDep = current.GetVersion()
		finalCurrentRev = 0
		finalCurrentIsNew = false
	}

	// Find most recent rampingDeploymentVersion by comparing old vs new format timestamps
	var finalRampingDep *deploymentspb.WorkerDeploymentVersion
	var finalRampingRev int64
	var finalRampPercentage float32

	oldRampingTime := ramping.GetRoutingUpdateTime().AsTime()
	newRampingTimeFromVersion := rampingVersionRoutingConfig.GetRampingVersionChangedTime().AsTime()
	newRampingTimeFromPercentage := rampingVersionRoutingConfig.GetRampingVersionPercentageChangedTime().AsTime()

	// Choose the most recent timestamp between the two fields
	newRampingTime := newRampingTimeFromVersion
	if newRampingTimeFromPercentage.After(newRampingTimeFromVersion) {
		newRampingTime = newRampingTimeFromPercentage
	}

	if newRampingTime.After(oldRampingTime) || newRampingTime.Equal(oldRampingTime) {
		// New format ramping is more recent
		finalRampingDep = DeploymentVersionFromDeployment(DeploymentFromExternalDeploymentVersion(rampingVersionRoutingConfig.GetRampingDeploymentVersion()))
		finalRampingRev = rampingVersionRoutingConfig.GetRevisionNumber()
		finalRampPercentage = rampingVersionRoutingConfig.GetRampingVersionPercentage()
		finalRampingIsNew = true
	} else {
		// Old format ramping is more recent
		finalRampingDep = ramping.GetVersion()
		finalRampingRev = 0
		finalRampPercentage = ramping.GetRampPercentage()
		finalRampingIsNew = false
	}

	// Apply ramp logic using final values
	if finalRampPercentage <= 0 {
		// No ramp
		return finalCurrentDep, finalCurrentRev, finalCurrentIsNew
	} else if finalRampPercentage == 100 {
		return finalRampingDep, finalRampingRev, finalRampingIsNew
	}
	// Partial ramp. Decide based on workflow ID
	wfRampThreshold := calcRampThreshold(workflowId)
	if wfRampThreshold <= float64(finalRampPercentage) {
		return finalRampingDep, finalRampingRev, finalRampingIsNew
	}
	return finalCurrentDep, finalCurrentRev, finalCurrentIsNew
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
func CalculateTaskQueueVersioningInfo(deployments *persistencespb.DeploymentData) (*deploymentspb.DeploymentVersionData, *deploymentspb.DeploymentVersionData, *deploymentpb.RoutingConfig, *deploymentpb.RoutingConfig) {
	if deployments == nil {
		return nil, nil, nil, nil
	}

	var current *deploymentspb.DeploymentVersionData
	ramping := deployments.GetUnversionedRampData() // nil if there is no unversioned ramp

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

	if deployments.GetDeploymentsData() != nil {

		for _, deploymentInfo := range deployments.GetDeploymentsData() {
			routingConfig := deploymentInfo.GetRoutingConfig()
			if routingConfig == nil {
				continue
			}

			// fmt.Println("The revision number is ", routingConfig.GetRevisionNumber())
			// fmt.Println("The current version is ", routingConfig.GetCurrentDeploymentVersion())
			// fmt.Println("The ramping version is ", routingConfig.GetRampingDeploymentVersion())

			// Choose current/ramping based on the deployment having the most recent routingConfig update time.
			if t := routingConfig.GetCurrentVersionChangedTime().AsTime(); t.After(routingConfigLatestCurrentVersion.GetCurrentVersionChangedTime().AsTime()) {
				routingConfigLatestCurrentVersion = routingConfig
			}

			if t := routingConfig.GetRampingVersionChangedTime().AsTime(); t.After(routingConfigLatestRampingVersion.GetRampingVersionChangedTime().AsTime()) {
				routingConfigLatestRampingVersion = routingConfig
			} else if t := routingConfig.GetRampingVersionPercentageChangedTime().AsTime(); t.After(routingConfigLatestRampingVersion.GetRampingVersionPercentageChangedTime().AsTime()) {
				routingConfigLatestRampingVersion = routingConfig
			}
		}

		// if routingConfigLatestCurrentVersion != nil {
		// 	current = &deploymentspb.DeploymentVersionData{
		// 		Version: &deploymentspb.WorkerDeploymentVersion{
		// 			DeploymentName: routingConfigLatestCurrentVersion.GetCurrentDeploymentVersion().GetDeploymentName(),
		// 			BuildId:        routingConfigLatestCurrentVersion.GetCurrentDeploymentVersion().GetBuildId(),
		// 		},
		// 		RoutingUpdateTime: routingConfigLatestCurrentVersion.GetCurrentVersionChangedTime(),
		// 		CurrentSinceTime:  routingConfigLatestCurrentVersion.GetCurrentVersionChangedTime(), // TODO (Shivam): We may not need this field in our internal protos now.
		// 		Status:            enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,                 //  TODO (Shivam): We may not need this field in our internal protos now.
		// 	}
		// }

		// if routingConfigLatestRampingVersion != nil {
		// 	// Choosing the max of the two timestamp fields to get the most recent update time.
		// 	rampingUpdateTime := routingConfigLatestRampingVersion.GetRampingVersionChangedTime()
		// 	if routingConfigLatestRampingVersion.GetRampingVersionPercentageChangedTime().AsTime().After(rampingUpdateTime.AsTime()) {
		// 		rampingUpdateTime = routingConfigLatestRampingVersion.GetRampingVersionPercentageChangedTime()
		// 	}

		// 	var version *deploymentspb.WorkerDeploymentVersion
		// 	if routingConfigLatestRampingVersion.GetRampingDeploymentVersion() != nil {
		// 		version = &deploymentspb.WorkerDeploymentVersion{
		// 			DeploymentName: routingConfigLatestRampingVersion.GetRampingDeploymentVersion().GetDeploymentName(),
		// 			BuildId:        routingConfigLatestRampingVersion.GetRampingDeploymentVersion().GetBuildId(),
		// 		}
		// 	}

		// 	ramping = &deploymentspb.DeploymentVersionData{
		// 		Version:           version,
		// 		RoutingUpdateTime: rampingUpdateTime,
		// 		RampingSinceTime:  routingConfigLatestRampingVersion.GetRampingVersionChangedTime(), // TODO (Shivam): We may not need this field in our internal protos now.
		// 		RampPercentage:    routingConfigLatestRampingVersion.GetRampingVersionPercentage(),
		// 		Status:            enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, //  TODO (Shivam): We may not need this field in our internal protos now.
		// 	}
		// }
	}

	return current, ramping, routingConfigLatestCurrentVersion, routingConfigLatestRampingVersion
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
	return v.GetDeploymentName() + WorkerDeploymentVersionIdDelimiterV31 + v.GetBuildId()
}

func WorkerDeploymentVersionToStringV32(v *deploymentspb.WorkerDeploymentVersion) string {
	if v == nil {
		return ""
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionIdDelimiter + v.GetBuildId()
}

func ExternalWorkerDeploymentVersionToString(v *deploymentpb.WorkerDeploymentVersion) string {
	if v == nil {
		return ""
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionIdDelimiter + v.GetBuildId()
}

func ExternalWorkerDeploymentVersionToStringV31(v *deploymentpb.WorkerDeploymentVersion) string {
	if v == nil {
		return UnversionedVersionId
	}
	return v.GetDeploymentName() + WorkerDeploymentVersionIdDelimiterV31 + v.GetBuildId()
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
	before, after, found := strings.Cut(s, WorkerDeploymentVersionIdDelimiterV31)
	// Also try parsing via the v32 delimiter in case user is using an old CLI/SDK but passing new version strings.
	before32, after32, found32 := strings.Cut(s, WorkerDeploymentVersionIdDelimiter)
	if !found && !found32 {
		return nil, fmt.Errorf("expected delimiter '%s' or '%s' not found in version string %s", WorkerDeploymentVersionIdDelimiter, WorkerDeploymentVersionIdDelimiterV31, s)
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
	before, after, found := strings.Cut(s, WorkerDeploymentVersionIdDelimiter)
	if !found {
		return nil, fmt.Errorf("expected delimiter '%s' not found in version string %s", WorkerDeploymentVersionIdDelimiter, s)
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

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment workflows
func GenerateDeploymentWorkflowID(deploymentName string) string {
	return WorkerDeploymentWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + deploymentName
}

func GetDeploymentNameFromWorkflowID(workflowID string) string {
	_, deploymentName, _ := strings.Cut(workflowID, WorkerDeploymentVersionWorkflowIDDelimeter)
	return deploymentName
}

// GenerateVersionWorkflowID is a helper that generates a system accepted
// workflowID which are used in our Worker Deployment Version workflows
func GenerateVersionWorkflowID(deploymentName string, buildID string) string {
	versionString := ExternalWorkerDeploymentVersionToString(&deploymentpb.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	})
	return WorkerDeploymentVersionWorkflowIDPrefix + WorkerDeploymentVersionWorkflowIDDelimeter + versionString
}
