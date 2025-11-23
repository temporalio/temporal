package deployment

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute/sadefs"
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

	// Prefixes, Delimiters and Keys
	DeploymentWorkflowIDPrefix       = "temporal-sys-deployment"
	DeploymentSeriesWorkflowIDPrefix = "temporal-sys-deployment-series"
	DeploymentWorkflowIDDelimiter    = ":"
	DeploymentWorkflowIDEscape       = "|"
	DeploymentWorkflowIDInitialSize  = (2 * len(DeploymentWorkflowIDDelimiter)) + len(DeploymentWorkflowIDPrefix)
	SeriesFieldName                  = "DeploymentSeries"
	BuildIDFieldName                 = "BuildID"

	// Application error names for rejected updates
	errNoChangeType                  = "errNoChange"
	errMaxTaskQueuesInDeploymentType = "errMaxTaskQueuesInDeployment"
)

var (
	DeploymentVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		sadefs.WorkflowType,
		DeploymentWorkflowType,
		sadefs.TemporalNamespaceDivision,
		DeploymentNamespaceDivision,
		sadefs.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)
)

// ValidateDeploymentWfParams is a helper that verifies if the fields used for generating
// deployment related workflowID's are valid
func ValidateDeploymentWfParams(fieldName string, field string, maxIDLengthLimit int) error {
	// Length checks
	if field == "" {
		return serviceerror.NewInvalidArgumentf("%v cannot be empty", fieldName)
	}

	// Length of each field should be: (MaxIDLengthLimit - prefix and delimiter length) / 2
	if len(field) > (maxIDLengthLimit-DeploymentWorkflowIDInitialSize)/2 {
		return serviceerror.NewInvalidArgumentf("size of %v larger than the maximum allowed", fieldName)
	}

	return nil
}

// EscapeChar is a helper which escapes the DeploymentWorkflowIDDelimiter character
// in the input string
func escapeChar(s string) string {
	s = strings.Replace(s, DeploymentWorkflowIDEscape, DeploymentWorkflowIDEscape+DeploymentWorkflowIDEscape, -1)
	s = strings.Replace(s, DeploymentWorkflowIDDelimiter, DeploymentWorkflowIDEscape+DeploymentWorkflowIDDelimeter, -1)
	return s
}

func GenerateDeploymentSeriesWorkflowID(deploymentSeriesName string) string {
	// escaping the reserved workflow delimiter (|) from the inputs, if present
	escapedSeriesName := escapeChar(deploymentSeriesName)
	return DeploymentSeriesWorkflowIDPrefix + DeploymentWorkflowIDDelimiter + escapedSeriesName
}

// GenerateDeploymentWorkflowID is a helper that generates a system accepted
// workflowID which are used in our deployment workflows
func GenerateDeploymentWorkflowID(seriesName string, buildID string) string {
	escapedSeriesName := escapeChar(seriesName)
	escapedBuildId := escapeChar(buildID)

	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimiter + escapedSeriesName + DeploymentWorkflowIDDelimiter + escapedBuildId
}

func GenerateDeploymentWorkflowIDForPatternMatching(seriesName string) string {
	escapedSeriesName := escapeChar(seriesName)

	return DeploymentWorkflowIDPrefix + DeploymentWorkflowIDDelimiter + escapedSeriesName + DeploymentWorkflowIDDelimiter
}

// BuildQueryWithSeriesFilter is a helper which builds a query for pattern matching based on the
// provided seriesName
func BuildQueryWithSeriesFilter(seriesName string) string {
	workflowID := GenerateDeploymentWorkflowIDForPatternMatching(seriesName)
	escapedSeriesEntry := sqlparser.String(sqlparser.NewStrVal([]byte(workflowID)))

	query := fmt.Sprintf("%s AND %s STARTS_WITH %s", DeploymentVisibilityBaseListQuery, sadefs.WorkflowID, escapedSeriesEntry)
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
