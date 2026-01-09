package sadefs

import (
	"fmt"
	"maps"
	"regexp"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

const (
	NamespaceID           = "NamespaceId"
	WorkflowID            = "WorkflowId"
	RunID                 = "RunId"
	WorkflowType          = "WorkflowType"
	StartTime             = "StartTime"
	ExecutionTime         = "ExecutionTime"
	CloseTime             = "CloseTime"
	ExecutionStatus       = "ExecutionStatus"
	TaskQueue             = "TaskQueue"
	HistoryLength         = "HistoryLength"
	ExecutionDuration     = "ExecutionDuration"
	StateTransitionCount  = "StateTransitionCount"
	TemporalChangeVersion = "TemporalChangeVersion"
	BinaryChecksums       = "BinaryChecksums"
	BatcherNamespace      = "BatcherNamespace"
	BatcherUser           = "BatcherUser"
	HistorySizeBytes      = "HistorySizeBytes"
	ParentWorkflowID      = "ParentWorkflowId"
	ParentRunID           = "ParentRunId"
	RootWorkflowID        = "RootWorkflowId"
	RootRunID             = "RootRunId"

	TemporalNamespaceDivision = "TemporalNamespaceDivision"

	// These fields are not in Elasticsearch mappings definition and therefore are not indexed.
	MemoEncoding      = "MemoEncoding"
	Memo              = "Memo"
	VisibilityTaskKey = "VisibilityTaskKey"

	// Added to workflows started by a schedule.
	TemporalScheduledStartTime = "TemporalScheduledStartTime"
	TemporalScheduledById      = "TemporalScheduledById"

	// Used by scheduler workflow.
	TemporalSchedulePaused = "TemporalSchedulePaused"

	ReservedPrefix = "Temporal"

	// Query clause that mentions TemporalNamespaceDivision to disable special handling of that
	// search attribute in visibility.
	matchAnyNamespaceDivision = TemporalNamespaceDivision + ` IS NULL OR ` + TemporalNamespaceDivision + ` IS NOT NULL`

	// A user may specify a ScheduleID in a query even if a ScheduleId search attribute isn't defined for the namespace.
	// In such a case, ScheduleId is effectively a fake search attribute. Of course, a user may optionally choose to
	// define a custom ScheduleId search attribute, in which case the query using the ScheduleId would operate just like
	// any other custom search attribute.
	ScheduleID = "ScheduleId"

	// TODO: Remove this hardcoded constant.
	ActivityID = "ActivityId"

	// TemporalPauseInfo is a search attribute that stores the information about paused entities in the workflow.
	// Format of a single paused entity: "<key>:<value>".
	//  * <key> is something that can be used to identify the filtering condition
	//  * <value> is the value of the corresponding filtering condition.
	// examples:
	//   - for paused activities, manual pause, we may have 2 <key>:<value> pairs:
	//     * "Activity:MyCoolActivityType"
	//     * "Reason:ManualActivityPause"
	//     * or
	//     * "Policy:<some policy id>"
	//   - for paused workflows, we may have the following <key>:<value> pairs:
	//     * "Workflow:WorkflowID"
	//     * "Reason:ManualWorkflowPause"
	TemporalPauseInfo = "TemporalPauseInfo"

	// BuildIds is a KeywordList that holds information about current and past build ids
	// used by the workflow. Used for Worker Versioning
	// This SA is deprecated as of Versioning GA. Instead, users should use one of the following SAs for different purposes:
	// TemporalWorkflowVersioningBehavior, TemporalWorkerDeployment, TemporalWorkerDeploymentVersion, TemporalUsedWorkerDeploymentVersions.
	BuildIds = "BuildIds"

	// TemporalWorkerDeploymentVersion stores the current Worker Deployment Version
	// associated with the execution. It is updated at workflow task completion when
	// the SDK says what Version completed the workflow task. It can have a value for
	// unversioned workflows, if they are processed by an unversioned deployment.
	TemporalWorkerDeploymentVersion = "TemporalWorkerDeploymentVersion"

	// TemporalWorkerDeployment stores the current Worker Deployment associated with
	// the execution. It is updated at workflow task completion when the SDK says what
	// Worker Deployment completed the workflow task. It can have a value for
	// unversioned workflows, if they are processed by an unversioned deployment.
	TemporalWorkerDeployment = "TemporalWorkerDeployment"

	// TemporalWorkflowVersioningBehavior stores the current Versioning Behavior of the
	// execution. It is updated at workflow task completion when the server gets the
	// behavior (`auto_upgrade` or `pinned`) from the SDK. Empty for unversioned workflows.
	TemporalWorkflowVersioningBehavior = "TemporalWorkflowVersioningBehavior"

	// TemporalUsedWorkerDeploymentVersions is a KeywordList that holds all Worker Deployment
	// Versions that have completed workflow tasks for this workflow execution. Used for tracking
	// deployment version usage history. Format: "<deployment_name>:<build_id>" per entry.
	TemporalUsedWorkerDeploymentVersions = "TemporalUsedWorkerDeploymentVersions"

	// TemporalReportedProblems is a search attribute that stores the information about problems
	// the workflow has encountered in making progress. It is updated after successive workflow task
	// failures with the last workflow task failure cause. After the workflow task is completed
	// successfully, the search attribute is removed. Format of a single problem:
	// "category=<category> cause=<cause>".
	TemporalReportedProblems = "TemporalReportedProblems"
)

var (
	// system are internal search attributes which are passed and stored as separate fields.
	system = map[string]enumspb.IndexedValueType{
		WorkflowID:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		RunID:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		WorkflowType:         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		StartTime:            enumspb.INDEXED_VALUE_TYPE_DATETIME,
		ExecutionTime:        enumspb.INDEXED_VALUE_TYPE_DATETIME,
		CloseTime:            enumspb.INDEXED_VALUE_TYPE_DATETIME,
		ExecutionStatus:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TaskQueue:            enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		HistoryLength:        enumspb.INDEXED_VALUE_TYPE_INT,
		ExecutionDuration:    enumspb.INDEXED_VALUE_TYPE_INT,
		StateTransitionCount: enumspb.INDEXED_VALUE_TYPE_INT,
		HistorySizeBytes:     enumspb.INDEXED_VALUE_TYPE_INT,
		ParentWorkflowID:     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		ParentRunID:          enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		RootWorkflowID:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		RootRunID:            enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}

	// predefinedWhiteList contains a subset of predefined Search Attributes (SAs)
	// that are currently allowed for use in production environments. These attributes
	// are internal and were not originally intended for end-user usage, but may be
	// in active use by users at the moment.
	//
	// The long-term plan is to deprecate and disallow the use of these attributes
	// once it is confirmed that they are no longer being relied upon in any
	// production workflows. Until then, this whitelist acts as a temporary allowance
	// to ensure backward compatibility and avoid breaking existing use cases.
	predefinedWhiteList = map[string]enumspb.IndexedValueType{
		TemporalChangeVersion:      enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		BinaryChecksums:            enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		BuildIds:                   enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		BatcherNamespace:           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BatcherUser:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalScheduledStartTime: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		TemporalScheduledById:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalSchedulePaused:     enumspb.INDEXED_VALUE_TYPE_BOOL,
		TemporalNamespaceDivision:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalPauseInfo:          enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		TemporalReportedProblems:   enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	}

	// predefined are internal search attributes which are passed and stored in SearchAttributes object together with custom search attributes.
	// Attributes listed here but not in predefinedWhiteList are considered internal-only and are banned from user-facing usage.
	predefined = map[string]enumspb.IndexedValueType{
		TemporalChangeVersion: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		BinaryChecksums:       enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		// This SA is deprecated as of Versioning GA. Instead, users should use one of the following SAs for different purposes:
		// TemporalWorkflowVersioningBehavior, TemporalWorkerDeployment, TemporalWorkerDeploymentVersion, TemporalUsedWorkerDeploymentVersions.
		BuildIds:                             enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		BatcherNamespace:                     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		BatcherUser:                          enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalScheduledStartTime:           enumspb.INDEXED_VALUE_TYPE_DATETIME,
		TemporalScheduledById:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalSchedulePaused:               enumspb.INDEXED_VALUE_TYPE_BOOL,
		TemporalNamespaceDivision:            enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalPauseInfo:                    enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		TemporalReportedProblems:             enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		TemporalWorkerDeploymentVersion:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalWorkflowVersioningBehavior:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalWorkerDeployment:             enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		TemporalUsedWorkerDeploymentVersions: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	}

	// reserved are internal field names that can't be used as search attribute names.
	reserved = map[string]struct{}{
		NamespaceID:  {},
		MemoEncoding: {},
		Memo:         {},
		// Used in the Elasticsearch bulk processor, not needed in SQL databases.
		VisibilityTaskKey: {},
	}

	sqlDbSystemNameToColName = map[string]string{
		NamespaceID:          "namespace_id",
		WorkflowID:           "workflow_id",
		RunID:                "run_id",
		WorkflowType:         "workflow_type_name",
		StartTime:            "start_time",
		ExecutionTime:        "execution_time",
		CloseTime:            "close_time",
		ExecutionStatus:      "status",
		TaskQueue:            "task_queue",
		HistoryLength:        "history_length",
		HistorySizeBytes:     "history_size_bytes",
		ExecutionDuration:    "execution_duration",
		StateTransitionCount: "state_transition_count",
		Memo:                 "memo",
		MemoEncoding:         "encoding",
		ParentWorkflowID:     "parent_workflow_id",
		ParentRunID:          "parent_run_id",
		RootWorkflowID:       "root_workflow_id",
		RootRunID:            "root_run_id",
	}

	defaultNumDBCustomSearchAttributes = map[enumspb.IndexedValueType]int{
		enumspb.INDEXED_VALUE_TYPE_BOOL:         3,
		enumspb.INDEXED_VALUE_TYPE_INT:          3,
		enumspb.INDEXED_VALUE_TYPE_DOUBLE:       3,
		enumspb.INDEXED_VALUE_TYPE_DATETIME:     3,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD:      10,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST: 3,
		enumspb.INDEXED_VALUE_TYPE_TEXT:         3,
	}

	dbCustomSearchAttributeFieldNameRE = func() map[enumspb.IndexedValueType]*regexp.Regexp {
		res := map[enumspb.IndexedValueType]*regexp.Regexp{}
		for t := range defaultNumDBCustomSearchAttributes {
			res[t] = regexp.MustCompile(fmt.Sprintf(`^%s(0[1-9]|[1-9][0-9])$`, t.String()))
		}
		return res
	}()
)

// System returns a clone of the system search attributes map.
func System() map[string]enumspb.IndexedValueType {
	return maps.Clone(system)
}

// Predefined returns a clone of the predefined search attributes map.
func Predefined() map[string]enumspb.IndexedValueType {
	return maps.Clone(predefined)
}

// PredefinedWhiteList returns a clone of the predefined whitelist search attributes map.
func PredefinedWhiteList() map[string]enumspb.IndexedValueType {
	return maps.Clone(predefinedWhiteList)
}

// Reserved returns a clone of the reserved field names map.
func Reserved() map[string]struct{} {
	return maps.Clone(reserved)
}

// IsSystem returns true if name is system search attribute
func IsSystem(name string) bool {
	_, ok := system[name]
	return ok
}

// IsReserved returns true if name is system reserved and can't be used as custom search attribute name.
func IsReserved(name string) bool {
	if _, ok := system[name]; ok {
		return true
	}
	if _, ok := predefined[name]; ok {
		return true
	}
	if _, ok := reserved[name]; ok {
		return true
	}
	return strings.HasPrefix(name, ReservedPrefix)
}

// IsMappable returns true if name can be mapped to the alias.
// Mappable search attributes are those that can be defined by the user.
func IsMappable(name string) bool {
	if _, ok := system[name]; ok {
		return false
	}
	if _, ok := predefined[name]; ok {
		return false
	}
	return true
}

// GetSqlDbColName maps system and reserved search attributes to column names for SQL tables.
// If the input is not a system or reserved search attribute, then it returns the input.
func GetSqlDbColName(name string) string {
	if fieldName, ok := sqlDbSystemNameToColName[name]; ok {
		return fieldName
	}
	return name
}

func GetDBIndexSearchAttributes(
	override map[enumspb.IndexedValueType]int,
) *persistencespb.IndexSearchAttributes {
	csa := map[string]enumspb.IndexedValueType{}
	for saType, defaultNumAttrs := range defaultNumDBCustomSearchAttributes {
		numAttrs := defaultNumAttrs
		if value, ok := override[saType]; ok {
			numAttrs = value
		}
		for i := range numAttrs {
			csa[fmt.Sprintf("%s%02d", saType.String(), i+1)] = saType
		}
	}
	return &persistencespb.IndexSearchAttributes{
		CustomSearchAttributes: csa,
	}
}

func IsPreallocatedCSAFieldName(name string, valueType enumspb.IndexedValueType) bool {
	re := dbCustomSearchAttributeFieldNameRE[valueType]
	return re != nil && re.MatchString(name)
}

var chasmSearchAttributePattern = regexp.MustCompile(`^Temporal(Bool|Datetime|Int|Double|Text|Keyword|LowCardinalityKeyword|KeywordList)(0[1-9]|[1-9][0-9])$`)

// IsChasmSearchAttribute checks if a field name matches the pattern for CHASM search attributes.
// CHASM search attributes follow the pattern: Temporal<Type><NN> where NN is 01-99
// Examples: TemporalInt01, TemporalDatetime02, TemporalDouble01, etc.
func IsChasmSearchAttribute(name string) bool {
	if !strings.HasPrefix(name, ReservedPrefix) {
		return false
	}
	return chasmSearchAttributePattern.MatchString(name)
}

// QueryWithAnyNamespaceDivision returns a modified workflow visibility query that disables
// special handling of namespace division and so matches workflows in all namespace divisions.
// Normally a query that didn't explicitly mention TemporalNamespaceDivision would be limited
// to the default (empty string) namespace division.
func QueryWithAnyNamespaceDivision(query string) string {
	if strings.TrimSpace(query) == "" {
		return matchAnyNamespaceDivision
	}
	return fmt.Sprintf(`(%s) AND (%s)`, query, matchAnyNamespaceDivision)
}
