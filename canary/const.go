package canary

import (
	"time"

	"go.temporal.io/temporal/workflow"
)

// global constants
const (
	workflowRetentionDays         = int32(1)
	activityWorkerMaxExecutors    = 256
	scheduleToStartTimeout        = 3 * time.Minute
	decisionTaskTimeout           = 10 * time.Second
	activityTaskTimeout           = 3 * time.Minute
	childWorkflowTimeout          = 6 * time.Minute
	taskListName                  = "canary-task-queue"
	ctxKeyActivityRuntime         = "runtime"
	ctxKeyActivityArchivalRuntime = "runtime-archival"
	ctxKeyActivitySystemClient    = "system-client"
	archivalNamespace             = "canary-archival-namespace"
	systemNamespace               = "temporal-system"
	archivalTaskListName          = "canary-archival-task-queue"
)

// workflowVersion represents the current version of every single
// workflow function in this canary. Every workflow function verifies
// that the decision task it is executing is compatible with this version
// Bump this version whenever a backward incompatible change for any workflow
// also see beingWorkflow function
const workflowVersion = workflow.Version(3)
const workflowChangeID = "initial version"

const (
	cronJobTimeout         = 9 * time.Minute
	cronWFExecutionTimeout = 18 * time.Minute
)

// wfType/activityType refers to the friendly short names given to
// workflows and activities - at the time of registration, these names
// will be used to associate with a workflow or activity function
const (
	wfTypeCron                 = "workflow.cron"
	wfTypeSanity               = "workflow.sanity"
	wfTypeEcho                 = "workflow.echo"
	wfTypeSignal               = "workflow.signal"
	wfTypeSignalExternal       = "workflow.signal.external"
	wfTypeVisibility           = "workflow.visibility"
	wfTypeSearchAttributes     = "workflow.searchAttributes"
	wfTypeConcurrentExec       = "workflow.concurrent-execution"
	wfTypeQuery                = "workflow.query"
	wfTypeTimeout              = "workflow.timeout"
	wfTypeLocalActivity        = "workflow.localactivity"
	wfTypeCancellation         = "workflow.cancellation"
	wfTypeCancellationExternal = "workflow.cancellation.external"
	wfTypeRetry                = "workflow.retry"
	wfTypeResetBase            = "workflow.reset.base"
	wfTypeReset                = "workflow.reset"
	wfTypeHistoryArchival      = "workflow.archival.history"
	wfTypeVisibilityArchival   = "workflow.archival.visibility"
	wfTypeArchivalExternal     = "workflow.archival.external"
	wfTypeBatch                = "workflow.batch"
	wfTypeBatchParent          = "workflow.batch.parent"
	wfTypeBatchChild           = "workflow.batch.child"

	activityTypeEcho               = "activity.echo"
	activityTypeCron               = "activity.cron"
	activityTypeSignal             = "activity.signal"
	activityTypeVisibility         = "activity.visibility"
	activityTypeSearchAttributes   = "activity.searchAttributes"
	activityTypeConcurrentExec     = "activity.concurrent-execution"
	activityTypeQuery1             = "activity.query1"
	activityTypeQuery2             = "activity.query2"
	activityTypeTimeout            = "activity.timeout"
	activityTypeCancellation       = "activity.cancellation"
	activityTypeCancellationChild  = "activity.cancellation.child"
	activityTypeRetryOnTimeout     = "activity.retry-on-timeout"
	activityTypeRetryOnFailure     = "activity.retry-on-failure"
	activityTypeTriggerReset       = "activity.reset.trigger"
	activityTypeVerifyReset        = "activity.reset.verify"
	activityTypeResetBase          = "activity.reset.base"
	activityTypeHistoryArchival    = "activity.archival.history"
	activityTypeVisibilityArchival = "activity.archival.visibility"
	activityTypeLargeResult        = "activity.largeResult"
	activityTypeVerifyBatch        = "activity.batch.verify"
	activityTypeStartBatch         = "activity.batch.start.batch"
)
