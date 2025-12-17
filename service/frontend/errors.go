package frontend

import (
	"go.temporal.io/api/serviceerror"
)

var (
	errInvalidTaskToken                                   = serviceerror.NewInvalidArgument("Invalid TaskToken.")
	errDeserializingToken                                 = serviceerror.NewInvalidArgument("Error deserializing task token.")
	errTaskQueueNotSet                                    = serviceerror.NewInvalidArgument("TaskQueue is not set on request.")
	errExecutionNotSet                                    = serviceerror.NewInvalidArgument("Execution is not set on request.")
	errWorkflowIDNotSet                                   = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errActivityIDNotSet                                   = serviceerror.NewInvalidArgument("ActivityId is not set on request.")
	errActivityIDOrTypeNotSet                             = serviceerror.NewInvalidArgument("Either Activity.Id or Activity.Type should be set on request.")
	errSignalNameNotSet                                   = serviceerror.NewInvalidArgument("SignalName is not set on request.")
	errInvalidRunID                                       = serviceerror.NewInvalidArgument("Invalid RunId.")
	errInvalidNextPageToken                               = serviceerror.NewInvalidArgument("Invalid NextPageToken.")                                 // DEPRECATED
	errNextPageTokenRunIDMismatch                         = serviceerror.NewInvalidArgument("RunId in the request does not match the NextPageToken.") // DEPRECATED
	errQueryNotSet                                        = serviceerror.NewInvalidArgument("WorkflowQuery is not set on request.")
	errQueryTypeNotSet                                    = serviceerror.NewInvalidArgument("QueryType is not set on request.")
	errRequestNotSet                                      = serviceerror.NewInvalidArgument("Request is nil.")
	errRequestIDNotSet                                    = serviceerror.NewInvalidArgument("RequestId is not set on request.")
	errWorkflowTypeNotSet                                 = serviceerror.NewInvalidArgument("WorkflowType is not set on request.")
	errInvalidWorkflowExecutionTimeoutSeconds             = serviceerror.NewInvalidArgument("An invalid WorkflowExecutionTimeoutSeconds is set on request.")
	errInvalidWorkflowRunTimeoutSeconds                   = serviceerror.NewInvalidArgument("An invalid WorkflowRunTimeoutSeconds is set on request.")
	errInvalidWorkflowTaskTimeoutSeconds                  = serviceerror.NewInvalidArgument("An invalid WorkflowTaskTimeoutSeconds is set on request.")
	errQueryDisallowedForNamespace                        = serviceerror.NewInvalidArgument("Namespace is not allowed to query, please contact temporal team to re-enable queries.")
	errClusterNameNotSet                                  = serviceerror.NewInvalidArgument("Cluster name is not set.")
	errEmptyReplicationInfo                               = serviceerror.NewInvalidArgument("Replication task info is not set.")
	errTaskRangeNotSet                                    = serviceerror.NewInvalidArgument("Task range is not set")
	errHistoryNotFound                                    = serviceerror.NewInvalidArgument("Requested workflow history not found, may have passed retention period.")
	errNamespaceTooLong                                   = serviceerror.NewInvalidArgument("Namespace length exceeds limit.")
	errWorkflowTypeTooLong                                = serviceerror.NewInvalidArgument("WorkflowType length exceeds limit.")
	errWorkflowIDTooLong                                  = serviceerror.NewInvalidArgument("WorkflowId length exceeds limit.")
	errWorkflowRuleIDTooLong                              = serviceerror.NewInvalidArgument("Workflow Rule Id length exceeds limit.")
	errSignalNameTooLong                                  = serviceerror.NewInvalidArgument("SignalName length exceeds limit.")
	errTaskQueueTooLong                                   = serviceerror.NewInvalidArgument("TaskQueue length exceeds limit.")
	errRequestIDTooLong                                   = serviceerror.NewInvalidArgument("RequestId length exceeds limit.")
	errIdentityTooLong                                    = serviceerror.NewInvalidArgument("Identity length exceeds limit.")
	errNotesTooLong                                       = serviceerror.NewInvalidArgument("Schedule notes exceeds limit.")
	errEarliestTimeIsGreaterThanLatestTime                = serviceerror.NewInvalidArgument("EarliestTime in StartTimeFilter should not be larger than LatestTime.")
	errClusterIsNotConfiguredForVisibilityArchival        = serviceerror.NewInvalidArgument("Cluster is not configured for visibility archival.")
	errClusterIsNotConfiguredForReadingArchivalVisibility = serviceerror.NewInvalidArgument("Cluster is not configured for reading archived visibility records.")
	errNamespaceIsNotConfiguredForVisibilityArchival      = serviceerror.NewInvalidArgument("Namespace is not configured for visibility archival.")
	errSearchAttributesNotSet                             = serviceerror.NewInvalidArgument("SearchAttributes are not set on request.")
	errInvalidPageSize                                    = serviceerror.NewInvalidArgument("Invalid PageSize.")                                 // DEPRECATED
	errInvalidPaginationToken                             = serviceerror.NewInvalidArgument("Invalid pagination token.")                         // DEPRECATED
	errInvalidFirstNextEventCombination                   = serviceerror.NewInvalidArgument("Invalid FirstEventId and NextEventId combination.") // DEPRECATED
	errInvalidVersionHistories                            = serviceerror.NewInvalidArgument("Invalid version histories.")                        // DEPRECATED
	errInvalidEventQueryRange                             = serviceerror.NewInvalidArgument("Invalid event query range.")                        // DEPRECATED
	errDLQTypeIsNotSupported                              = serviceerror.NewInvalidArgument("The DLQ type is not supported.")
	errFailureMustHaveApplicationFailureInfo              = serviceerror.NewInvalidArgument("Failure must have ApplicationFailureInfo.")
	errStatusFilterMustBeNotRunning                       = serviceerror.NewInvalidArgument("StatusFilter must be specified and must be not Running.")
	errCronNotAllowed                                     = serviceerror.NewInvalidArgument("Scheduled workflow must not contain CronSchedule")
	errIDReusePolicyNotAllowed                            = serviceerror.NewInvalidArgument("Scheduled workflow must not contain WorkflowIDReusePolicy")
	errBatchJobIDNotSet                                   = serviceerror.NewInvalidArgument("JobId is not set on request.")
	errNamespaceNotSet                                    = serviceerror.NewInvalidArgument("Namespace is not set on request.")
	errReasonNotSet                                       = serviceerror.NewInvalidArgument("Reason is not set on request.")
	errBatchOperationNotSet                               = serviceerror.NewInvalidArgument("Batch operation is not set on request.")
	errCronAndStartDelaySet                               = serviceerror.NewInvalidArgument("CronSchedule and WorkflowStartDelay may not be used together.")
	errInvalidWorkflowStartDelaySeconds                   = serviceerror.NewInvalidArgument("An invalid WorkflowStartDelaySeconds is set on request.")
	errRaceConditionAddingSearchAttributes                = serviceerror.NewUnavailable("Generated search attributes mapping unavailable.")
	errUseVersioningWithoutBuildId                        = serviceerror.NewInvalidArgument("WorkerVersionStamp must be present if UseVersioning is true.")
	errUseVersioningWithoutNormalName                     = serviceerror.NewInvalidArgument("NormalName must be set on sticky queue when UseVersioning is true or DeploymentOptions are set.")
	errBuildIdTooLong                                     = serviceerror.NewInvalidArgument("Build ID exceeds configured limit.workerBuildIdSize, use a shorter build ID.")
	errDeploymentOptionsNotSet                            = serviceerror.NewInvalidArgument("Deployment name and build id must be set together.")
	errIncompatibleIDReusePolicyTerminateIfRunning        = serviceerror.NewInvalidArgument("Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy")
	errIncompatibleIDReusePolicyRejectDuplicate           = serviceerror.NewInvalidArgument("Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE cannot be used together with WorkflowIdConflictPolicy WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING")
	errUseEnhancedDescribeOnStickyQueue                   = serviceerror.NewInvalidArgument("Enhanced DescribeTaskQueue is not valid for a sticky queue, use api_mode=UNSPECIFIED or a normal queue.")
	errUseEnhancedDescribeOnNonRootQueue                  = serviceerror.NewInvalidArgument("Enhanced DescribeTaskQueue is not valid for non-root queue partitions, use api_mode=UNSPECIFIED or a normal queue root name.")
	errTaskQueuePartitionInvalid                          = serviceerror.NewInvalidArgument("Task Queue Partition invalid, use a different Task Queue or Task Queue Type")
	errMultiOpWorkflowIdInconsistent                      = serviceerror.NewInvalidArgument("WorkflowId is not consistent with previous operation(s).")
	errMultiOpStartCronSchedule                           = serviceerror.NewInvalidArgument("CronSchedule is not allowed.")
	errMultiOpUpdateFirstExecutionRunId                   = serviceerror.NewInvalidArgument("FirstExecutionRunId is not allowed.")
	errMultiOpUpdateExecutionRunId                        = serviceerror.NewInvalidArgument("RunId is not allowed.")
	errMultiOpEagerWorkflow                               = serviceerror.NewInvalidArgument("RequestEagerExecution is not supported.")
	errMultiOpStartDelay                                  = serviceerror.NewInvalidArgument("WorkflowStartDelay is not supported.")
	errMultiOpNamespaceMismatch                           = serviceerror.NewInvalidArgument("Operation namespace did not match request's namespace.")
	errMultiOpNotStartAndUpdate                           = serviceerror.NewInvalidArgument("Operations have to be exactly [Start, Update].")
	errMultiOpAborted                                     = serviceerror.NewMultiOperationAborted("Operation was aborted.")

	errUpdateMetaNotSet       = serviceerror.NewInvalidArgument("Update meta is not set on request.")
	errUpdateInputNotSet      = serviceerror.NewInvalidArgument("Update input is not set on request.")
	errUpdateNameNotSet       = serviceerror.NewInvalidArgument("Update name is not set on request.")
	errUpdateIDTooLong        = serviceerror.NewInvalidArgument("UpdateId length exceeds limit.")
	errUpdateRefNotSet        = serviceerror.NewInvalidArgument("UpdateRef is not set on request.")
	errUpdateWaitPolicyNotSet = serviceerror.NewInvalidArgument("WaitPolicy is not set on request.")
	errSourceClusterNotSet    = serviceerror.NewInvalidArgument("SourceCluster is not set on request.")
	errTargetClusterNotSet    = serviceerror.NewInvalidArgument("TargetCluster is not set on request.")
	errInvalidDLQJobToken     = serviceerror.NewInvalidArgument("Invalid DLQ job token.")

	errPageSizeTooBigMessage = "PageSize is larger than allowed %d."

	errSearchAttributeIsReservedMessage               = "Search attribute %s is reserved by system."
	errSearchAttributeAlreadyExistsMessage            = "Search attribute %s already exists."
	errSearchAttributeDoesntExistMessage              = "Search attribute %s doesn't exist."
	errUnknownSearchAttributeTypeMessage              = "Unknown search attribute type: %v."
	errUnableToGetSearchAttributesMessage             = "Unable to get search attributes: %v."
	errUnableToRemoveNonCustomSearchAttributesMessage = "Unable to remove non-custom search attributes: %v."
	errUnableToSaveSearchAttributesMessage            = "Unable to save search attributes: %v."
	errUnableToStartWorkflowMessage                   = "Unable to start %s workflow: %v."
	errWorkflowReturnedErrorMessage                   = "Workflow %s returned an error: %v."
	errUnableConnectRemoteClusterMessage              = "Unable connect to remote cluster %s with error: %v."
	errInvalidRemoteClusterInfo                       = "Unable connect to remote cluster with invalid config: %v."
	errUnableToStoreClusterInfo                       = "Unable to persist cluster info with error: %v."
	errUnableToDeleteClusterInfo                      = "Unable to delete cluster info with error: %v."
	errUnableToGetNamespaceInfoMessage                = "Unable to get namespace %v info with error: %v"
	errUnableToCreateFrontendClientMessage            = "Unable to create frontend client with error: %v."
	errTooManySearchAttributesMessage                 = "Unable to create search attributes: cannot have more than %d search attribute of type %s."
	errUnsupportedIDConflictPolicy                    = "Invalid WorkflowIDConflictPolicy: %v is not supported for this operation."

	errListNotAllowed      = serviceerror.NewPermissionDenied("List is disabled on this namespace.", "")
	errSchedulesNotAllowed = serviceerror.NewPermissionDenied("Schedules are disabled on this namespace.", "")

	errDeploymentsNotAllowed        = serviceerror.NewPermissionDenied("Deployments (deprecated) are disabled on this namespace.", "")
	errDeploymentVersionsNotAllowed = serviceerror.NewPermissionDenied("Worker Deployment Versions are disabled on this namespace.", "")

	errBatchAPINotAllowed                = serviceerror.NewPermissionDenied("Batch operation feature are disabled on this namespace.", "")
	errBatchOpsWorkflowFilterNotSet      = serviceerror.NewInvalidArgument("Workflow executions and visibility filter are not set on request.")
	errBatchOpsWorkflowFiltersNotAllowed = serviceerror.NewInvalidArgument("Workflow executions and visibility filter are both set on request. Only one of them is allowed.")
	errBatchOpsMaxWorkflowExecutionCount = serviceerror.NewInvalidArgument("Workflow executions count exceeded.")

	errUpdateWorkflowExecutionAPINotAllowed           = serviceerror.NewPermissionDenied("UpdateWorkflowExecution operation is disabled on this namespace.", "")
	errUpdateWorkflowExecutionAsyncAcceptedNotAllowed = serviceerror.NewPermissionDenied("UpdateWorkflowExecution issued asynchronously and waiting on update accepted is disabled on this namespace.", "")
	errUpdateWorkflowExecutionAsyncAdmittedNotAllowed = serviceerror.NewPermissionDenied("UpdateWorkflowExecution issued asynchronously and waiting on update admitted is not supported.", "")
	errMultiOperationAPINotAllowed                    = serviceerror.NewPermissionDenied("ExecuteMultiOperation API is disabled on this namespace.", "")

	errWorkerVersioningV1_0NotAllowed         = serviceerror.NewPermissionDenied("Worker versioning v0.1 (Version Set-based, deprecated) is disabled on this namespace.", "")
	errWorkerVersioningV2_0NotAllowed         = serviceerror.NewPermissionDenied("Worker versioning v0.2 (Rules-based, deprecated) is disabled on this namespace.", "")
	errWorkerVersioningWorkflowAPIsNotAllowed = serviceerror.NewPermissionDenied("Worker versioning in workflow progress APIs is disabled on this namespace.", "")

	errListHistoryTasksNotAllowed = serviceerror.NewPermissionDenied("ListHistoryTasks feature is disabled on this cluster.", "")
)
