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

package frontend

import "go.temporal.io/api/serviceerror"

var (
	errInvalidTaskToken                                   = serviceerror.NewInvalidArgument("Invalid TaskToken.")
	errTaskQueueNotSet                                    = serviceerror.NewInvalidArgument("TaskQueue is not set on request.")
	errExecutionNotSet                                    = serviceerror.NewInvalidArgument("Execution is not set on request.")
	errWorkflowIDNotSet                                   = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errActivityIDNotSet                                   = serviceerror.NewInvalidArgument("ActivityId is not set on request.")
	errSignalNameNotSet                                   = serviceerror.NewInvalidArgument("SignalName is not set on request.")
	errInvalidRunID                                       = serviceerror.NewInvalidArgument("Invalid RunId.")
	errInvalidNextPageToken                               = serviceerror.NewInvalidArgument("Invalid NextPageToken.")
	errNextPageTokenRunIDMismatch                         = serviceerror.NewInvalidArgument("RunId in the request does not match the NextPageToken.")
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
	errInvalidPageSize                                    = serviceerror.NewInvalidArgument("Invalid PageSize.")
	errInvalidPaginationToken                             = serviceerror.NewInvalidArgument("Invalid pagination token.")
	errInvalidFirstNextEventCombination                   = serviceerror.NewInvalidArgument("Invalid FirstEventId and NextEventId combination.")
	errInvalidVersionHistories                            = serviceerror.NewInvalidArgument("Invalid version histories.")
	errInvalidEventQueryRange                             = serviceerror.NewInvalidArgument("Invalid event query range.")
	errDLQTypeIsNotSupported                              = serviceerror.NewInvalidArgument("The DLQ type is not supported.")
	errFailureMustHaveApplicationFailureInfo              = serviceerror.NewInvalidArgument("Failure must have ApplicationFailureInfo.")
	errStatusFilterMustBeNotRunning                       = serviceerror.NewInvalidArgument("StatusFilter must be specified and must be not Running.")
	errCronNotAllowed                                     = serviceerror.NewInvalidArgument("Scheduled workflow must not contain CronSchedule")
	errIDReusePolicyNotAllowed                            = serviceerror.NewInvalidArgument("Scheduled workflow must not contain WorkflowIDReusePolicy")
	errUnableDeleteSystemNamespace                        = serviceerror.NewInvalidArgument("Unable to delete system namespace.")
	errBatchJobIDNotSet                                   = serviceerror.NewInvalidArgument("JobId is not set on request.")
	errNamespaceNotSet                                    = serviceerror.NewInvalidArgument("Namespace is not set on request.")
	errReasonNotSet                                       = serviceerror.NewInvalidArgument("Reason is not set on request.")
	errBatchOperationNotSet                               = serviceerror.NewInvalidArgument("Batch operation is not set on request.")
	errCronAndStartDelaySet                               = serviceerror.NewInvalidArgument("CronSchedule and WorkflowStartDelay may not be used together.")
	errInvalidWorkflowStartDelaySeconds                   = serviceerror.NewInvalidArgument("An invalid WorkflowStartDelaySeconds is set on request.")
	errRaceConditionAddingSearchAttributes                = serviceerror.NewUnavailable("Generated search attributes mapping unavailble.")
	errUseVersioningWithoutBuildId                        = serviceerror.NewInvalidArgument("WorkerVersionStamp must be present if UseVersioning is true.")
	errUseVersioningWithoutNormalName                     = serviceerror.NewInvalidArgument("NormalName must be set on sticky queue if UseVersioning is true.")
	errBuildIdTooLong                                     = serviceerror.NewInvalidArgument("Build ID exceeds configured limit.workerBuildIdSize, use a shorter build ID.")

	errUpdateMetaNotSet       = serviceerror.NewInvalidArgument("Update meta is not set on request.")
	errUpdateInputNotSet      = serviceerror.NewInvalidArgument("Update input is not set on request.")
	errUpdateNameNotSet       = serviceerror.NewInvalidArgument("Update name is not set on request.")
	errUpdateIDTooLong        = serviceerror.NewInvalidArgument("UpdateId length exceeds limit.")
	errUpdateRefNotSet        = serviceerror.NewInvalidArgument("UpdateRef is not set on request.")
	errUpdateWaitPolicyNotSet = serviceerror.NewInvalidArgument("WaitPolicy is not set on request.")

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
	errUnableToGetNamespaceInfoMessage                = "Unable to get namespace info with error: %v"
	errUnableToCreateFrontendClientMessage            = "Unable to create frontend client with error: %v."
	errTooManySearchAttributesMessage                 = "Unable to create search attributes: cannot have more than %d search attribute of type %s."

	errListNotAllowed      = serviceerror.NewPermissionDenied("List is disabled on this namespace.", "")
	errSchedulesNotAllowed = serviceerror.NewPermissionDenied("Schedules are disabled on this namespace.", "")

	errBatchAPINotAllowed                = serviceerror.NewPermissionDenied("Batch operation feature are disabled on this namespace.", "")
	errBatchOpsWorkflowFilterNotSet      = serviceerror.NewInvalidArgument("Workflow executions and visibility filter are not set on request.")
	errBatchOpsWorkflowFiltersNotAllowed = serviceerror.NewInvalidArgument("Workflow executions and visibility filter are both set on request. Only one of them is allowed.")
	errBatchOpsMaxWorkflowExecutionCount = serviceerror.NewInvalidArgument("Workflow executions count exceeded.")

	errUpdateWorkflowExecutionAPINotAllowed = serviceerror.NewPermissionDenied("UpdateWorkflowExecution operation is disabled on this namespace.", "")
	errWorkerVersioningNotAllowed           = serviceerror.NewPermissionDenied("Worker versioning is disabled on this namespace.", "")
)
