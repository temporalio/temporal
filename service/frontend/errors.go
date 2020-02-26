// Copyright (c) 2019 Temporal Technologies, Inc.
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

import "go.temporal.io/temporal-proto/serviceerror"

var (
	errDomainNotSet                                       = serviceerror.NewInvalidArgument("Domain not set on request.")
	errTaskTokenNotSet                                    = serviceerror.NewInvalidArgument("Task token not set on request.")
	errInvalidTaskToken                                   = serviceerror.NewInvalidArgument("Invalid TaskToken.")
	errTaskListNotSet                                     = serviceerror.NewInvalidArgument("TaskList is not set on request.")
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
	errInvalidRetention                                   = serviceerror.NewInvalidArgument("RetentionDays is invalid.")
	errInvalidExecutionStartToCloseTimeoutSeconds         = serviceerror.NewInvalidArgument("A valid ExecutionStartToCloseTimeoutSeconds is not set on request.")
	errInvalidTaskStartToCloseTimeoutSeconds              = serviceerror.NewInvalidArgument("A valid TaskStartToCloseTimeoutSeconds is not set on request.")
	errQueryDisallowedForDomain                           = serviceerror.NewInvalidArgument("Domain is not allowed to query, please contact cadence team to re-enable queries.")
	errClusterNameNotSet                                  = serviceerror.NewInvalidArgument("Cluster name is not set.")
	errEmptyReplicationInfo                               = serviceerror.NewInvalidArgument("Replication task info is not set.")
	errHistoryNotFound                                    = serviceerror.NewInvalidArgument("Requested workflow history not found, may have passed retention period.")
	errDomainTooLong                                      = serviceerror.NewInvalidArgument("Domain length exceeds limit.")
	errWorkflowTypeTooLong                                = serviceerror.NewInvalidArgument("WorkflowType length exceeds limit.")
	errWorkflowIDTooLong                                  = serviceerror.NewInvalidArgument("WorkflowId length exceeds limit.")
	errSignalNameTooLong                                  = serviceerror.NewInvalidArgument("SignalName length exceeds limit.")
	errTaskListTooLong                                    = serviceerror.NewInvalidArgument("TaskList length exceeds limit.")
	errRequestIDTooLong                                   = serviceerror.NewInvalidArgument("RequestId length exceeds limit.")
	errIdentityTooLong                                    = serviceerror.NewInvalidArgument("Identity length exceeds limit.")
	errStartTimeFilterNotSet                              = serviceerror.NewInvalidArgument("StartTimeFilter is not set on request.")
	errEarliestTimeIsGreaterThanLatestTime                = serviceerror.NewInvalidArgument("EarliestTime in StartTimeFilter should not be larger than LatestTime.")
	errPageSizeTooBig                                     = serviceerror.NewInvalidArgument("PageSize is larger than allowed %d.")
	errClusterIsNotConfiguredForVisibilityArchival        = serviceerror.NewInvalidArgument("Cluster is not configured for visibility archival.")
	errClusterIsNotConfiguredForReadingArchivalVisibility = serviceerror.NewInvalidArgument("Cluster is not configured for reading archived visibility records.")
	errDomainIsNotConfiguredForVisibilityArchival         = serviceerror.NewInvalidArgument("Domain is not configured for visibility archival.")
	errSearchAttributesNotSet                             = serviceerror.NewInvalidArgument("SearchAttributes are not set on request.")
	errAdvancedVisibilityStoreIsNotConfigured             = serviceerror.NewInvalidArgument("AdvancedVisibilityStore is not configured for this cluster.")
	errKeyIsReservedBySystem                              = serviceerror.NewInvalidArgument("Key [%s] is reserved by system.")
	errKeyIsAlreadyWhitelisted                            = serviceerror.NewInvalidArgument("Key [%s] is already whitelist.")
	errInvalidPageSize                                    = serviceerror.NewInvalidArgument("Invalid PageSize.")
	errInvalidPaginationToken                             = serviceerror.NewInvalidArgument("Invalid pagination token.")
	errInvalidFirstNextEventCombination                   = serviceerror.NewInvalidArgument("Invalid FirstEventId and NextEventId combination.")
	errInvalidStartEventCombination                       = serviceerror.NewInvalidArgument("Invalid StartEventId and StartEventVersion combination.")
	errInvalidEndEventCombination                         = serviceerror.NewInvalidArgument("Invalid EndEventId and EndEventVersion combination.")
	errInvalidVersionHistories                            = serviceerror.NewInvalidArgument("Invalid version histories.")
	errInvalidEventQueryRange                             = serviceerror.NewInvalidArgument("Invalid event query range.")
	errUnknownValueType                                   = serviceerror.NewInvalidArgument("Unknown value type, %v.")
	errDLQTypeIsNotSupported                              = serviceerror.NewInvalidArgument("The DLQ type is not supported.")

	errFailedUpdateDynamicConfig = serviceerror.NewInternal("Failed to update dynamic config, err: %v.")
	errFailedToCreateESIndex     = serviceerror.NewInternal("Failed to create ES index, err: %v.")
	errFailedToUpdateESMapping   = serviceerror.NewInternal("Failed to update ES mapping, err: %v.")

	errNoPermission = serviceerror.NewPermissionDenied("No permission to do this operation.")
	errUnauthorized = serviceerror.NewPermissionDenied("Request unauthorized.")

	errServiceBusy = serviceerror.NewResourceExhausted("Too many outstanding requests to the service.")
)
