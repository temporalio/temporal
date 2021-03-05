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

package tag

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

// All logging tags are defined in this file.
// To help finding available tags, we recommend that all tags to be categorized and placed in the corresponding section.
// We currently have those categories:
//   0. Common tags that can't be categorized(or belong to more than one)
//   1. Workflow: these tags are information that are useful to our customer, like workflow-id/run-id/task-queue/...
//   2. System : these tags are internal information which usually cannot be understood by our customers,

///////////////////  Common tags defined here ///////////////////

// Error returns tag for Error
func Error(err error) Tag {
	return NewErrorTag("error", err)
}

// ClusterName returns tag for ClusterName
func ClusterName(clusterName string) Tag {
	return NewStringTag("cluster-name", clusterName)
}

// Timestamp returns tag for Timestamp
func Timestamp(timestamp time.Time) Tag {
	return NewTimeTag("timestamp", timestamp)
}

// Timestamp returns tag for Timestamp
func TimestampPtr(t *time.Time) Tag {
	return NewTimeTag("timestamp", timestamp.TimeValue(t))
}

///////////////////  Workflow tags defined here: ( wf is short for workflow) ///////////////////

// WorkflowAction returns tag for WorkflowAction
func workflowAction(action string) Tag {
	return NewPredefinedStringTag("wf-action", action)
}

// WorkflowListFilterType returns tag for WorkflowListFilterType
func workflowListFilterType(listFilterType string) Tag {
	return NewPredefinedStringTag("wf-list-filter-type", listFilterType)
}

// general

// WorkflowError returns tag for WorkflowError
func WorkflowError(error error) Tag { return NewErrorTag("wf-error", error) }

// WorkflowTimeoutType returns tag for WorkflowTimeoutType
func WorkflowTimeoutType(timeoutType enumspb.TimeoutType) Tag {
	return NewStringTag("wf-timeout-type", timeoutType.String())
}

// WorkflowPollContextTimeout returns tag for WorkflowPollContextTimeout
func WorkflowPollContextTimeout(pollContextTimeout time.Duration) Tag {
	return NewDurationTag("wf-poll-context-timeout", pollContextTimeout)
}

// WorkflowHandlerName returns tag for WorkflowHandlerName
func WorkflowHandlerName(handlerName string) Tag {
	return NewStringTag("wf-handler-name", handlerName)
}

// WorkflowID returns tag for WorkflowID
func WorkflowID(workflowID string) Tag {
	return NewStringTag("wf-id", workflowID)
}

// WorkflowType returns tag for WorkflowType
func WorkflowType(wfType string) Tag {
	return NewStringTag("wf-type", wfType)
}

// WorkflowState returns tag for WorkflowState
func WorkflowState(s enumsspb.WorkflowExecutionState) Tag {
	return NewStringTag("wf-state", s.String())
}

// WorkflowRunID returns tag for WorkflowRunID
func WorkflowRunID(runID string) Tag {
	return NewStringTag("wf-run-id", runID)
}

// WorkflowResetBaseRunID returns tag for WorkflowResetBaseRunID
func WorkflowResetBaseRunID(runID string) Tag {
	return NewStringTag("wf-reset-base-run-id", runID)
}

// WorkflowResetNewRunID returns tag for WorkflowResetNewRunID
func WorkflowResetNewRunID(runID string) Tag {
	return NewStringTag("wf-reset-new-run-id", runID)
}

// WorkflowBinaryChecksum returns tag for WorkflowBinaryChecksum
func WorkflowBinaryChecksum(cs string) Tag {
	return NewStringTag("wf-binary-checksum", cs)
}

// WorkflowActivityID returns tag for WorkflowActivityID
func WorkflowActivityID(id string) Tag {
	return NewStringTag("wf-activity-id", id)
}

// WorkflowTimerID returns tag for WorkflowTimerID
func WorkflowTimerID(id string) Tag {
	return NewStringTag("wf-timer-id", id)
}

// WorkflowBeginningRunID returns tag for WorkflowBeginningRunID
func WorkflowBeginningRunID(beginningRunID string) Tag {
	return NewStringTag("wf-beginning-run-id", beginningRunID)
}

// WorkflowEndingRunID returns tag for WorkflowEndingRunID
func WorkflowEndingRunID(endingRunID string) Tag {
	return NewStringTag("wf-ending-run-id", endingRunID)
}

// WorkflowTaskTimeoutSeconds returns tag for WorkflowTaskTimeoutSeconds
func WorkflowTaskTimeoutSeconds(s int64) Tag {
	return NewInt64("workflow-task-timeout", s)
}

// WorkflowTaskTimeoutSeconds returns tag for WorkflowTaskTimeoutSeconds
func WorkflowTaskTimeout(s *time.Duration) Tag {
	return NewDurationPtrTag("workflow-task-timeout", s)
}

// QueryID returns tag for QueryID
func QueryID(queryID string) Tag {
	return NewStringTag("query-id", queryID)
}

// BlobSizeViolationOperation returns tag for BlobSizeViolationOperation
func BlobSizeViolationOperation(operation string) Tag {
	return NewStringTag("blob-size-violation-operation", operation)
}

// namespace related

// WorkflowNamespaceID returns tag for WorkflowNamespaceID
func WorkflowNamespaceID(namespaceID string) Tag {
	return NewStringTag("wf-namespace-id", namespaceID)
}

// WorkflowNamespace returns tag for WorkflowNamespace
func WorkflowNamespace(namespace string) Tag {
	return NewStringTag("wf-namespace", namespace)
}

// WorkflowNamespaceIDs returns tag for WorkflowNamespaceIDs
func WorkflowNamespaceIDs(namespaceIDs interface{}) Tag {
	return NewObjectTag("wf-namespace-ids", namespaceIDs)
}

// history event ID related

// WorkflowEventID returns tag for WorkflowEventID
func WorkflowEventID(eventID int64) Tag {
	return NewInt64("wf-history-event-id", eventID)
}

// WorkflowScheduleID returns tag for WorkflowScheduleID
func WorkflowScheduleID(scheduleID int64) Tag {
	return NewInt64("wf-schedule-id", scheduleID)
}

// WorkflowStartedID returns tag for WorkflowStartedID
func WorkflowStartedID(id int64) Tag {
	return NewInt64("wf-started-id", id)
}

// WorkflowStartedID returns tag for WorkflowStartedTimestamp
func WorkflowStartedTimestamp(t *time.Time) Tag {
	return NewTimePtrTag("wf-started-timestamp", t)
}

// WorkflowInitiatedID returns tag for WorkflowInitiatedID
func WorkflowInitiatedID(id int64) Tag {
	return NewInt64("wf-initiated-id", id)
}

// WorkflowFirstEventID returns tag for WorkflowFirstEventID
func WorkflowFirstEventID(firstEventID int64) Tag {
	return NewInt64("wf-first-event-id", firstEventID)
}

// WorkflowNextEventID returns tag for WorkflowNextEventID
func WorkflowNextEventID(nextEventID int64) Tag {
	return NewInt64("wf-next-event-id", nextEventID)
}

// WorkflowBeginningFirstEventID returns tag for WorkflowBeginningFirstEventID
func WorkflowBeginningFirstEventID(beginningFirstEventID int64) Tag {
	return NewInt64("wf-begining-first-event-id", beginningFirstEventID)
}

// WorkflowEndingNextEventID returns tag for WorkflowEndingNextEventID
func WorkflowEndingNextEventID(endingNextEventID int64) Tag {
	return NewInt64("wf-ending-next-event-id", endingNextEventID)
}

// WorkflowResetNextEventID returns tag for WorkflowResetNextEventID
func WorkflowResetNextEventID(resetNextEventID int64) Tag {
	return NewInt64("wf-reset-next-event-id", resetNextEventID)
}

// history tree

// WorkflowTreeID returns tag for WorkflowTreeID
func WorkflowTreeID(treeID string) Tag {
	return NewStringTag("wf-tree-id", treeID)
}

// WorkflowBranchID returns tag for WorkflowBranchID
func WorkflowBranchID(branchID string) Tag {
	return NewStringTag("wf-branch-id", branchID)
}

// workflow task

// WorkflowCommandType returns tag for WorkflowCommandType
func WorkflowCommandType(commandType enumspb.CommandType) Tag {
	return NewStringTag("command-type", commandType.String())
}

// WorkflowQueryType returns tag for WorkflowQueryType
func WorkflowQueryType(qt string) Tag {
	return NewStringTag("wf-query-type", qt)
}

// WorkflowTaskFailedCause returns tag for WorkflowTaskFailedCause
func WorkflowTaskFailedCause(workflowTaskFailCause enumspb.WorkflowTaskFailedCause) Tag {
	return NewStringTag("workflow-task-fail-cause", workflowTaskFailCause.String())
}

// WorkflowTaskQueueType returns tag for WorkflowTaskQueueType
func WorkflowTaskQueueType(taskQueueType enumspb.TaskQueueType) Tag {
	return NewStringTag("wf-task-queue-type", taskQueueType.String())
}

// WorkflowTaskQueueName returns tag for WorkflowTaskQueueName
func WorkflowTaskQueueName(taskQueueName string) Tag {
	return NewStringTag("wf-task-queue-name", taskQueueName)
}

// size limit

// WorkflowSize returns tag for WorkflowSize
func WorkflowSize(workflowSize int64) Tag {
	return NewInt64("wf-size", workflowSize)
}

// WorkflowSignalCount returns tag for SignalCount
func WorkflowSignalCount(signalCount int64) Tag {
	return NewInt64("wf-signal-count", signalCount)
}

// WorkflowHistorySize returns tag for HistorySize
func WorkflowHistorySize(historySize int) Tag {
	return NewInt("wf-history-size", historySize)
}

// WorkflowHistorySizeBytes returns tag for HistorySizeBytes
func WorkflowHistorySizeBytes(historySizeBytes int) Tag {
	return NewInt("wf-history-size-bytes", historySizeBytes)
}

// WorkflowEventCount returns tag for EventCount
func WorkflowEventCount(eventCount int) Tag {
	return NewInt("wf-event-count", eventCount)
}

///////////////////  System tags defined here:  ///////////////////
// Tags with pre-define values

// Component returns tag for Component
func component(component string) Tag {
	return NewPredefinedStringTag("component", component)
}

// Lifecycle returns tag for Lifecycle
func lifecycle(lifecycle string) Tag {
	return NewPredefinedStringTag("lifecycle", lifecycle)
}

// StoreOperation returns tag for StoreOperation
func storeOperation(storeOperation string) Tag {
	return NewPredefinedStringTag("store-operation", storeOperation)
}

// OperationResult returns tag for OperationResult
func operationResult(operationResult string) Tag {
	return NewPredefinedStringTag("operation-result", operationResult)
}

// ErrorType returns tag for ErrorType
func errorType(errorType string) Tag {
	return NewPredefinedStringTag("error", errorType)
}

// Shardupdate returns tag for Shardupdate
func shardupdate(shardupdate string) Tag {
	return NewPredefinedStringTag("shard-update", shardupdate)
}

// general

// Service returns tag for Service
func Service(sv string) Tag {
	return NewStringTag("service", sv)
}

// Addresses returns tag for Addresses
func Addresses(ads []string) Tag {
	return NewObjectTag("addresses", ads)
}

// ListenerName returns tag for ListenerName
func ListenerName(name string) Tag {
	return NewStringTag("listener-name", name)
}

// Address return tag for Address
func Address(ad string) Tag {
	return NewStringTag("address", ad)
}

// HostID return tag for HostID
func HostID(hid string) Tag {
	return NewStringTag("hostId", hid)
}

// Env return tag for runtime environment
func Env(env string) Tag {
	return NewStringTag("env", env)
}

// Key returns tag for Key
func Key(k string) Tag {
	return NewStringTag("key", k)
}

// Name returns tag for Name
func Name(k string) Tag {
	return NewStringTag("name", k)
}

// Value returns tag for Value
func Value(v interface{}) Tag {
	return NewObjectTag("value", v)
}

// ValueType returns tag for ValueType
func ValueType(v interface{}) Tag {
	return NewStringTag("value-type", fmt.Sprintf("%T", v))
}

// DefaultValue returns tag for DefaultValue
func DefaultValue(v interface{}) Tag {
	return NewObjectTag("default-value", v)
}

// IgnoredValue returns tag for IgnoredValue
func IgnoredValue(v interface{}) Tag {
	return NewObjectTag("ignored-value", v)
}

// Port returns tag for Port
func Port(p int) Tag {
	return NewInt("port", p)
}

// CursorTimestamp returns tag for CursorTimestamp
func CursorTimestamp(timestamp time.Time) Tag {
	return NewTimeTag("cursor-timestamp", timestamp)
}

// MetricScope returns tag for MetricScope
func MetricScope(metricScope int) Tag {
	return NewInt("metric-scope", metricScope)
}

// StoreType returns tag for StoreType
func StoreType(storeType string) Tag {
	return NewPredefinedStringTag("store-type", storeType)
}

// DetailInfo returns tag for DetailInfo
func DetailInfo(i string) Tag {
	return NewStringTag("detail-info", i)
}

// Counter returns tag for Counter
func Counter(c int) Tag {
	return NewInt("counter", c)
}

// Number returns tag for Number
func Number(n int64) Tag {
	return NewInt64("number", n)
}

// NextNumber returns tag for NextNumber
func NextNumber(n int64) Tag {
	return NewInt64("next-number", n)
}

// Bool returns tag for Bool
func Bool(b bool) Tag {
	return NewBoolTag("bool", b)
}

// history engine shard

// ShardID returns tag for ShardID
func ShardID(shardID int32) Tag {
	return NewInt32("shard-id", shardID)
}

// ShardItem returns tag for ShardItem
func ShardItem(shardItem interface{}) Tag {
	return NewObjectTag("shard-item", shardItem)
}

// ShardTime returns tag for ShardTime
func ShardTime(shardTime interface{}) Tag {
	return NewObjectTag("shard-time", shardTime)
}

// ShardReplicationAck returns tag for ShardReplicationAck
func ShardReplicationAck(shardReplicationAck int64) Tag {
	return NewInt64("shard-replication-ack", shardReplicationAck)
}

// PreviousShardRangeID returns tag for PreviousShardRangeID
func PreviousShardRangeID(id int64) Tag {
	return NewInt64("previous-shard-range-id", id)
}

// ShardRangeID returns tag for ShardRangeID
func ShardRangeID(id int64) Tag {
	return NewInt64("shard-range-id", id)
}

// ReadLevel returns tag for ReadLevel
func ReadLevel(lv int64) Tag {
	return NewInt64("read-level", lv)
}

// MinLevel returns tag for MinLevel
func MinLevel(lv int64) Tag {
	return NewInt64("min-level", lv)
}

// MaxLevel returns tag for MaxLevel
func MaxLevel(lv int64) Tag {
	return NewInt64("max-level", lv)
}

// ShardTransferAcks returns tag for ShardTransferAcks
func ShardTransferAcks(shardTransferAcks interface{}) Tag {
	return NewObjectTag("shard-transfer-acks", shardTransferAcks)
}

// ShardTimerAcks returns tag for ShardTimerAcks
func ShardTimerAcks(shardTimerAcks interface{}) Tag {
	return NewObjectTag("shard-timer-acks", shardTimerAcks)
}

// task queue processor

// Task returns tag for Task
func Task(task interface{}) Tag {
	return NewObjectTag("queue-task", task)
}

// TaskID returns tag for TaskID
func TaskID(taskID int64) Tag {
	return NewInt64("queue-task-id", taskID)
}

// TaskType returns tag for TaskType for queue processor
func TaskType(taskType enumsspb.TaskType) Tag {
	return NewStringTag("queue-task-type", taskType.String())
}

// TaskVersion returns tag for TaskVersion
func TaskVersion(taskVersion int64) Tag {
	return NewInt64("queue-task-version", taskVersion)
}

// TaskVisibilityTimestamp returns tag for task visibilityTimestamp
func TaskVisibilityTimestamp(timestamp int64) Tag {
	return NewInt64("queue-task-visibility-timestamp", timestamp)
}

// NumberProcessed returns tag for NumberProcessed
func NumberProcessed(n int) Tag {
	return NewInt("number-processed", n)
}

// NumberDeleted returns tag for NumberDeleted
func NumberDeleted(n int) Tag {
	return NewInt("number-deleted", n)
}

// TimerTaskStatus returns tag for TimerTaskStatus
func TimerTaskStatus(timerTaskStatus int32) Tag {
	return NewInt32("timer-task-status", timerTaskStatus)
}

// retry

// Attempt returns tag for Attempt
func Attempt(attempt int32) Tag {
	return NewInt32("attempt", attempt)
}

// AttemptCount returns tag for AttemptCount
func AttemptCount(attemptCount int64) Tag {
	return NewInt64("attempt-count", attemptCount)
}

// AttemptStart returns tag for AttemptStart
func AttemptStart(attemptStart time.Time) Tag {
	return NewTimeTag("attempt-start", attemptStart)
}

// AttemptEnd returns tag for AttemptEnd
func AttemptEnd(attemptEnd time.Time) Tag {
	return NewTimeTag("attempt-end", attemptEnd)
}

// ScheduleAttempt returns tag for ScheduleAttempt
func ScheduleAttempt(scheduleAttempt int32) Tag {
	return NewInt32("schedule-attempt", scheduleAttempt)
}

// ElasticSearch

// ESRequest returns tag for ESRequest
func ESRequest(ESRequest string) Tag {
	return NewStringTag("es-request", ESRequest)
}

// ESResponseStatus returns tag for ESResponse status
func ESResponseStatus(status int) Tag {
	return NewInt("es-response-status", status)
}

// ESResponseError returns tag for ESResponse error
func ESResponseError(msg string) Tag {
	return NewStringTag("es-response-error", msg)
}

// ESKey returns tag for ESKey
func ESKey(ESKey string) Tag {
	return NewStringTag("es-mapping-key", ESKey)
}

// ESValue returns tag for ESValue
func ESValue(ESValue []byte) Tag {
	// convert value to string type so that the value logged is human readable
	return NewStringTag("es-mapping-value", string(ESValue))
}

// ESConfig returns tag for ESConfig
func ESConfig(c interface{}) Tag {
	return NewObjectTag("es-config", c)
}

// ESField returns tag for ESField
func ESField(ESField string) Tag {
	return NewStringTag("es-Field", ESField)
}

// ESDocID returns tag for ESDocID
func ESDocID(id string) Tag {
	return NewStringTag("es-doc-id", id)
}

// LoggingCallAtKey is reserved tag
const LoggingCallAtKey = "logging-call-at"

// SysStackTrace returns tag for SysStackTrace
func SysStackTrace(stackTrace string) Tag {
	return NewStringTag("sys-stack-trace", stackTrace)
}

// TokenLastEventID returns tag for TokenLastEventID
func TokenLastEventID(id int64) Tag {
	return NewInt64("token-last-event-id", id)
}

///////////////////  XDC tags defined here: xdc- ///////////////////

// SourceCluster returns tag for SourceCluster
func SourceCluster(sourceCluster string) Tag {
	return NewStringTag("xdc-source-cluster", sourceCluster)
}

// PrevActiveCluster returns tag for PrevActiveCluster
func PrevActiveCluster(prevActiveCluster string) Tag {
	return NewStringTag("xdc-prev-active-cluster", prevActiveCluster)
}

// FailoverMsg returns tag for FailoverMsg
func FailoverMsg(failoverMsg string) Tag {
	return NewStringTag("xdc-failover-msg", failoverMsg)
}

// FailoverVersion returns tag for Version
func FailoverVersion(version int64) Tag {
	return NewInt64("xdc-failover-version", version)
}

// CurrentVersion returns tag for CurrentVersion
func CurrentVersion(currentVersion int64) Tag {
	return NewInt64("xdc-current-version", currentVersion)
}

// IncomingVersion returns tag for IncomingVersion
func IncomingVersion(incomingVersion int64) Tag {
	return NewInt64("xdc-incoming-version", incomingVersion)
}

// ReplicationInfo returns tag for ReplicationInfo
func ReplicationInfo(replicationInfo interface{}) Tag {
	return NewObjectTag("xdc-replication-info", replicationInfo)
}

// ReplicationState returns tag for ReplicationState
func ReplicationState(replicationState interface{}) Tag {
	return NewObjectTag("xdc-replication-state", replicationState)
}

// FirstEventVersion returns tag for FirstEventVersion
func FirstEventVersion(version int64) Tag {
	return NewInt64("xdc-first-event-version", version)
}

// LastEventVersion returns tag for LastEventVersion
func LastEventVersion(version int64) Tag {
	return NewInt64("xdc-last-event-version", version)
}

// TokenLastEventVersion returns tag for TokenLastEventVersion
func TokenLastEventVersion(version int64) Tag {
	return NewInt64("xdc-token-last-event-version", version)
}

///////////////////  Archival tags defined here: archival- ///////////////////
// archival request tags

// ArchivalCallerServiceName returns tag for the service name calling archival client
func ArchivalCallerServiceName(callerServiceName string) Tag {
	return NewStringTag("archival-caller-service-name", callerServiceName)
}

// ArchivalArchiveAttemptedInline returns tag for whether archival is attempted inline before signal is sent.
func ArchivalArchiveAttemptedInline(archiveInline bool) Tag {
	return NewBoolTag("archival-archive-attempted-inline", archiveInline)
}

// ArchivalRequestNamespaceID returns tag for RequestNamespaceID
func ArchivalRequestNamespaceID(requestNamespaceID string) Tag {
	return NewStringTag("archival-request-namespace-id", requestNamespaceID)
}

// ArchivalRequestNamespace returns tag for RequestNamespace
func ArchivalRequestNamespace(requestNamespace string) Tag {
	return NewStringTag("archival-request-namespace", requestNamespace)
}

// ArchivalRequestWorkflowID returns tag for RequestWorkflowID
func ArchivalRequestWorkflowID(requestWorkflowID string) Tag {
	return NewStringTag("archival-request-workflow-id", requestWorkflowID)
}

// ArchvialRequestWorkflowType returns tag for RequestWorkflowType
func ArchvialRequestWorkflowType(requestWorkflowType string) Tag {
	return NewStringTag("archival-request-workflow-type", requestWorkflowType)
}

// ArchivalRequestRunID returns tag for RequestRunID
func ArchivalRequestRunID(requestRunID string) Tag {
	return NewStringTag("archival-request-run-id", requestRunID)
}

// ArchivalRequestBranchToken returns tag for RequestBranchToken
func ArchivalRequestBranchToken(requestBranchToken []byte) Tag {
	return NewObjectTag("archival-request-branch-token", requestBranchToken)
}

// ArchivalRequestNextEventID returns tag for RequestNextEventID
func ArchivalRequestNextEventID(requestNextEventID int64) Tag {
	return NewInt64("archival-request-next-event-id", requestNextEventID)
}

// ArchivalRequestCloseFailoverVersion returns tag for RequestCloseFailoverVersion
func ArchivalRequestCloseFailoverVersion(requestCloseFailoverVersion int64) Tag {
	return NewInt64("archival-request-close-failover-version", requestCloseFailoverVersion)
}

// ArchivalRequestCloseTimestamp returns tag for RequestCloseTimestamp
func ArchivalRequestCloseTimestamp(requestCloseTimeStamp *time.Time) Tag {
	return NewTimeTag("archival-request-close-timestamp", timestamp.TimeValue(requestCloseTimeStamp))
}

// ArchivalRequestStatus returns tag for RequestStatus
func ArchivalRequestStatus(requestStatus string) Tag {
	return NewStringTag("archival-request-status", requestStatus)
}

// ArchivalURI returns tag for Archival URI
func ArchivalURI(URI string) Tag {
	return NewStringTag("archival-URI", URI)
}

// ArchivalArchiveFailReason returns tag for ArchivalArchiveFailReason
func ArchivalArchiveFailReason(archiveFailReason string) Tag {
	return NewStringTag("archival-archive-fail-reason", archiveFailReason)
}

// ArchivalDeleteHistoryFailReason returns tag for ArchivalDeleteHistoryFailReason
func ArchivalDeleteHistoryFailReason(deleteHistoryFailReason string) Tag {
	return NewStringTag("archival-delete-history-fail-reason", deleteHistoryFailReason)
}

// ArchivalVisibilityQuery returns tag for the query for getting archived visibility record
func ArchivalVisibilityQuery(query string) Tag {
	return NewStringTag("archival-visibility-query", query)
}

// The following logger tags are only used by internal archiver implemention.
// TODO: move them to internal repo once temporal plugin model is in place.

// ArchivalBlobKey returns tag for BlobKey
func ArchivalBlobKey(blobKey string) Tag {
	return NewStringTag("archival-blob-key", blobKey)
}

// ArchivalDeterministicConstructionCheckFailReason returns tag for ArchivalDeterministicConstructionCheckFailReason
func ArchivalDeterministicConstructionCheckFailReason(deterministicConstructionCheckFailReason string) Tag {
	return NewStringTag("archival-deterministic-construction-check-fail-reason", deterministicConstructionCheckFailReason)
}

// ArchivalNonDeterministicBlobKey returns tag for randomly generated NonDeterministicBlobKey
func ArchivalNonDeterministicBlobKey(nondeterministicBlobKey string) Tag {
	return NewStringTag("archival-non-deterministic-blob-key", nondeterministicBlobKey)
}

// ArchivalBlobIntegrityCheckFailReason returns tag for ArchivalBlobIntegrityCheckFailReason
func ArchivalBlobIntegrityCheckFailReason(blobIntegrityCheckFailReason string) Tag {
	return NewStringTag("archival-blob-integrity-check-fail-reason", blobIntegrityCheckFailReason)
}

// ArchivalBlobstoreContextTimeout returns tag for ArchivalBlobstoreContextTimeout
func ArchivalBlobstoreContextTimeout(blobstoreContextTimeout time.Duration) Tag {
	return NewDurationTag("archival-blobstore-context-timeout", blobstoreContextTimeout)
}

// TransportType returns tag for transportType
func TransportType(transportType string) Tag {
	return NewStringTag("transport-type", transportType)
}

// ActivityInfo returns tag for activity info
func ActivityInfo(activityInfo interface{}) Tag {
	return NewObjectTag("activity-info", activityInfo)
}

// WorkflowTaskRequestId returns tag for workflow task RequestId
func WorkflowTaskRequestId(s string) Tag {
	return NewStringTag("workflow-task-request-id", s)
}

// AckLevel returns tag for ack level
func AckLevel(s interface{}) Tag {
	return NewObjectTag("ack-level", s)
}

// MinQueryLevel returns tag for query level
func MinQueryLevel(s time.Time) Tag {
	return NewTimeTag("min-query-level", s)
}

// MaxQueryLevel returns tag for query level
func MaxQueryLevel(s time.Time) Tag {
	return NewTimeTag("max-query-level", s)
}

// BootstrapHostPorts returns tag for bootstrap host ports
func BootstrapHostPorts(s string) Tag {
	return NewStringTag("bootstrap-hostports", s)
}
