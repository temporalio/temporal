// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	"github.com/uber/cadence/common/persistence"
)

// All logging tags are defined in this file.
// To help finding available tags, we recommend that all tags to be categorized and placed in the corresponding section.
// We currently have those categories:
//   0. Common tags that can't be categorized(or belong to more than one)
//   1. Workflow: these tags are information that are useful to our customer, like workflow-id/run-id/task-list/...
//   2. System : these tags are internal information which usually cannot be understood by our customers,

///////////////////  Common tags defined here ///////////////////

// Error returns tag for Error
func Error(err error) Tag {
	return newErrorTag("error", err)
}

// ClusterName returns tag for ClusterName
func ClusterName(clusterName string) Tag {
	return newStringTag("cluster-name", clusterName)
}

// Timestamp returns tag for Timestamp
func Timestamp(timestamp time.Time) Tag {
	return newTimeTag("timestamp", timestamp)
}

///////////////////  Workflow tags defined here: ( wf is short for workflow) ///////////////////

// WorkflowAction returns tag for WorkflowAction
func workflowAction(action string) Tag {
	return newPredefinedStringTag("wf-action", action)
}

// WorkflowListFilterType returns tag for WorkflowListFilterType
func workflowListFilterType(listFilterType string) Tag {
	return newPredefinedStringTag("wf-list-filter-type", listFilterType)
}

// general

// WorkflowError returns tag for WorkflowError
func WorkflowError(error error) Tag { return newErrorTag("wf-error", error) }

// WorkflowTimeoutType returns tag for WorkflowTimeoutType
func WorkflowTimeoutType(timeoutType int64) Tag {
	return newIntegerTag("wf-timeout-type", timeoutType)
}

// WorkflowPollContextTimeout returns tag for WorkflowPollContextTimeout
func WorkflowPollContextTimeout(pollContextTimeout time.Duration) Tag {
	return newDurationTag("wf-poll-context-timeout", pollContextTimeout)
}

// WorkflowHandlerName returns tag for WorkflowHandlerName
func WorkflowHandlerName(handlerName string) Tag {
	return newStringTag("wf-handler-name", handlerName)
}

// WorkflowID returns tag for WorkflowID
func WorkflowID(workflowID string) Tag {
	return newStringTag("wf-id", workflowID)
}

// WorkflowType returns tag for WorkflowType
func WorkflowType(wfType string) Tag {
	return newStringTag("wf-type", wfType)
}

// WorkflowRunID returns tag for WorkflowRunID
func WorkflowRunID(runID string) Tag {
	return newStringTag("wf-run-id", runID)
}

// WorkflowBeginningRunID returns tag for WorkflowBeginningRunID
func WorkflowBeginningRunID(beginningRunID string) Tag {
	return newStringTag("wf-beginning-run-id", beginningRunID)
}

// WorkflowEndingRunID returns tag for WorkflowEndingRunID
func WorkflowEndingRunID(endingRunID string) Tag {
	return newStringTag("wf-ending-run-id", endingRunID)
}

// domain related

// WorkflowDomainID returns tag for WorkflowDomainID
func WorkflowDomainID(domainID string) Tag {
	return newStringTag("wf-domain-id", domainID)
}

// WorkflowDomainName returns tag for WorkflowDomainName
func WorkflowDomainName(domainName string) Tag {
	return newStringTag("wf-domain-name", domainName)
}

// WorkflowDomainIDs returns tag for WorkflowDomainIDs
func WorkflowDomainIDs(domainIDs []string) Tag {
	return newObjectTag("wf-domain-ids", domainIDs)
}

// history event ID related

// WorkflowEventID returns tag for WorkflowEventID
func WorkflowEventID(eventID int64) Tag {
	return newIntegerTag("wf-history-event-id", eventID)
}

// WorkflowScheduleID returns tag for WorkflowScheduleID
func WorkflowScheduleID(scheduleID int64) Tag {
	return newIntegerTag("wf-schedule-id", scheduleID)
}

// WorkflowFirstEventID returns tag for WorkflowFirstEventID
func WorkflowFirstEventID(firstEventID int64) Tag {
	return newIntegerTag("wf-first-event-id", firstEventID)
}

// WorkflowNextEventID returns tag for WorkflowNextEventID
func WorkflowNextEventID(nextEventID int64) Tag {
	return newIntegerTag("wf-next-event-id", nextEventID)
}

// WorkflowBeginningFirstEventID returns tag for WorkflowBeginningFirstEventID
func WorkflowBeginningFirstEventID(beginningFirstEventID int64) Tag {
	return newIntegerTag("wf-begining-first-event-id", beginningFirstEventID)
}

// WorkflowEndingNextEventID returns tag for WorkflowEndingNextEventID
func WorkflowEndingNextEventID(endingNextEventID int64) Tag {
	return newIntegerTag("wf-ending-next-event-id", endingNextEventID)
}

// WorkflowResetNextEventID returns tag for WorkflowResetNextEventID
func WorkflowResetNextEventID(resetNextEventID int64) Tag {
	return newIntegerTag("wf-reset-next-event-id", resetNextEventID)
}

// history tree

// WorkflowTreeID returns tag for WorkflowTreeID
func WorkflowTreeID(treeID string) Tag {
	return newStringTag("wf-tree-id", treeID)
}

// WorkflowBranchID returns tag for WorkflowBranchID
func WorkflowBranchID(branchID string) Tag {
	return newStringTag("wf-branch-id", branchID)
}

// workflow task

// WorkflowDecisionType returns tag for WorkflowDecisionType
func WorkflowDecisionType(decisionType int64) Tag {
	return newIntegerTag("wf-decision-type", decisionType)
}

// WorkflowDecisionFailCause returns tag for WorkflowDecisionFailCause
func WorkflowDecisionFailCause(decisionFailCause int64) Tag {
	return newIntegerTag("wf-decision-fail-cause", decisionFailCause)
}

// WorkflowTaskListType returns tag for WorkflowTaskListType
func WorkflowTaskListType(taskListType int64) Tag {
	return newIntegerTag("wf-task-list-type", taskListType)
}

// WorkflowTaskListName returns tag for WorkflowTaskListName
func WorkflowTaskListName(taskListName string) Tag {
	return newStringTag("wf-task-list-name", taskListName)
}

// size limit

// WorkflowSize returns tag for WorkflowSize
func WorkflowSize(workflowSize int64) Tag {
	return newIntegerTag("wf-size", workflowSize)
}

// WorkflowSignalCount returns tag for SignalCount
func WorkflowSignalCount(signalCount int64) Tag {
	return newIntegerTag("wf-signal-count", signalCount)
}

// WorkflowHistorySize returns tag for HistorySize
func WorkflowHistorySize(historySize int64) Tag {
	return newIntegerTag("wf-history-size", historySize)
}

// WorkflowHistorySizeBytes returns tag for HistorySizeBytes
func WorkflowHistorySizeBytes(historySizeBytes int64) Tag {
	return newIntegerTag("wf-history-size-bytes", historySizeBytes)
}

// WorkflowEventCount returns tag for EventCount
func WorkflowEventCount(eventCount int64) Tag {
	return newIntegerTag("wf-event-count", eventCount)
}

///////////////////  System tags defined here:  ///////////////////
// Tags with pre-define values

// Component returns tag for Component
func component(component string) Tag {
	return newPredefinedStringTag("component", component)
}

// Lifecycle returns tag for Lifecycle
func lifecycle(lifecycle string) Tag {
	return newPredefinedStringTag("lifecycle", lifecycle)
}

// StoreOperation returns tag for StoreOperation
func storeOperation(storeOperation string) Tag {
	return newPredefinedStringTag("store-operation", storeOperation)
}

// OperationResult returns tag for OperationResult
func operationResult(operationResult string) Tag {
	return newPredefinedStringTag("operation-result", operationResult)
}

// ErrorType returns tag for ErrorType
func errorType(errorType string) Tag {
	return newPredefinedStringTag("error", errorType)
}

// Shardupdate returns tag for Shardupdate
func shardupdate(shardupdate string) Tag {
	return newPredefinedStringTag("shard-update", shardupdate)
}

// general

// CursorTimestamp returns tag for CursorTimestamp
func CursorTimestamp(timestamp time.Time) Tag {
	return newTimeTag("cursor-timestamp", timestamp)
}

// MetricScope returns tag for MetricScope
func MetricScope(metricScope int64) Tag {
	return newIntegerTag("metric-scope", metricScope)
}

// history engine shard

// ShardID returns tag for ShardID
func ShardID(shardID int64) Tag {
	return newIntegerTag("shard-id", shardID)
}

// ShardTime returns tag for ShardTime
func ShardTime(shardTime time.Time) Tag {
	return newTimeTag("shard-time", shardTime)
}

// ShardReplicationAck returns tag for ShardReplicationAck
func ShardReplicationAck(shardReplicationAck int64) Tag {
	return newIntegerTag("shard-replication-ack", shardReplicationAck)
}

// ShardTransferAcks returns tag for ShardTransferAcks
func ShardTransferAcks(shardTransferAcks int64) Tag {
	return newIntegerTag("shard-transfer-acks", shardTransferAcks)
}

// ShardTimerAcks returns tag for ShardTimerAcks
func ShardTimerAcks(shardTimerAcks int64) Tag {
	return newIntegerTag("shard-timer-acks", shardTimerAcks)
}

// task queue processor

// TaskID returns tag for TaskID
func TaskID(taskID int64) Tag {
	return newIntegerTag("queue-task-id", taskID)
}

// TaskType returns tag for TaskType for queue processor
func TaskType(taskType int64) Tag {
	return newIntegerTag("queue-task-type", taskType)
}

// TimerTaskStatus returns tag for TimerTaskStatus
func TimerTaskStatus(timerTaskStatus int64) Tag {
	return newIntegerTag("timer-task-status", timerTaskStatus)
}

// retry

// Attempt returns tag for Attempt
func Attempt(attempt int64) Tag {
	return newIntegerTag("attempt", attempt)
}

// AttemptCount returns tag for AttemptCount
func AttemptCount(attemptCount int64) Tag {
	return newIntegerTag("attempt-count", attemptCount)
}

// AttemptStart returns tag for AttemptStart
func AttemptStart(attemptStart time.Time) Tag {
	return newTimeTag("attempt-start", attemptStart)
}

// AttemptEnd returns tag for AttemptEnd
func AttemptEnd(attemptEnd time.Time) Tag {
	return newTimeTag("attempt-end", attemptEnd)
}

// ScheduleAttempt returns tag for ScheduleAttempt
func ScheduleAttempt(scheduleAttempt int64) Tag {
	return newIntegerTag("schedule-attempt", scheduleAttempt)
}

// ElastiSearch

// ESRequest returns tag for ESRequest
func ESRequest(ESRequest string) Tag {
	return newStringTag("es-request", ESRequest)
}

// ESKey returns tag for ESKey
func ESKey(ESKey string) Tag {
	return newStringTag("es-mapping-key", ESKey)
}

// ESField returns tag for ESField
func ESField(ESField string) Tag {
	return newStringTag("es-field", ESField)
}

// LoggingCallAtKey is reserved tag
const LoggingCallAtKey = "logging-call-at"

func loggingCallAt(position string) Tag {
	return newStringTag(LoggingCallAtKey, position)
}

// SysStackTrace returns tag for SysStackTrace
func SysStackTrace(stackTrace string) Tag {
	return newStringTag("sys-stack-trace", stackTrace)
}

// Kafka related

// KafkaTopicName returns tag for TopicName
func KafkaTopicName(topicName string) Tag {
	return newStringTag("kafka-topic-name", topicName)
}

// KafkaConsumerName returns tag for ConsumerName
func KafkaConsumerName(consumerName string) Tag {
	return newStringTag("kafka-consumer-name", consumerName)
}

// KafkaPartition returns tag for Partition
func KafkaPartition(partition int64) Tag {
	return newIntegerTag("kafka-partition", partition)
}

// KafkaPartitionKey returns tag for PartitionKey
func KafkaPartitionKey(partitionKey interface{}) Tag {
	return newObjectTag("kafka-partition-key", partitionKey)
}

// KafkaOffset returns tag for Offset
func KafkaOffset(offset int64) Tag {
	return newIntegerTag("kafka-offset", offset)
}

///////////////////  XDC tags defined here: xdc- ///////////////////

// SourceCluster returns tag for SourceCluster
func SourceCluster(sourceCluster string) Tag {
	return newStringTag("xdc-source-cluster", sourceCluster)
}

// PrevActiveCluster returns tag for PrevActiveCluster
func PrevActiveCluster(prevActiveCluster string) Tag {
	return newStringTag("xdc-prev-active-cluster", prevActiveCluster)
}

// FailoverMsg returns tag for FailoverMsg
func FailoverMsg(failoverMsg string) Tag {
	return newStringTag("xdc-failover-msg", failoverMsg)
}

// FailoverVersion returns tag for Version
func FailoverVersion(version int64) Tag {
	return newIntegerTag("xdc-failover-version", version)
}

// CurrentVersion returns tag for CurrentVersion
func CurrentVersion(currentVersion int64) Tag {
	return newIntegerTag("xdc-current-version", currentVersion)
}

// IncomingVersion returns tag for IncomingVersion
func IncomingVersion(incomingVersion int64) Tag {
	return newIntegerTag("xdc-incoming-version", incomingVersion)
}

// ReplicationInfo returns tag for ReplicationInfo
func ReplicationInfo(replicationInfo *persistence.ReplicationInfo) Tag {
	return newObjectTag("xdc-replication-info", replicationInfo)
}

// ReplicationState returns tag for ReplicationState
func ReplicationState(replicationState *persistence.ReplicationState) Tag {
	return newObjectTag("xdc-replication-state", replicationState)
}

///////////////////  Archival tags defined here: archival- ///////////////////
// archival request tags

// ArchivalRequestDomainID returns tag for RequestDomainID
func ArchivalRequestDomainID(requestDomainID string) Tag {
	return newStringTag("archival-request-domain-id", requestDomainID)
}

// ArchivalRequestWorkflowID returns tag for RequestWorkflowID
func ArchivalRequestWorkflowID(requestWorkflowID string) Tag {
	return newStringTag("archival-request-workflow-id", requestWorkflowID)
}

// ArchivalRequestRunID returns tag for RequestRunID
func ArchivalRequestRunID(requestRunID string) Tag {
	return newStringTag("archival-request-run-id", requestRunID)
}

// ArchivalRequestEventStoreVersion returns tag for RequestEventStoreVersion
func ArchivalRequestEventStoreVersion(requestEventStoreVersion int64) Tag {
	return newIntegerTag("archival-request-event-store-version", requestEventStoreVersion)
}

// ArchivalRequestBranchToken returns tag for RequestBranchToken
func ArchivalRequestBranchToken(requestBranchToken string) Tag {
	return newStringTag("archival-request-branch-token", requestBranchToken)
}

// ArchivalRequestNextEventID returns tag for RequestNextEventID
func ArchivalRequestNextEventID(requestNextEventID int64) Tag {
	return newIntegerTag("archival-request-next-event-id", requestNextEventID)
}

// ArchivalRequestCloseFailoverVersion returns tag for RequestCloseFailoverVersion
func ArchivalRequestCloseFailoverVersion(requestCloseFailoverVersion int64) Tag {
	return newIntegerTag("archival-request-close-failover-version", requestCloseFailoverVersion)
}

// archival tags (blob tags)

// ArchivalBucket returns tag for Bucket
func ArchivalBucket(bucket string) Tag {
	return newStringTag("archival-bucket", bucket)
}

// ArchivalBlobKey returns tag for BlobKey
func ArchivalBlobKey(blobKey string) Tag {
	return newStringTag("archival-blob-key", blobKey)
}

// archival tags (file blobstore tags)

// ArchivalFileBlobstoreBlobPath returns tag for FileBlobstoreBlobPath
func ArchivalFileBlobstoreBlobPath(fileBlobstoreBlobPath string) Tag {
	return newStringTag("archival-file-blobstore-blob-path", fileBlobstoreBlobPath)
}

// ArchivalFileBlobstoreMetadataPath returns tag for FileBlobstoreMetadataPath
func ArchivalFileBlobstoreMetadataPath(fileBlobstoreMetadataPath string) Tag {
	return newStringTag("archival-file-blobstore-metadata-path", fileBlobstoreMetadataPath)
}

// ArchivalClusterArchivalStatus returns tag for ClusterArchivalStatus
func ArchivalClusterArchivalStatus(clusterArchivalStatus interface{}) Tag {
	return newObjectTag("archival-cluster-archival-status", clusterArchivalStatus)
}

// ArchivalUploadSkipReason returns tag for UploadSkipReason
func ArchivalUploadSkipReason(uploadSkipReason string) Tag {
	return newStringTag("archival-upload-skip-reason", uploadSkipReason)
}

// UploadFailReason returns tag for UploadFailReason
func UploadFailReason(uploadFailReason string) Tag {
	return newStringTag("archival-upload-fail-reason", uploadFailReason)
}
