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

namespace java com.temporalio.temporal.sqlblobs

include "shared.thrift"

struct DomainInfo {
  10: optional string name
  12: optional string description
  14: optional string owner
  16: optional i32 status
  18: optional i16 retentionDays
  20: optional bool emitMetric
  22: optional string archivalBucket
  24: optional i16 archivalStatus
  26: optional i64 (js.type = "Long") configVersion
  28: optional i64 (js.type = "Long") notificationVersion
  30: optional i64 (js.type = "Long") failoverNotificationVersion
  32: optional i64 (js.type = "Long") failoverVersion
  34: optional string activeClusterName
  36: optional list<string> clusters
  38: optional map<string, string> data
  39: optional binary badBinaries
  40: optional string badBinariesEncoding
  42: optional i16 historyArchivalStatus
  44: optional string historyArchivalURI
  46: optional i16 visibilityArchivalStatus
  48: optional string visibilityArchivalURI
}

struct ReplicationInfo {
  10: optional i64 (js.type = "Long") version
  12: optional i64 (js.type = "Long") lastEventID
}

struct WorkflowExecutionInfo {
  10: optional binary parentDomainID
  12: optional string parentWorkflowID
  14: optional binary parentRunID
  16: optional i64 (js.type = "Long") initiatedID
  18: optional i64 (js.type = "Long") completionEventBatchID
  20: optional binary completionEvent
  22: optional string completionEventEncoding
  24: optional string taskList
  26: optional string workflowTypeName
  28: optional i32 workflowTimeoutSeconds
  30: optional i32 decisionTaskTimeoutSeconds
  32: optional binary executionContext
  34: optional i32 state
  36: optional i32 closeStatus
  38: optional i64 (js.type = "Long") startVersion
  40: optional i64 (js.type = "Long") currentVersion
  44: optional i64 (js.type = "Long") lastWriteEventID
  46: optional map<string, ReplicationInfo> lastReplicationInfo
  48: optional i64 (js.type = "Long") lastEventTaskID
  50: optional i64 (js.type = "Long") lastFirstEventID
  52: optional i64 (js.type = "Long") lastProcessedEvent
  54: optional i64 (js.type = "Long") startTimeNanos
  56: optional i64 (js.type = "Long") lastUpdatedTimeNanos
  58: optional i64 (js.type = "Long") decisionVersion
  60: optional i64 (js.type = "Long") decisionScheduleID
  62: optional i64 (js.type = "Long") decisionStartedID
  64: optional i32 decisionTimeout
  66: optional i64 (js.type = "Long") decisionAttempt
  68: optional i64 (js.type = "Long") decisionStartedTimestampNanos
  69: optional i64 (js.type = "Long") decisionScheduledTimestampNanos
  70: optional bool cancelRequested
  71: optional i64 (js.type = "Long") decisionOriginalScheduledTimestampNanos
  72: optional string createRequestID
  74: optional string decisionRequestID
  76: optional string cancelRequestID
  78: optional string stickyTaskList
  80: optional i64 (js.type = "Long") stickyScheduleToStartTimeout
  82: optional i64 (js.type = "Long") retryAttempt
  84: optional i32 retryInitialIntervalSeconds
  86: optional i32 retryMaximumIntervalSeconds
  88: optional i32 retryMaximumAttempts
  90: optional i32 retryExpirationSeconds
  92: optional double retryBackoffCoefficient
  94: optional i64 (js.type = "Long") retryExpirationTimeNanos
  96: optional list<string> retryNonRetryableErrors
  98: optional bool hasRetryPolicy
  100: optional string cronSchedule
  102: optional i32 eventStoreVersion
  104: optional binary eventBranchToken
  106: optional i64 (js.type = "Long") signalCount
  108: optional i64 (js.type = "Long") historySize
  110: optional string clientLibraryVersion
  112: optional string clientFeatureVersion
  114: optional string clientImpl
  115: optional binary autoResetPoints
  116: optional string autoResetPointsEncoding
  118: optional map<string, binary> searchAttributes
  120: optional map<string, binary> memo
  122: optional binary versionHistories
  124: optional string versionHistoriesEncoding
}

struct ActivityInfo {
  10: optional i64 (js.type = "Long") version
  12: optional i64 (js.type = "Long") scheduledEventBatchID
  14: optional binary scheduledEvent
  16: optional string scheduledEventEncoding
  18: optional i64 (js.type = "Long") scheduledTimeNanos
  20: optional i64 (js.type = "Long") startedID
  22: optional binary startedEvent
  24: optional string startedEventEncoding
  26: optional i64 (js.type = "Long") startedTimeNanos
  28: optional string activityID
  30: optional string requestID
  32: optional i32 scheduleToStartTimeoutSeconds
  34: optional i32 scheduleToCloseTimeoutSeconds
  36: optional i32 startToCloseTimeoutSeconds
  38: optional i32 heartbeatTimeoutSeconds
  40: optional bool cancelRequested
  42: optional i64 (js.type = "Long") cancelRequestID
  44: optional i32 timerTaskStatus
  46: optional i32 attempt
  48: optional string taskList
  50: optional string startedIdentity
  52: optional bool hasRetryPolicy
  54: optional i32 retryInitialIntervalSeconds
  56: optional i32 retryMaximumIntervalSeconds
  58: optional i32 retryMaximumAttempts
  60: optional i64 (js.type = "Long") retryExpirationTimeNanos
  62: optional double retryBackoffCoefficient
  64: optional list<string> retryNonRetryableErrors
  66: optional string retryLastFailureReason
  68: optional string retryLastWorkerIdentity
  70: optional binary retryLastFailureDetails
}

struct ChildExecutionInfo {
  10: optional i64 (js.type = "Long") version
  12: optional i64 (js.type = "Long") initiatedEventBatchID
  14: optional i64 (js.type = "Long") startedID
  16: optional binary initiatedEvent
  18: optional string initiatedEventEncoding
  20: optional string startedWorkflowID
  22: optional binary startedRunID
  24: optional binary startedEvent
  26: optional string startedEventEncoding
  28: optional string createRequestID
  30: optional string domainName
  32: optional string workflowTypeName
  35: optional i32 parentClosePolicy
}
