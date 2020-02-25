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

namespace java com.temporalio.temporal.replicator

include "shared.thrift"

enum ReplicationTaskType {
  Domain
  History
  SyncShardStatus
  SyncActivity
  HistoryMetadata
  HistoryV2
}

enum DomainOperation {
  Create
  Update
}

struct DomainTaskAttributes {
  05: optional DomainOperation domainOperation
  10: optional string id
  20: optional shared.DomainInfo info
  30: optional shared.DomainConfiguration config
  40: optional shared.DomainReplicationConfiguration replicationConfig
  50: optional i64 (js.type = "Long") configVersion
  60: optional i64 (js.type = "Long") failoverVersion
}

struct HistoryTaskAttributes {
  05: optional list<string> targetClusters
  10: optional string domainId
  20: optional string workflowId
  30: optional string runId
  40: optional i64 (js.type = "Long") firstEventId
  50: optional i64 (js.type = "Long") nextEventId
  60: optional i64 (js.type = "Long") version
  70: optional map<string, shared.ReplicationInfo> replicationInfo
  80: optional shared.History history
  90: optional shared.History newRunHistory
  100: optional i32 eventStoreVersion
  110: optional i32 newRunEventStoreVersion
  120: optional bool resetWorkflow
  130: optional bool newRunNDC
}

struct HistoryMetadataTaskAttributes {
  05: optional list<string> targetClusters
  10: optional string domainId
  20: optional string workflowId
  30: optional string runId
  40: optional i64 (js.type = "Long") firstEventId
  50: optional i64 (js.type = "Long") nextEventId
}

struct SyncShardStatusTaskAttributes {
  10: optional string sourceCluster
  20: optional i64 (js.type = "Long") shardId
  30: optional i64 (js.type = "Long") timestamp
}

struct SyncActivityTaskAttributes {
  10: optional string domainId
  20: optional string workflowId
  30: optional string runId
  40: optional i64 (js.type = "Long") version
  50: optional i64 (js.type = "Long") scheduledId
  60: optional i64 (js.type = "Long") scheduledTime
  70: optional i64 (js.type = "Long") startedId
  80: optional i64 (js.type = "Long") startedTime
  90: optional i64 (js.type = "Long") lastHeartbeatTime
  100: optional binary details
  110: optional i32 attempt
  120: optional string lastFailureReason
  130: optional string lastWorkerIdentity
  140: optional binary lastFailureDetails
  150: optional shared.VersionHistory versionHistory
}

struct HistoryTaskV2Attributes {
  05: optional i64 (js.type = "Long") taskId
  10: optional string domainId
  20: optional string workflowId
  30: optional string runId
  40: optional list<shared.VersionHistoryItem> versionHistoryItems
  50: optional shared.DataBlob events
  // new run events does not need version history since there is no prior events
  70: optional shared.DataBlob newRunEvents
}

struct ReplicationTask {
  10: optional ReplicationTaskType taskType
  11: optional i64 (js.type = "Long") sourceTaskId
  20: optional DomainTaskAttributes domainTaskAttributes
  30: optional HistoryTaskAttributes historyTaskAttributes  // TODO deprecate once NDC migration is done
  40: optional SyncShardStatusTaskAttributes syncShardStatusTaskAttributes
  50: optional SyncActivityTaskAttributes syncActivityTaskAttributes
  60: optional HistoryMetadataTaskAttributes historyMetadataTaskAttributes // TODO deprecate once kafka deprecation is done
  70: optional HistoryTaskV2Attributes historyTaskV2Attributes
}

struct ReplicationToken {
  10: optional i32 shardID
  // lastRetrivedMessageId is where the next fetch should begin with
  20: optional i64 (js.type = "Long") lastRetrievedMessageId
  // lastProcessedMessageId is the last messageId that is processed on the passive side.
  // This can be different than lastRetrievedMessageId if passive side supports prefetching messages.
  30: optional i64 (js.type = "Long") lastProcessedMessageId
}

struct SyncShardStatus {
    10: optional i64 (js.type = "Long") timestamp
}

struct ReplicationMessages {
  10: optional list<ReplicationTask> replicationTasks
  // This can be different than the last taskId in the above list, because sender can decide to skip tasks (e.g. for completed workflows).
  20: optional i64 (js.type = "Long") lastRetrievedMessageId
  30: optional bool hasMore // Hint for flow control
  40: optional SyncShardStatus syncShardStatus
}

struct ReplicationTaskInfo {
  10: optional string domainID
  20: optional string workflowID
  30: optional string runID
  40: optional i16 taskType
  50: optional i64 (js.type = "Long") taskID
  60: optional i64 (js.type = "Long") version
  70: optional i64 (js.type = "Long") firstEventID
  80: optional i64 (js.type = "Long") nextEventID
  90: optional i64 (js.type = "Long") scheduledID
}

struct GetReplicationMessagesRequest {
  10: optional list<ReplicationToken> tokens
  20: optional string clusterName
}

struct GetReplicationMessagesResponse {
  10: optional map<i32, ReplicationMessages> messagesByShard
}

struct GetDomainReplicationMessagesRequest {
  // lastRetrievedMessageId is where the next fetch should begin with
  10: optional i64 (js.type = "Long") lastRetrievedMessageId
  // lastProcessedMessageId is the last messageId that is processed on the passive side.
  // This can be different than lastRetrievedMessageId if passive side supports prefetching messages.
  20: optional i64 (js.type = "Long") lastProcessedMessageId
  // clusterName is the name of the pulling cluster
  30: optional string clusterName
}

struct GetDomainReplicationMessagesResponse {
  10: optional ReplicationMessages messages
}

struct GetDLQReplicationMessagesRequest {
  10: optional list<ReplicationTaskInfo> taskInfos
}

struct GetDLQReplicationMessagesResponse {
  10: optional list<ReplicationTask> replicationTasks
}

enum DLQType {
  Replication,
  Domain,
}

struct ReadDLQMessagesRequest{
  10: optional DLQType type
  20: optional i32 shardID
  30: optional string sourceCluster
  40: optional i64 (js.type = "Long") inclusiveEndMessageID
  50: optional i32 maximumPageSize
  60: optional binary nextPageToken
}

struct ReadDLQMessagesResponse{
  10: optional DLQType type
  20: optional list<ReplicationTask> replicationTasks
  30: optional binary nextPageToken
}

struct PurgeDLQMessagesRequest{
  10: optional DLQType type
  20: optional i32 shardID
  30: optional string sourceCluster
  40: optional i64 (js.type = "Long") inclusiveEndMessageID
}

struct MergeDLQMessagesRequest{
  10: optional DLQType type
  20: optional i32 shardID
  30: optional string sourceCluster
  40: optional i64 (js.type = "Long") inclusiveEndMessageID
  50: optional i32 maximumPageSize
  60: optional binary nextPageToken
}

struct MergeDLQMessagesResponse{
  10: optional binary nextPageToken
}
