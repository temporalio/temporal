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

namespace java com.uber.cadence.replicator

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

struct SyncActicvityTaskAttributes {
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
  50: optional SyncActicvityTaskAttributes syncActicvityTaskAttributes
  60: optional HistoryMetadataTaskAttributes historyMetadataTaskAttributes // TODO deprecate once kafka deprecation is done
  70: optional HistoryTaskV2Attributes historyTaskV2Attributes
}

struct ReplicationToken {
  10: optional i32 shardID
  // lastRetrivedMessageId is where the next fetch should begin with
  20: optional i64 (js.type = "Long") lastRetrivedMessageId
  // lastProcessedMessageId is the last messageId that is processed on the passive side.
  // This can be different than lastRetrivedMessageId if passive side supports prefetching messages.
  30: optional i64 (js.type = "Long") lastProcessedMessageId
}

struct ReplicationMessages {
  10: optional list<ReplicationTask> replicationTasks
  // This can be different than the last taskId in the above list, because sender can decide to skip tasks (e.g. for completed workflows).
  20: optional i64 (js.type = "Long") lastRetrivedMessageId
  30: optional bool hasMore // Hint for flow control
}

struct GetReplicationMessagesRequest {
  10: optional list<ReplicationToken> tokens
}

struct GetReplicationMessagesResponse {
  10: optional map<i32, ReplicationMessages> messagesByShard
}

struct GetDomainReplicationMessagesRequest {
  // lastRetrivedMessageId is where the next fetch should begin with
  10: optional i64 (js.type = "Long") lastRetrivedMessageId
  // lastProcessedMessageId is the last messageId that is processed on the passive side.
  // This can be different than lastRetrivedMessageId if passive side supports prefetching messages.
  20: optional i64 (js.type = "Long") lastProcessedMessageId
  // clusterName is the name of the pulling cluster
  30: optional string clusterName
}

struct GetDomainReplicationMessagesResponse {
  10: optional ReplicationMessages messages
}