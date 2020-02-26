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

namespace java com.temporalio.temporal.admin

include "shared.thrift"
include "replicator.thrift"

struct DescribeWorkflowExecutionRequest {
  10: optional string                       domain
  20: optional shared.WorkflowExecution     execution
}

struct DescribeWorkflowExecutionResponse {
  10: optional string shardId
  20: optional string historyAddr
  40: optional string mutableStateInCache
  50: optional string mutableStateInDatabase
}

struct GetWorkflowExecutionRawHistoryRequest {
  10: optional string domain
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") firstEventId
  40: optional i64 (js.type = "Long") nextEventId
  50: optional i32 maximumPageSize
  60: optional binary nextPageToken
}

struct GetWorkflowExecutionRawHistoryResponse {
  10: optional binary nextPageToken
  20: optional list<shared.DataBlob> historyBatches
  30: optional map<string, shared.ReplicationInfo> replicationInfo
  40: optional i32 eventStoreVersion
}

/**
  * StartEventId defines the beginning of the event to fetch. The first event is exclusive.
  * EndEventId and EndEventVersion defines the end of the event to fetch. The end event is exclusive.
  **/
struct GetWorkflowExecutionRawHistoryV2Request {
  10: optional string domain
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") startEventId
  40: optional i64 (js.type = "Long") startEventVersion
  50: optional i64 (js.type = "Long") endEventId
  60: optional i64 (js.type = "Long") endEventVersion
  70: optional i32 maximumPageSize
  80: optional binary nextPageToken
}

struct GetWorkflowExecutionRawHistoryV2Response {
  10: optional binary nextPageToken
  20: optional list<shared.DataBlob> historyBatches
  30: optional shared.VersionHistory versionHistory
}

struct AddSearchAttributeRequest {
  10: optional map<string, shared.IndexedValueType> searchAttribute
  20: optional string securityToken
}

struct HostInfo {
  10: optional string Identity
}

struct RingInfo {
  10: optional string role
  20: optional i32 memberCount
  30: optional list<HostInfo> members
}

struct MembershipInfo {
  10: optional HostInfo currentHost
  20: optional list<string> reachableMembers
  30: optional list<RingInfo> rings
}

struct DescribeClusterResponse {
  10: optional shared.SupportedClientVersions supportedClientVersions
  20: optional MembershipInfo membershipInfo
}
