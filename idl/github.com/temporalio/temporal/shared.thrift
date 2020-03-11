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

namespace java com.temporalio.temporal

enum WorkflowIdReusePolicy {
  /*
   * allow start a workflow execution using the same workflow ID,
   * when workflow not running, and the last execution close state is in
   * [terminated, cancelled, timeouted, failed].
   */
  AllowDuplicateFailedOnly,
  /*
   * allow start a workflow execution using the same workflow ID,
   * when workflow not running.
   */
  AllowDuplicate,
  /*
   * do not allow start a workflow execution using the same workflow ID at all
   */
  RejectDuplicate,
}

enum DomainStatus {
  REGISTERED,
  DEPRECATED,
  DELETED,
}

enum TimeoutType {
    StartToClose,
    ScheduleToStart,
    ScheduleToClose,
    Heartbeat,
}

enum ParentClosePolicy {
	ABANDON,
	REQUEST_CANCEL,
	TERMINATE,
}


// whenever this list of decision is changed
// do change the mutableStateBuilder.go
// function shouldBufferEvent
// to make sure wo do the correct event ordering
enum DecisionType {
  ScheduleActivityTask,
  RequestCancelActivityTask,
  StartTimer,
  CompleteWorkflowExecution,
  FailWorkflowExecution,
  CancelTimer,
  CancelWorkflowExecution,
  RequestCancelExternalWorkflowExecution,
  RecordMarker,
  ContinueAsNewWorkflowExecution,
  StartChildWorkflowExecution,
  SignalExternalWorkflowExecution,
  UpsertWorkflowSearchAttributes,
}

enum EventType {
  WorkflowExecutionStarted,
  WorkflowExecutionCompleted,
  WorkflowExecutionFailed,
  WorkflowExecutionTimedOut,
  DecisionTaskScheduled,
  DecisionTaskStarted,
  DecisionTaskCompleted,
  DecisionTaskTimedOut
  DecisionTaskFailed,
  ActivityTaskScheduled,
  ActivityTaskStarted,
  ActivityTaskCompleted,
  ActivityTaskFailed,
  ActivityTaskTimedOut,
  ActivityTaskCancelRequested,
  RequestCancelActivityTaskFailed,
  ActivityTaskCanceled,
  TimerStarted,
  TimerFired,
  CancelTimerFailed,
  TimerCanceled,
  WorkflowExecutionCancelRequested,
  WorkflowExecutionCanceled,
  RequestCancelExternalWorkflowExecutionInitiated,
  RequestCancelExternalWorkflowExecutionFailed,
  ExternalWorkflowExecutionCancelRequested,
  MarkerRecorded,
  WorkflowExecutionSignaled,
  WorkflowExecutionTerminated,
  WorkflowExecutionContinuedAsNew,
  StartChildWorkflowExecutionInitiated,
  StartChildWorkflowExecutionFailed,
  ChildWorkflowExecutionStarted,
  ChildWorkflowExecutionCompleted,
  ChildWorkflowExecutionFailed,
  ChildWorkflowExecutionCanceled,
  ChildWorkflowExecutionTimedOut,
  ChildWorkflowExecutionTerminated,
  SignalExternalWorkflowExecutionInitiated,
  SignalExternalWorkflowExecutionFailed,
  ExternalWorkflowExecutionSignaled,
  UpsertWorkflowSearchAttributes,
}

enum DecisionTaskFailedCause {
  UNHANDLED_DECISION,
  BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
  BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES,
  BAD_START_TIMER_ATTRIBUTES,
  BAD_CANCEL_TIMER_ATTRIBUTES,
  BAD_RECORD_MARKER_ATTRIBUTES,
  BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES,
  BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES,
  BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES,
  BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
  BAD_CONTINUE_AS_NEW_ATTRIBUTES,
  START_TIMER_DUPLICATE_ID,
  RESET_STICKY_TASKLIST,
  WORKFLOW_WORKER_UNHANDLED_FAILURE,
  BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
  BAD_START_CHILD_EXECUTION_ATTRIBUTES,
  FORCE_CLOSE_DECISION,
  FAILOVER_CLOSE_DECISION,
  BAD_SIGNAL_INPUT_SIZE,
  RESET_WORKFLOW,
  BAD_BINARY,
  SCHEDULE_ACTIVITY_DUPLICATE_ID,
  BAD_SEARCH_ATTRIBUTES,
}

enum CancelExternalWorkflowExecutionFailedCause {
  UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION,
}

enum SignalExternalWorkflowExecutionFailedCause {
  UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION,
}

enum ChildWorkflowExecutionFailedCause {
  WORKFLOW_ALREADY_RUNNING,
}

// TODO: when migrating to gRPC, add a running / none status,
//  currently, customer is using null / nil as an indication
//  that workflow is still running
enum WorkflowExecutionCloseStatus {
  COMPLETED,
  FAILED,
  CANCELED,
  TERMINATED,
  CONTINUED_AS_NEW,
  TIMED_OUT,
}

enum QueryTaskCompletedType {
  COMPLETED,
  FAILED,
}

enum QueryResultType {
  ANSWERED,
  FAILED,
}

enum PendingActivityState {
  SCHEDULED,
  STARTED,
  CANCEL_REQUESTED,
}

enum HistoryEventFilterType {
  ALL_EVENT,
  CLOSE_EVENT,
}

enum TaskListKind {
  NORMAL,
  STICKY,
}

enum ArchivalStatus {
  DISABLED,
  ENABLED,
}

enum IndexedValueType {
  STRING,
  KEYWORD,
  INT,
  DOUBLE,
  BOOL,
  DATETIME,
}

struct Header {
    10: optional map<string, binary> fields
}

struct WorkflowType {
  10: optional string name
}

struct ActivityType {
  10: optional string name
}

struct TaskList {
  10: optional string name
  20: optional TaskListKind kind
}

enum EncodingType {
  ThriftRW,
  JSON,
}

enum QueryRejectCondition {
  // NOT_OPEN indicates that query should be rejected if workflow is not open
  NOT_OPEN
  // NOT_COMPLETED_CLEANLY indicates that query should be rejected if workflow did not complete cleanly
  NOT_COMPLETED_CLEANLY
}

enum QueryConsistencyLevel {
  // EVENTUAL indicates that query should be eventually consistent
  EVENTUAL
  // STRONG indicates that any events that came before query should be reflected in workflow state before running query
  STRONG
}

struct DataBlob {
  10: optional EncodingType EncodingType
  20: optional binary Data
}

struct ReplicationInfo {
  10: optional i64 (js.type = "Long") version
  20: optional i64 (js.type = "Long") lastEventId
}

struct TaskListMetadata {
  10: optional double maxTasksPerSecond
}

struct SearchAttributes {
  10: optional map<string,binary> indexedFields
}

struct WorkerVersionInfo {
  10: optional string impl
  20: optional string featureVersion
}

struct WorkflowExecutionConfiguration {
  10: optional TaskList taskList
  20: optional i32 executionStartToCloseTimeoutSeconds
  30: optional i32 taskStartToCloseTimeoutSeconds
//  40: optional ChildPolicy childPolicy -- Removed but reserve the IDL order number
}

struct ResetPoints{
  10: optional list<ResetPointInfo> points
}

 struct ResetPointInfo{
  10: optional string binaryChecksum
  20: optional string runId
  30: optional i64 firstDecisionCompletedId
  40: optional i64 (js.type = "Long") createdTimeNano
  50: optional i64 (js.type = "Long") expiringTimeNano //the time that the run is deleted due to retention
  60: optional bool resettable                         // false if the resset point has pending childWFs/reqCancels/signalExternals.
}

struct WorkflowExecutionCompletedEventAttributes {
  10: optional binary result
  20: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct WorkflowExecutionFailedEventAttributes {
  10: optional string reason
  20: optional binary details
  30: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct WorkflowExecutionTimedOutEventAttributes {
  10: optional TimeoutType timeoutType
}

enum ContinueAsNewInitiator {
  Decider,
  RetryPolicy,
  CronSchedule,
}


struct WorkflowExecutionFilter {
  10: optional string workflowId
  20: optional string runId
}

struct WorkflowTypeFilter {
  10: optional string name
}

struct StartTimeFilter {
  10: optional i64 (js.type = "Long") earliestTime
  20: optional i64 (js.type = "Long") latestTime
}

struct DomainInfo {
  10: optional string name
  20: optional DomainStatus status
  30: optional string description
  40: optional string ownerEmail
  // A key-value map for any customized purpose
  50: optional map<string,string> data
  60: optional string uuid
}

struct UpdateDomainInfo {
  10: optional string description
  20: optional string ownerEmail
  // A key-value map for any customized purpose
  30: optional map<string,string> data
}

struct ClusterReplicationConfiguration {
 10: optional string clusterName
}

struct DomainReplicationConfiguration {
 10: optional string activeClusterName
 20: optional list<ClusterReplicationConfiguration> clusters
}

struct RegisterDomainRequest {
  10: optional string name
  20: optional string description
  30: optional string ownerEmail
  40: optional i32 workflowExecutionRetentionPeriodInDays
  50: optional bool emitMetric = true
  60: optional list<ClusterReplicationConfiguration> clusters
  70: optional string activeClusterName
  // A key-value map for any customized purpose
  80: optional map<string,string> data
  90: optional string securityToken
  120: optional bool isGlobalDomain
  130: optional ArchivalStatus historyArchivalStatus
  140: optional string historyArchivalURI
  150: optional ArchivalStatus visibilityArchivalStatus
  160: optional string visibilityArchivalURI
}

struct StartWorkflowExecutionResponse {
  10: optional string runId
}

struct PollForDecisionTaskRequest {
  10: optional string domain
  20: optional TaskList taskList
  30: optional string identity
  40: optional string binaryChecksum
}

struct StickyExecutionAttributes {
  10: optional TaskList workerTaskList
  20: optional i32 scheduleToStartTimeoutSeconds
}

struct RespondDecisionTaskFailedRequest {
  10: optional binary taskToken
  20: optional DecisionTaskFailedCause cause
  30: optional binary details
  40: optional string identity
  50: optional string binaryChecksum
}

struct PollForActivityTaskRequest {
  10: optional string domain
  20: optional TaskList taskList
  30: optional string identity
  40: optional TaskListMetadata taskListMetadata
}

struct RecordActivityTaskHeartbeatRequest {
  10: optional binary taskToken
  20: optional binary details
  30: optional string identity
}

struct RecordActivityTaskHeartbeatByIDRequest {
  10: optional string domain
  20: optional string workflowID
  30: optional string runID
  40: optional string activityID
  50: optional binary details
  60: optional string identity
}

struct RecordActivityTaskHeartbeatResponse {
  10: optional bool cancelRequested
}

struct RespondActivityTaskCompletedRequest {
  10: optional binary taskToken
  20: optional binary result
  30: optional string identity
}

struct RespondActivityTaskFailedRequest {
  10: optional binary taskToken
  20: optional string reason
  30: optional binary details
  40: optional string identity
}

struct RespondActivityTaskCanceledRequest {
  10: optional binary taskToken
  20: optional binary details
  30: optional string identity
}

struct RespondActivityTaskCompletedByIDRequest {
  10: optional string domain
  20: optional string workflowID
  30: optional string runID
  40: optional string activityID
  50: optional binary result
  60: optional string identity
}

struct RespondActivityTaskFailedByIDRequest {
  10: optional string domain
  20: optional string workflowID
  30: optional string runID
  40: optional string activityID
  50: optional string reason
  60: optional binary details
  70: optional string identity
}

struct RespondActivityTaskCanceledByIDRequest {
  10: optional string domain
  20: optional string workflowID
  30: optional string runID
  40: optional string activityID
  50: optional binary details
  60: optional string identity
}

struct ResetWorkflowExecutionResponse {
  10: optional string runId
}

struct ListOpenWorkflowExecutionsRequest {
  10: optional string domain
  20: optional i32 maximumPageSize
  30: optional binary nextPageToken
  40: optional StartTimeFilter StartTimeFilter
  50: optional WorkflowExecutionFilter executionFilter
  60: optional WorkflowTypeFilter typeFilter
}

struct ListClosedWorkflowExecutionsRequest {
  10: optional string domain
  20: optional i32 maximumPageSize
  30: optional binary nextPageToken
  40: optional StartTimeFilter StartTimeFilter
  50: optional WorkflowExecutionFilter executionFilter
  60: optional WorkflowTypeFilter typeFilter
  70: optional WorkflowExecutionCloseStatus statusFilter
}

struct ListWorkflowExecutionsRequest {
  10: optional string domain
  20: optional i32 pageSize
  30: optional binary nextPageToken
  40: optional string query
}
struct ListArchivedWorkflowExecutionsRequest {
  10: optional string domain
  20: optional i32 pageSize
  30: optional binary nextPageToken
  40: optional string query
}

struct CountWorkflowExecutionsRequest {
  10: optional string domain
  20: optional string query
}

struct CountWorkflowExecutionsResponse {
  10: optional i64 count
}

struct GetSearchAttributesResponse {
  10: optional map<string, IndexedValueType> keys
}

struct QueryRejected {
  10: optional WorkflowExecutionCloseStatus closeStatus
}

struct QueryWorkflowResponse {
  10: optional binary queryResult
  20: optional QueryRejected queryRejected
}

struct WorkflowQuery {
  10: optional string queryType
  20: optional binary queryArgs
}


struct ResetStickyTaskListResponse {
    // The reason to keep this response is to allow returning
    // information in the future.
}

struct RespondQueryTaskCompletedRequest {
  10: optional binary taskToken
  20: optional QueryTaskCompletedType completedType
  30: optional binary queryResult
  40: optional string errorMessage
  50: optional WorkerVersionInfo workerVersionInfo
}

struct WorkflowQueryResult {
  10: optional QueryResultType resultType
  20: optional binary answer
  30: optional string errorMessage
}

struct PendingActivityInfo {
  10: optional string activityID
  20: optional ActivityType activityType
  30: optional PendingActivityState state
  40: optional binary heartbeatDetails
  50: optional i64 (js.type = "Long") lastHeartbeatTimestamp
  60: optional i64 (js.type = "Long") lastStartedTimestamp
  70: optional i32 attempt
  80: optional i32 maximumAttempts
  90: optional i64 (js.type = "Long") scheduledTimestamp
  100: optional i64 (js.type = "Long") expirationTimestamp
  110: optional string lastFailureReason
  120: optional string lastWorkerIdentity
  130: optional binary lastFailureDetails
}

struct PendingChildExecutionInfo {
  10: optional string workflowID
  20: optional string runID
  30: optional string workflowTypName
  40: optional i64 (js.type = "Long") initiatedID
  50: optional ParentClosePolicy parentClosePolicy
}
struct DescribeTaskListRequest {
  10: optional string domain
  20: optional TaskList taskList
  30: optional TaskListType taskListType
  40: optional bool includeTaskListStatus
}

struct DescribeTaskListResponse {
  10: optional list<PollerInfo> pollers
  20: optional TaskListStatus taskListStatus
}

struct ListTaskListPartitionsRequest {
  10: optional string domain
  20: optional TaskList taskList
}

struct TaskListPartitionMetadata {
  10: optional string key
  20: optional string ownerHostName
}

struct ListTaskListPartitionsResponse {
  10: optional list<TaskListPartitionMetadata> activityTaskListPartitions
  20: optional list<TaskListPartitionMetadata> decisionTaskListPartitions
}

struct TaskListStatus {
  10: optional i64 (js.type = "Long") backlogCountHint
  20: optional i64 (js.type = "Long") readLevel
  30: optional i64 (js.type = "Long") ackLevel
  35: optional double ratePerSecond
  40: optional TaskIDBlock taskIDBlock
}

struct TaskIDBlock {
  10: optional i64 (js.type = "Long")  startID
  20: optional i64 (js.type = "Long")  endID
}


struct RemoveTaskRequest {
  10: optional i32                      shardID
  20: optional i32                      type
  30: optional i64 (js.type = "Long")   taskID
}

struct CloseShardRequest {
  10: optional i32               shardID
}

struct DescribeHistoryHostResponse{
  10: optional i32                  numberOfShards
  20: optional list<i32>            shardIDs
  30: optional DomainCacheInfo      domainCache
  40: optional string               shardControllerStatus
  50: optional string               address
}

struct DomainCacheInfo{
  10: optional i64 numOfItemsInCacheByID
  20: optional i64 numOfItemsInCacheByName
}

enum TaskListType {
  /*
   * Decision type of tasklist
   */
  Decision,
  /*
   * Activity type of tasklist
   */
  Activity,
}

struct PollerInfo {
  // Unix Nano
  10: optional i64 (js.type = "Long")  lastAccessTime
  20: optional string identity
  30: optional double ratePerSecond
}

struct RetryPolicy {
  // Interval of the first retry. If coefficient is 1.0 then it is used for all retries.
  10: optional i32 initialIntervalInSeconds

  // Coefficient used to calculate the next retry interval.
  // The next retry interval is previous interval multiplied by the coefficient.
  // Must be 1 or larger.
  20: optional double backoffCoefficient

  // Maximum interval between retries. Exponential backoff leads to interval increase.
  // This value is the cap of the increase. Default is 100x of initial interval.
  30: optional i32 maximumIntervalInSeconds

  // Maximum number of attempts. When exceeded the retries stop even if not expired yet.
  // Must be 1 or bigger. Default is unlimited.
  40: optional i32 maximumAttempts

  // Non-Retriable errors. Will stop retrying if error matches this list.
  50: optional list<string> nonRetriableErrorReasons

  // Expiration time for the whole retry process.
  60: optional i32 expirationIntervalInSeconds
}

// SupportedClientVersions contains the support versions for client library
struct SupportedClientVersions{
  10: optional string goSdk
  20: optional string javaSdk
}

// ClusterInfo contains information about cadence cluster
struct ClusterInfo{
  10: optional SupportedClientVersions supportedClientVersions
}


