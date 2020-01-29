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

package adapter

import (
	"go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/go/replicator"
	"github.com/temporalio/temporal/.gen/go/shared"
)

func toProtoBool(in *bool) *common.BoolValue {
	if in == nil {
		return nil
	}

	return &common.BoolValue{Value: *in}
}

func toThriftBool(in *common.BoolValue) *bool {
	if in == nil {
		return nil
	}

	return &in.Value
}

func toProtoDomainInfo(in *shared.DomainInfo) *common.DomainInfo {
	if in == nil {
		return nil
	}
	return &common.DomainInfo{
		Name:        in.GetName(),
		Status:      enums.DomainStatus(in.GetStatus()),
		Description: in.GetDescription(),
		OwnerEmail:  in.GetOwnerEmail(),
		Data:        in.GetData(),
		Uuid:        in.GetUUID(),
	}
}

func toProtoDomainReplicationConfiguration(in *shared.DomainReplicationConfiguration) *common.DomainReplicationConfiguration {
	if in == nil {
		return nil
	}
	return &common.DomainReplicationConfiguration{
		ActiveClusterName: in.GetActiveClusterName(),
		Clusters:          toProtoClusterReplicationConfigurations(in.GetClusters()),
	}
}

func toProtoDomainConfiguration(in *shared.DomainConfiguration) *common.DomainConfiguration {
	if in == nil {
		return nil
	}
	return &common.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: in.GetWorkflowExecutionRetentionPeriodInDays(),
		EmitMetric:                             toProtoBool(in.EmitMetric),
		BadBinaries:                            toProtoBadBinaries(in.GetBadBinaries()),
		HistoryArchivalStatus:                  ToProtoArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     in.GetVisibilityArchivalURI(),
		VisibilityArchivalStatus:               ToProtoArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  in.GetVisibilityArchivalURI(),
	}
}

func toProtoBadBinaries(in *shared.BadBinaries) *common.BadBinaries {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.BadBinaryInfo, len(in.GetBinaries()))

	for key, value := range in.GetBinaries() {
		ret[key] = toProtoBadBinaryInfo(value)
	}

	return &common.BadBinaries{
		Binaries: ret,
	}
}

func toProtoBadBinaryInfo(in *shared.BadBinaryInfo) *common.BadBinaryInfo {
	if in == nil {
		return nil
	}
	return &common.BadBinaryInfo{
		Reason:          in.GetReason(),
		Operator:        in.GetOperator(),
		CreatedTimeNano: in.GetCreatedTimeNano(),
	}
}

func toThriftClusterReplicationConfigurations(in []*common.ClusterReplicationConfiguration) []*shared.ClusterReplicationConfiguration {
	var ret []*shared.ClusterReplicationConfiguration
	for _, cluster := range in {
		ret = append(ret, &shared.ClusterReplicationConfiguration{ClusterName: &cluster.ClusterName})
	}

	return ret
}

func toProtoClusterReplicationConfigurations(in []*shared.ClusterReplicationConfiguration) []*common.ClusterReplicationConfiguration {
	var ret []*common.ClusterReplicationConfiguration
	for _, cluster := range in {
		ret = append(ret, &common.ClusterReplicationConfiguration{ClusterName: *cluster.ClusterName})
	}

	return ret
}

func toThriftUpdateDomainInfo(in *common.UpdateDomainInfo) *shared.UpdateDomainInfo {
	if in == nil {
		return nil
	}
	return &shared.UpdateDomainInfo{
		Description: &in.Description,
		OwnerEmail:  &in.OwnerEmail,
		Data:        in.Data,
	}
}
func toThriftDomainConfiguration(in *common.DomainConfiguration) *shared.DomainConfiguration {
	if in == nil {
		return nil
	}
	return &shared.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: &in.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             toThriftBool(in.EmitMetric),
		BadBinaries:                            ToThriftBadBinaries(in.BadBinaries),
		HistoryArchivalStatus:                  ToThriftArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     &in.HistoryArchivalURI,
		VisibilityArchivalStatus:               ToThriftArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  &in.VisibilityArchivalURI,
	}
}
func toThriftDomainReplicationConfiguration(in *common.DomainReplicationConfiguration) *shared.DomainReplicationConfiguration {
	if in == nil {
		return nil
	}
	return &shared.DomainReplicationConfiguration{
		ActiveClusterName: &in.ActiveClusterName,
		Clusters:          toThriftClusterReplicationConfigurations(in.Clusters),
	}
}

// ToThriftBadBinaries ...
func ToThriftBadBinaries(in *common.BadBinaries) *shared.BadBinaries {
	if in == nil {
		return nil
	}
	ret := make(map[string]*shared.BadBinaryInfo, len(in.Binaries))

	for key, value := range in.Binaries {
		ret[key] = toThriftBadBinaryInfo(value)
	}

	return &shared.BadBinaries{
		Binaries: ret,
	}
}

func toThriftBadBinaryInfo(in *common.BadBinaryInfo) *shared.BadBinaryInfo {
	if in == nil {
		return nil
	}
	return &shared.BadBinaryInfo{
		Reason:          &in.Reason,
		Operator:        &in.Operator,
		CreatedTimeNano: &in.CreatedTimeNano,
	}
}

func toThriftWorkflowType(in *common.WorkflowType) *shared.WorkflowType {
	if in == nil {
		return nil
	}
	return &shared.WorkflowType{
		Name: &in.Name,
	}
}

func toThriftTaskList(in *common.TaskList) *shared.TaskList {
	if in == nil {
		return nil
	}
	return &shared.TaskList{
		Name: &in.Name,
		Kind: toThriftTaskListKind(in.Kind),
	}
}
func toThriftRetryPolicy(in *common.RetryPolicy) *shared.RetryPolicy {
	if in == nil {
		return nil
	}
	return &shared.RetryPolicy{
		InitialIntervalInSeconds:    &in.InitialIntervalInSeconds,
		BackoffCoefficient:          &in.BackoffCoefficient,
		MaximumIntervalInSeconds:    &in.MaximumIntervalInSeconds,
		MaximumAttempts:             &in.MaximumAttempts,
		NonRetriableErrorReasons:    in.NonRetriableErrorReasons,
		ExpirationIntervalInSeconds: &in.ExpirationIntervalInSeconds,
	}
}
func toThriftMemo(in *common.Memo) *shared.Memo {
	if in == nil {
		return nil
	}
	return &shared.Memo{
		Fields: in.Fields,
	}
}
func toThriftHeader(in *common.Header) *shared.Header {
	if in == nil {
		return nil
	}
	return &shared.Header{
		Fields: in.Fields,
	}
}
func toThriftSearchAttributes(in *common.SearchAttributes) *shared.SearchAttributes {
	if in == nil {
		return nil
	}
	return &shared.SearchAttributes{
		IndexedFields: in.IndexedFields,
	}
}

// ToThriftWorkflowExecution ...
func ToThriftWorkflowExecution(in *common.WorkflowExecution) *shared.WorkflowExecution {
	if in == nil {
		return nil
	}
	return &shared.WorkflowExecution{
		WorkflowId: &in.WorkflowId,
		RunId:      &in.RunId,
	}
}

func toProtoWorkflowType(in *shared.WorkflowType) *common.WorkflowType {
	if in == nil {
		return nil
	}
	return &common.WorkflowType{
		Name: in.GetName(),
	}
}

// ToProtoWorkflowExecution ...
func ToProtoWorkflowExecution(in *shared.WorkflowExecution) *common.WorkflowExecution {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecution{
		WorkflowId: in.GetWorkflowId(),
		RunId:      in.GetRunId(),
	}
}

func toProtoTaskList(in *shared.TaskList) *common.TaskList {
	if in == nil {
		return nil
	}
	return &common.TaskList{
		Name: in.GetName(),
		Kind: enums.TaskListKind(in.GetKind()),
	}
}

func toProtoRetryPolicy(in *shared.RetryPolicy) *common.RetryPolicy {
	if in == nil {
		return nil
	}
	return &common.RetryPolicy{
		InitialIntervalInSeconds:    in.GetInitialIntervalInSeconds(),
		BackoffCoefficient:          in.GetBackoffCoefficient(),
		MaximumIntervalInSeconds:    in.GetMaximumIntervalInSeconds(),
		MaximumAttempts:             in.GetMaximumAttempts(),
		NonRetriableErrorReasons:    in.GetNonRetriableErrorReasons(),
		ExpirationIntervalInSeconds: in.GetExpirationIntervalInSeconds(),
	}
}

func toProtoMemo(in *shared.Memo) *common.Memo {
	if in == nil {
		return nil
	}
	return &common.Memo{
		Fields: in.GetFields(),
	}
}

func toProtoSearchAttributes(in *shared.SearchAttributes) *common.SearchAttributes {
	if in == nil {
		return nil
	}
	return &common.SearchAttributes{
		IndexedFields: in.GetIndexedFields(),
	}
}

func toProtoResetPoints(in *shared.ResetPoints) *common.ResetPoints {
	if in == nil {
		return nil
	}
	var points []*common.ResetPointInfo
	for _, point := range in.GetPoints() {
		points = append(points, toProtoResetPointInfo(point))
	}

	return &common.ResetPoints{
		Points: points,
	}
}

func toProtoHeader(in *shared.Header) *common.Header {
	if in == nil {
		return nil
	}
	return &common.Header{
		Fields: in.GetFields(),
	}
}

func toProtoActivityType(in *shared.ActivityType) *common.ActivityType {
	if in == nil {
		return nil
	}
	return &common.ActivityType{
		Name: in.GetName(),
	}
}

func toProtoResetPointInfo(in *shared.ResetPointInfo) *common.ResetPointInfo {
	if in == nil {
		return nil
	}
	return &common.ResetPointInfo{
		BinaryChecksum:           in.GetBinaryChecksum(),
		RunId:                    in.GetRunId(),
		FirstDecisionCompletedId: in.GetFirstDecisionCompletedId(),
		CreatedTimeNano:          in.GetCreatedTimeNano(),
		ExpiringTimeNano:         in.GetExpiringTimeNano(),
		Resettable:               in.GetResettable(),
	}
}

func toProtoWorkflowExecutionInfo(in *shared.WorkflowExecutionInfo) *common.WorkflowExecutionInfo {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionInfo{
		Execution:        ToProtoWorkflowExecution(in.GetExecution()),
		Type:             toProtoWorkflowType(in.GetType()),
		StartTime:        in.GetStartTime(),
		CloseTime:        in.GetCloseTime(),
		CloseStatus:      toProtoWorkflowExecutionCloseStatus(in.CloseStatus),
		HistoryLength:    in.GetHistoryLength(),
		ParentDomainId:   in.GetParentDomainId(),
		ParentExecution:  ToProtoWorkflowExecution(in.GetParentExecution()),
		ExecutionTime:    in.GetExecutionTime(),
		Memo:             toProtoMemo(in.GetMemo()),
		SearchAttributes: toProtoSearchAttributes(in.GetSearchAttributes()),
		AutoResetPoints:  toProtoResetPoints(in.GetAutoResetPoints()),
	}
}

func toThriftStartTimeFilter(in *common.StartTimeFilter) *shared.StartTimeFilter {
	if in == nil {
		return nil
	}
	return &shared.StartTimeFilter{
		EarliestTime: &in.EarliestTime,
		LatestTime:   &in.LatestTime,
	}
}

func toThriftWorkflowExecutionFilter(in *common.WorkflowExecutionFilter) *shared.WorkflowExecutionFilter {
	if in == nil {
		return nil
	}
	return &shared.WorkflowExecutionFilter{
		WorkflowId: &in.WorkflowId,
		RunId:      &in.RunId,
	}
}

func toThriftWorkflowStatusFilter(in *common.StatusFilter) *shared.WorkflowExecutionCloseStatus {
	if in == nil {
		return nil
	}
	return toThriftWorkflowExecutionCloseStatus(in.CloseStatus)
}

func toThriftWorkflowTypeFilter(in *common.WorkflowTypeFilter) *shared.WorkflowTypeFilter {
	if in == nil {
		return nil
	}
	return &shared.WorkflowTypeFilter{
		Name: &in.Name,
	}
}

func toProtoWorkflowExecutionInfos(in []*shared.WorkflowExecutionInfo) []*common.WorkflowExecutionInfo {
	if in == nil {
		return nil
	}

	var executions []*common.WorkflowExecutionInfo
	for _, execution := range in {
		executions = append(executions, toProtoWorkflowExecutionInfo(execution))
	}
	return executions
}

func toProtoWorkflowExecutionConfiguration(in *shared.WorkflowExecutionConfiguration) *common.WorkflowExecutionConfiguration {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionConfiguration{
		TaskList:                            toProtoTaskList(in.GetTaskList()),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
	}
}

func toProtoPendingActivityInfos(in []*shared.PendingActivityInfo) []*common.PendingActivityInfo {
	if in == nil {
		return nil
	}

	var infos []*common.PendingActivityInfo
	for _, info := range in {
		infos = append(infos, toProtoPendingActivityInfo(info))
	}
	return infos
}

func toProtoPendingChildExecutionInfos(in []*shared.PendingChildExecutionInfo) []*common.PendingChildExecutionInfo {
	if in == nil {
		return nil
	}

	var infos []*common.PendingChildExecutionInfo
	for _, info := range in {
		infos = append(infos, toProtoPendingChildExecutionInfo(info))
	}
	return infos
}

func toProtoPendingActivityInfo(in *shared.PendingActivityInfo) *common.PendingActivityInfo {
	if in == nil {
		return nil
	}
	return &common.PendingActivityInfo{
		ActivityID:             in.GetActivityID(),
		ActivityType:           toProtoActivityType(in.GetActivityType()),
		State:                  enums.PendingActivityState(in.GetState()),
		HeartbeatDetails:       in.GetHeartbeatDetails(),
		LastHeartbeatTimestamp: in.GetLastHeartbeatTimestamp(),
		LastStartedTimestamp:   in.GetLastStartedTimestamp(),
		Attempt:                in.GetAttempt(),
		MaximumAttempts:        in.GetMaximumAttempts(),
		ScheduledTimestamp:     in.GetScheduledTimestamp(),
		ExpirationTimestamp:    in.GetExpirationTimestamp(),
		LastFailureReason:      in.GetLastFailureReason(),
		LastWorkerIdentity:     in.GetLastWorkerIdentity(),
		LastFailureDetails:     in.GetLastFailureDetails(),
	}
}

func toProtoPendingChildExecutionInfo(in *shared.PendingChildExecutionInfo) *common.PendingChildExecutionInfo {
	if in == nil {
		return nil
	}
	return &common.PendingChildExecutionInfo{
		WorkflowID:        in.GetWorkflowID(),
		RunID:             in.GetRunID(),
		WorkflowTypName:   in.GetWorkflowTypName(),
		InitiatedID:       in.GetInitiatedID(),
		ParentClosePolicy: enums.ParentClosePolicy(in.GetParentClosePolicy()),
	}
}

func toProtoWorkflowQuery(in *shared.WorkflowQuery) *common.WorkflowQuery {
	if in == nil {
		return nil
	}
	return &common.WorkflowQuery{
		QueryType: in.GetQueryType(),
		QueryArgs: in.GetQueryArgs(),
	}
}

func toProtoWorkflowQueries(in map[string]*shared.WorkflowQuery) map[string]*common.WorkflowQuery {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.WorkflowQuery, len(in))
	for k, v := range in {
		ret[k] = toProtoWorkflowQuery(v)
	}

	return ret
}

// toThriftActivityType ...
func toThriftActivityType(in *common.ActivityType) *shared.ActivityType {
	if in == nil {
		return nil
	}
	return &shared.ActivityType{
		Name: &in.Name,
	}
}

// toThriftStickyExecutionAttributes ...
func toThriftStickyExecutionAttributes(in *common.StickyExecutionAttributes) *shared.StickyExecutionAttributes {
	if in == nil {
		return nil
	}
	return &shared.StickyExecutionAttributes{
		WorkerTaskList:                toThriftTaskList(in.WorkerTaskList),
		ScheduleToStartTimeoutSeconds: &in.ScheduleToStartTimeoutSeconds,
	}
}

func toThriftWorkflowQueryResults(in map[string]*common.WorkflowQueryResult) map[string]*shared.WorkflowQueryResult {
	if in == nil {
		return nil
	}

	ret := make(map[string]*shared.WorkflowQueryResult, len(in))
	for k, v := range in {
		ret[k] = toThriftWorkflowQueryResult(v)
	}

	return ret
}

// toThriftWorkflowQueryResult ...
func toThriftWorkflowQueryResult(in *common.WorkflowQueryResult) *shared.WorkflowQueryResult {
	if in == nil {
		return nil
	}
	return &shared.WorkflowQueryResult{
		ResultType:   toThriftQueryResultType(in.ResultType),
		Answer:       in.Answer,
		ErrorMessage: &in.ErrorMessage,
	}
}

// toThriftTaskListMetadata ...
func toThriftTaskListMetadata(in *common.TaskListMetadata) *shared.TaskListMetadata {
	if in == nil {
		return nil
	}
	return &shared.TaskListMetadata{
		MaxTasksPerSecond: &in.MaxTasksPerSecond,
	}
}

// toThriftWorkflowQuery ...
func toThriftWorkflowQuery(in *common.WorkflowQuery) *shared.WorkflowQuery {
	if in == nil {
		return nil
	}
	return &shared.WorkflowQuery{
		QueryType: &in.QueryType,
		QueryArgs: in.QueryArgs,
	}
}

// toThriftReplicationToken ...
func toThriftReplicationToken(in *common.ReplicationToken) *replicator.ReplicationToken {
	if in == nil {
		return nil
	}
	return &replicator.ReplicationToken{
		ShardID:                &in.ShardID,
		LastRetrievedMessageId: &in.LastRetrievedMessageId,
		LastProcessedMessageId: &in.LastProcessedMessageId,
	}
}
func toThriftReplicationTokens(in []*common.ReplicationToken) []*replicator.ReplicationToken {
	if in == nil {
		return nil
	}

	var ret []*replicator.ReplicationToken
	for _, item := range in {
		ret = append(ret, toThriftReplicationToken(item))
	}
	return ret
}

// ToThriftDataBlob ...
func ToThriftDataBlob(in *common.DataBlob) *shared.DataBlob {
	if in == nil {
		return nil
	}
	return &shared.DataBlob{
		EncodingType: toThriftEncodingType(in.EncodingType),
		Data:         in.Data,
	}
}

// toProtoQueryRejected ...
func toProtoQueryRejected(in *shared.QueryRejected) *common.QueryRejected {
	if in == nil {
		return nil
	}
	return &common.QueryRejected{
		CloseStatus: toProtoWorkflowExecutionCloseStatus(in.CloseStatus),
	}
}

// toProtoPollerInfo ...
func toProtoPollerInfo(in *shared.PollerInfo) *common.PollerInfo {
	if in == nil {
		return nil
	}
	return &common.PollerInfo{
		LastAccessTime: in.GetLastAccessTime(),
		Identity:       in.GetIdentity(),
		RatePerSecond:  in.GetRatePerSecond(),
	}
}

func toProtoPollerInfos(in []*shared.PollerInfo) []*common.PollerInfo {
	if in == nil {
		return nil
	}

	var ret []*common.PollerInfo
	for _, item := range in {
		ret = append(ret, toProtoPollerInfo(item))
	}
	return ret
}

// toProtoTaskListStatus ...
func toProtoTaskListStatus(in *shared.TaskListStatus) *common.TaskListStatus {
	if in == nil {
		return nil
	}
	return &common.TaskListStatus{
		BacklogCountHint: in.GetBacklogCountHint(),
		ReadLevel:        in.GetReadLevel(),
		AckLevel:         in.GetAckLevel(),
		RatePerSecond:    in.GetRatePerSecond(),
		TaskIDBlock:      toProtoTaskIDBlock(in.GetTaskIDBlock()),
	}
}

// toProtoTaskIDBlock ...
func toProtoTaskIDBlock(in *shared.TaskIDBlock) *common.TaskIDBlock {
	if in == nil {
		return nil
	}
	return &common.TaskIDBlock{
		StartID: in.GetStartID(),
		EndID:   in.GetEndID(),
	}
}

// toProtoReplicationMessages ...
func toProtoReplicationMessages(in *replicator.ReplicationMessages) *common.ReplicationMessages {
	if in == nil {
		return nil
	}
	return &common.ReplicationMessages{
		ReplicationTasks:       toProtoReplicationTasks(in.GetReplicationTasks()),
		LastRetrievedMessageId: in.GetLastRetrievedMessageId(),
		HasMore:                in.GetHasMore(),
		SyncShardStatus:        toProtoSyncShardStatusTask(in.GetSyncShardStatus()),
	}
}

func toProtoSyncShardStatusTask(in *replicator.SyncShardStatus) *common.SyncShardStatus {
	if in == nil {
		return nil
	}
	return &common.SyncShardStatus{
		Timestamp: in.GetTimestamp(),
	}
}

func toProtoReplicationTasks(in []*replicator.ReplicationTask) []*common.ReplicationTask {
	if in == nil {
		return nil
	}

	var ret []*common.ReplicationTask
	for _, item := range in {
		ret = append(ret, toProtoReplicationTask(item))
	}
	return ret
}

// toProtoReplicationTask ...
func toProtoReplicationTask(in *replicator.ReplicationTask) *common.ReplicationTask {
	if in == nil {
		return nil
	}

	ret := &common.ReplicationTask{
		TaskType:     enums.ReplicationTaskType(in.GetTaskType()),
		SourceTaskId: in.GetSourceTaskId(),
	}

	switch ret.TaskType {
	case enums.ReplicationTaskTypeDomain:
		ret.Attributes = &common.ReplicationTask_DomainTaskAttributes{DomainTaskAttributes: ToProtoDomainTaskAttributes(in.GetDomainTaskAttributes())}
	case enums.ReplicationTaskTypeHistory:
		ret.Attributes = &common.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: toProtoHistoryTaskAttributes(in.GetHistoryTaskAttributes())}
	case enums.ReplicationTaskTypeSyncShardStatus:
		ret.Attributes = &common.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: toProtoSyncShardStatusTaskAttributes(in.GetSyncShardStatusTaskAttributes())}
	case enums.ReplicationTaskTypeSyncActivity:
		ret.Attributes = &common.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: toProtoSyncActivityTaskAttributes(in.GetSyncActivityTaskAttributes())}
	case enums.ReplicationTaskTypeHistoryMetadata:
		ret.Attributes = &common.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: toProtoHistoryMetadataTaskAttributes(in.GetHistoryMetadataTaskAttributes())}
	case enums.ReplicationTaskTypeHistoryV2:
		ret.Attributes = &common.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: toProtoHistoryTaskV2Attributes(in.GetHistoryTaskV2Attributes())}
	}

	return ret
}

// ToProtoDomainTaskAttributes ...
func ToProtoDomainTaskAttributes(in *replicator.DomainTaskAttributes) *common.DomainTaskAttributes {
	if in == nil {
		return nil
	}
	return &common.DomainTaskAttributes{
		DomainOperation:   enums.DomainOperation(in.GetDomainOperation()),
		Id:                in.GetID(),
		Info:              toProtoDomainInfo(in.GetInfo()),
		Config:            toProtoDomainConfiguration(in.GetConfig()),
		ReplicationConfig: toProtoDomainReplicationConfiguration(in.GetReplicationConfig()),
		ConfigVersion:     in.GetConfigVersion(),
		FailoverVersion:   in.GetFailoverVersion(),
	}
}

// toProtoHistoryTaskAttributes ...
func toProtoHistoryTaskAttributes(in *replicator.HistoryTaskAttributes) *common.HistoryTaskAttributes {
	if in == nil {
		return nil
	}
	return &common.HistoryTaskAttributes{
		TargetClusters:          in.GetTargetClusters(),
		DomainId:                in.GetDomainId(),
		WorkflowId:              in.GetWorkflowId(),
		RunId:                   in.GetRunId(),
		FirstEventId:            in.GetFirstEventId(),
		NextEventId:             in.GetNextEventId(),
		Version:                 in.GetVersion(),
		ReplicationInfo:         toProtoReplicationInfos(in.GetReplicationInfo()),
		History:                 toProtoHistory(in.GetHistory()),
		NewRunHistory:           toProtoHistory(in.GetNewRunHistory()),
		EventStoreVersion:       in.GetEventStoreVersion(),
		NewRunEventStoreVersion: in.GetNewRunEventStoreVersion(),
		ResetWorkflow:           in.GetResetWorkflow(),
		NewRunNDC:               in.GetNewRunNDC(),
	}
}

func toProtoReplicationInfos(in map[string]*shared.ReplicationInfo) map[string]*common.ReplicationInfo {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.ReplicationInfo, len(in))
	for k, v := range in {
		ret[k] = toProtoReplicationInfo(v)
	}

	return ret
}

// toProtoReplicationInfo ...
func toProtoReplicationInfo(in *shared.ReplicationInfo) *common.ReplicationInfo {
	if in == nil {
		return nil
	}
	return &common.ReplicationInfo{
		Version:     in.GetVersion(),
		LastEventId: in.GetLastEventId(),
	}
}

// toProtoHistoryMetadataTaskAttributes ...
func toProtoHistoryMetadataTaskAttributes(in *replicator.HistoryMetadataTaskAttributes) *common.HistoryMetadataTaskAttributes {
	if in == nil {
		return nil
	}
	return &common.HistoryMetadataTaskAttributes{
		TargetClusters: in.GetTargetClusters(),
		DomainId:       in.GetDomainId(),
		WorkflowId:     in.GetWorkflowId(),
		RunId:          in.GetRunId(),
		FirstEventId:   in.GetFirstEventId(),
		NextEventId:    in.GetNextEventId(),
	}
}

// toProtoSyncShardStatusTaskAttributes ...
func toProtoSyncShardStatusTaskAttributes(in *replicator.SyncShardStatusTaskAttributes) *common.SyncShardStatusTaskAttributes {
	if in == nil {
		return nil
	}
	return &common.SyncShardStatusTaskAttributes{
		SourceCluster: in.GetSourceCluster(),
		ShardId:       in.GetShardId(),
		Timestamp:     in.GetTimestamp(),
	}
}

// toProtoSyncActivityTaskAttributes ...
func toProtoSyncActivityTaskAttributes(in *replicator.SyncActivityTaskAttributes) *common.SyncActivityTaskAttributes {
	if in == nil {
		return nil
	}
	return &common.SyncActivityTaskAttributes{
		DomainId:           in.GetDomainId(),
		WorkflowId:         in.GetWorkflowId(),
		RunId:              in.GetRunId(),
		Version:            in.GetVersion(),
		ScheduledId:        in.GetScheduledId(),
		ScheduledTime:      in.GetScheduledTime(),
		StartedId:          in.GetStartedId(),
		StartedTime:        in.GetStartedTime(),
		LastHeartbeatTime:  in.GetLastHeartbeatTime(),
		Details:            in.GetDetails(),
		Attempt:            in.GetAttempt(),
		LastFailureReason:  in.GetLastFailureReason(),
		LastWorkerIdentity: in.GetLastWorkerIdentity(),
		LastFailureDetails: in.GetLastFailureDetails(),
		VersionHistory:     toProtoVersionHistory(in.GetVersionHistory()),
	}
}

// toProtoVersionHistoryItem ...
func toProtoVersionHistoryItem(in *shared.VersionHistoryItem) *common.VersionHistoryItem {
	if in == nil {
		return nil
	}
	return &common.VersionHistoryItem{
		EventID: in.GetEventID(),
		Version: in.GetVersion(),
	}
}

// toProtoVersionHistory ...
func toProtoVersionHistory(in *shared.VersionHistory) *common.VersionHistory {
	if in == nil {
		return nil
	}
	return &common.VersionHistory{
		BranchToken: in.GetBranchToken(),
		Items:       ToProtoVersionHistoryItems(in.GetItems()),
	}
}

// ToProtoVersionHistoryItems ...
func ToProtoVersionHistoryItems(in []*shared.VersionHistoryItem) []*common.VersionHistoryItem {
	if in == nil {
		return nil
	}

	var ret []*common.VersionHistoryItem
	for _, item := range in {
		ret = append(ret, toProtoVersionHistoryItem(item))
	}
	return ret
}

// toProtoHistoryTaskV2Attributes ...
func toProtoHistoryTaskV2Attributes(in *replicator.HistoryTaskV2Attributes) *common.HistoryTaskV2Attributes {
	if in == nil {
		return nil
	}
	return &common.HistoryTaskV2Attributes{
		TaskId:              in.GetTaskId(),
		DomainId:            in.GetDomainId(),
		WorkflowId:          in.GetWorkflowId(),
		RunId:               in.GetRunId(),
		VersionHistoryItems: ToProtoVersionHistoryItems(in.GetVersionHistoryItems()),
		Events:              ToProtoDataBlob(in.GetEvents()),
		NewRunEvents:        ToProtoDataBlob(in.GetNewRunEvents()),
	}
}

// ToProtoDataBlob ...
func ToProtoDataBlob(in *shared.DataBlob) *common.DataBlob {
	if in == nil {
		return nil
	}
	return &common.DataBlob{
		EncodingType: toProtoEncodingType(in.GetEncodingType()),
		Data:         in.GetData(),
	}
}

func toProtoIndexedValueTypes(in map[string]shared.IndexedValueType) map[string]enums.IndexedValueType {
	if in == nil {
		return nil
	}

	ret := make(map[string]enums.IndexedValueType, len(in))
	for k, v := range in {
		ret[k] = enums.IndexedValueType(v)
	}

	return ret
}

func toProtoReplicationMessagess(in map[int32]*replicator.ReplicationMessages) map[int32]*common.ReplicationMessages {
	if in == nil {
		return nil
	}

	ret := make(map[int32]*common.ReplicationMessages, len(in))
	for k, v := range in {
		ret[k] = toProtoReplicationMessages(v)
	}

	return ret
}

func toThriftWorkerVersionInfo(in *common.WorkerVersionInfo) *shared.WorkerVersionInfo {
	if in == nil {
		return nil
	}
	return &shared.WorkerVersionInfo{
		Impl:           &in.Impl,
		FeatureVersion: &in.FeatureVersion,
	}
}

func toProtoSupportedClientVersions(in *shared.SupportedClientVersions) *common.SupportedClientVersions {
	if in == nil {
		return nil
	}
	return &common.SupportedClientVersions{
		GoSdk:   in.GetGoSdk(),
		JavaSdk: in.GetJavaSdk(),
	}
}

func toProtoTaskListPartitionMetadatas(in []*shared.TaskListPartitionMetadata) []*common.TaskListPartitionMetadata {
	if in == nil {
		return nil
	}

	var ret []*common.TaskListPartitionMetadata
	for _, item := range in {
		ret = append(ret, toProtoTaskListPartitionMetadata(item))
	}
	return ret
}

func toProtoTaskListPartitionMetadata(in *shared.TaskListPartitionMetadata) *common.TaskListPartitionMetadata {
	if in == nil {
		return nil
	}
	return &common.TaskListPartitionMetadata{
		Key:           in.GetKey(),
		OwnerHostName: in.GetOwnerHostName(),
	}
}

// ToThriftResetPoints ...
func ToThriftResetPoints(in *common.ResetPoints) *shared.ResetPoints {
	if in == nil {
		return nil
	}

	return &shared.ResetPoints{
		Points: toThriftResetPointInfos(in.Points),
	}
}

func toThriftResetPointInfos(in []*common.ResetPointInfo) []*shared.ResetPointInfo {
	if in == nil {
		return nil
	}

	var ret []*shared.ResetPointInfo
	for _, item := range in {
		ret = append(ret, toThriftResetPointInfo(item))
	}
	return ret
}

func toThriftResetPointInfo(in *common.ResetPointInfo) *shared.ResetPointInfo {
	if in == nil {
		return nil
	}

	return &shared.ResetPointInfo{
		BinaryChecksum:           &in.BinaryChecksum,
		RunId:                    &in.RunId,
		FirstDecisionCompletedId: &in.FirstDecisionCompletedId,
		CreatedTimeNano:          &in.CreatedTimeNano,
		ExpiringTimeNano:         &in.ExpiringTimeNano,
		Resettable:               &in.Resettable,
	}
}

// ToThriftVersionHistory ...
func ToThriftVersionHistory(in *common.VersionHistory) *shared.VersionHistory {
	if in == nil {
		return nil
	}

	return &shared.VersionHistory{
		BranchToken: in.BranchToken,
		Items:       ToThriftVersionHistoryItems(in.Items),
	}
}

// ToThriftVersionHistoryItems ...
func ToThriftVersionHistoryItems(in []*common.VersionHistoryItem) []*shared.VersionHistoryItem {
	if in == nil {
		return nil
	}

	var ret []*shared.VersionHistoryItem
	for _, item := range in {
		ret = append(ret, toThriftVersionHistoryItem(item))
	}
	return ret
}

func toThriftVersionHistoryItem(in *common.VersionHistoryItem) *shared.VersionHistoryItem {
	if in == nil {
		return nil
	}

	return &shared.VersionHistoryItem{
		EventID: &in.EventID,
		Version: &in.Version,
	}

}

// ToThriftReplicationInfos ...
func ToThriftReplicationInfos(in map[string]*common.ReplicationInfo) map[string]*shared.ReplicationInfo {
	if in == nil {
		return nil
	}
	ret := make(map[string]*shared.ReplicationInfo, len(in))

	for key, value := range in {
		ret[key] = toThriftReplicationInfo(value)
	}

	return ret
}

func toThriftReplicationInfo(in *common.ReplicationInfo) *shared.ReplicationInfo {
	if in == nil {
		return nil
	}

	return &shared.ReplicationInfo{
		Version:     &in.Version,
		LastEventId: &in.LastEventId,
	}
}

// ToThriftDomainTaskAttributes ...
func ToThriftDomainTaskAttributes(in *common.DomainTaskAttributes) *replicator.DomainTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.DomainTaskAttributes{
		DomainOperation:   toThriftDomainOperation(in.DomainOperation),
		ID:                &in.Id,
		Info:              toThriftDomainInfo(in.Info),
		Config:            toThriftDomainConfiguration(in.Config),
		ReplicationConfig: toThriftDomainReplicationConfiguration(in.ReplicationConfig),
		ConfigVersion:     &in.ConfigVersion,
		FailoverVersion:   &in.FailoverVersion,
	}
}

// toThriftDomainInfo ...
func toThriftDomainInfo(in *common.DomainInfo) *shared.DomainInfo {
	if in == nil {
		return nil
	}
	return &shared.DomainInfo{
		Name:        &in.Name,
		Status:      toThriftDomainStatus(in.Status),
		Description: &in.Description,
		OwnerEmail:  &in.OwnerEmail,
		Data:        in.Data,
		UUID:        &in.Uuid,
	}
}

// ToProtoReplicationToken ...
func ToProtoReplicationToken(in *replicator.ReplicationToken) *common.ReplicationToken {
	if in == nil {
		return nil
	}
	return &common.ReplicationToken{
		ShardID:                in.GetShardID(),
		LastRetrievedMessageId: in.GetLastRetrievedMessageId(),
		LastProcessedMessageId: in.GetLastProcessedMessageId(),
	}
}

// ToThriftReplicationMessagesByShard ...
func ToThriftReplicationMessagesByShard(in map[int32]*common.ReplicationMessages) map[int32]*replicator.ReplicationMessages {
	if in == nil {
		return nil
	}
	ret := make(map[int32]*replicator.ReplicationMessages, len(in))

	for key, value := range in {
		ret[key] = ToThriftReplicationMessages(value)
	}

	return ret
}

// ToThriftReplicationMessages ...
func ToThriftReplicationMessages(in *common.ReplicationMessages) *replicator.ReplicationMessages {
	if in == nil {
		return nil
	}
	return &replicator.ReplicationMessages{
		ReplicationTasks:       toThriftReplicationTasks(in.ReplicationTasks),
		LastRetrievedMessageId: &in.LastRetrievedMessageId,
		HasMore:                &in.HasMore,
		SyncShardStatus:        toThriftSyncShardStatus(in.SyncShardStatus),
	}
}

// toThriftSyncShardStatus ...
func toThriftSyncShardStatus(in *common.SyncShardStatus) *replicator.SyncShardStatus {
	if in == nil {
		return nil
	}
	return &replicator.SyncShardStatus{
		Timestamp: &in.Timestamp,
	}
}

// toThriftReplicationTasks ...
func toThriftReplicationTasks(in []*common.ReplicationTask) []*replicator.ReplicationTask {
	if in == nil {
		return nil
	}

	var ret []*replicator.ReplicationTask
	for _, item := range in {
		ret = append(ret, ToThriftReplicationTask(item))
	}
	return ret
}

// ToThriftReplicationTask ...
func ToThriftReplicationTask(in *common.ReplicationTask) *replicator.ReplicationTask {
	if in == nil {
		return nil
	}
	ret := &replicator.ReplicationTask{
		TaskType:     toThriftReplicationTaskType(in.TaskType),
		SourceTaskId: &in.SourceTaskId,
	}

	switch in.TaskType {
	case enums.ReplicationTaskTypeDomain:
		ret.DomainTaskAttributes = toThriftDomainTaskAttributes(in.GetDomainTaskAttributes())
	case enums.ReplicationTaskTypeHistory:
		ret.HistoryTaskAttributes = toThriftHistoryTaskAttributes(in.GetHistoryTaskAttributes())
	case enums.ReplicationTaskTypeSyncShardStatus:
		ret.SyncShardStatusTaskAttributes = toThriftSyncShardStatusTaskAttributes(in.GetSyncShardStatusTaskAttributes())
	case enums.ReplicationTaskTypeSyncActivity:
		ret.SyncActivityTaskAttributes = toThriftSyncActivityTaskAttributes(in.GetSyncActivityTaskAttributes())
	case enums.ReplicationTaskTypeHistoryMetadata:
		ret.HistoryMetadataTaskAttributes = toThriftHistoryMetadataTaskAttributes(in.GetHistoryMetadataTaskAttributes())
	case enums.ReplicationTaskTypeHistoryV2:
		ret.HistoryTaskV2Attributes = toThriftHistoryTaskV2Attributes(in.GetHistoryTaskV2Attributes())
	}

	return ret
}

// toThriftDomainTaskAttributes ...
func toThriftDomainTaskAttributes(in *common.DomainTaskAttributes) *replicator.DomainTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.DomainTaskAttributes{
		DomainOperation:   toThriftDomainOperation(in.DomainOperation),
		ID:                &in.Id,
		Info:              toThriftDomainInfo(in.Info),
		Config:            toThriftDomainConfiguration(in.Config),
		ReplicationConfig: toThriftDomainReplicationConfiguration(in.ReplicationConfig),
		ConfigVersion:     &in.ConfigVersion,
		FailoverVersion:   &in.FailoverVersion,
	}
}

// toThriftHistoryTaskAttributes ...
func toThriftHistoryTaskAttributes(in *common.HistoryTaskAttributes) *replicator.HistoryTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.HistoryTaskAttributes{
		TargetClusters:          in.TargetClusters,
		DomainId:                &in.DomainId,
		WorkflowId:              &in.WorkflowId,
		RunId:                   &in.RunId,
		FirstEventId:            &in.FirstEventId,
		NextEventId:             &in.NextEventId,
		Version:                 &in.Version,
		ReplicationInfo:         ToThriftReplicationInfos(in.ReplicationInfo),
		History:                 ToThriftHistory(in.History),
		NewRunHistory:           ToThriftHistory(in.NewRunHistory),
		EventStoreVersion:       &in.EventStoreVersion,
		NewRunEventStoreVersion: &in.NewRunEventStoreVersion,
		ResetWorkflow:           &in.ResetWorkflow,
		NewRunNDC:               &in.NewRunNDC,
	}
}

// toThriftHistoryMetadataTaskAttributes ...
func toThriftHistoryMetadataTaskAttributes(in *common.HistoryMetadataTaskAttributes) *replicator.HistoryMetadataTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.HistoryMetadataTaskAttributes{
		TargetClusters: in.TargetClusters,
		DomainId:       &in.DomainId,
		WorkflowId:     &in.WorkflowId,
		RunId:          &in.RunId,
		FirstEventId:   &in.FirstEventId,
		NextEventId:    &in.NextEventId,
	}
}

// toThriftSyncShardStatusTaskAttributes ...
func toThriftSyncShardStatusTaskAttributes(in *common.SyncShardStatusTaskAttributes) *replicator.SyncShardStatusTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.SyncShardStatusTaskAttributes{
		SourceCluster: &in.SourceCluster,
		ShardId:       &in.ShardId,
		Timestamp:     &in.Timestamp,
	}
}

// toThriftSyncActivityTaskAttributes ...
func toThriftSyncActivityTaskAttributes(in *common.SyncActivityTaskAttributes) *replicator.SyncActivityTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.SyncActivityTaskAttributes{
		DomainId:           &in.DomainId,
		WorkflowId:         &in.WorkflowId,
		RunId:              &in.RunId,
		Version:            &in.Version,
		ScheduledId:        &in.ScheduledId,
		ScheduledTime:      &in.ScheduledTime,
		StartedId:          &in.StartedId,
		StartedTime:        &in.StartedTime,
		LastHeartbeatTime:  &in.LastHeartbeatTime,
		Details:            in.Details,
		Attempt:            &in.Attempt,
		LastFailureReason:  &in.LastFailureReason,
		LastWorkerIdentity: &in.LastWorkerIdentity,
		LastFailureDetails: in.LastFailureDetails,
		VersionHistory:     ToThriftVersionHistory(in.VersionHistory),
	}
}

// toThriftHistoryTaskV2Attributes ...
func toThriftHistoryTaskV2Attributes(in *common.HistoryTaskV2Attributes) *replicator.HistoryTaskV2Attributes {
	if in == nil {
		return nil
	}
	return &replicator.HistoryTaskV2Attributes{
		TaskId:              &in.TaskId,
		DomainId:            &in.DomainId,
		WorkflowId:          &in.WorkflowId,
		RunId:               &in.RunId,
		VersionHistoryItems: ToThriftVersionHistoryItems(in.VersionHistoryItems),
		Events:              ToThriftDataBlob(in.Events),
		NewRunEvents:        ToThriftDataBlob(in.NewRunEvents),
	}
}

// ToProtoDomainCacheInfo ...
func ToProtoDomainCacheInfo(in *shared.DomainCacheInfo) *common.DomainCacheInfo {
	if in == nil {
		return nil
	}
	return &common.DomainCacheInfo{
		NumOfItemsInCacheByID:   in.GetNumOfItemsInCacheByID(),
		NumOfItemsInCacheByName: in.GetNumOfItemsInCacheByName(),
	}
}

// ToProtoDataBlobs ...
func ToProtoDataBlobs(in []*shared.DataBlob) []*common.DataBlob {
	if in == nil {
		return nil
	}

	var ret []*common.DataBlob
	for _, item := range in {
		ret = append(ret, ToProtoDataBlob(item))
	}
	return ret
}

// ToThriftDataBlobs ...
func ToThriftDataBlobs(in []*common.DataBlob) []*shared.DataBlob {
	if in == nil {
		return nil
	}

	var ret []*shared.DataBlob
	for _, item := range in {
		ret = append(ret, ToThriftDataBlob(item))
	}
	return ret
}

func toThriftIndexedValueTypes(in map[string]enums.IndexedValueType) map[string]shared.IndexedValueType {
	if in == nil {
		return nil
	}

	ret := make(map[string]shared.IndexedValueType, len(in))
	for k, v := range in {
		ret[k] = shared.IndexedValueType(v)
	}

	return ret
}

// ToThriftReplicationTaskInfo ...
func ToThriftReplicationTaskInfo(in *common.ReplicationTaskInfo) *replicator.ReplicationTaskInfo {
	if in == nil {
		return nil
	}

	taskType := int16(in.TaskType)

	return &replicator.ReplicationTaskInfo{
		DomainID:     &in.DomainId,
		WorkflowID:   &in.WorkflowId,
		RunID:        &in.RunId,
		TaskType:     &taskType,
		TaskID:       &in.TaskId,
		Version:      &in.Version,
		FirstEventID: &in.FirstEventId,
		NextEventID:  &in.NextEventId,
		ScheduledID:  &in.ScheduledId,
	}
}

// ToThriftReplicationTaskInfos ...
func ToThriftReplicationTaskInfos(in []*common.ReplicationTaskInfo) []*replicator.ReplicationTaskInfo {
	if in == nil {
		return nil
	}

	var ret []*replicator.ReplicationTaskInfo
	for _, item := range in {
		ret = append(ret, ToThriftReplicationTaskInfo(item))
	}
	return ret
}
