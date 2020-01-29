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

// Code generated by generate-adapter. DO NOT EDIT.

package adapter

import (
	"go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/go/replicator"
	"github.com/temporalio/temporal/.gen/go/shared"
)

func ToProtoBool(in *bool) *common.BoolValue {
	if in == nil {
		return nil
	}

	return &common.BoolValue{Value: *in}
}

func ToThriftBool(in *common.BoolValue) *bool {
	if in == nil {
		return nil
	}

	return &in.Value
}

func ToProtoDomainInfo(in *shared.DomainInfo) *common.DomainInfo {
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

func ToProtoDomainReplicationConfiguration(in *shared.DomainReplicationConfiguration) *common.DomainReplicationConfiguration {
	if in == nil {
		return nil
	}
	return &common.DomainReplicationConfiguration{
		ActiveClusterName: in.GetActiveClusterName(),
		Clusters:          ToProtoClusterReplicationConfigurations(in.GetClusters()),
	}
}

func ToProtoDomainConfiguration(in *shared.DomainConfiguration) *common.DomainConfiguration {
	if in == nil {
		return nil
	}
	return &common.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: in.GetWorkflowExecutionRetentionPeriodInDays(),
		EmitMetric:                             ToProtoBool(in.EmitMetric),
		BadBinaries:                            ToProtoBadBinaries(in.GetBadBinaries()),
		HistoryArchivalStatus:                  ToProtoArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     in.GetVisibilityArchivalURI(),
		VisibilityArchivalStatus:               ToProtoArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  in.GetVisibilityArchivalURI(),
	}
}

func ToProtoBadBinaries(in *shared.BadBinaries) *common.BadBinaries {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.BadBinaryInfo, len(in.GetBinaries()))

	for key, value := range in.GetBinaries() {
		ret[key] = ToProtoBadBinaryInfo(value)
	}

	return &common.BadBinaries{
		Binaries: ret,
	}
}

func ToProtoBadBinaryInfo(in *shared.BadBinaryInfo) *common.BadBinaryInfo {
	if in == nil {
		return nil
	}
	return &common.BadBinaryInfo{
		Reason:          in.GetReason(),
		Operator:        in.GetOperator(),
		CreatedTimeNano: in.GetCreatedTimeNano(),
	}
}

func ToThriftClusterReplicationConfigurations(in []*common.ClusterReplicationConfiguration) []*shared.ClusterReplicationConfiguration {
	var ret []*shared.ClusterReplicationConfiguration
	for _, cluster := range in {
		ret = append(ret, &shared.ClusterReplicationConfiguration{ClusterName: &cluster.ClusterName})
	}

	return ret
}

func ToProtoClusterReplicationConfigurations(in []*shared.ClusterReplicationConfiguration) []*common.ClusterReplicationConfiguration {
	var ret []*common.ClusterReplicationConfiguration
	for _, cluster := range in {
		ret = append(ret, &common.ClusterReplicationConfiguration{ClusterName: *cluster.ClusterName})
	}

	return ret
}

func ToThriftUpdateDomainInfo(in *common.UpdateDomainInfo) *shared.UpdateDomainInfo {
	if in == nil {
		return nil
	}
	return &shared.UpdateDomainInfo{
		Description: &in.Description,
		OwnerEmail:  &in.OwnerEmail,
		Data:        in.Data,
	}
}
func ToThriftDomainConfiguration(in *common.DomainConfiguration) *shared.DomainConfiguration {
	if in == nil {
		return nil
	}
	return &shared.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: &in.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             ToThriftBool(in.EmitMetric),
		BadBinaries:                            ToThriftBadBinaries(in.BadBinaries),
		HistoryArchivalStatus:                  ToThriftArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     &in.HistoryArchivalURI,
		VisibilityArchivalStatus:               ToThriftArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  &in.VisibilityArchivalURI,
	}
}
func ToThriftDomainReplicationConfiguration(in *common.DomainReplicationConfiguration) *shared.DomainReplicationConfiguration {
	if in == nil {
		return nil
	}
	return &shared.DomainReplicationConfiguration{
		ActiveClusterName: &in.ActiveClusterName,
		Clusters:          ToThriftClusterReplicationConfigurations(in.Clusters),
	}
}

// ToThriftBadBinaries ...
func ToThriftBadBinaries(in *common.BadBinaries) *shared.BadBinaries {
	if in == nil {
		return nil
	}
	ret := make(map[string]*shared.BadBinaryInfo, len(in.Binaries))

	for key, value := range in.Binaries {
		ret[key] = ToThriftBadBinaryInfo(value)
	}

	return &shared.BadBinaries{
		Binaries: ret,
	}
}

func ToThriftBadBinaryInfo(in *common.BadBinaryInfo) *shared.BadBinaryInfo {
	if in == nil {
		return nil
	}
	return &shared.BadBinaryInfo{
		Reason:          &in.Reason,
		Operator:        &in.Operator,
		CreatedTimeNano: &in.CreatedTimeNano,
	}
}

func ToThriftWorkflowType(in *common.WorkflowType) *shared.WorkflowType {
	if in == nil {
		return nil
	}
	return &shared.WorkflowType{
		Name: &in.Name,
	}
}

func ToThriftTaskList(in *common.TaskList) *shared.TaskList {
	if in == nil {
		return nil
	}
	return &shared.TaskList{
		Name: &in.Name,
		Kind: ToThriftTaskListKind(in.Kind),
	}
}
func ToThriftRetryPolicy(in *common.RetryPolicy) *shared.RetryPolicy {
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
func ToThriftMemo(in *common.Memo) *shared.Memo {
	if in == nil {
		return nil
	}
	return &shared.Memo{
		Fields: in.Fields,
	}
}
func ToThriftHeader(in *common.Header) *shared.Header {
	if in == nil {
		return nil
	}
	return &shared.Header{
		Fields: in.Fields,
	}
}
func ToThriftSearchAttributes(in *common.SearchAttributes) *shared.SearchAttributes {
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

func ToProtoWorkflowType(in *shared.WorkflowType) *common.WorkflowType {
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

func ToProtoTaskList(in *shared.TaskList) *common.TaskList {
	if in == nil {
		return nil
	}
	return &common.TaskList{
		Name: in.GetName(),
		Kind: enums.TaskListKind(in.GetKind()),
	}
}

func ToProtoRetryPolicy(in *shared.RetryPolicy) *common.RetryPolicy {
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

func ToProtoMemo(in *shared.Memo) *common.Memo {
	if in == nil {
		return nil
	}
	return &common.Memo{
		Fields: in.GetFields(),
	}
}

func ToProtoSearchAttributes(in *shared.SearchAttributes) *common.SearchAttributes {
	if in == nil {
		return nil
	}
	return &common.SearchAttributes{
		IndexedFields: in.GetIndexedFields(),
	}
}

func ToProtoResetPoints(in *shared.ResetPoints) *common.ResetPoints {
	if in == nil {
		return nil
	}
	var points []*common.ResetPointInfo
	for _, point := range in.GetPoints() {
		points = append(points, ToProtoResetPointInfo(point))
	}

	return &common.ResetPoints{
		Points: points,
	}
}

func ToProtoHeader(in *shared.Header) *common.Header {
	if in == nil {
		return nil
	}
	return &common.Header{
		Fields: in.GetFields(),
	}
}

func ToProtoActivityType(in *shared.ActivityType) *common.ActivityType {
	if in == nil {
		return nil
	}
	return &common.ActivityType{
		Name: in.GetName(),
	}
}

func ToProtoResetPointInfo(in *shared.ResetPointInfo) *common.ResetPointInfo {
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

func ToProtoWorkflowExecutionInfo(in *shared.WorkflowExecutionInfo) *common.WorkflowExecutionInfo {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionInfo{
		Execution:        ToProtoWorkflowExecution(in.GetExecution()),
		Type:             ToProtoWorkflowType(in.GetType()),
		StartTime:        in.GetStartTime(),
		CloseTime:        in.GetCloseTime(),
		CloseStatus:      ToProtoWorkflowExecutionCloseStatus(in.CloseStatus),
		HistoryLength:    in.GetHistoryLength(),
		ParentDomainId:   in.GetParentDomainId(),
		ParentExecution:  ToProtoWorkflowExecution(in.GetParentExecution()),
		ExecutionTime:    in.GetExecutionTime(),
		Memo:             ToProtoMemo(in.GetMemo()),
		SearchAttributes: ToProtoSearchAttributes(in.GetSearchAttributes()),
		AutoResetPoints:  ToProtoResetPoints(in.GetAutoResetPoints()),
	}
}

func ToThriftStartTimeFilter(in *common.StartTimeFilter) *shared.StartTimeFilter {
	if in == nil {
		return nil
	}
	return &shared.StartTimeFilter{
		EarliestTime: &in.EarliestTime,
		LatestTime:   &in.LatestTime,
	}
}

func ToThriftWorkflowExecutionFilter(in *common.WorkflowExecutionFilter) *shared.WorkflowExecutionFilter {
	if in == nil {
		return nil
	}
	return &shared.WorkflowExecutionFilter{
		WorkflowId: &in.WorkflowId,
		RunId:      &in.RunId,
	}
}

func ToThriftWorkflowStatusFilter(in *common.StatusFilter) *shared.WorkflowExecutionCloseStatus {
	if in == nil {
		return nil
	}
	return ToThriftWorkflowExecutionCloseStatus(in.CloseStatus)
}

func ToThriftWorkflowTypeFilter(in *common.WorkflowTypeFilter) *shared.WorkflowTypeFilter {
	if in == nil {
		return nil
	}
	return &shared.WorkflowTypeFilter{
		Name: &in.Name,
	}
}

func ToProtoWorkflowExecutionInfos(in []*shared.WorkflowExecutionInfo) []*common.WorkflowExecutionInfo {
	if in == nil {
		return nil
	}

	var executions []*common.WorkflowExecutionInfo
	for _, execution := range in {
		executions = append(executions, ToProtoWorkflowExecutionInfo(execution))
	}
	return executions
}

func ToProtoWorkflowExecutionConfiguration(in *shared.WorkflowExecutionConfiguration) *common.WorkflowExecutionConfiguration {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionConfiguration{
		TaskList:                            ToProtoTaskList(in.GetTaskList()),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
	}
}

func ToProtoPendingActivityInfos(in []*shared.PendingActivityInfo) []*common.PendingActivityInfo {
	if in == nil {
		return nil
	}

	var infos []*common.PendingActivityInfo
	for _, info := range in {
		infos = append(infos, ToProtoPendingActivityInfo(info))
	}
	return infos
}

func ToProtoPendingChildExecutionInfos(in []*shared.PendingChildExecutionInfo) []*common.PendingChildExecutionInfo {
	if in == nil {
		return nil
	}

	var infos []*common.PendingChildExecutionInfo
	for _, info := range in {
		infos = append(infos, ToProtoPendingChildExecutionInfo(info))
	}
	return infos
}

func ToProtoPendingActivityInfo(in *shared.PendingActivityInfo) *common.PendingActivityInfo {
	if in == nil {
		return nil
	}
	return &common.PendingActivityInfo{
		ActivityID:             in.GetActivityID(),
		ActivityType:           ToProtoActivityType(in.GetActivityType()),
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

func ToProtoPendingChildExecutionInfo(in *shared.PendingChildExecutionInfo) *common.PendingChildExecutionInfo {
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

func ToProtoWorkflowQuery(in *shared.WorkflowQuery) *common.WorkflowQuery {
	if in == nil {
		return nil
	}
	return &common.WorkflowQuery{
		QueryType: in.GetQueryType(),
		QueryArgs: in.GetQueryArgs(),
	}
}

func ToProtoWorkflowQueries(in map[string]*shared.WorkflowQuery) map[string]*common.WorkflowQuery {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.WorkflowQuery, len(in))
	for k, v := range in {
		ret[k] = ToProtoWorkflowQuery(v)
	}

	return ret
}

// ToThriftActivityType ...
func ToThriftActivityType(in *common.ActivityType) *shared.ActivityType {
	if in == nil {
		return nil
	}
	return &shared.ActivityType{
		Name: &in.Name,
	}
}

// ToThriftStickyExecutionAttributes ...
func ToThriftStickyExecutionAttributes(in *common.StickyExecutionAttributes) *shared.StickyExecutionAttributes {
	if in == nil {
		return nil
	}
	return &shared.StickyExecutionAttributes{
		WorkerTaskList:                ToThriftTaskList(in.WorkerTaskList),
		ScheduleToStartTimeoutSeconds: &in.ScheduleToStartTimeoutSeconds,
	}
}

func ToThriftWorkflowQueryResults(in map[string]*common.WorkflowQueryResult) map[string]*shared.WorkflowQueryResult {
	if in == nil {
		return nil
	}

	ret := make(map[string]*shared.WorkflowQueryResult, len(in))
	for k, v := range in {
		ret[k] = ToThriftWorkflowQueryResult(v)
	}

	return ret
}

// ToThriftWorkflowQueryResult ...
func ToThriftWorkflowQueryResult(in *common.WorkflowQueryResult) *shared.WorkflowQueryResult {
	if in == nil {
		return nil
	}
	return &shared.WorkflowQueryResult{
		ResultType:   ToThriftQueryResultType(in.ResultType),
		Answer:       in.Answer,
		ErrorMessage: &in.ErrorMessage,
	}
}

// ToThriftTaskListMetadata ...
func ToThriftTaskListMetadata(in *common.TaskListMetadata) *shared.TaskListMetadata {
	if in == nil {
		return nil
	}
	return &shared.TaskListMetadata{
		MaxTasksPerSecond: &in.MaxTasksPerSecond,
	}
}

// ToThriftWorkflowQuery ...
func ToThriftWorkflowQuery(in *common.WorkflowQuery) *shared.WorkflowQuery {
	if in == nil {
		return nil
	}
	return &shared.WorkflowQuery{
		QueryType: &in.QueryType,
		QueryArgs: in.QueryArgs,
	}
}

// ToThriftReplicationToken ...
func ToThriftReplicationToken(in *common.ReplicationToken) *replicator.ReplicationToken {
	if in == nil {
		return nil
	}
	return &replicator.ReplicationToken{
		ShardID:                &in.ShardID,
		LastRetrievedMessageId: &in.LastRetrievedMessageId,
		LastProcessedMessageId: &in.LastProcessedMessageId,
	}
}
func ToThriftReplicationTokens(in []*common.ReplicationToken) []*replicator.ReplicationToken {
	if in == nil {
		return nil
	}

	var ret []*replicator.ReplicationToken
	for _, item := range in {
		ret = append(ret, ToThriftReplicationToken(item))
	}
	return ret
}

// ToThriftDataBlob ...
func ToThriftDataBlob(in *common.DataBlob) *shared.DataBlob {
	if in == nil {
		return nil
	}
	return &shared.DataBlob{
		EncodingType: ToThriftEncodingType(in.EncodingType),
		Data:         in.Data,
	}
}

// ToProtoQueryRejected ...
func ToProtoQueryRejected(in *shared.QueryRejected) *common.QueryRejected {
	if in == nil {
		return nil
	}
	return &common.QueryRejected{
		CloseStatus: ToProtoWorkflowExecutionCloseStatus(in.CloseStatus),
	}
}

// ToProtoPollerInfo ...
func ToProtoPollerInfo(in *shared.PollerInfo) *common.PollerInfo {
	if in == nil {
		return nil
	}
	return &common.PollerInfo{
		LastAccessTime: in.GetLastAccessTime(),
		Identity:       in.GetIdentity(),
		RatePerSecond:  in.GetRatePerSecond(),
	}
}

func ToProtoPollerInfos(in []*shared.PollerInfo) []*common.PollerInfo {
	if in == nil {
		return nil
	}

	var ret []*common.PollerInfo
	for _, item := range in {
		ret = append(ret, ToProtoPollerInfo(item))
	}
	return ret
}

// ToProtoTaskListStatus ...
func ToProtoTaskListStatus(in *shared.TaskListStatus) *common.TaskListStatus {
	if in == nil {
		return nil
	}
	return &common.TaskListStatus{
		BacklogCountHint: in.GetBacklogCountHint(),
		ReadLevel:        in.GetReadLevel(),
		AckLevel:         in.GetAckLevel(),
		RatePerSecond:    in.GetRatePerSecond(),
		TaskIDBlock:      ToProtoTaskIDBlock(in.GetTaskIDBlock()),
	}
}

// ToProtoTaskIDBlock ...
func ToProtoTaskIDBlock(in *shared.TaskIDBlock) *common.TaskIDBlock {
	if in == nil {
		return nil
	}
	return &common.TaskIDBlock{
		StartID: in.GetStartID(),
		EndID:   in.GetEndID(),
	}
}

// ToProtoReplicationMessages ...
func ToProtoReplicationMessages(in *replicator.ReplicationMessages) *common.ReplicationMessages {
	if in == nil {
		return nil
	}
	return &common.ReplicationMessages{
		ReplicationTasks:       ToProtoReplicationTasks(in.GetReplicationTasks()),
		LastRetrievedMessageId: in.GetLastRetrievedMessageId(),
		HasMore:                in.GetHasMore(),
		SyncShardStatus:        ToProtoSyncShardStatusTask(in.GetSyncShardStatus()),
	}
}

func ToProtoSyncShardStatusTask(in *replicator.SyncShardStatus) *common.SyncShardStatus {
	if in == nil {
		return nil
	}
	return &common.SyncShardStatus{
		Timestamp: in.GetTimestamp(),
	}
}

func ToProtoReplicationTasks(in []*replicator.ReplicationTask) []*common.ReplicationTask {
	if in == nil {
		return nil
	}

	var ret []*common.ReplicationTask
	for _, item := range in {
		ret = append(ret, ToProtoReplicationTask(item))
	}
	return ret
}

// ToProtoReplicationTask ...
func ToProtoReplicationTask(in *replicator.ReplicationTask) *common.ReplicationTask {
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
		ret.Attributes = &common.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: ToProtoHistoryTaskAttributes(in.GetHistoryTaskAttributes())}
	case enums.ReplicationTaskTypeSyncShardStatus:
		ret.Attributes = &common.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: ToProtoSyncShardStatusTaskAttributes(in.GetSyncShardStatusTaskAttributes())}
	case enums.ReplicationTaskTypeSyncActivity:
		ret.Attributes = &common.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: ToProtoSyncActivityTaskAttributes(in.GetSyncActivityTaskAttributes())}
	case enums.ReplicationTaskTypeHistoryMetadata:
		ret.Attributes = &common.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: ToProtoHistoryMetadataTaskAttributes(in.GetHistoryMetadataTaskAttributes())}
	case enums.ReplicationTaskTypeHistoryV2:
		ret.Attributes = &common.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: ToProtoHistoryTaskV2Attributes(in.GetHistoryTaskV2Attributes())}
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
		Info:              ToProtoDomainInfo(in.GetInfo()),
		Config:            ToProtoDomainConfiguration(in.GetConfig()),
		ReplicationConfig: ToProtoDomainReplicationConfiguration(in.GetReplicationConfig()),
		ConfigVersion:     in.GetConfigVersion(),
		FailoverVersion:   in.GetFailoverVersion(),
	}
}

// ToProtoHistoryTaskAttributes ...
func ToProtoHistoryTaskAttributes(in *replicator.HistoryTaskAttributes) *common.HistoryTaskAttributes {
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
		ReplicationInfo:         ToProtoReplicationInfos(in.GetReplicationInfo()),
		History:                 ToProtoHistory(in.GetHistory()),
		NewRunHistory:           ToProtoHistory(in.GetNewRunHistory()),
		EventStoreVersion:       in.GetEventStoreVersion(),
		NewRunEventStoreVersion: in.GetNewRunEventStoreVersion(),
		ResetWorkflow:           in.GetResetWorkflow(),
		NewRunNDC:               in.GetNewRunNDC(),
	}
}

func ToProtoReplicationInfos(in map[string]*shared.ReplicationInfo) map[string]*common.ReplicationInfo {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.ReplicationInfo, len(in))
	for k, v := range in {
		ret[k] = ToProtoReplicationInfo(v)
	}

	return ret
}

// ToProtoReplicationInfo ...
func ToProtoReplicationInfo(in *shared.ReplicationInfo) *common.ReplicationInfo {
	if in == nil {
		return nil
	}
	return &common.ReplicationInfo{
		Version:     in.GetVersion(),
		LastEventId: in.GetLastEventId(),
	}
}

// ToProtoHistoryMetadataTaskAttributes ...
func ToProtoHistoryMetadataTaskAttributes(in *replicator.HistoryMetadataTaskAttributes) *common.HistoryMetadataTaskAttributes {
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

// ToProtoSyncShardStatusTaskAttributes ...
func ToProtoSyncShardStatusTaskAttributes(in *replicator.SyncShardStatusTaskAttributes) *common.SyncShardStatusTaskAttributes {
	if in == nil {
		return nil
	}
	return &common.SyncShardStatusTaskAttributes{
		SourceCluster: in.GetSourceCluster(),
		ShardId:       in.GetShardId(),
		Timestamp:     in.GetTimestamp(),
	}
}

// ToProtoSyncActivityTaskAttributes ...
func ToProtoSyncActivityTaskAttributes(in *replicator.SyncActivityTaskAttributes) *common.SyncActivityTaskAttributes {
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
		VersionHistory:     ToProtoVersionHistory(in.GetVersionHistory()),
	}
}

// ToProtoVersionHistoryItem ...
func ToProtoVersionHistoryItem(in *shared.VersionHistoryItem) *common.VersionHistoryItem {
	if in == nil {
		return nil
	}
	return &common.VersionHistoryItem{
		EventID: in.GetEventID(),
		Version: in.GetVersion(),
	}
}

// ToProtoVersionHistory ...
func ToProtoVersionHistory(in *shared.VersionHistory) *common.VersionHistory {
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
		ret = append(ret, ToProtoVersionHistoryItem(item))
	}
	return ret
}

// ToProtoHistoryTaskV2Attributes ...
func ToProtoHistoryTaskV2Attributes(in *replicator.HistoryTaskV2Attributes) *common.HistoryTaskV2Attributes {
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
		EncodingType: ToProtoEncodingType(in.GetEncodingType()),
		Data:         in.GetData(),
	}
}

func ToProtoIndexedValueTypes(in map[string]shared.IndexedValueType) map[string]enums.IndexedValueType {
	if in == nil {
		return nil
	}

	ret := make(map[string]enums.IndexedValueType, len(in))
	for k, v := range in {
		ret[k] = enums.IndexedValueType(v)
	}

	return ret
}

func ToProtoReplicationMessagess(in map[int32]*replicator.ReplicationMessages) map[int32]*common.ReplicationMessages {
	if in == nil {
		return nil
	}

	ret := make(map[int32]*common.ReplicationMessages, len(in))
	for k, v := range in {
		ret[k] = ToProtoReplicationMessages(v)
	}

	return ret
}

func ToThriftWorkerVersionInfo(in *common.WorkerVersionInfo) *shared.WorkerVersionInfo {
	if in == nil {
		return nil
	}
	return &shared.WorkerVersionInfo{
		Impl:           &in.Impl,
		FeatureVersion: &in.FeatureVersion,
	}
}

func ToProtoSupportedClientVersions(in *shared.SupportedClientVersions) *common.SupportedClientVersions {
	if in == nil {
		return nil
	}
	return &common.SupportedClientVersions{
		GoSdk:   in.GetGoSdk(),
		JavaSdk: in.GetJavaSdk(),
	}
}

func ToProtoTaskListPartitionMetadatas(in []*shared.TaskListPartitionMetadata) []*common.TaskListPartitionMetadata {
	if in == nil {
		return nil
	}

	var ret []*common.TaskListPartitionMetadata
	for _, item := range in {
		ret = append(ret, ToProtoTaskListPartitionMetadata(item))
	}
	return ret
}

func ToProtoTaskListPartitionMetadata(in *shared.TaskListPartitionMetadata) *common.TaskListPartitionMetadata {
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
		Points: ToThriftResetPointInfos(in.Points),
	}
}

func ToThriftResetPointInfos(in []*common.ResetPointInfo) []*shared.ResetPointInfo {
	if in == nil {
		return nil
	}

	var ret []*shared.ResetPointInfo
	for _, item := range in {
		ret = append(ret, ToThriftResetPointInfo(item))
	}
	return ret
}

func ToThriftResetPointInfo(in *common.ResetPointInfo) *shared.ResetPointInfo {
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
		ret = append(ret, ToThriftVersionHistoryItem(item))
	}
	return ret
}

func ToThriftVersionHistoryItem(in *common.VersionHistoryItem) *shared.VersionHistoryItem {
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
		ret[key] = ToThriftReplicationInfo(value)
	}

	return ret
}

func ToThriftReplicationInfo(in *common.ReplicationInfo) *shared.ReplicationInfo {
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
		DomainOperation:   ToThriftDomainOperation(in.DomainOperation),
		ID:                &in.Id,
		Info:              ToThriftDomainInfo(in.Info),
		Config:            ToThriftDomainConfiguration(in.Config),
		ReplicationConfig: ToThriftDomainReplicationConfiguration(in.ReplicationConfig),
		ConfigVersion:     &in.ConfigVersion,
		FailoverVersion:   &in.FailoverVersion,
	}
}

// ToThriftDomainInfo ...
func ToThriftDomainInfo(in *common.DomainInfo) *shared.DomainInfo {
	if in == nil {
		return nil
	}
	return &shared.DomainInfo{
		Name:        &in.Name,
		Status:      ToThriftDomainStatus(in.Status),
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
		ReplicationTasks:       ToThriftReplicationTasks(in.ReplicationTasks),
		LastRetrievedMessageId: &in.LastRetrievedMessageId,
		HasMore:                &in.HasMore,
		SyncShardStatus:        ToThriftSyncShardStatus(in.SyncShardStatus),
	}
}

// ToThriftSyncShardStatus ...
func ToThriftSyncShardStatus(in *common.SyncShardStatus) *replicator.SyncShardStatus {
	if in == nil {
		return nil
	}
	return &replicator.SyncShardStatus{
		Timestamp: &in.Timestamp,
	}
}

// ToThriftReplicationTasks ...
func ToThriftReplicationTasks(in []*common.ReplicationTask) []*replicator.ReplicationTask {
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
		TaskType:     ToThriftReplicationTaskType(in.TaskType),
		SourceTaskId: &in.SourceTaskId,
	}

	switch in.TaskType {
	case enums.ReplicationTaskTypeDomain:
		ret.DomainTaskAttributes = ToThriftDomainTaskAttributes(in.GetDomainTaskAttributes())
	case enums.ReplicationTaskTypeHistory:
		ret.HistoryTaskAttributes = ToThriftHistoryTaskAttributes(in.GetHistoryTaskAttributes())
	case enums.ReplicationTaskTypeSyncShardStatus:
		ret.SyncShardStatusTaskAttributes = ToThriftSyncShardStatusTaskAttributes(in.GetSyncShardStatusTaskAttributes())
	case enums.ReplicationTaskTypeSyncActivity:
		ret.SyncActivityTaskAttributes = ToThriftSyncActivityTaskAttributes(in.GetSyncActivityTaskAttributes())
	case enums.ReplicationTaskTypeHistoryMetadata:
		ret.HistoryMetadataTaskAttributes = ToThriftHistoryMetadataTaskAttributes(in.GetHistoryMetadataTaskAttributes())
	case enums.ReplicationTaskTypeHistoryV2:
		ret.HistoryTaskV2Attributes = ToThriftHistoryTaskV2Attributes(in.GetHistoryTaskV2Attributes())
	}

	return ret
}

// ToThriftHistoryTaskAttributes ...
func ToThriftHistoryTaskAttributes(in *common.HistoryTaskAttributes) *replicator.HistoryTaskAttributes {
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

// ToThriftHistoryMetadataTaskAttributes ...
func ToThriftHistoryMetadataTaskAttributes(in *common.HistoryMetadataTaskAttributes) *replicator.HistoryMetadataTaskAttributes {
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

// ToThriftSyncShardStatusTaskAttributes ...
func ToThriftSyncShardStatusTaskAttributes(in *common.SyncShardStatusTaskAttributes) *replicator.SyncShardStatusTaskAttributes {
	if in == nil {
		return nil
	}
	return &replicator.SyncShardStatusTaskAttributes{
		SourceCluster: &in.SourceCluster,
		ShardId:       &in.ShardId,
		Timestamp:     &in.Timestamp,
	}
}

// ToThriftSyncActivityTaskAttributes ...
func ToThriftSyncActivityTaskAttributes(in *common.SyncActivityTaskAttributes) *replicator.SyncActivityTaskAttributes {
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

// ToThriftHistoryTaskV2Attributes ...
func ToThriftHistoryTaskV2Attributes(in *common.HistoryTaskV2Attributes) *replicator.HistoryTaskV2Attributes {
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

func ToThriftIndexedValueTypes(in map[string]enums.IndexedValueType) map[string]shared.IndexedValueType {
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
