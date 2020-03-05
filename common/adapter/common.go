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
	"github.com/gogo/protobuf/types"
	"go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/go/shared"
)

func ToProtoBool(in *bool) *types.BoolValue {
	if in == nil {
		return nil
	}

	return &types.BoolValue{Value: *in}
}

func ToProtoDouble(in *float64) *types.DoubleValue {
	if in == nil {
		return nil
	}

	return &types.DoubleValue{Value: *in}
}

func ToThriftBool(in *types.BoolValue) *bool {
	if in == nil {
		return nil
	}

	return &in.Value
}

func ToThriftDouble(in *types.DoubleValue) *float64 {
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
		HistoryArchivalURI:                     in.GetHistoryArchivalURI(),
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

	return &common.ResetPoints{
		Points: ToProtoResetPointInfos(in.GetPoints()),
	}
}

func ToProtoResetPointInfos(in []*shared.ResetPointInfo) []*common.ResetPointInfo {
	if in == nil {
		return nil
	}
	var points []*common.ResetPointInfo
	for _, point := range in {
		points = append(points, ToProtoResetPointInfo(point))
	}

	return points
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
		MaxTasksPerSecond: ToThriftDouble(in.MaxTasksPerSecond),
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

func ToThriftQueryRejected(in *common.QueryRejected) *shared.QueryRejected {
	if in == nil {
		return nil
	}
	return &shared.QueryRejected{
		CloseStatus: ToThriftWorkflowExecutionCloseStatus(in.CloseStatus),
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

func ToProtoVersionHistories(in *shared.VersionHistories) *common.VersionHistories {
	if in == nil {
		return nil
	}
	return &common.VersionHistories{
		CurrentVersionHistoryIndex: in.GetCurrentVersionHistoryIndex(),
		Histories:                  ToProtoVersionHistoryArray(in.GetHistories()),
	}
}

func ToProtoVersionHistoryArray(in []*shared.VersionHistory) []*common.VersionHistory {
	if in == nil {
		return nil
	}

	var ret []*common.VersionHistory
	for _, item := range in {
		ret = append(ret, ToProtoVersionHistory(item))
	}
	return ret
}

func ToThriftWorkflowQueries(in map[string]*common.WorkflowQuery) map[string]*shared.WorkflowQuery {
	if in == nil {
		return nil
	}

	ret := make(map[string]*shared.WorkflowQuery, len(in))
	for k, v := range in {
		ret[k] = ToThriftWorkflowQuery(v)
	}

	return ret
}

// ToProtoTaskListMetadata ...
func ToProtoTaskListMetadata(in *shared.TaskListMetadata) *common.TaskListMetadata {
	if in == nil {
		return nil
	}
	return &common.TaskListMetadata{
		MaxTasksPerSecond: ToProtoDouble(in.MaxTasksPerSecond),
	}
}
