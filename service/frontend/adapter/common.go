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
	"github.com/temporalio/temporal-proto/common"
	"github.com/temporalio/temporal-proto/enums"
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
		Clusters:          toProtoClusterReplicationConfigurations(in.Clusters),
	}
}

func toProtoDomainConfiguration(in *shared.DomainConfiguration) *common.DomainConfiguration {
	if in == nil {
		return nil
	}
	return &common.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: in.GetWorkflowExecutionRetentionPeriodInDays(),
		EmitMetric:                             toProtoBool(in.EmitMetric),
		BadBinaries:                            toProtoBadBinaries(in.BadBinaries),
		HistoryArchivalStatus:                  toProtoArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     in.GetVisibilityArchivalURI(),
		VisibilityArchivalStatus:               toProtoArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  in.GetVisibilityArchivalURI(),
	}
}

func toProtoBadBinaries(in *shared.BadBinaries) *common.BadBinaries {
	if in == nil {
		return nil
	}

	ret := make(map[string]*common.BadBinaryInfo, len(in.Binaries))

	for key, value := range in.Binaries {
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
		BadBinaries:                            toThriftBadBinaries(in.BadBinaries),
		HistoryArchivalStatus:                  toThriftArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     &in.HistoryArchivalURI,
		VisibilityArchivalStatus:               toThriftArchivalStatus(in.VisibilityArchivalStatus),
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

func toThriftBadBinaries(in *common.BadBinaries) *shared.BadBinaries {
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

func toThriftWorkflowExecution(in *common.WorkflowExecution) *shared.WorkflowExecution {
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

func toProtoWorkflowExecution(in *shared.WorkflowExecution) *common.WorkflowExecution {
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
		Execution:        toProtoWorkflowExecution(in.GetExecution()),
		Type:             toProtoWorkflowType(in.GetType()),
		StartTime:        in.GetStartTime(),
		CloseTime:        in.GetCloseTime(),
		CloseStatus:      enums.WorkflowExecutionCloseStatus(in.GetCloseStatus()),
		HistoryLength:    in.GetHistoryLength(),
		ParentDomainId:   in.GetParentDomainId(),
		ParentExecution:  toProtoWorkflowExecution(in.GetParentExecution()),
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
