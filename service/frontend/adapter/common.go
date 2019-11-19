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
		Status:      toProtoDomainStatus(in.GetStatus()),
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

func toProtoHistory(in *shared.History) *common.History {
	if in == nil {
		return nil
	}
	var events []*common.HistoryEvent
	for _, event := range in.Events {
		events = append(events, toProtoHistoryEvent(event))
	}
	return &common.History{
		Events: events,
	}
}

func toProtoHistoryEvent(in *shared.HistoryEvent) *common.HistoryEvent {
	if in == nil {
		return nil
	}
	return &common.HistoryEvent{
		EventId:                                 in.GetEventId(),
		Timestamp:                               in.GetTimestamp(),
		EventType:                               enums.EventType(in.GetEventType()),
		Version:                                 in.GetVersion(),
		TaskId:                                  in.GetTaskId(),
		WorkflowExecutionStartedEventAttributes: nil,
		WorkflowExecutionCompletedEventAttributes:                      nil,
		WorkflowExecutionFailedEventAttributes:                         nil,
		WorkflowExecutionTimedOutEventAttributes:                       nil,
		DecisionTaskScheduledEventAttributes:                           nil,
		DecisionTaskStartedEventAttributes:                             nil,
		DecisionTaskCompletedEventAttributes:                           nil,
		DecisionTaskTimedOutEventAttributes:                            nil,
		DecisionTaskFailedEventAttributes:                              nil,
		ActivityTaskScheduledEventAttributes:                           nil,
		ActivityTaskStartedEventAttributes:                             nil,
		ActivityTaskCompletedEventAttributes:                           nil,
		ActivityTaskFailedEventAttributes:                              nil,
		ActivityTaskTimedOutEventAttributes:                            nil,
		TimerStartedEventAttributes:                                    nil,
		TimerFiredEventAttributes:                                      nil,
		ActivityTaskCancelRequestedEventAttributes:                     nil,
		RequestCancelActivityTaskFailedEventAttributes:                 nil,
		ActivityTaskCanceledEventAttributes:                            nil,
		TimerCanceledEventAttributes:                                   nil,
		CancelTimerFailedEventAttributes:                               nil,
		MarkerRecordedEventAttributes:                                  nil,
		WorkflowExecutionSignaledEventAttributes:                       nil,
		WorkflowExecutionTerminatedEventAttributes:                     nil,
		WorkflowExecutionCancelRequestedEventAttributes:                nil,
		WorkflowExecutionCanceledEventAttributes:                       nil,
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: nil,
		RequestCancelExternalWorkflowExecutionFailedEventAttributes:    nil,
		ExternalWorkflowExecutionCancelRequestedEventAttributes:        nil,
		WorkflowExecutionContinuedAsNewEventAttributes:                 nil,
		StartChildWorkflowExecutionInitiatedEventAttributes:            nil,
		StartChildWorkflowExecutionFailedEventAttributes:               nil,
		ChildWorkflowExecutionStartedEventAttributes:                   nil,
		ChildWorkflowExecutionCompletedEventAttributes:                 nil,
		ChildWorkflowExecutionFailedEventAttributes:                    nil,
		ChildWorkflowExecutionCanceledEventAttributes:                  nil,
		ChildWorkflowExecutionTimedOutEventAttributes:                  nil,
		ChildWorkflowExecutionTerminatedEventAttributes:                nil,
		SignalExternalWorkflowExecutionInitiatedEventAttributes:        nil,
		SignalExternalWorkflowExecutionFailedEventAttributes:           nil,
		ExternalWorkflowExecutionSignaledEventAttributes:               nil,
		UpsertWorkflowSearchAttributesEventAttributes:                  nil,
	}
}
