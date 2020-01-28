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
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/go/shared"
)

// ToThriftRegisterDomainRequest converts gRPC to Thrift
func ToThriftRegisterDomainRequest(in *workflowservice.RegisterDomainRequest) *shared.RegisterDomainRequest {
	if in == nil {
		return nil
	}
	return &shared.RegisterDomainRequest{
		Name:                                   &in.Name,
		Description:                            &in.Description,
		OwnerEmail:                             &in.OwnerEmail,
		WorkflowExecutionRetentionPeriodInDays: &in.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             &in.EmitMetric,
		Clusters:                               toThriftClusterReplicationConfigurations(in.Clusters),
		ActiveClusterName:                      &in.ActiveClusterName,
		Data:                                   in.Data,
		SecurityToken:                          &in.SecurityToken,
		IsGlobalDomain:                         &in.IsGlobalDomain,
		HistoryArchivalStatus:                  ToThriftArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     &in.HistoryArchivalURI,
		VisibilityArchivalStatus:               ToThriftArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  &in.VisibilityArchivalURI,
	}
}

// ToThriftDescribeDomainRequest ...
func ToThriftDescribeDomainRequest(in *workflowservice.DescribeDomainRequest) *shared.DescribeDomainRequest {
	if in == nil {
		return nil
	}
	return &shared.DescribeDomainRequest{
		Name: &in.Name,
		UUID: &in.Uuid,
	}
}

// ToProtoDescribeDomainResponse ...
func ToProtoDescribeDomainResponse(in *shared.DescribeDomainResponse) *workflowservice.DescribeDomainResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.DescribeDomainResponse{
		DomainInfo:               toProtoDomainInfo(in.DomainInfo),
		Configuration:            toProtoDomainConfiguration(in.Configuration),
		ReplicationConfiguration: toProtoDomainReplicationConfiguration(in.ReplicationConfiguration),
		FailoverVersion:          in.GetFailoverVersion(),
		IsGlobalDomain:           in.GetIsGlobalDomain(),
	}
}

// ToThriftListDomainsRequest ...
func ToThriftListDomainsRequest(in *workflowservice.ListDomainsRequest) *shared.ListDomainsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListDomainsRequest{
		PageSize:      &in.PageSize,
		NextPageToken: in.NextPageToken,
	}
}

// ToProtoListDomainsResponse ...
func ToProtoListDomainsResponse(in *shared.ListDomainsResponse) *workflowservice.ListDomainsResponse {
	if in == nil {
		return nil
	}
	var ret []*workflowservice.DescribeDomainResponse
	for _, domain := range in.Domains {
		ret = append(ret, ToProtoDescribeDomainResponse(domain))
	}

	return &workflowservice.ListDomainsResponse{
		Domains:       ret,
		NextPageToken: in.NextPageToken,
	}
}

// ToThriftUpdateDomainRequest ...
func ToThriftUpdateDomainRequest(in *workflowservice.UpdateDomainRequest) *shared.UpdateDomainRequest {
	if in == nil {
		return nil
	}
	return &shared.UpdateDomainRequest{
		Name:                     &in.Name,
		UpdatedInfo:              toThriftUpdateDomainInfo(in.UpdatedInfo),
		Configuration:            toThriftDomainConfiguration(in.Configuration),
		ReplicationConfiguration: toThriftDomainReplicationConfiguration(in.ReplicationConfiguration),
		SecurityToken:            &in.SecurityToken,
		DeleteBadBinary:          &in.DeleteBadBinary,
	}
}

// ToProtoUpdateDomainResponse ...
func ToProtoUpdateDomainResponse(in *shared.UpdateDomainResponse) *workflowservice.UpdateDomainResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.UpdateDomainResponse{
		DomainInfo:               toProtoDomainInfo(in.DomainInfo),
		Configuration:            toProtoDomainConfiguration(in.Configuration),
		ReplicationConfiguration: toProtoDomainReplicationConfiguration(in.ReplicationConfiguration),
		FailoverVersion:          in.GetFailoverVersion(),
		IsGlobalDomain:           in.GetIsGlobalDomain(),
	}
}

// ToThriftDeprecateDomainRequest ...
func ToThriftDeprecateDomainRequest(in *workflowservice.DeprecateDomainRequest) *shared.DeprecateDomainRequest {
	if in == nil {
		return nil
	}
	return &shared.DeprecateDomainRequest{
		Name:          &in.Name,
		SecurityToken: &in.SecurityToken,
	}
}

// ToThriftStartWorkflowExecutionRequest ...
func ToThriftStartWorkflowExecutionRequest(in *workflowservice.StartWorkflowExecutionRequest) *shared.StartWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.StartWorkflowExecutionRequest{
		Domain:                              &in.Domain,
		WorkflowId:                          &in.WorkflowId,
		WorkflowType:                        toThriftWorkflowType(in.WorkflowType),
		TaskList:                            toThriftTaskList(in.TaskList),
		Input:                               in.Input,
		ExecutionStartToCloseTimeoutSeconds: &in.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      &in.TaskStartToCloseTimeoutSeconds,
		Identity:                            &in.Identity,
		RequestId:                           &in.RequestId,
		WorkflowIdReusePolicy:               toThriftWorkflowIDReusePolicy(in.WorkflowIdReusePolicy),
		RetryPolicy:                         toThriftRetryPolicy(in.RetryPolicy),
		CronSchedule:                        &in.CronSchedule,
		Memo:                                toThriftMemo(in.Memo),
		SearchAttributes:                    toThriftSearchAttributes(in.SearchAttributes),
		Header:                              toThriftHeader(in.Header),
	}
}

// ToProtoStartWorkflowExecutionResponse ...
func ToProtoStartWorkflowExecutionResponse(in *shared.StartWorkflowExecutionResponse) *workflowservice.StartWorkflowExecutionResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.StartWorkflowExecutionResponse{
		RunId: in.GetRunId(),
	}
}

// ToThriftGetWorkflowExecutionHistoryRequest ...
func ToThriftGetWorkflowExecutionHistoryRequest(in *workflowservice.GetWorkflowExecutionHistoryRequest) *shared.GetWorkflowExecutionHistoryRequest {
	if in == nil {
		return nil
	}
	return &shared.GetWorkflowExecutionHistoryRequest{
		Domain:                 &in.Domain,
		Execution:              ToThriftWorkflowExecution(in.Execution),
		MaximumPageSize:        &in.MaximumPageSize,
		NextPageToken:          in.NextPageToken,
		WaitForNewEvent:        &in.WaitForNewEvent,
		HistoryEventFilterType: toThriftHistoryEventFilterType(in.HistoryEventFilterType),
	}
}

// ToProtoGetWorkflowExecutionHistoryResponse ...
func ToProtoGetWorkflowExecutionHistoryResponse(in *shared.GetWorkflowExecutionHistoryResponse) *workflowservice.GetWorkflowExecutionHistoryResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       toProtoHistory(in.History),
		NextPageToken: in.NextPageToken,
		Archived:      in.GetArchived(),
	}
}

// ToThriftRequestCancelWorkflowExecutionRequest ...
func ToThriftRequestCancelWorkflowExecutionRequest(in *workflowservice.RequestCancelWorkflowExecutionRequest) *shared.RequestCancelWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.RequestCancelWorkflowExecutionRequest{
		Domain:            &in.Domain,
		WorkflowExecution: ToThriftWorkflowExecution(in.WorkflowExecution),
		Identity:          &in.Identity,
		RequestId:         &in.RequestId,
	}
}

// ToThriftSignalWorkflowExecutionRequest ...
func ToThriftSignalWorkflowExecutionRequest(in *workflowservice.SignalWorkflowExecutionRequest) *shared.SignalWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.SignalWorkflowExecutionRequest{
		Domain:            &in.Domain,
		WorkflowExecution: ToThriftWorkflowExecution(in.WorkflowExecution),
		SignalName:        &in.SignalName,
		Input:             in.Input,
		Identity:          &in.Identity,
		RequestId:         &in.RequestId,
		Control:           in.Control,
	}
}

// ToProtoSignalWithStartWorkflowExecutionResponse ...
func ToProtoSignalWithStartWorkflowExecutionResponse(in *shared.StartWorkflowExecutionResponse) *workflowservice.SignalWithStartWorkflowExecutionResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.SignalWithStartWorkflowExecutionResponse{
		RunId: in.GetRunId(),
	}
}

// ToThriftSignalWithStartWorkflowExecutionRequest ...
func ToThriftSignalWithStartWorkflowExecutionRequest(in *workflowservice.SignalWithStartWorkflowExecutionRequest) *shared.SignalWithStartWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.SignalWithStartWorkflowExecutionRequest{
		Domain:                              &in.Domain,
		WorkflowId:                          &in.WorkflowId,
		WorkflowType:                        toThriftWorkflowType(in.WorkflowType),
		TaskList:                            toThriftTaskList(in.TaskList),
		Input:                               in.Input,
		ExecutionStartToCloseTimeoutSeconds: &in.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      &in.TaskStartToCloseTimeoutSeconds,
		Identity:                            &in.Identity,
		RequestId:                           &in.RequestId,
		WorkflowIdReusePolicy:               toThriftWorkflowIDReusePolicy(in.WorkflowIdReusePolicy),
		SignalName:                          &in.SignalName,
		SignalInput:                         in.SignalInput,
		Control:                             in.Control,
		RetryPolicy:                         toThriftRetryPolicy(in.RetryPolicy),
		CronSchedule:                        &in.CronSchedule,
		Memo:                                toThriftMemo(in.Memo),
		SearchAttributes:                    toThriftSearchAttributes(in.SearchAttributes),
		Header:                              toThriftHeader(in.Header),
	}
}

// ToProtoResetWorkflowExecutionResponse ...
func ToProtoResetWorkflowExecutionResponse(in *shared.ResetWorkflowExecutionResponse) *workflowservice.ResetWorkflowExecutionResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ResetWorkflowExecutionResponse{
		RunId: in.GetRunId(),
	}
}

// ToThriftResetWorkflowExecutionRequest ...
func ToThriftResetWorkflowExecutionRequest(in *workflowservice.ResetWorkflowExecutionRequest) *shared.ResetWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.ResetWorkflowExecutionRequest{
		Domain:                &in.Domain,
		WorkflowExecution:     ToThriftWorkflowExecution(in.WorkflowExecution),
		Reason:                &in.Reason,
		DecisionFinishEventId: &in.DecisionFinishEventId,
		RequestId:             &in.RequestId,
	}
}

// ToThriftTerminateWorkflowExecutionRequest ...
func ToThriftTerminateWorkflowExecutionRequest(in *workflowservice.TerminateWorkflowExecutionRequest) *shared.TerminateWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.TerminateWorkflowExecutionRequest{
		Domain:            &in.Domain,
		WorkflowExecution: ToThriftWorkflowExecution(in.WorkflowExecution),
		Reason:            &in.Reason,
		Details:           in.Details,
		Identity:          &in.Identity,
	}
}

// ToProtoListOpenWorkflowExecutionsResponse ...
func ToProtoListOpenWorkflowExecutionsResponse(in *shared.ListOpenWorkflowExecutionsResponse) *workflowservice.ListOpenWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}

	return &workflowservice.ListOpenWorkflowExecutionsResponse{
		Executions:    toProtoWorkflowExecutionInfos(in.GetExecutions()),
		NextPageToken: in.GetNextPageToken(),
	}
}

// ToThriftListOpenWorkflowExecutionsRequest ...
func ToThriftListOpenWorkflowExecutionsRequest(in *workflowservice.ListOpenWorkflowExecutionsRequest) *shared.ListOpenWorkflowExecutionsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListOpenWorkflowExecutionsRequest{
		Domain:          &in.Domain,
		MaximumPageSize: &in.MaximumPageSize,
		NextPageToken:   in.NextPageToken,
		StartTimeFilter: toThriftStartTimeFilter(in.StartTimeFilter),
		ExecutionFilter: toThriftWorkflowExecutionFilter(in.GetExecutionFilter()),
		TypeFilter:      toThriftWorkflowTypeFilter(in.GetTypeFilter()),
	}
}

// ToProtoListClosedWorkflowExecutionsResponse ...
func ToProtoListClosedWorkflowExecutionsResponse(in *shared.ListClosedWorkflowExecutionsResponse) *workflowservice.ListClosedWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ListClosedWorkflowExecutionsResponse{
		Executions:    toProtoWorkflowExecutionInfos(in.GetExecutions()),
		NextPageToken: in.GetNextPageToken(),
	}
}

// ToThriftListClosedWorkflowExecutionsRequest ...
func ToThriftListClosedWorkflowExecutionsRequest(in *workflowservice.ListClosedWorkflowExecutionsRequest) *shared.ListClosedWorkflowExecutionsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListClosedWorkflowExecutionsRequest{
		Domain:          &in.Domain,
		MaximumPageSize: &in.MaximumPageSize,
		NextPageToken:   in.NextPageToken,
		StartTimeFilter: toThriftStartTimeFilter(in.StartTimeFilter),
		ExecutionFilter: toThriftWorkflowExecutionFilter(in.GetExecutionFilter()),
		TypeFilter:      toThriftWorkflowTypeFilter(in.GetTypeFilter()),
		StatusFilter:    toThriftWorkflowStatusFilter(in.GetStatusFilter()),
	}
}

// ToProtoListWorkflowExecutionsResponse ...
func ToProtoListWorkflowExecutionsResponse(in *shared.ListWorkflowExecutionsResponse) *workflowservice.ListWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ListWorkflowExecutionsResponse{
		Executions:    toProtoWorkflowExecutionInfos(in.GetExecutions()),
		NextPageToken: in.GetNextPageToken(),
	}
}

// ToThriftListWorkflowExecutionsRequest ...
func ToThriftListWorkflowExecutionsRequest(in *workflowservice.ListWorkflowExecutionsRequest) *shared.ListWorkflowExecutionsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListWorkflowExecutionsRequest{
		Domain:        &in.Domain,
		PageSize:      &in.PageSize,
		NextPageToken: in.NextPageToken,
		Query:         &in.Query,
	}
}

// ToThriftScanWorkflowExecutionsRequest ...
func ToThriftScanWorkflowExecutionsRequest(in *workflowservice.ScanWorkflowExecutionsRequest) *shared.ListWorkflowExecutionsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListWorkflowExecutionsRequest{
		Domain:        &in.Domain,
		PageSize:      &in.PageSize,
		NextPageToken: in.NextPageToken,
		Query:         &in.Query,
	}
}

// ToProtoScanWorkflowExecutionsResponse ...
func ToProtoScanWorkflowExecutionsResponse(in *shared.ListWorkflowExecutionsResponse) *workflowservice.ScanWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ScanWorkflowExecutionsResponse{
		Executions:    toProtoWorkflowExecutionInfos(in.GetExecutions()),
		NextPageToken: in.GetNextPageToken(),
	}
}

// ToProtoListArchivedWorkflowExecutionsResponse ...
func ToProtoListArchivedWorkflowExecutionsResponse(in *shared.ListArchivedWorkflowExecutionsResponse) *workflowservice.ListArchivedWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ListArchivedWorkflowExecutionsResponse{
		Executions:    toProtoWorkflowExecutionInfos(in.GetExecutions()),
		NextPageToken: in.GetNextPageToken(),
	}
}

// ToThriftListArchivedWorkflowExecutionsRequest ...
func ToThriftListArchivedWorkflowExecutionsRequest(in *workflowservice.ListArchivedWorkflowExecutionsRequest) *shared.ListArchivedWorkflowExecutionsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListArchivedWorkflowExecutionsRequest{
		Domain:        &in.Domain,
		PageSize:      &in.PageSize,
		NextPageToken: in.NextPageToken,
		Query:         &in.Query,
	}
}

// ToProtoCountWorkflowExecutionsResponse ...
func ToProtoCountWorkflowExecutionsResponse(in *shared.CountWorkflowExecutionsResponse) *workflowservice.CountWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.CountWorkflowExecutionsResponse{
		Count: in.GetCount(),
	}
}

// ToThriftCountWorkflowExecutionsRequest ...
func ToThriftCountWorkflowExecutionsRequest(in *workflowservice.CountWorkflowExecutionsRequest) *shared.CountWorkflowExecutionsRequest {
	if in == nil {
		return nil
	}
	return &shared.CountWorkflowExecutionsRequest{
		Domain: &in.Domain,
		Query:  &in.Query,
	}
}

// ToProtoDescribeWorkflowExecutionResponse ...
func ToProtoDescribeWorkflowExecutionResponse(in *shared.DescribeWorkflowExecutionResponse) *workflowservice.DescribeWorkflowExecutionResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{
		ExecutionConfiguration: toProtoWorkflowExecutionConfiguration(in.GetExecutionConfiguration()),
		WorkflowExecutionInfo:  toProtoWorkflowExecutionInfo(in.GetWorkflowExecutionInfo()),
		PendingActivities:      toProtoPendingActivityInfos(in.GetPendingActivities()),
		PendingChildren:        toProtoPendingChildExecutionInfos(in.GetPendingChildren()),
	}
}

// ToThriftDescribeWorkflowExecutionRequest ...
func ToThriftDescribeWorkflowExecutionRequest(in *workflowservice.DescribeWorkflowExecutionRequest) *shared.DescribeWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &shared.DescribeWorkflowExecutionRequest{
		Domain:    &in.Domain,
		Execution: ToThriftWorkflowExecution(in.Execution),
	}
}

// ToThriftPollForDecisionTaskRequest ...
func ToThriftPollForDecisionTaskRequest(in *workflowservice.PollForDecisionTaskRequest) *shared.PollForDecisionTaskRequest {
	if in == nil {
		return nil
	}
	return &shared.PollForDecisionTaskRequest{
		Domain:         &in.Domain,
		TaskList:       toThriftTaskList(in.TaskList),
		Identity:       &in.Identity,
		BinaryChecksum: &in.BinaryChecksum,
	}
}

// ToProtoPollForDecisionTaskResponse ...
func ToProtoPollForDecisionTaskResponse(in *shared.PollForDecisionTaskResponse) *workflowservice.PollForDecisionTaskResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.PollForDecisionTaskResponse{
		TaskToken:                 in.GetTaskToken(),
		WorkflowExecution:         ToProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:              toProtoWorkflowType(in.GetWorkflowType()),
		PreviousStartedEventId:    in.GetPreviousStartedEventId(),
		StartedEventId:            in.GetStartedEventId(),
		Attempt:                   in.GetAttempt(),
		BacklogCountHint:          in.GetBacklogCountHint(),
		History:                   toProtoHistory(in.GetHistory()),
		NextPageToken:             in.GetNextPageToken(),
		Query:                     toProtoWorkflowQuery(in.GetQuery()),
		WorkflowExecutionTaskList: toProtoTaskList(in.GetWorkflowExecutionTaskList()),
		ScheduledTimestamp:        in.GetScheduledTimestamp(),
		StartedTimestamp:          in.GetStartedTimestamp(),
		Queries:                   toProtoWorkflowQueries(in.GetQueries()),
	}
}

// ToProtoRespondDecisionTaskCompletedResponse ...
func ToProtoRespondDecisionTaskCompletedResponse(in *shared.RespondDecisionTaskCompletedResponse) *workflowservice.RespondDecisionTaskCompletedResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.RespondDecisionTaskCompletedResponse{
		DecisionTask: ToProtoPollForDecisionTaskResponse(in.GetDecisionTask()),
	}
}

// ToProtoPollForActivityTaskResponse ...
func ToProtoPollForActivityTaskResponse(in *shared.PollForActivityTaskResponse) *workflowservice.PollForActivityTaskResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.PollForActivityTaskResponse{
		TaskToken:                       in.GetTaskToken(),
		WorkflowExecution:               ToProtoWorkflowExecution(in.GetWorkflowExecution()),
		ActivityId:                      in.GetActivityId(),
		ActivityType:                    toProtoActivityType(in.GetActivityType()),
		Input:                           in.GetInput(),
		ScheduledTimestamp:              in.GetScheduledTimestamp(),
		ScheduleToCloseTimeoutSeconds:   in.GetScheduleToCloseTimeoutSeconds(),
		StartedTimestamp:                in.GetStartedTimestamp(),
		StartToCloseTimeoutSeconds:      in.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeoutSeconds:         in.GetHeartbeatTimeoutSeconds(),
		Attempt:                         in.GetAttempt(),
		ScheduledTimestampOfThisAttempt: in.GetScheduledTimestampOfThisAttempt(),
		HeartbeatDetails:                in.GetHeartbeatDetails(),
		WorkflowType:                    toProtoWorkflowType(in.GetWorkflowType()),
		WorkflowDomain:                  in.GetWorkflowDomain(),
		Header:                          toProtoHeader(in.GetHeader()),
	}
}

// ToProtoRecordActivityTaskHeartbeatResponse ...
func ToProtoRecordActivityTaskHeartbeatResponse(in *shared.RecordActivityTaskHeartbeatResponse) *workflowservice.RecordActivityTaskHeartbeatResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.RecordActivityTaskHeartbeatResponse{
		CancelRequested: in.GetCancelRequested(),
	}
}

// ToProtoRecordActivityTaskHeartbeatByIDResponse ...
func ToProtoRecordActivityTaskHeartbeatByIDResponse(in *shared.RecordActivityTaskHeartbeatResponse) *workflowservice.RecordActivityTaskHeartbeatByIDResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.RecordActivityTaskHeartbeatByIDResponse{
		CancelRequested: in.GetCancelRequested(),
	}
}

// ToThriftRespondDecisionTaskCompletedRequest ...
func ToThriftRespondDecisionTaskCompletedRequest(in *workflowservice.RespondDecisionTaskCompletedRequest) *shared.RespondDecisionTaskCompletedRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondDecisionTaskCompletedRequest{
		TaskToken:                  in.TaskToken,
		Decisions:                  toThriftDecisions(in.Decisions),
		ExecutionContext:           in.ExecutionContext,
		Identity:                   &in.Identity,
		StickyAttributes:           toThriftStickyExecutionAttributes(in.StickyAttributes),
		ReturnNewDecisionTask:      &in.ReturnNewDecisionTask,
		ForceCreateNewDecisionTask: &in.ForceCreateNewDecisionTask,
		BinaryChecksum:             &in.BinaryChecksum,
		QueryResults:               toThriftWorkflowQueryResults(in.QueryResults),
	}
}

// ToThriftRespondDecisionTaskFailedRequest ...
func ToThriftRespondDecisionTaskFailedRequest(in *workflowservice.RespondDecisionTaskFailedRequest) *shared.RespondDecisionTaskFailedRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondDecisionTaskFailedRequest{
		TaskToken:      in.TaskToken,
		Cause:          toThriftDecisionTaskFailedCause(in.Cause),
		Details:        in.Details,
		Identity:       &in.Identity,
		BinaryChecksum: &in.BinaryChecksum,
	}
}

// ToThriftPollForActivityTaskRequest ...
func ToThriftPollForActivityTaskRequest(in *workflowservice.PollForActivityTaskRequest) *shared.PollForActivityTaskRequest {
	if in == nil {
		return nil
	}
	return &shared.PollForActivityTaskRequest{
		Domain:           &in.Domain,
		TaskList:         toThriftTaskList(in.TaskList),
		Identity:         &in.Identity,
		TaskListMetadata: toThriftTaskListMetadata(in.TaskListMetadata),
	}
}

// ToThriftRecordActivityTaskHeartbeatRequest ...
func ToThriftRecordActivityTaskHeartbeatRequest(in *workflowservice.RecordActivityTaskHeartbeatRequest) *shared.RecordActivityTaskHeartbeatRequest {
	if in == nil {
		return nil
	}
	return &shared.RecordActivityTaskHeartbeatRequest{
		TaskToken: in.TaskToken,
		Details:   in.Details,
		Identity:  &in.Identity,
	}
}

// ToThriftRecordActivityTaskHeartbeatByIDRequest ...
func ToThriftRecordActivityTaskHeartbeatByIDRequest(in *workflowservice.RecordActivityTaskHeartbeatByIDRequest) *shared.RecordActivityTaskHeartbeatByIDRequest {
	if in == nil {
		return nil
	}
	return &shared.RecordActivityTaskHeartbeatByIDRequest{
		Domain:     &in.Domain,
		WorkflowID: &in.WorkflowID,
		RunID:      &in.RunID,
		ActivityID: &in.ActivityID,
		Details:    in.Details,
		Identity:   &in.Identity,
	}
}

// ToThriftRespondActivityTaskCompletedRequest ...
func ToThriftRespondActivityTaskCompletedRequest(in *workflowservice.RespondActivityTaskCompletedRequest) *shared.RespondActivityTaskCompletedRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondActivityTaskCompletedRequest{
		TaskToken: in.TaskToken,
		Result:    in.Result,
		Identity:  &in.Identity,
	}
}

// ToThriftRespondActivityTaskCompletedByIDRequest ...
func ToThriftRespondActivityTaskCompletedByIDRequest(in *workflowservice.RespondActivityTaskCompletedByIDRequest) *shared.RespondActivityTaskCompletedByIDRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondActivityTaskCompletedByIDRequest{
		Domain:     &in.Domain,
		WorkflowID: &in.WorkflowID,
		RunID:      &in.RunID,
		ActivityID: &in.ActivityID,
		Result:     in.Result,
		Identity:   &in.Identity,
	}
}

// ToThriftRespondActivityTaskFailedRequest ...
func ToThriftRespondActivityTaskFailedRequest(in *workflowservice.RespondActivityTaskFailedRequest) *shared.RespondActivityTaskFailedRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondActivityTaskFailedRequest{
		TaskToken: in.TaskToken,
		Reason:    &in.Reason,
		Details:   in.Details,
		Identity:  &in.Identity,
	}
}

// ToThriftRespondActivityTaskFailedByIDRequest ...
func ToThriftRespondActivityTaskFailedByIDRequest(in *workflowservice.RespondActivityTaskFailedByIDRequest) *shared.RespondActivityTaskFailedByIDRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondActivityTaskFailedByIDRequest{
		Domain:     &in.Domain,
		WorkflowID: &in.WorkflowID,
		RunID:      &in.RunID,
		ActivityID: &in.ActivityID,
		Reason:     &in.Reason,
		Details:    in.Details,
		Identity:   &in.Identity,
	}
}

// ToThriftRespondActivityTaskCanceledRequest ...
func ToThriftRespondActivityTaskCanceledRequest(in *workflowservice.RespondActivityTaskCanceledRequest) *shared.RespondActivityTaskCanceledRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondActivityTaskCanceledRequest{
		TaskToken: in.TaskToken,
		Details:   in.Details,
		Identity:  &in.Identity,
	}
}

// ToThriftRespondActivityTaskCanceledByIDRequest ...
func ToThriftRespondActivityTaskCanceledByIDRequest(in *workflowservice.RespondActivityTaskCanceledByIDRequest) *shared.RespondActivityTaskCanceledByIDRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondActivityTaskCanceledByIDRequest{
		Domain:     &in.Domain,
		WorkflowID: &in.WorkflowID,
		RunID:      &in.RunID,
		ActivityID: &in.ActivityID,
		Details:    in.Details,
		Identity:   &in.Identity,
	}
}

// ToThriftRespondQueryTaskCompletedRequest ...
func ToThriftRespondQueryTaskCompletedRequest(in *workflowservice.RespondQueryTaskCompletedRequest) *shared.RespondQueryTaskCompletedRequest {
	if in == nil {
		return nil
	}
	return &shared.RespondQueryTaskCompletedRequest{
		TaskToken:         in.TaskToken,
		CompletedType:     toThriftQueryTaskCompletedType(in.CompletedType),
		QueryResult:       in.QueryResult,
		ErrorMessage:      &in.ErrorMessage,
		WorkerVersionInfo: toThriftWorkerVersionInfo(in.WorkerVersionInfo),
	}
}

// ToThriftResetStickyTaskListRequest ...
func ToThriftResetStickyTaskListRequest(in *workflowservice.ResetStickyTaskListRequest) *shared.ResetStickyTaskListRequest {
	if in == nil {
		return nil
	}
	return &shared.ResetStickyTaskListRequest{
		Domain:    &in.Domain,
		Execution: ToThriftWorkflowExecution(in.Execution),
	}
}

// ToThriftQueryWorkflowRequest ...
func ToThriftQueryWorkflowRequest(in *workflowservice.QueryWorkflowRequest) *shared.QueryWorkflowRequest {
	if in == nil {
		return nil
	}
	return &shared.QueryWorkflowRequest{
		Domain:                &in.Domain,
		Execution:             ToThriftWorkflowExecution(in.Execution),
		Query:                 toThriftWorkflowQuery(in.Query),
		QueryRejectCondition:  toThriftQueryRejectCondition(in.QueryRejectCondition),
		QueryConsistencyLevel: toThriftQueryConsistencyLevel(in.QueryConsistencyLevel),
	}
}

// ToThriftDescribeTaskListRequest ...
func ToThriftDescribeTaskListRequest(in *workflowservice.DescribeTaskListRequest) *shared.DescribeTaskListRequest {
	if in == nil {
		return nil
	}
	return &shared.DescribeTaskListRequest{
		Domain:                &in.Domain,
		TaskList:              toThriftTaskList(in.TaskList),
		TaskListType:          toThriftTaskListType(in.TaskListType),
		IncludeTaskListStatus: &in.IncludeTaskListStatus,
	}
}

// ToProtoGetSearchAttributesResponse ...
func ToProtoGetSearchAttributesResponse(in *shared.GetSearchAttributesResponse) *workflowservice.GetSearchAttributesResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.GetSearchAttributesResponse{
		Keys: toProtoIndexedValueTypes(in.GetKeys()),
	}
}

// ToProtoQueryWorkflowResponse ...
func ToProtoQueryWorkflowResponse(in *shared.QueryWorkflowResponse) *workflowservice.QueryWorkflowResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.QueryWorkflowResponse{
		QueryResult:   in.GetQueryResult(),
		QueryRejected: toProtoQueryRejected(in.GetQueryRejected()),
	}
}

// ToProtoDescribeTaskListResponse ...
func ToProtoDescribeTaskListResponse(in *shared.DescribeTaskListResponse) *workflowservice.DescribeTaskListResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.DescribeTaskListResponse{
		Pollers:        toProtoPollerInfos(in.GetPollers()),
		TaskListStatus: toProtoTaskListStatus(in.GetTaskListStatus()),
	}
}

// ToThriftListTaskListPartitionsRequest ...
func ToThriftListTaskListPartitionsRequest(in *workflowservice.ListTaskListPartitionsRequest) *shared.ListTaskListPartitionsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListTaskListPartitionsRequest{
		Domain:   &in.Domain,
		TaskList: toThriftTaskList(in.TaskList),
	}
}

// ToProtoGetClusterInfoResponse ...
func ToProtoGetClusterInfoResponse(in *shared.ClusterInfo) *workflowservice.GetClusterInfoResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.GetClusterInfoResponse{
		SupportedClientVersions: toProtoSupportedClientVersions(in.SupportedClientVersions),
	}
}

// ToProtoListTaskListPartitionsResponse ...
func ToProtoListTaskListPartitionsResponse(in *shared.ListTaskListPartitionsResponse) *workflowservice.ListTaskListPartitionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ListTaskListPartitionsResponse{
		ActivityTaskListPartitions: toProtoTaskListPartitionMetadatas(in.ActivityTaskListPartitions),
		DecisionTaskListPartitions: toProtoTaskListPartitionMetadatas(in.DecisionTaskListPartitions),
	}
}

// ToThriftGetWorkflowExecutionRawHistoryRequest ...
func ToThriftGetWorkflowExecutionRawHistoryRequest(in *workflowservice.GetWorkflowExecutionRawHistoryRequest) *shared.GetWorkflowExecutionRawHistoryRequest {
	if in == nil {
		return nil
	}
	return &shared.GetWorkflowExecutionRawHistoryRequest{
		Domain:          &in.Domain,
		Execution:       ToThriftWorkflowExecution(in.Execution),
		MaximumPageSize: &in.MaximumPageSize,
		NextPageToken:   in.NextPageToken,
	}
}

// ToProtoGetWorkflowExecutionRawHistoryResponse ...
func ToProtoGetWorkflowExecutionRawHistoryResponse(in *shared.GetWorkflowExecutionRawHistoryResponse) *workflowservice.GetWorkflowExecutionRawHistoryResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.GetWorkflowExecutionRawHistoryResponse{
		RawHistory:    ToProtoDataBlobs(in.GetRawHistory()),
		NextPageToken: in.GetNextPageToken(),
	}
}
