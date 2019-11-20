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
	"github.com/temporalio/temporal-proto/workflowservice"
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
		HistoryArchivalStatus:                  toThriftArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     &in.HistoryArchivalURI,
		VisibilityArchivalStatus:               toThriftArchivalStatus(in.VisibilityArchivalStatus),
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

// ToThriftListDomainRequest ...
func ToThriftListDomainRequest(in *workflowservice.ListDomainsRequest) *shared.ListDomainsRequest {
	if in == nil {
		return nil
	}
	return &shared.ListDomainsRequest{
		PageSize:      &in.PageSize,
		NextPageToken: in.NextPageToken,
	}
}

// ToProtoListDomainResponse ...
func ToProtoListDomainResponse(in *shared.ListDomainsResponse) *workflowservice.ListDomainsResponse {
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
		Execution:              toThriftWorkflowExecution(in.Execution),
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
		WorkflowExecution: toThriftWorkflowExecution(in.WorkflowExecution),
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
		WorkflowExecution: toThriftWorkflowExecution(in.WorkflowExecution),
		SignalName:        &in.SignalName,
		Input:             in.Input,
		Identity:          &in.Identity,
		RequestId:         &in.RequestId,
		Control:           in.Control,
	}
}

// ToProtoSignalWithStartWorkflowExecutionResponse ...
func ToProtoSignalWithStartWorkflowExecutionResponse(in *shared.StartWorkflowExecutionResponse) *workflowservice.StartWorkflowExecutionResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.StartWorkflowExecutionResponse{
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
		WorkflowExecution:     toThriftWorkflowExecution(in.WorkflowExecution),
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
		WorkflowExecution: toThriftWorkflowExecution(in.WorkflowExecution),
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
		ExecutionFilter: toThriftWorkflowExecutionFilter(in.ExecutionFilter),
		TypeFilter:      toThriftWorkflowTypeFilter(in.TypeFilter),
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
		ExecutionFilter: toThriftWorkflowExecutionFilter(in.ExecutionFilter),
		TypeFilter:      toThriftWorkflowTypeFilter(in.TypeFilter),
		StatusFilter:    toThriftWorkflowExecutionCloseStatus(in.StatusFilter),
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

// ToProtoArchivedWorkflowExecutionsResponse ...
func ToProtoArchivedWorkflowExecutionsResponse(in *shared.ListArchivedWorkflowExecutionsResponse) *workflowservice.ListArchivedWorkflowExecutionsResponse {
	if in == nil {
		return nil
	}
	return &workflowservice.ListArchivedWorkflowExecutionsResponse{
		Executions:    toProtoWorkflowExecutionInfos(in.GetExecutions()),
		NextPageToken: in.GetNextPageToken(),
	}
}

// ToThriftArchivedWorkflowExecutionsRequest ...
func ToThriftArchivedWorkflowExecutionsRequest(in *workflowservice.ListArchivedWorkflowExecutionsRequest) *shared.ListArchivedWorkflowExecutionsRequest {
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
		Execution: toThriftWorkflowExecution(in.Execution),
	}
}
