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
func ToThriftRegisterDomainRequest(request *workflowservice.RegisterDomainRequest) *shared.RegisterDomainRequest {
	return &shared.RegisterDomainRequest{
		Name:                                   &request.Name,
		Description:                            &request.Description,
		OwnerEmail:                             &request.OwnerEmail,
		WorkflowExecutionRetentionPeriodInDays: &request.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             &request.EmitMetric,
		Clusters:                               toThriftClusterReplicationConfigurations(request.Clusters),
		ActiveClusterName:                      &request.ActiveClusterName,
		Data:                                   request.Data,
		SecurityToken:                          &request.SecurityToken,
		IsGlobalDomain:                         &request.IsGlobalDomain,
		HistoryArchivalStatus:                  toThriftArchivalStatus(request.HistoryArchivalStatus),
		HistoryArchivalURI:                     &request.HistoryArchivalURI,
		VisibilityArchivalStatus:               toThriftArchivalStatus(request.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  &request.VisibilityArchivalURI,
	}
}

// ToThriftDescribeDomainRequest ...
func ToThriftDescribeDomainRequest(in *workflowservice.DescribeDomainRequest) *shared.DescribeDomainRequest {
	return &shared.DescribeDomainRequest{
		Name: &in.Name,
		UUID: &in.Uuid,
	}
}

// ToProtoDescribeDomainResponse ...
func ToProtoDescribeDomainResponse(in *shared.DescribeDomainResponse) *workflowservice.DescribeDomainResponse {
	return &workflowservice.DescribeDomainResponse{
		DomainInfo:               toProtoDomainInfo(in.DomainInfo),
		Configuration:            toProtoDomainConfiguration(in.Configuration),
		ReplicationConfiguration: toProtoDomainReplicationConfiguration(in.ReplicationConfiguration),
		FailoverVersion:          *in.FailoverVersion,
		IsGlobalDomain:           *in.IsGlobalDomain,
	}
}

// ToThriftListDomainRequest ...
func ToThriftListDomainRequest(in *workflowservice.ListDomainsRequest) *shared.ListDomainsRequest {
	return &shared.ListDomainsRequest{
		PageSize:      &in.PageSize,
		NextPageToken: in.NextPageToken,
	}
}

// ToProtoListDomainResponse ...
func ToProtoListDomainResponse(in *shared.ListDomainsResponse) *workflowservice.ListDomainsResponse {
	var ret []*workflowservice.DescribeDomainResponse
	for _, domain := range in.Domains {
		ret = append(ret, ToProtoDescribeDomainResponse(domain))
	}

	return &workflowservice.ListDomainsResponse{
		Domains:       ret,
		NextPageToken: in.NextPageToken,
	}
}

func ToThriftUpdateDomainRequest(in *workflowservice.UpdateDomainRequest) *shared.UpdateDomainRequest {
	return &shared.UpdateDomainRequest{
		Name:                     &in.Name,
		UpdatedInfo:              toThriftUpdateDomainInfo(in.UpdatedInfo),
		Configuration:            toThriftDomainConfiguration(in.Configuration),
		ReplicationConfiguration: toThriftDomainReplicationConfiguration(in.ReplicationConfiguration),
		SecurityToken:            &in.SecurityToken,
		DeleteBadBinary:          &in.DeleteBadBinary,
	}
}

func ToProtoUpdateDomainResponse(in *shared.UpdateDomainResponse) *workflowservice.UpdateDomainResponse {
	return &workflowservice.UpdateDomainResponse{
		DomainInfo:               toProtoDomainInfo(in.DomainInfo),
		Configuration:            toProtoDomainConfiguration(in.Configuration),
		ReplicationConfiguration: toProtoDomainReplicationConfiguration(in.ReplicationConfiguration),
		FailoverVersion:          *in.FailoverVersion,
		IsGlobalDomain:           *in.IsGlobalDomain,
	}
}

func ToThriftDeprecateDomainRequest(in *workflowservice.DeprecateDomainRequest) *shared.DeprecateDomainRequest {
	return &shared.DeprecateDomainRequest{
		Name:          &in.Name,
		SecurityToken: &in.SecurityToken,
	}
}
