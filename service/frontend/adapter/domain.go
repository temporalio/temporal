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
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/tpb"
)

// RegisterDomainRequest converts gRPC to Thrift
func RegisterDomainRequest(registerRequest *tpb.RegisterDomainRequest) *shared.RegisterDomainRequest {
	var clusters []*shared.ClusterReplicationConfiguration
	for _, cluster := range registerRequest.Clusters {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{ClusterName: &cluster.ClusterName})
	}

	return &shared.RegisterDomainRequest{
		Name:                                   &registerRequest.Name,
		Description:                            &registerRequest.Description,
		OwnerEmail:                             &registerRequest.OwnerEmail,
		WorkflowExecutionRetentionPeriodInDays: &registerRequest.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             &registerRequest.EmitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      &registerRequest.ActiveClusterName,
		Data:                                   registerRequest.Data,
		SecurityToken:                          &registerRequest.SecurityToken,
		IsGlobalDomain:                         &registerRequest.IsGlobalDomain,
		HistoryArchivalStatus:                  archivalStatus(registerRequest.HistoryArchivalStatus),
		HistoryArchivalURI:                     &registerRequest.HistoryArchivalURI,
		VisibilityArchivalStatus:               archivalStatus(registerRequest.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  &registerRequest.VisibilityArchivalURI,
	}
}
