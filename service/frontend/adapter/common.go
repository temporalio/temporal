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
	"github.com/temporalio/temporal/.gen/go/shared"
)

func toProtoDomainInfo(in *shared.DomainInfo) *common.DomainInfo {
	return &common.DomainInfo{
		Name:        *in.Name,
		Status:      toProtoDomainStatus(*in.Status),
		Description: *in.Description,
		OwnerEmail:  *in.OwnerEmail,
		Data:        in.Data,
		Uuid:        *in.UUID,
	}
}

func toProtoDomainReplicationConfiguration(in *shared.DomainReplicationConfiguration) *common.DomainReplicationConfiguration {
	return &common.DomainReplicationConfiguration{
		ActiveClusterName: *in.ActiveClusterName,
		Clusters:          toProtoClusterReplicationConfigurations(in.Clusters),
	}
}

func toProtoDomainConfiguration(in *shared.DomainConfiguration) *common.DomainConfiguration {
	return &common.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: *in.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             *in.EmitMetric,
		BadBinaries:                            toProtoBadBinaries(in.BadBinaries),
		HistoryArchivalStatus:                  toProtoArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     *in.VisibilityArchivalURI,
		VisibilityArchivalStatus:               toProtoArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  *in.VisibilityArchivalURI,
	}
}

func toProtoBadBinaries(in *shared.BadBinaries) *common.BadBinaries {
	ret := make(map[string]*common.BadBinaryInfo, len(in.Binaries))

	for key, value := range in.Binaries {
		ret[key] = toProtoBadBinaryInfo(value)
	}

	return &common.BadBinaries{
		Binaries: ret,
	}
}

func toProtoBadBinaryInfo(in *shared.BadBinaryInfo) *common.BadBinaryInfo {
	return &common.BadBinaryInfo{
		Reason:          *in.Reason,
		Operator:        *in.Operator,
		CreatedTimeNano: *in.CreatedTimeNano,
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
