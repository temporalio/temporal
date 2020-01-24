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

	"github.com/temporalio/temporal/.gen/go/admin"
	"github.com/temporalio/temporal/.gen/proto/adminservice"

	"github.com/temporalio/temporal/.gen/go/shared"
)

// ToProtoAdminDescribeWorkflowExecutionResponse ...
func ToProtoAdminDescribeWorkflowExecutionResponse(in *admin.DescribeWorkflowExecutionResponse) *adminservice.DescribeWorkflowExecutionResponse {
	if in == nil {
		return nil
	}
	return &adminservice.DescribeWorkflowExecutionResponse{
		ShardId:                in.GetShardId(),
		HistoryAddr:            in.GetHistoryAddr(),
		MutableStateInCache:    in.GetMutableStateInCache(),
		MutableStateInDatabase: in.GetMutableStateInDatabase(),
	}
}

// ToProtoDescribeHistoryHostResponse ...
func ToProtoDescribeHistoryHostResponse(in *shared.DescribeHistoryHostResponse) *adminservice.DescribeHistoryHostResponse {
	if in == nil {
		return nil
	}
	return &adminservice.DescribeHistoryHostResponse{
		NumberOfShards:        in.GetNumberOfShards(),
		ShardIDs:              in.GetShardIDs(),
		DomainCache:           ToProtoDomainCacheInfo(in.GetDomainCache()),
		ShardControllerStatus: in.GetShardControllerStatus(),
		Address:               in.GetAddress(),
	}
}

// ToProtoGetWorkflowExecutionRawHistoryResponse ...
func ToProtoGetWorkflowExecutionRawHistoryResponse(in *admin.GetWorkflowExecutionRawHistoryResponse) *adminservice.GetWorkflowExecutionRawHistoryResponse {
	if in == nil {
		return nil
	}
	return &adminservice.GetWorkflowExecutionRawHistoryResponse{
		NextPageToken:     in.GetNextPageToken(),
		HistoryBatches:    ToProtoDataBlobs(in.GetHistoryBatches()),
		ReplicationInfo:   toProtoReplicationInfos(in.GetReplicationInfo()),
		EventStoreVersion: in.GetEventStoreVersion(),
	}
}

// ToProtoGetWorkflowExecutionRawHistoryV2Response ...
func ToProtoGetWorkflowExecutionRawHistoryV2Response(in *admin.GetWorkflowExecutionRawHistoryV2Response) *adminservice.GetWorkflowExecutionRawHistoryV2Response {
	if in == nil {
		return nil
	}
	return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		NextPageToken:  in.GetNextPageToken(),
		HistoryBatches: ToProtoDataBlobs(in.GetHistoryBatches()),
		VersionHistory: toProtoVersionHistory(in.GetVersionHistory()),
	}
}

// ToProtoDescribeClusterResponse ...
func ToProtoDescribeClusterResponse(in *admin.DescribeClusterResponse) *adminservice.DescribeClusterResponse {
	if in == nil {
		return nil
	}
	return &adminservice.DescribeClusterResponse{
		SupportedClientVersions: toProtoSupportedClientVersions(in.GetSupportedClientVersions()),
		MembershipInfo:          ToProtoMembershipInfo(in.GetMembershipInfo()),
	}
}

// ToThriftDescribeWorkflowExecutionRequest ...
func ToThriftAdminDescribeWorkflowExecutionRequest(in *adminservice.DescribeWorkflowExecutionRequest) *admin.DescribeWorkflowExecutionRequest {
	if in == nil {
		return nil
	}
	return &admin.DescribeWorkflowExecutionRequest{
		Domain:    &in.Domain,
		Execution: ToThriftWorkflowExecution(in.Execution),
	}
}

// ToThriftDescribeHistoryHostRequest ...
func ToThriftDescribeHistoryHostRequest(in *adminservice.DescribeHistoryHostRequest) *shared.DescribeHistoryHostRequest {
	if in == nil {
		return nil
	}
	return &shared.DescribeHistoryHostRequest{
		HostAddress:      &in.HostAddress,
		ShardIdForHost:   &in.ShardIdForHost,
		ExecutionForHost: ToThriftWorkflowExecution(in.ExecutionForHost),
	}
}

// ToThriftCloseShardRequest ...
func ToThriftCloseShardRequest(in *adminservice.CloseShardRequest) *shared.CloseShardRequest {
	if in == nil {
		return nil
	}
	return &shared.CloseShardRequest{
		ShardID: &in.ShardID,
	}
}

// ToThriftRemoveTaskRequest ...
func ToThriftRemoveTaskRequest(in *adminservice.RemoveTaskRequest) *shared.RemoveTaskRequest {
	if in == nil {
		return nil
	}
	return &shared.RemoveTaskRequest{
		ShardID: &in.ShardID,
		Type:    &in.Type,
		TaskID:  &in.TaskID,
	}
}

// ToThriftGetWorkflowExecutionRawHistoryRequest ...
func ToThriftGetWorkflowExecutionRawHistoryRequest(in *adminservice.GetWorkflowExecutionRawHistoryRequest) *admin.GetWorkflowExecutionRawHistoryRequest {
	if in == nil {
		return nil
	}
	return &admin.GetWorkflowExecutionRawHistoryRequest{
		Domain:          &in.Domain,
		Execution:       ToThriftWorkflowExecution(in.Execution),
		FirstEventId:    &in.FirstEventId,
		NextEventId:     &in.NextEventId,
		MaximumPageSize: &in.MaximumPageSize,
		NextPageToken:   in.NextPageToken,
	}
}

// ToThriftGetWorkflowExecutionRawHistoryV2Request ...
func ToThriftGetWorkflowExecutionRawHistoryV2Request(in *adminservice.GetWorkflowExecutionRawHistoryV2Request) *admin.GetWorkflowExecutionRawHistoryV2Request {
	if in == nil {
		return nil
	}
	return &admin.GetWorkflowExecutionRawHistoryV2Request{
		Domain:            &in.Domain,
		Execution:         ToThriftWorkflowExecution(in.Execution),
		StartEventId:      &in.StartEventId,
		StartEventVersion: &in.StartEventVersion,
		EndEventId:        &in.EndEventId,
		EndEventVersion:   &in.EndEventVersion,
		MaximumPageSize:   &in.MaximumPageSize,
		NextPageToken:     in.NextPageToken,
	}
}

// ToThriftAddSearchAttributeRequest ...
func ToThriftAddSearchAttributeRequest(in *adminservice.AddSearchAttributeRequest) *admin.AddSearchAttributeRequest {
	if in == nil {
		return nil
	}
	return &admin.AddSearchAttributeRequest{
		SearchAttribute: toThriftIndexedValueTypes(in.SearchAttribute),
		SecurityToken:   &in.SecurityToken,
	}
}

// ToProtoMembershipInfo ...
func ToProtoMembershipInfo(in *admin.MembershipInfo) *common.MembershipInfo {
	if in == nil {
		return nil
	}
	return &common.MembershipInfo{
		CurrentHost:      ToProtoHostInfo(in.GetCurrentHost()),
		ReachableMembers: in.GetReachableMembers(),
		Rings:            ToProtoRingInfos(in.GetRings()),
	}
}

// ToProtoHostInfo ...
func ToProtoHostInfo(in *admin.HostInfo) *common.HostInfo {
	if in == nil {
		return nil
	}
	return &common.HostInfo{
		Identity: in.GetIdentity(),
	}
}

// ToProtoRingInfo ...
func ToProtoRingInfo(in *admin.RingInfo) *common.RingInfo {
	if in == nil {
		return nil
	}
	return &common.RingInfo{
		Role:        in.GetRole(),
		MemberCount: in.GetMemberCount(),
		Members:     ToProtoHostInfos(in.GetMembers()),
	}
}

// ToProtoRingInfos ...
func ToProtoRingInfos(in []*admin.RingInfo) []*common.RingInfo {
	if in == nil {
		return nil
	}

	var ret []*common.RingInfo
	for _, item := range in {
		ret = append(ret, ToProtoRingInfo(item))
	}
	return ret
}

// ToProtoHostInfos ...
func ToProtoHostInfos(in []*admin.HostInfo) []*common.HostInfo {
	if in == nil {
		return nil
	}

	var ret []*common.HostInfo
	for _, item := range in {
		ret = append(ret, ToProtoHostInfo(item))
	}
	return ret
}
