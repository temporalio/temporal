// The MIT License
//
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

// Code generated by protoc-gen-go. DO NOT EDIT.
// plugins:
// 	protoc-gen-go
// 	protoc
// source: temporal/server/api/matchingservice/v1/service.proto

package matchingservice

import (
	reflect "reflect"
	unsafe "unsafe"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_temporal_server_api_matchingservice_v1_service_proto protoreflect.FileDescriptor

const file_temporal_server_api_matchingservice_v1_service_proto_rawDesc = "" +
	"\n" +
	"4temporal/server/api/matchingservice/v1/service.proto\x12&temporal.server.api.matchingservice.v1\x1a=temporal/server/api/matchingservice/v1/request_response.proto2\xb4.\n" +
	"\x0fMatchingService\x12\xa6\x01\n" +
	"\x15PollWorkflowTaskQueue\x12D.temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueRequest\x1aE.temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueResponse\"\x00\x12\xa6\x01\n" +
	"\x15PollActivityTaskQueue\x12D.temporal.server.api.matchingservice.v1.PollActivityTaskQueueRequest\x1aE.temporal.server.api.matchingservice.v1.PollActivityTaskQueueResponse\"\x00\x12\x94\x01\n" +
	"\x0fAddWorkflowTask\x12>.temporal.server.api.matchingservice.v1.AddWorkflowTaskRequest\x1a?.temporal.server.api.matchingservice.v1.AddWorkflowTaskResponse\"\x00\x12\x94\x01\n" +
	"\x0fAddActivityTask\x12>.temporal.server.api.matchingservice.v1.AddActivityTaskRequest\x1a?.temporal.server.api.matchingservice.v1.AddActivityTaskResponse\"\x00\x12\x8e\x01\n" +
	"\rQueryWorkflow\x12<.temporal.server.api.matchingservice.v1.QueryWorkflowRequest\x1a=.temporal.server.api.matchingservice.v1.QueryWorkflowResponse\"\x00\x12\xb2\x01\n" +
	"\x19RespondQueryTaskCompleted\x12H.temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedRequest\x1aI.temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedResponse\"\x00\x12\x9a\x01\n" +
	"\x11DispatchNexusTask\x12@.temporal.server.api.matchingservice.v1.DispatchNexusTaskRequest\x1aA.temporal.server.api.matchingservice.v1.DispatchNexusTaskResponse\"\x00\x12\x9d\x01\n" +
	"\x12PollNexusTaskQueue\x12A.temporal.server.api.matchingservice.v1.PollNexusTaskQueueRequest\x1aB.temporal.server.api.matchingservice.v1.PollNexusTaskQueueResponse\"\x00\x12\xb2\x01\n" +
	"\x19RespondNexusTaskCompleted\x12H.temporal.server.api.matchingservice.v1.RespondNexusTaskCompletedRequest\x1aI.temporal.server.api.matchingservice.v1.RespondNexusTaskCompletedResponse\"\x00\x12\xa9\x01\n" +
	"\x16RespondNexusTaskFailed\x12E.temporal.server.api.matchingservice.v1.RespondNexusTaskFailedRequest\x1aF.temporal.server.api.matchingservice.v1.RespondNexusTaskFailedResponse\"\x00\x12\xa6\x01\n" +
	"\x15CancelOutstandingPoll\x12D.temporal.server.api.matchingservice.v1.CancelOutstandingPollRequest\x1aE.temporal.server.api.matchingservice.v1.CancelOutstandingPollResponse\"\x00\x12\x9a\x01\n" +
	"\x11DescribeTaskQueue\x12@.temporal.server.api.matchingservice.v1.DescribeTaskQueueRequest\x1aA.temporal.server.api.matchingservice.v1.DescribeTaskQueueResponse\"\x00\x12\xb5\x01\n" +
	"\x1aDescribeTaskQueuePartition\x12I.temporal.server.api.matchingservice.v1.DescribeTaskQueuePartitionRequest\x1aJ.temporal.server.api.matchingservice.v1.DescribeTaskQueuePartitionResponse\"\x00\x12\xac\x01\n" +
	"\x17ListTaskQueuePartitions\x12F.temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsRequest\x1aG.temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsResponse\"\x00\x12\xc7\x01\n" +
	" UpdateWorkerBuildIdCompatibility\x12O.temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityRequest\x1aP.temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityResponse\"\x00\x12\xbe\x01\n" +
	"\x1dGetWorkerBuildIdCompatibility\x12L.temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityRequest\x1aM.temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityResponse\"\x00\x12\xa3\x01\n" +
	"\x14GetTaskQueueUserData\x12C.temporal.server.api.matchingservice.v1.GetTaskQueueUserDataRequest\x1aD.temporal.server.api.matchingservice.v1.GetTaskQueueUserDataResponse\"\x00\x12\xb8\x01\n" +
	"\x1bUpdateWorkerVersioningRules\x12J.temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesRequest\x1aK.temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesResponse\"\x00\x12\xaf\x01\n" +
	"\x18GetWorkerVersioningRules\x12G.temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesRequest\x1aH.temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesResponse\"\x00\x12\xa9\x01\n" +
	"\x16SyncDeploymentUserData\x12E.temporal.server.api.matchingservice.v1.SyncDeploymentUserDataRequest\x1aF.temporal.server.api.matchingservice.v1.SyncDeploymentUserDataResponse\"\x00\x12\x9a\x01\n" +
	"\x11GetTaskQueueStats\x12@.temporal.server.api.matchingservice.v1.GetTaskQueueStatsRequest\x1aA.temporal.server.api.matchingservice.v1.GetTaskQueueStatsResponse\"\x00\x12\xb5\x01\n" +
	"\x1aGetTaskQueuePartitionStats\x12I.temporal.server.api.matchingservice.v1.GetTaskQueuePartitionStatsRequest\x1aJ.temporal.server.api.matchingservice.v1.GetTaskQueuePartitionStatsResponse\"\x00\x12\xd9\x01\n" +
	"&ApplyTaskQueueUserDataReplicationEvent\x12U.temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventRequest\x1aV.temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventResponse\"\x00\x12\xb5\x01\n" +
	"\x1aGetBuildIdTaskQueueMapping\x12I.temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingRequest\x1aJ.temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingResponse\"\x00\x12\xb8\x01\n" +
	"\x1bForceLoadTaskQueuePartition\x12J.temporal.server.api.matchingservice.v1.ForceLoadTaskQueuePartitionRequest\x1aK.temporal.server.api.matchingservice.v1.ForceLoadTaskQueuePartitionResponse\"\x00\x12\xa3\x01\n" +
	"\x14ForceUnloadTaskQueue\x12C.temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueRequest\x1aD.temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueResponse\"\x00\x12\xbe\x01\n" +
	"\x1dForceUnloadTaskQueuePartition\x12L.temporal.server.api.matchingservice.v1.ForceUnloadTaskQueuePartitionRequest\x1aM.temporal.server.api.matchingservice.v1.ForceUnloadTaskQueuePartitionResponse\"\x00\x12\xac\x01\n" +
	"\x17UpdateTaskQueueUserData\x12F.temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataRequest\x1aG.temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataResponse\"\x00\x12\xb5\x01\n" +
	"\x1aReplicateTaskQueueUserData\x12I.temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataRequest\x1aJ.temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataResponse\"\x00\x12\xca\x01\n" +
	"!CheckTaskQueueUserDataPropagation\x12P.temporal.server.api.matchingservice.v1.CheckTaskQueueUserDataPropagationRequest\x1aQ.temporal.server.api.matchingservice.v1.CheckTaskQueueUserDataPropagationResponse\"\x00\x12\xa0\x01\n" +
	"\x13CreateNexusEndpoint\x12B.temporal.server.api.matchingservice.v1.CreateNexusEndpointRequest\x1aC.temporal.server.api.matchingservice.v1.CreateNexusEndpointResponse\"\x00\x12\xa0\x01\n" +
	"\x13UpdateNexusEndpoint\x12B.temporal.server.api.matchingservice.v1.UpdateNexusEndpointRequest\x1aC.temporal.server.api.matchingservice.v1.UpdateNexusEndpointResponse\"\x00\x12\xa0\x01\n" +
	"\x13DeleteNexusEndpoint\x12B.temporal.server.api.matchingservice.v1.DeleteNexusEndpointRequest\x1aC.temporal.server.api.matchingservice.v1.DeleteNexusEndpointResponse\"\x00\x12\x9d\x01\n" +
	"\x12ListNexusEndpoints\x12A.temporal.server.api.matchingservice.v1.ListNexusEndpointsRequest\x1aB.temporal.server.api.matchingservice.v1.ListNexusEndpointsResponse\"\x00B>Z<go.temporal.io/server/api/matchingservice/v1;matchingserviceb\x06proto3"

var file_temporal_server_api_matchingservice_v1_service_proto_goTypes = []any{
	(*PollWorkflowTaskQueueRequest)(nil),                   // 0: temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueRequest
	(*PollActivityTaskQueueRequest)(nil),                   // 1: temporal.server.api.matchingservice.v1.PollActivityTaskQueueRequest
	(*AddWorkflowTaskRequest)(nil),                         // 2: temporal.server.api.matchingservice.v1.AddWorkflowTaskRequest
	(*AddActivityTaskRequest)(nil),                         // 3: temporal.server.api.matchingservice.v1.AddActivityTaskRequest
	(*QueryWorkflowRequest)(nil),                           // 4: temporal.server.api.matchingservice.v1.QueryWorkflowRequest
	(*RespondQueryTaskCompletedRequest)(nil),               // 5: temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedRequest
	(*DispatchNexusTaskRequest)(nil),                       // 6: temporal.server.api.matchingservice.v1.DispatchNexusTaskRequest
	(*PollNexusTaskQueueRequest)(nil),                      // 7: temporal.server.api.matchingservice.v1.PollNexusTaskQueueRequest
	(*RespondNexusTaskCompletedRequest)(nil),               // 8: temporal.server.api.matchingservice.v1.RespondNexusTaskCompletedRequest
	(*RespondNexusTaskFailedRequest)(nil),                  // 9: temporal.server.api.matchingservice.v1.RespondNexusTaskFailedRequest
	(*CancelOutstandingPollRequest)(nil),                   // 10: temporal.server.api.matchingservice.v1.CancelOutstandingPollRequest
	(*DescribeTaskQueueRequest)(nil),                       // 11: temporal.server.api.matchingservice.v1.DescribeTaskQueueRequest
	(*DescribeTaskQueuePartitionRequest)(nil),              // 12: temporal.server.api.matchingservice.v1.DescribeTaskQueuePartitionRequest
	(*ListTaskQueuePartitionsRequest)(nil),                 // 13: temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsRequest
	(*UpdateWorkerBuildIdCompatibilityRequest)(nil),        // 14: temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityRequest
	(*GetWorkerBuildIdCompatibilityRequest)(nil),           // 15: temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityRequest
	(*GetTaskQueueUserDataRequest)(nil),                    // 16: temporal.server.api.matchingservice.v1.GetTaskQueueUserDataRequest
	(*UpdateWorkerVersioningRulesRequest)(nil),             // 17: temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesRequest
	(*GetWorkerVersioningRulesRequest)(nil),                // 18: temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesRequest
	(*SyncDeploymentUserDataRequest)(nil),                  // 19: temporal.server.api.matchingservice.v1.SyncDeploymentUserDataRequest
	(*GetTaskQueueStatsRequest)(nil),                       // 20: temporal.server.api.matchingservice.v1.GetTaskQueueStatsRequest
	(*GetTaskQueuePartitionStatsRequest)(nil),              // 21: temporal.server.api.matchingservice.v1.GetTaskQueuePartitionStatsRequest
	(*ApplyTaskQueueUserDataReplicationEventRequest)(nil),  // 22: temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventRequest
	(*GetBuildIdTaskQueueMappingRequest)(nil),              // 23: temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingRequest
	(*ForceLoadTaskQueuePartitionRequest)(nil),             // 24: temporal.server.api.matchingservice.v1.ForceLoadTaskQueuePartitionRequest
	(*ForceUnloadTaskQueueRequest)(nil),                    // 25: temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueRequest
	(*ForceUnloadTaskQueuePartitionRequest)(nil),           // 26: temporal.server.api.matchingservice.v1.ForceUnloadTaskQueuePartitionRequest
	(*UpdateTaskQueueUserDataRequest)(nil),                 // 27: temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataRequest
	(*ReplicateTaskQueueUserDataRequest)(nil),              // 28: temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataRequest
	(*CheckTaskQueueUserDataPropagationRequest)(nil),       // 29: temporal.server.api.matchingservice.v1.CheckTaskQueueUserDataPropagationRequest
	(*CreateNexusEndpointRequest)(nil),                     // 30: temporal.server.api.matchingservice.v1.CreateNexusEndpointRequest
	(*UpdateNexusEndpointRequest)(nil),                     // 31: temporal.server.api.matchingservice.v1.UpdateNexusEndpointRequest
	(*DeleteNexusEndpointRequest)(nil),                     // 32: temporal.server.api.matchingservice.v1.DeleteNexusEndpointRequest
	(*ListNexusEndpointsRequest)(nil),                      // 33: temporal.server.api.matchingservice.v1.ListNexusEndpointsRequest
	(*PollWorkflowTaskQueueResponse)(nil),                  // 34: temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueResponse
	(*PollActivityTaskQueueResponse)(nil),                  // 35: temporal.server.api.matchingservice.v1.PollActivityTaskQueueResponse
	(*AddWorkflowTaskResponse)(nil),                        // 36: temporal.server.api.matchingservice.v1.AddWorkflowTaskResponse
	(*AddActivityTaskResponse)(nil),                        // 37: temporal.server.api.matchingservice.v1.AddActivityTaskResponse
	(*QueryWorkflowResponse)(nil),                          // 38: temporal.server.api.matchingservice.v1.QueryWorkflowResponse
	(*RespondQueryTaskCompletedResponse)(nil),              // 39: temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedResponse
	(*DispatchNexusTaskResponse)(nil),                      // 40: temporal.server.api.matchingservice.v1.DispatchNexusTaskResponse
	(*PollNexusTaskQueueResponse)(nil),                     // 41: temporal.server.api.matchingservice.v1.PollNexusTaskQueueResponse
	(*RespondNexusTaskCompletedResponse)(nil),              // 42: temporal.server.api.matchingservice.v1.RespondNexusTaskCompletedResponse
	(*RespondNexusTaskFailedResponse)(nil),                 // 43: temporal.server.api.matchingservice.v1.RespondNexusTaskFailedResponse
	(*CancelOutstandingPollResponse)(nil),                  // 44: temporal.server.api.matchingservice.v1.CancelOutstandingPollResponse
	(*DescribeTaskQueueResponse)(nil),                      // 45: temporal.server.api.matchingservice.v1.DescribeTaskQueueResponse
	(*DescribeTaskQueuePartitionResponse)(nil),             // 46: temporal.server.api.matchingservice.v1.DescribeTaskQueuePartitionResponse
	(*ListTaskQueuePartitionsResponse)(nil),                // 47: temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsResponse
	(*UpdateWorkerBuildIdCompatibilityResponse)(nil),       // 48: temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityResponse
	(*GetWorkerBuildIdCompatibilityResponse)(nil),          // 49: temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityResponse
	(*GetTaskQueueUserDataResponse)(nil),                   // 50: temporal.server.api.matchingservice.v1.GetTaskQueueUserDataResponse
	(*UpdateWorkerVersioningRulesResponse)(nil),            // 51: temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesResponse
	(*GetWorkerVersioningRulesResponse)(nil),               // 52: temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesResponse
	(*SyncDeploymentUserDataResponse)(nil),                 // 53: temporal.server.api.matchingservice.v1.SyncDeploymentUserDataResponse
	(*GetTaskQueueStatsResponse)(nil),                      // 54: temporal.server.api.matchingservice.v1.GetTaskQueueStatsResponse
	(*GetTaskQueuePartitionStatsResponse)(nil),             // 55: temporal.server.api.matchingservice.v1.GetTaskQueuePartitionStatsResponse
	(*ApplyTaskQueueUserDataReplicationEventResponse)(nil), // 56: temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventResponse
	(*GetBuildIdTaskQueueMappingResponse)(nil),             // 57: temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingResponse
	(*ForceLoadTaskQueuePartitionResponse)(nil),            // 58: temporal.server.api.matchingservice.v1.ForceLoadTaskQueuePartitionResponse
	(*ForceUnloadTaskQueueResponse)(nil),                   // 59: temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueResponse
	(*ForceUnloadTaskQueuePartitionResponse)(nil),          // 60: temporal.server.api.matchingservice.v1.ForceUnloadTaskQueuePartitionResponse
	(*UpdateTaskQueueUserDataResponse)(nil),                // 61: temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataResponse
	(*ReplicateTaskQueueUserDataResponse)(nil),             // 62: temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataResponse
	(*CheckTaskQueueUserDataPropagationResponse)(nil),      // 63: temporal.server.api.matchingservice.v1.CheckTaskQueueUserDataPropagationResponse
	(*CreateNexusEndpointResponse)(nil),                    // 64: temporal.server.api.matchingservice.v1.CreateNexusEndpointResponse
	(*UpdateNexusEndpointResponse)(nil),                    // 65: temporal.server.api.matchingservice.v1.UpdateNexusEndpointResponse
	(*DeleteNexusEndpointResponse)(nil),                    // 66: temporal.server.api.matchingservice.v1.DeleteNexusEndpointResponse
	(*ListNexusEndpointsResponse)(nil),                     // 67: temporal.server.api.matchingservice.v1.ListNexusEndpointsResponse
}
var file_temporal_server_api_matchingservice_v1_service_proto_depIdxs = []int32{
	0,  // 0: temporal.server.api.matchingservice.v1.MatchingService.PollWorkflowTaskQueue:input_type -> temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueRequest
	1,  // 1: temporal.server.api.matchingservice.v1.MatchingService.PollActivityTaskQueue:input_type -> temporal.server.api.matchingservice.v1.PollActivityTaskQueueRequest
	2,  // 2: temporal.server.api.matchingservice.v1.MatchingService.AddWorkflowTask:input_type -> temporal.server.api.matchingservice.v1.AddWorkflowTaskRequest
	3,  // 3: temporal.server.api.matchingservice.v1.MatchingService.AddActivityTask:input_type -> temporal.server.api.matchingservice.v1.AddActivityTaskRequest
	4,  // 4: temporal.server.api.matchingservice.v1.MatchingService.QueryWorkflow:input_type -> temporal.server.api.matchingservice.v1.QueryWorkflowRequest
	5,  // 5: temporal.server.api.matchingservice.v1.MatchingService.RespondQueryTaskCompleted:input_type -> temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedRequest
	6,  // 6: temporal.server.api.matchingservice.v1.MatchingService.DispatchNexusTask:input_type -> temporal.server.api.matchingservice.v1.DispatchNexusTaskRequest
	7,  // 7: temporal.server.api.matchingservice.v1.MatchingService.PollNexusTaskQueue:input_type -> temporal.server.api.matchingservice.v1.PollNexusTaskQueueRequest
	8,  // 8: temporal.server.api.matchingservice.v1.MatchingService.RespondNexusTaskCompleted:input_type -> temporal.server.api.matchingservice.v1.RespondNexusTaskCompletedRequest
	9,  // 9: temporal.server.api.matchingservice.v1.MatchingService.RespondNexusTaskFailed:input_type -> temporal.server.api.matchingservice.v1.RespondNexusTaskFailedRequest
	10, // 10: temporal.server.api.matchingservice.v1.MatchingService.CancelOutstandingPoll:input_type -> temporal.server.api.matchingservice.v1.CancelOutstandingPollRequest
	11, // 11: temporal.server.api.matchingservice.v1.MatchingService.DescribeTaskQueue:input_type -> temporal.server.api.matchingservice.v1.DescribeTaskQueueRequest
	12, // 12: temporal.server.api.matchingservice.v1.MatchingService.DescribeTaskQueuePartition:input_type -> temporal.server.api.matchingservice.v1.DescribeTaskQueuePartitionRequest
	13, // 13: temporal.server.api.matchingservice.v1.MatchingService.ListTaskQueuePartitions:input_type -> temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsRequest
	14, // 14: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerBuildIdCompatibility:input_type -> temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityRequest
	15, // 15: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerBuildIdCompatibility:input_type -> temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityRequest
	16, // 16: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueueUserData:input_type -> temporal.server.api.matchingservice.v1.GetTaskQueueUserDataRequest
	17, // 17: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerVersioningRules:input_type -> temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesRequest
	18, // 18: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerVersioningRules:input_type -> temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesRequest
	19, // 19: temporal.server.api.matchingservice.v1.MatchingService.SyncDeploymentUserData:input_type -> temporal.server.api.matchingservice.v1.SyncDeploymentUserDataRequest
	20, // 20: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueueStats:input_type -> temporal.server.api.matchingservice.v1.GetTaskQueueStatsRequest
	21, // 21: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueuePartitionStats:input_type -> temporal.server.api.matchingservice.v1.GetTaskQueuePartitionStatsRequest
	22, // 22: temporal.server.api.matchingservice.v1.MatchingService.ApplyTaskQueueUserDataReplicationEvent:input_type -> temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventRequest
	23, // 23: temporal.server.api.matchingservice.v1.MatchingService.GetBuildIdTaskQueueMapping:input_type -> temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingRequest
	24, // 24: temporal.server.api.matchingservice.v1.MatchingService.ForceLoadTaskQueuePartition:input_type -> temporal.server.api.matchingservice.v1.ForceLoadTaskQueuePartitionRequest
	25, // 25: temporal.server.api.matchingservice.v1.MatchingService.ForceUnloadTaskQueue:input_type -> temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueRequest
	26, // 26: temporal.server.api.matchingservice.v1.MatchingService.ForceUnloadTaskQueuePartition:input_type -> temporal.server.api.matchingservice.v1.ForceUnloadTaskQueuePartitionRequest
	27, // 27: temporal.server.api.matchingservice.v1.MatchingService.UpdateTaskQueueUserData:input_type -> temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataRequest
	28, // 28: temporal.server.api.matchingservice.v1.MatchingService.ReplicateTaskQueueUserData:input_type -> temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataRequest
	29, // 29: temporal.server.api.matchingservice.v1.MatchingService.CheckTaskQueueUserDataPropagation:input_type -> temporal.server.api.matchingservice.v1.CheckTaskQueueUserDataPropagationRequest
	30, // 30: temporal.server.api.matchingservice.v1.MatchingService.CreateNexusEndpoint:input_type -> temporal.server.api.matchingservice.v1.CreateNexusEndpointRequest
	31, // 31: temporal.server.api.matchingservice.v1.MatchingService.UpdateNexusEndpoint:input_type -> temporal.server.api.matchingservice.v1.UpdateNexusEndpointRequest
	32, // 32: temporal.server.api.matchingservice.v1.MatchingService.DeleteNexusEndpoint:input_type -> temporal.server.api.matchingservice.v1.DeleteNexusEndpointRequest
	33, // 33: temporal.server.api.matchingservice.v1.MatchingService.ListNexusEndpoints:input_type -> temporal.server.api.matchingservice.v1.ListNexusEndpointsRequest
	34, // 34: temporal.server.api.matchingservice.v1.MatchingService.PollWorkflowTaskQueue:output_type -> temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueResponse
	35, // 35: temporal.server.api.matchingservice.v1.MatchingService.PollActivityTaskQueue:output_type -> temporal.server.api.matchingservice.v1.PollActivityTaskQueueResponse
	36, // 36: temporal.server.api.matchingservice.v1.MatchingService.AddWorkflowTask:output_type -> temporal.server.api.matchingservice.v1.AddWorkflowTaskResponse
	37, // 37: temporal.server.api.matchingservice.v1.MatchingService.AddActivityTask:output_type -> temporal.server.api.matchingservice.v1.AddActivityTaskResponse
	38, // 38: temporal.server.api.matchingservice.v1.MatchingService.QueryWorkflow:output_type -> temporal.server.api.matchingservice.v1.QueryWorkflowResponse
	39, // 39: temporal.server.api.matchingservice.v1.MatchingService.RespondQueryTaskCompleted:output_type -> temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedResponse
	40, // 40: temporal.server.api.matchingservice.v1.MatchingService.DispatchNexusTask:output_type -> temporal.server.api.matchingservice.v1.DispatchNexusTaskResponse
	41, // 41: temporal.server.api.matchingservice.v1.MatchingService.PollNexusTaskQueue:output_type -> temporal.server.api.matchingservice.v1.PollNexusTaskQueueResponse
	42, // 42: temporal.server.api.matchingservice.v1.MatchingService.RespondNexusTaskCompleted:output_type -> temporal.server.api.matchingservice.v1.RespondNexusTaskCompletedResponse
	43, // 43: temporal.server.api.matchingservice.v1.MatchingService.RespondNexusTaskFailed:output_type -> temporal.server.api.matchingservice.v1.RespondNexusTaskFailedResponse
	44, // 44: temporal.server.api.matchingservice.v1.MatchingService.CancelOutstandingPoll:output_type -> temporal.server.api.matchingservice.v1.CancelOutstandingPollResponse
	45, // 45: temporal.server.api.matchingservice.v1.MatchingService.DescribeTaskQueue:output_type -> temporal.server.api.matchingservice.v1.DescribeTaskQueueResponse
	46, // 46: temporal.server.api.matchingservice.v1.MatchingService.DescribeTaskQueuePartition:output_type -> temporal.server.api.matchingservice.v1.DescribeTaskQueuePartitionResponse
	47, // 47: temporal.server.api.matchingservice.v1.MatchingService.ListTaskQueuePartitions:output_type -> temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsResponse
	48, // 48: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerBuildIdCompatibility:output_type -> temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityResponse
	49, // 49: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerBuildIdCompatibility:output_type -> temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityResponse
	50, // 50: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueueUserData:output_type -> temporal.server.api.matchingservice.v1.GetTaskQueueUserDataResponse
	51, // 51: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerVersioningRules:output_type -> temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesResponse
	52, // 52: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerVersioningRules:output_type -> temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesResponse
	53, // 53: temporal.server.api.matchingservice.v1.MatchingService.SyncDeploymentUserData:output_type -> temporal.server.api.matchingservice.v1.SyncDeploymentUserDataResponse
	54, // 54: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueueStats:output_type -> temporal.server.api.matchingservice.v1.GetTaskQueueStatsResponse
	55, // 55: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueuePartitionStats:output_type -> temporal.server.api.matchingservice.v1.GetTaskQueuePartitionStatsResponse
	56, // 56: temporal.server.api.matchingservice.v1.MatchingService.ApplyTaskQueueUserDataReplicationEvent:output_type -> temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventResponse
	57, // 57: temporal.server.api.matchingservice.v1.MatchingService.GetBuildIdTaskQueueMapping:output_type -> temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingResponse
	58, // 58: temporal.server.api.matchingservice.v1.MatchingService.ForceLoadTaskQueuePartition:output_type -> temporal.server.api.matchingservice.v1.ForceLoadTaskQueuePartitionResponse
	59, // 59: temporal.server.api.matchingservice.v1.MatchingService.ForceUnloadTaskQueue:output_type -> temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueResponse
	60, // 60: temporal.server.api.matchingservice.v1.MatchingService.ForceUnloadTaskQueuePartition:output_type -> temporal.server.api.matchingservice.v1.ForceUnloadTaskQueuePartitionResponse
	61, // 61: temporal.server.api.matchingservice.v1.MatchingService.UpdateTaskQueueUserData:output_type -> temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataResponse
	62, // 62: temporal.server.api.matchingservice.v1.MatchingService.ReplicateTaskQueueUserData:output_type -> temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataResponse
	63, // 63: temporal.server.api.matchingservice.v1.MatchingService.CheckTaskQueueUserDataPropagation:output_type -> temporal.server.api.matchingservice.v1.CheckTaskQueueUserDataPropagationResponse
	64, // 64: temporal.server.api.matchingservice.v1.MatchingService.CreateNexusEndpoint:output_type -> temporal.server.api.matchingservice.v1.CreateNexusEndpointResponse
	65, // 65: temporal.server.api.matchingservice.v1.MatchingService.UpdateNexusEndpoint:output_type -> temporal.server.api.matchingservice.v1.UpdateNexusEndpointResponse
	66, // 66: temporal.server.api.matchingservice.v1.MatchingService.DeleteNexusEndpoint:output_type -> temporal.server.api.matchingservice.v1.DeleteNexusEndpointResponse
	67, // 67: temporal.server.api.matchingservice.v1.MatchingService.ListNexusEndpoints:output_type -> temporal.server.api.matchingservice.v1.ListNexusEndpointsResponse
	34, // [34:68] is the sub-list for method output_type
	0,  // [0:34] is the sub-list for method input_type
	0,  // [0:0] is the sub-list for extension type_name
	0,  // [0:0] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_matchingservice_v1_service_proto_init() }
func file_temporal_server_api_matchingservice_v1_service_proto_init() {
	if File_temporal_server_api_matchingservice_v1_service_proto != nil {
		return
	}
	file_temporal_server_api_matchingservice_v1_request_response_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_matchingservice_v1_service_proto_rawDesc), len(file_temporal_server_api_matchingservice_v1_service_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_temporal_server_api_matchingservice_v1_service_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_matchingservice_v1_service_proto_depIdxs,
	}.Build()
	File_temporal_server_api_matchingservice_v1_service_proto = out.File
	file_temporal_server_api_matchingservice_v1_service_proto_goTypes = nil
	file_temporal_server_api_matchingservice_v1_service_proto_depIdxs = nil
}
