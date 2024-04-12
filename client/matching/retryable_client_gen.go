// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.

package matching

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/backoff"
)

func (c *retryableClient) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.AddActivityTaskResponse, error) {
	var resp *matchingservice.AddActivityTaskResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.AddActivityTask(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.AddWorkflowTaskResponse, error) {
	var resp *matchingservice.AddWorkflowTaskResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.AddWorkflowTask(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	request *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, error) {
	var resp *matchingservice.ApplyTaskQueueUserDataReplicationEventResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ApplyTaskQueueUserDataReplicationEvent(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CancelOutstandingPoll(
	ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
	opts ...grpc.CallOption,
) (*matchingservice.CancelOutstandingPollResponse, error) {
	var resp *matchingservice.CancelOutstandingPollResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.CancelOutstandingPoll(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CreateNexusIncomingService(
	ctx context.Context,
	request *matchingservice.CreateNexusIncomingServiceRequest,
	opts ...grpc.CallOption,
) (*matchingservice.CreateNexusIncomingServiceResponse, error) {
	var resp *matchingservice.CreateNexusIncomingServiceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.CreateNexusIncomingService(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DeleteNexusIncomingService(
	ctx context.Context,
	request *matchingservice.DeleteNexusIncomingServiceRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DeleteNexusIncomingServiceResponse, error) {
	var resp *matchingservice.DeleteNexusIncomingServiceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DeleteNexusIncomingService(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DescribeTaskQueueResponse, error) {
	var resp *matchingservice.DescribeTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	var resp *matchingservice.DescribeTaskQueuePartitionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeTaskQueuePartition(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DispatchNexusTask(
	ctx context.Context,
	request *matchingservice.DispatchNexusTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	var resp *matchingservice.DispatchNexusTaskResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DispatchNexusTask(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ForceUnloadTaskQueue(
	ctx context.Context,
	request *matchingservice.ForceUnloadTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ForceUnloadTaskQueueResponse, error) {
	var resp *matchingservice.ForceUnloadTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ForceUnloadTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetBuildIdTaskQueueMapping(
	ctx context.Context,
	request *matchingservice.GetBuildIdTaskQueueMappingRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetBuildIdTaskQueueMappingResponse, error) {
	var resp *matchingservice.GetBuildIdTaskQueueMappingResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetBuildIdTaskQueueMapping(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.GetTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetTaskQueueUserDataResponse, error) {
	var resp *matchingservice.GetTaskQueueUserDataResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetTaskQueueUserData(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.GetWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {
	var resp *matchingservice.GetWorkerBuildIdCompatibilityResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.GetWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetWorkerVersioningRulesResponse, error) {
	var resp *matchingservice.GetWorkerVersioningRulesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkerVersioningRules(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListNexusIncomingServices(
	ctx context.Context,
	request *matchingservice.ListNexusIncomingServicesRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ListNexusIncomingServicesResponse, error) {
	var resp *matchingservice.ListNexusIncomingServicesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListNexusIncomingServices(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ListTaskQueuePartitionsResponse, error) {
	var resp *matchingservice.ListTaskQueuePartitionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListTaskQueuePartitions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollActivityTaskQueueResponse, error) {
	var resp *matchingservice.PollActivityTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PollActivityTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollNexusTaskQueue(
	ctx context.Context,
	request *matchingservice.PollNexusTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	var resp *matchingservice.PollNexusTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PollNexusTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	var resp *matchingservice.PollWorkflowTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PollWorkflowTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*matchingservice.QueryWorkflowResponse, error) {
	var resp *matchingservice.QueryWorkflowResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.QueryWorkflow(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReplicateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.ReplicateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {
	var resp *matchingservice.ReplicateTaskQueueUserDataResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ReplicateTaskQueueUserData(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondNexusTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondNexusTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondNexusTaskCompletedResponse, error) {
	var resp *matchingservice.RespondNexusTaskCompletedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondNexusTaskCompleted(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondNexusTaskFailed(
	ctx context.Context,
	request *matchingservice.RespondNexusTaskFailedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondNexusTaskFailedResponse, error) {
	var resp *matchingservice.RespondNexusTaskFailedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondNexusTaskFailed(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondQueryTaskCompletedResponse, error) {
	var resp *matchingservice.RespondQueryTaskCompletedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondQueryTaskCompleted(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateNexusIncomingService(
	ctx context.Context,
	request *matchingservice.UpdateNexusIncomingServiceRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateNexusIncomingServiceResponse, error) {
	var resp *matchingservice.UpdateNexusIncomingServiceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateNexusIncomingService(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.UpdateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
	var resp *matchingservice.UpdateTaskQueueUserDataResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateTaskQueueUserData(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	var resp *matchingservice.UpdateWorkerBuildIdCompatibilityResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.UpdateWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateWorkerVersioningRulesResponse, error) {
	var resp *matchingservice.UpdateWorkerVersioningRulesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateWorkerVersioningRules(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}
