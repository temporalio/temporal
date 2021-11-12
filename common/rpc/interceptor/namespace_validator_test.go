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

package interceptor

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

func TestNamespaceValidator_Intercept_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespace(namespace.Name("not-found-namespace")).Return(nil, serviceerror.NewNotFound("not-found"))

	nvi := NewNamespaceValidatorInterceptor(registry)
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/StartWorkflowExecution",
	}

	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "not-found-namespace"}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.StartWorkflowExecutionResponse{}, nil
	})

	assert.IsType(t, &serviceerror.NotFound{}, err)
	assert.False(t, handlerCalled)
}

func TestNamespaceValidator_Intercept_Status(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := namespace.NewMockRegistry(ctrl)
	testCases := []struct {
		state       enumspb.NamespaceState
		expectedErr error
		method      string
		req         interface{}
	}{
		// StartWorkflowExecution
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      "/temporal/StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: &serviceerror.InvalidArgument{},
			method:      "/temporal/StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.InvalidArgument{},
			method:      "/temporal/StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		// PollWorkflowTaskQueue
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      "/temporal/PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      "/temporal/PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.InvalidArgument{},
			method:      "/temporal/PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		// DescribeNamespace
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      "/temporal/DescribeNamespace",
			req:         &workflowservice.DescribeNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      "/temporal/DescribeNamespace",
			req:         &workflowservice.DescribeNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: nil,
			method:      "/temporal/DescribeNamespace",
			req:         &workflowservice.DescribeNamespaceRequest{Namespace: "test-namespace"},
		},
	}

	for _, testCase := range testCases {
		registry.EXPECT().GetNamespace(namespace.Name("test-namespace")).Return(namespace.FromPersistentState(
			&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Config:            &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					Info: &persistencespb.NamespaceInfo{
						State: testCase.state,
					},
				},
			}), nil)

		nvi := NewNamespaceValidatorInterceptor(registry)
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: testCase.method,
		}

		handlerCalled := false
		_, err := nvi.Intercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.StartWorkflowExecutionResponse{}, nil
		})

		if testCase.expectedErr != nil {
			assert.IsType(t, testCase.expectedErr, err)
			assert.False(t, handlerCalled)
		} else {
			assert.NoError(t, err)
			assert.True(t, handlerCalled)
		}
	}
}

func TestNamespaceValidator_Intercept_DescribeNamespace_Id(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace")).Return(namespace.FromPersistentState(
		&persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
				Info: &persistencespb.NamespaceInfo{
					State: enumspb.NAMESPACE_STATE_REGISTERED,
				},
			},
		}), nil)

	nvi := NewNamespaceValidatorInterceptor(registry)
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/StartWorkflowExecution",
	}

	req := &workflowservice.DescribeNamespaceRequest{Id: "test-namespace"}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.DescribeNamespaceResponse{}, nil
	})

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
}

func TestNamespaceValidator_Intercept_GetClusterInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := namespace.NewMockRegistry(ctrl)

	nvi := NewNamespaceValidatorInterceptor(registry)
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/GetClusterInfo",
	}

	req := &workflowservice.GetClusterInfoRequest{}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.GetClusterInfoResponse{}, nil
	})

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
}

func TestNamespaceValidator_Intercept_RegisterNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := namespace.NewMockRegistry(ctrl)

	nvi := NewNamespaceValidatorInterceptor(registry)
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/RegisterNamespace",
	}

	req := &workflowservice.RegisterNamespaceRequest{Namespace: "new-namespace"}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	})

	assert.NoError(t, err)
	assert.True(t, handlerCalled)

	req = &workflowservice.RegisterNamespaceRequest{}
	handlerCalled = false
	_, err = nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	})

	assert.IsType(t, &serviceerror.InvalidArgument{}, err)
	assert.False(t, handlerCalled)
}
