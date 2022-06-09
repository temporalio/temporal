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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

type (
	namespaceValidatorSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		mockRegistry *namespace.MockRegistry
	}
)

func TestNamespaceValidatorSuite(t *testing.T) {
	suite.Run(t, &namespaceValidatorSuite{})
}

func (s *namespaceValidatorSuite) SetupSuite() {
}

func (s *namespaceValidatorSuite) TearDownSuite() {
}

func (s *namespaceValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockRegistry = namespace.NewMockRegistry(s.controller)
}

func (s *namespaceValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *namespaceValidatorSuite) Test_Intercept_NamespaceNotSet() {
	taskToken, _ := common.NewProtoTaskTokenSerializer().Serialize(&tokenspb.Task{
		NamespaceId: "",
		WorkflowId:  "wid",
	})

	nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	testCases := []struct {
		expectedErr error
		req         interface{}
	}{
		{
			req:         &workflowservice.StartWorkflowExecutionRequest{},
			expectedErr: &serviceerror.InvalidArgument{},
		},
		{
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: "test-namespace", // Ignored, must be set on token.
				TaskToken: nil,
			},
			expectedErr: &serviceerror.InvalidArgument{},
		},
		{
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: "test-namespace", // Ignored, must be set on token.
				TaskToken: taskToken,
			},
			expectedErr: &serviceerror.InvalidArgument{},
		},
	}

	for _, testCase := range testCases {
		handlerCalled := false
		_, err := nvi.Intercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.StartWorkflowExecutionResponse{}, nil
		})

		if testCase.expectedErr != nil {
			s.IsType(testCase.expectedErr, err)
			s.False(handlerCalled)
		} else {
			s.NoError(err)
			s.True(handlerCalled)
		}
	}
}

func (s *namespaceValidatorSuite) Test_Intercept_NamespaceNotFound() {

	nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	s.mockRegistry.EXPECT().GetNamespace(namespace.Name("not-found-namespace")).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "not-found-namespace"}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.StartWorkflowExecutionResponse{}, nil
	})

	s.IsType(&serviceerror.NamespaceNotFound{}, err)
	s.False(handlerCalled)

	s.mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("not-found-namespace-id")).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	taskToken, _ := common.NewProtoTaskTokenSerializer().Serialize(&tokenspb.Task{
		NamespaceId: "not-found-namespace-id",
	})
	tokenReq := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "test-namespace",
		TaskToken: taskToken,
	}
	handlerCalled = false
	_, err = nvi.Intercept(context.Background(), tokenReq, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
	})

	s.IsType(&serviceerror.NamespaceNotFound{}, err)
	s.False(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_Intercept_StatusFromNamespace() {
	testCases := []struct {
		state            enumspb.NamespaceState
		replicationState enumspb.ReplicationState
		expectedErr      error
		method           string
		req              interface{}
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
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      "/temporal/StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      "/temporal/StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:            enumspb.NAMESPACE_STATE_REGISTERED,
			replicationState: enumspb.REPLICATION_STATE_HANDOVER,
			expectedErr:      errNamespaceHandover,
			method:           "/temporal/StartWorkflowExecution",
			req:              &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
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
		{
			state:            enumspb.NAMESPACE_STATE_REGISTERED,
			replicationState: enumspb.REPLICATION_STATE_HANDOVER,
			expectedErr:      nil,
			method:           "/temporal/DescribeNamespace",
			req:              &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		// PollWorkflowTaskQueue (default)
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
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      "/temporal/PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		// UpdateNamespace
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      "/temporal/UpdateNamespace",
			req:         &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      "/temporal/UpdateNamespace",
			req:         &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:            enumspb.NAMESPACE_STATE_REGISTERED,
			replicationState: enumspb.REPLICATION_STATE_HANDOVER,
			expectedErr:      nil,
			method:           "/temporal/UpdateNamespace",
			req:              &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      "/temporal/UpdateNamespace",
			req:         &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
	}

	for _, testCase := range testCases {
		s.mockRegistry.EXPECT().GetNamespace(namespace.Name("test-namespace")).Return(namespace.FromPersistentState(
			&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Config: &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
						State: testCase.replicationState,
					},
					Info: &persistencespb.NamespaceInfo{
						State: testCase.state,
					},
				},
			}), nil)

		nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: testCase.method,
		}

		handlerCalled := false
		_, err := nvi.Intercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.StartWorkflowExecutionResponse{}, nil
		})

		if testCase.expectedErr != nil {
			fmt.Printf("Method: %s\nExpected: %v\nActual:%v\n", testCase.method, testCase.expectedErr, err)
			s.IsType(testCase.expectedErr, err)
			s.False(handlerCalled)
		} else {
			s.NoError(err)
			s.True(handlerCalled)
		}
	}
}

func (s *namespaceValidatorSuite) Test_Intercept_StatusFromToken() {
	taskToken, _ := common.NewProtoTaskTokenSerializer().Serialize(&tokenspb.Task{
		NamespaceId: "test-namespace-id",
	})

	testCases := []struct {
		state       enumspb.NamespaceState
		expectedErr error
		method      string
		req         interface{}
	}{
		// RespondWorkflowTaskCompleted
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      "/temporal/RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
			},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      "/temporal/RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
			},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      "/temporal/RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
			},
		},
	}

	for _, testCase := range testCases {
		s.mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(namespace.FromPersistentState(
			&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Config:            &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					Info: &persistencespb.NamespaceInfo{
						State: testCase.state,
					},
				},
			}), nil)

		nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: testCase.method,
		}

		handlerCalled := false
		_, err := nvi.Intercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
		})

		if testCase.expectedErr != nil {
			s.IsType(testCase.expectedErr, err)
			s.False(handlerCalled)
		} else {
			s.NoError(err)
			s.True(handlerCalled)
		}
	}
}

func (s *namespaceValidatorSuite) Test_Intercept_DescribeNamespace_Id() {
	s.mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(namespace.FromPersistentState(
		&persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
				Info: &persistencespb.NamespaceInfo{
					State: enumspb.NAMESPACE_STATE_REGISTERED,
				},
			},
		}), nil)

	nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	req := &workflowservice.DescribeNamespaceRequest{Id: "test-namespace-id"}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.DescribeNamespaceResponse{}, nil
	})

	s.NoError(err)
	s.True(handlerCalled)

	req = &workflowservice.DescribeNamespaceRequest{}
	handlerCalled = false
	_, err = nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.DescribeNamespaceResponse{}, nil
	})

	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.False(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_Intercept_GetClusterInfo() {
	nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	// Example of API which doesn't have namespace field.
	req := &workflowservice.GetClusterInfoRequest{}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.GetClusterInfoResponse{}, nil
	})

	s.NoError(err)
	s.True(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_Intercept_RegisterNamespace() {
	nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(false))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	req := &workflowservice.RegisterNamespaceRequest{Namespace: "new-namespace"}
	handlerCalled := false
	_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	})

	s.NoError(err)
	s.True(handlerCalled)

	req = &workflowservice.RegisterNamespaceRequest{}
	handlerCalled = false
	_, err = nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	})

	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.False(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_Interceptor_TokenNamespaceEnforcement() {
	testCases := []struct {
		tokenNamespaceID                namespace.ID
		tokenNamespaceName              namespace.Name
		requestNamespaceID              namespace.ID
		requestNamespaceName            namespace.Name
		enableTokenNamespaceEnforcement bool
		expectedErr                     error
	}{
		{
			tokenNamespaceID:                "valid-id",
			tokenNamespaceName:              "valid-name",
			requestNamespaceID:              "valid-id",
			requestNamespaceName:            "valid-name",
			enableTokenNamespaceEnforcement: true,
			expectedErr:                     nil,
		},
		{
			tokenNamespaceID:                "valid-id",
			tokenNamespaceName:              "valid-name",
			requestNamespaceID:              "valid-id",
			requestNamespaceName:            "valid-name",
			enableTokenNamespaceEnforcement: false,
			expectedErr:                     nil,
		},
		{
			tokenNamespaceID:                "valid-id",
			tokenNamespaceName:              "valid-name",
			requestNamespaceID:              "invalid-id",
			requestNamespaceName:            "invalid-name",
			enableTokenNamespaceEnforcement: true,
			expectedErr:                     &serviceerror.InvalidArgument{},
		},
		{
			tokenNamespaceID:                "valid-id",
			tokenNamespaceName:              "valid-name",
			requestNamespaceID:              "invalid-id",
			requestNamespaceName:            "invalid-name",
			enableTokenNamespaceEnforcement: false,
			expectedErr:                     nil,
		},
	}

	for _, testCase := range testCases {
		taskToken, _ := common.NewProtoTaskTokenSerializer().Serialize(&tokenspb.Task{
			NamespaceId: testCase.tokenNamespaceID.String(),
		})
		tokenNamespace := namespace.FromPersistentState(
			&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Config:            &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					Info: &persistencespb.NamespaceInfo{
						Id:    testCase.tokenNamespaceID.String(),
						Name:  testCase.tokenNamespaceName.String(),
						State: enumspb.NAMESPACE_STATE_REGISTERED,
					},
				},
			})

		req := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: testCase.requestNamespaceName.String(),
			TaskToken: taskToken,
		}
		queryReq := &workflowservice.RespondQueryTaskCompletedRequest{
			Namespace: testCase.requestNamespaceName.String(),
			TaskToken: taskToken,
		}
		requestNamespace := namespace.FromPersistentState(
			&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Config:            &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					Info: &persistencespb.NamespaceInfo{
						Id:    testCase.requestNamespaceID.String(),
						Name:  testCase.requestNamespaceName.String(),
						State: enumspb.NAMESPACE_STATE_REGISTERED,
					},
				},
			})

		// 2 times because of RespondQueryTaskCompleted.
		s.mockRegistry.EXPECT().GetNamespace(testCase.requestNamespaceName).Return(requestNamespace, nil).Times(2)
		s.mockRegistry.EXPECT().GetNamespaceByID(testCase.tokenNamespaceID).Return(tokenNamespace, nil).Times(2)

		nvi := NewNamespaceValidatorInterceptor(s.mockRegistry, dynamicconfig.GetBoolPropertyFn(testCase.enableTokenNamespaceEnforcement))
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: "/temporal/RandomMethod",
		}

		handlerCalled := false
		_, err := nvi.Intercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
		})
		_, queryErr := nvi.Intercept(context.Background(), queryReq, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.RespondQueryTaskCompletedResponse{}, nil
		})

		if testCase.expectedErr != nil {
			s.IsType(testCase.expectedErr, err)
			s.IsType(testCase.expectedErr, queryErr)
			s.False(handlerCalled)
		} else {
			s.NoError(err)
			s.NoError(queryErr)
			s.True(handlerCalled)
		}
	}
}
