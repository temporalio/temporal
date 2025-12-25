package interceptor

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
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

func (s *namespaceValidatorSuite) Test_StateValidationIntercept_NamespaceNotSet() {
	taskToken, _ := tasktoken.NewSerializer().Serialize(&tokenspb.Task{
		NamespaceId: "",
		WorkflowId:  "wid",
	})

	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(100))
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
		_, err := nvi.StateValidationIntercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
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

func (s *namespaceValidatorSuite) Test_StateValidationIntercept_NamespaceNotFound() {

	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(100))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	s.mockRegistry.EXPECT().GetNamespace(namespace.Name("not-found-namespace")).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "not-found-namespace"}
	handlerCalled := false
	_, err := nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.StartWorkflowExecutionResponse{}, nil
	})

	s.IsType(&serviceerror.NamespaceNotFound{}, err)
	s.False(handlerCalled)

	s.mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("not-found-namespace-id")).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	taskToken, _ := tasktoken.NewSerializer().Serialize(&tokenspb.Task{
		NamespaceId: "not-found-namespace-id",
	})
	tokenReq := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "test-namespace",
		TaskToken: taskToken,
	}
	handlerCalled = false
	_, err = nvi.StateValidationIntercept(context.Background(), tokenReq, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
	})

	s.IsType(&serviceerror.NamespaceNotFound{}, err)
	s.False(handlerCalled)
}

type nexusRequest struct{}

func (*nexusRequest) GetNamespace() string {
	return "test-namespace"
}
func (s *namespaceValidatorSuite) Test_StateValidationIntercept_StatusFromNamespace() {

	testCases := []struct {
		state            enumspb.NamespaceState
		replicationState enumspb.ReplicationState
		expectedErr      error
		method           string
		req              any
	}{
		// StartWorkflowExecution
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "StartWorkflowExecution",
			req:         &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:            enumspb.NAMESPACE_STATE_REGISTERED,
			replicationState: enumspb.REPLICATION_STATE_HANDOVER,
			expectedErr:      common.ErrNamespaceHandover,
			method:           api.WorkflowServicePrefix + "StartWorkflowExecution",
			req:              &workflowservice.StartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		// SignalWithStartWorkflowExecution
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "SignalWithStartWorkflowExecution",
			req:         &workflowservice.SignalWithStartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "SignalWithStartWorkflowExecution",
			req:         &workflowservice.SignalWithStartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "SignalWithStartWorkflowExecution",
			req:         &workflowservice.SignalWithStartWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		// DeleteNamespace
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.OperatorServicePrefix + "DeleteNamespace",
			req:         &operatorservice.DeleteNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      api.OperatorServicePrefix + "DeleteNamespace",
			req:         &operatorservice.DeleteNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: nil,
			method:      api.OperatorServicePrefix + "DeleteNamespace",
			req:         &operatorservice.DeleteNamespaceRequest{Namespace: "test-namespace"},
		},
		// AdminService APIs
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.AdminServicePrefix + "DescribeMutableState",
			req:         &adminservice.DescribeMutableStateRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      api.AdminServicePrefix + "DescribeMutableState",
			req:         &adminservice.DescribeMutableStateRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: nil,
			method:      api.AdminServicePrefix + "DescribeMutableState",
			req:         &adminservice.DescribeMutableStateRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.AdminServicePrefix + "DeleteWorkflowExecution",
			req:         &adminservice.DeleteWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      api.AdminServicePrefix + "DeleteWorkflowExecution",
			req:         &adminservice.DeleteWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: nil,
			method:      api.AdminServicePrefix + "DeleteWorkflowExecution",
			req:         &adminservice.DeleteWorkflowExecutionRequest{Namespace: "test-namespace"},
		},
		// DispatchNexusTask
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.NexusServicePrefix + "DispatchNexusTask",
			req:         &nexusRequest{},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.NexusServicePrefix + "DispatchNexusTask",
			req:         &nexusRequest{},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.NexusServicePrefix + "DispatchNexusTask",
			req:         &nexusRequest{},
		},

		// =====================================================================
		//                       Default Allowed States
		// =====================================================================

		// DescribeNamespace
		{
			state:       enumspb.NAMESPACE_STATE_UNSPECIFIED,
			expectedErr: errNamespaceNotSet,
			method:      api.WorkflowServicePrefix + "DescribeNamespace",
			req:         &workflowservice.DescribeNamespaceRequest{},
		},
		{
			state:       enumspb.NAMESPACE_STATE_UNSPECIFIED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "DescribeNamespace",
			req:         &workflowservice.DescribeNamespaceRequest{Id: "test-namespace-id"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_UNSPECIFIED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "DescribeNamespace",
			req:         &workflowservice.DescribeNamespaceRequest{Namespace: "test-namespace"},
		},
		// RegisterNamespace
		{
			state:       enumspb.NAMESPACE_STATE_UNSPECIFIED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "RegisterNamespace",
			req:         &workflowservice.RegisterNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_UNSPECIFIED,
			expectedErr: errNamespaceNotSet,
			method:      api.WorkflowServicePrefix + "RegisterNamespace",
			req:         &workflowservice.RegisterNamespaceRequest{},
		},
		// PollWorkflowTaskQueue (default)
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "PollWorkflowTaskQueue",
			req:         &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "test-namespace"},
		},
		// UpdateNamespace
		{
			state:       enumspb.NAMESPACE_STATE_REGISTERED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "UpdateNamespace",
			req:         &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "UpdateNamespace",
			req:         &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:            enumspb.NAMESPACE_STATE_REGISTERED,
			replicationState: enumspb.REPLICATION_STATE_HANDOVER,
			expectedErr:      nil,
			method:           api.WorkflowServicePrefix + "UpdateNamespace",
			req:              &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "UpdateNamespace",
			req:         &workflowservice.UpdateNamespaceRequest{Namespace: "test-namespace"},
		},
	}

	for _, testCase := range testCases {
		s.T().Run(fmt.Sprintf("%s-%s", testCase.method, testCase.state.String()), func(t *testing.T) {
			_, isDescribeNamespace := testCase.req.(*workflowservice.DescribeNamespaceRequest)
			_, isRegisterNamespace := testCase.req.(*workflowservice.RegisterNamespaceRequest)
			if !isDescribeNamespace && !isRegisterNamespace {
				factory := namespace.NewDefaultReplicationResolverFactory()
				detail := &persistencespb.NamespaceDetail{
					Config: &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
						State: testCase.replicationState,
					},
					Info: &persistencespb.NamespaceInfo{
						State: testCase.state,
					},
				}
				ns, err := namespace.FromPersistentState(detail, factory(detail))
				s.Require().NoError(err)
				s.mockRegistry.EXPECT().GetNamespace(namespace.Name("test-namespace")).Return(ns, nil)
			}

			nvi := NewNamespaceValidatorInterceptor(
				s.mockRegistry,
				dynamicconfig.GetBoolPropertyFn(false),
				dynamicconfig.GetIntPropertyFn(100))
			serverInfo := &grpc.UnaryServerInfo{
				FullMethod: testCase.method,
			}

			handlerCalled := false
			_, err := nvi.StateValidationIntercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
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
		})
	}
}

func (s *namespaceValidatorSuite) Test_StateValidationIntercept_StatusFromToken() {
	taskToken, _ := tasktoken.NewSerializer().Serialize(&tokenspb.Task{
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
			method:      api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
			},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DEPRECATED,
			expectedErr: nil,
			method:      api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
			},
		},
		{
			state:       enumspb.NAMESPACE_STATE_DELETED,
			expectedErr: &serviceerror.NamespaceInvalidState{},
			method:      api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
			},
		},
	}

	for _, testCase := range testCases {
		factory := namespace.NewDefaultReplicationResolverFactory()
		detail := &persistencespb.NamespaceDetail{
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			Info: &persistencespb.NamespaceInfo{
				State: testCase.state,
			},
		}
		ns, nsErr := namespace.FromPersistentState(detail, factory(detail))
		s.Require().NoError(nsErr)
		s.mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil)

		nvi := NewNamespaceValidatorInterceptor(
			s.mockRegistry,
			dynamicconfig.GetBoolPropertyFn(false),
			dynamicconfig.GetIntPropertyFn(100))
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: testCase.method,
		}

		handlerCalled := false
		_, err := nvi.StateValidationIntercept(context.Background(), testCase.req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
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

func (s *namespaceValidatorSuite) Test_StateValidationIntercept_DescribeNamespace_Id() {
	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(100))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	req := &workflowservice.DescribeNamespaceRequest{Id: "test-namespace-id"}
	handlerCalled := false
	_, err := nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.DescribeNamespaceResponse{}, nil
	})

	s.NoError(err)
	s.True(handlerCalled)

	req = &workflowservice.DescribeNamespaceRequest{}
	handlerCalled = false
	_, err = nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.DescribeNamespaceResponse{}, nil
	})

	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.False(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_StateValidationIntercept_GetClusterInfo() {
	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(100))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	// Example of API which doesn't have namespace field.
	req := &workflowservice.GetClusterInfoRequest{}
	handlerCalled := false
	_, err := nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.GetClusterInfoResponse{}, nil
	})

	s.NoError(err)
	s.True(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_Intercept_RegisterNamespace() {
	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(100))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/random",
	}

	req := &workflowservice.RegisterNamespaceRequest{Namespace: "new-namespace"}
	handlerCalled := false
	_, err := nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	})

	s.NoError(err)
	s.True(handlerCalled)

	req = &workflowservice.RegisterNamespaceRequest{}
	handlerCalled = false
	_, err = nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	})

	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.False(handlerCalled)
}

func (s *namespaceValidatorSuite) Test_StateValidationIntercept_TokenNamespaceEnforcement() {
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
		taskToken, _ := tasktoken.NewSerializer().Serialize(&tokenspb.Task{
			NamespaceId: testCase.tokenNamespaceID.String(),
		})
		factory := namespace.NewDefaultReplicationResolverFactory()
		tokenDetail := &persistencespb.NamespaceDetail{
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			Info: &persistencespb.NamespaceInfo{
				Id:    testCase.tokenNamespaceID.String(),
				Name:  testCase.tokenNamespaceName.String(),
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
		}
		tokenNamespace, err := namespace.FromPersistentState(tokenDetail, factory(tokenDetail))
		s.Require().NoError(err)

		req := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: testCase.requestNamespaceName.String(),
			TaskToken: taskToken,
		}
		queryReq := &workflowservice.RespondQueryTaskCompletedRequest{
			Namespace: testCase.requestNamespaceName.String(),
			TaskToken: taskToken,
		}
		requestDetail := &persistencespb.NamespaceDetail{
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			Info: &persistencespb.NamespaceInfo{
				Id:    testCase.requestNamespaceID.String(),
				Name:  testCase.requestNamespaceName.String(),
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
		}
		requestNamespace, reqErr := namespace.FromPersistentState(requestDetail, factory(requestDetail))
		s.Require().NoError(reqErr)

		// 2 times because of RespondQueryTaskCompleted.
		s.mockRegistry.EXPECT().GetNamespace(testCase.requestNamespaceName).Return(requestNamespace, nil).Times(2)
		s.mockRegistry.EXPECT().GetNamespaceByID(testCase.tokenNamespaceID).Return(tokenNamespace, nil).Times(2)

		nvi := NewNamespaceValidatorInterceptor(
			s.mockRegistry,
			dynamicconfig.GetBoolPropertyFn(testCase.enableTokenNamespaceEnforcement),
			dynamicconfig.GetIntPropertyFn(100))
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: api.WorkflowServicePrefix + "RandomMethod",
		}

		handlerCalled := false
		_, err = nvi.StateValidationIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCalled = true
			return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
		})
		_, queryErr := nvi.StateValidationIntercept(context.Background(), queryReq, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
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

func (s *namespaceValidatorSuite) Test_Intercept_DescribeHistoryHostRequests() {
	// it's just a list of requests
	testCases := []struct {
		req any
	}{
		{
			req: &adminservice.DescribeHistoryHostRequest{},
		},
		{
			req: &adminservice.DescribeHistoryHostRequest{Namespace: "test-namespace"},
		},
	}

	for _, testCase := range testCases {
		nvi := NewNamespaceValidatorInterceptor(
			s.mockRegistry,
			dynamicconfig.GetBoolPropertyFn(false),
			dynamicconfig.GetIntPropertyFn(100),
		)
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: api.WorkflowServicePrefix + "random",
		}

		handlerCalled := false
		_, err := nvi.StateValidationIntercept(
			context.Background(),
			testCase.req,
			serverInfo,
			func(ctx context.Context, req any) (any, error) {
				handlerCalled = true
				return nil, nil
			},
		)
		s.NoError(err)
		s.True(handlerCalled)
	}
}

func (s *namespaceValidatorSuite) Test_Intercept_SearchAttributeRequests() {
	// it's just a list of requests
	testCases := []struct {
		req          any
		hasNamespace bool
	}{
		{
			req:          &adminservice.AddSearchAttributesRequest{},
			hasNamespace: false,
		},
		{
			req:          &adminservice.RemoveSearchAttributesRequest{},
			hasNamespace: false,
		},
		{
			req:          &adminservice.GetSearchAttributesRequest{},
			hasNamespace: false,
		},
		{
			req:          &operatorservice.AddSearchAttributesRequest{},
			hasNamespace: false,
		},
		{
			req:          &operatorservice.RemoveSearchAttributesRequest{},
			hasNamespace: false,
		},
		{
			req:          &operatorservice.ListSearchAttributesRequest{},
			hasNamespace: false,
		},
		{
			req:          &adminservice.AddSearchAttributesRequest{Namespace: "test-namespace"},
			hasNamespace: true,
		},
		{
			req:          &adminservice.RemoveSearchAttributesRequest{Namespace: "test-namespace"},
			hasNamespace: true,
		},
		{
			req:          &adminservice.GetSearchAttributesRequest{Namespace: "test-namespace"},
			hasNamespace: true,
		},
		{
			req:          &operatorservice.AddSearchAttributesRequest{Namespace: "test-namespace"},
			hasNamespace: true,
		},
		{
			req:          &operatorservice.RemoveSearchAttributesRequest{Namespace: "test-namespace"},
			hasNamespace: true,
		},
		{
			req:          &operatorservice.ListSearchAttributesRequest{Namespace: "test-namespace"},
			hasNamespace: true,
		},
	}

	for _, testCase := range testCases {
		if testCase.hasNamespace {
			s.mockRegistry.EXPECT().GetNamespace(namespace.Name("test-namespace")).Return(nil, nil)
		}

		nvi := NewNamespaceValidatorInterceptor(
			s.mockRegistry,
			dynamicconfig.GetBoolPropertyFn(false),
			dynamicconfig.GetIntPropertyFn(100),
		)
		serverInfo := &grpc.UnaryServerInfo{
			FullMethod: api.WorkflowServicePrefix + "random",
		}

		handlerCalled := false
		_, err := nvi.StateValidationIntercept(
			context.Background(),
			testCase.req,
			serverInfo,
			func(ctx context.Context, req any) (any, error) {
				handlerCalled = true
				return nil, nil
			},
		)
		s.NoError(err)
		s.True(handlerCalled)
	}
}

func (s *namespaceValidatorSuite) Test_NamespaceValidateIntercept() {
	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(10))
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: api.WorkflowServicePrefix + "random",
	}
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail1 := &persistencespb.NamespaceDetail{
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		Info: &persistencespb.NamespaceInfo{
			Id:    uuid.New().String(),
			Name:  "namespace",
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
	}
	requestNamespace, err := namespace.FromPersistentState(detail1, factory(detail1))
	s.NoError(err)
	detail2 := &persistencespb.NamespaceDetail{
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		Info: &persistencespb.NamespaceInfo{
			Id:    uuid.New().String(),
			Name:  "namespaceTooLong",
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
	}
	requestNamespaceTooLong, err2 := namespace.FromPersistentState(detail2, factory(detail2))
	s.NoError(err2)
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name("namespace")).Return(requestNamespace, nil).AnyTimes()
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name("namespaceTooLong")).Return(requestNamespaceTooLong, nil).AnyTimes()

	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace"}
	handlerCalled := false
	_, err = nvi.NamespaceValidateIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.StartWorkflowExecutionResponse{}, nil
	})
	s.True(handlerCalled)
	s.NoError(err)

	req = &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespaceTooLong"}
	handlerCalled = false
	_, err = nvi.NamespaceValidateIntercept(context.Background(), req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &workflowservice.StartWorkflowExecutionResponse{}, nil
	})
	s.False(handlerCalled)
	s.Error(err)
}

func (s *namespaceValidatorSuite) TestSetNamespace() {
	namespaceRequestName := uuid.New().String()
	namespaceEntryName := uuid.New().String()
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		Info: &persistencespb.NamespaceInfo{
			Id:    uuid.New().String(),
			Name:  namespaceEntryName,
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
	}
	namespaceEntry, err := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(err)

	nvi := NewNamespaceValidatorInterceptor(
		s.mockRegistry,
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetIntPropertyFn(10),
	)

	queryReq := &workflowservice.RespondQueryTaskCompletedRequest{}
	nvi.setNamespace(namespaceEntry, queryReq)
	s.Equal(namespaceEntryName, queryReq.Namespace)
	queryReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, queryReq)
	s.Equal(namespaceRequestName, queryReq.Namespace)

	completeWorkflowTaskReq := &workflowservice.RespondWorkflowTaskCompletedRequest{}
	nvi.setNamespace(namespaceEntry, completeWorkflowTaskReq)
	s.Equal(namespaceEntryName, completeWorkflowTaskReq.Namespace)
	completeWorkflowTaskReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, completeWorkflowTaskReq)
	s.Equal(namespaceRequestName, completeWorkflowTaskReq.Namespace)

	failWorkflowTaskReq := &workflowservice.RespondWorkflowTaskFailedRequest{}
	nvi.setNamespace(namespaceEntry, failWorkflowTaskReq)
	s.Equal(namespaceEntryName, failWorkflowTaskReq.Namespace)
	failWorkflowTaskReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, failWorkflowTaskReq)
	s.Equal(namespaceRequestName, failWorkflowTaskReq.Namespace)

	heartbeatActivityTaskReq := &workflowservice.RecordActivityTaskHeartbeatRequest{}
	nvi.setNamespace(namespaceEntry, heartbeatActivityTaskReq)
	s.Equal(namespaceEntryName, heartbeatActivityTaskReq.Namespace)
	heartbeatActivityTaskReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, heartbeatActivityTaskReq)
	s.Equal(namespaceRequestName, heartbeatActivityTaskReq.Namespace)

	cancelActivityTaskReq := &workflowservice.RespondActivityTaskCanceledRequest{}
	nvi.setNamespace(namespaceEntry, cancelActivityTaskReq)
	s.Equal(namespaceEntryName, cancelActivityTaskReq.Namespace)
	cancelActivityTaskReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, cancelActivityTaskReq)
	s.Equal(namespaceRequestName, cancelActivityTaskReq.Namespace)

	completeActivityTaskReq := &workflowservice.RespondActivityTaskCompletedRequest{}
	nvi.setNamespace(namespaceEntry, completeActivityTaskReq)
	s.Equal(namespaceEntryName, completeActivityTaskReq.Namespace)
	completeActivityTaskReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, completeActivityTaskReq)
	s.Equal(namespaceRequestName, completeActivityTaskReq.Namespace)

	failActivityTaskReq := &workflowservice.RespondActivityTaskFailedRequest{}
	nvi.setNamespace(namespaceEntry, failActivityTaskReq)
	s.Equal(namespaceEntryName, failActivityTaskReq.Namespace)
	failActivityTaskReq.Namespace = namespaceRequestName
	nvi.setNamespace(namespaceEntry, failActivityTaskReq)
	s.Equal(namespaceRequestName, failActivityTaskReq.Namespace)
}
