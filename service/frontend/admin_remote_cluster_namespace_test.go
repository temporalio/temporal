package frontend

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const remoteNamespaceTestAddress = "peer-frontend:7233"

func (s *adminHandlerSuite) remoteNamespaceTestClusters(enabled bool) map[string]cluster.ClusterInformation {
	return map[string]cluster.ClusterInformation{
		s.currentClusterName: {Enabled: true, RPCAddress: "local:7233"},
		// Replication streams are paused; reads only need the connection.
		"peer": {Enabled: enabled, ReplicationEnabled: false, RPCAddress: remoteNamespaceTestAddress},
	}
}

func remoteNamespaceTestResponse() *adminservice.GetNamespaceResponse {
	return &adminservice.GetNamespaceResponse{IsGlobalNamespace: true, ConfigVersion: 7}
}

func (s *adminHandlerSuite) Test_GetRemoteClusterNamespace_RejectsInvalidRequests() {
	for _, test := range []struct {
		name     string
		request  *adminservice.GetRemoteClusterNamespaceRequest
		clusters map[string]cluster.ClusterInformation
		code     codes.Code
		message  string
	}{
		{name: "nil request", code: codes.InvalidArgument},
		{
			name:    "empty cluster name",
			request: &adminservice.GetRemoteClusterNamespaceRequest{Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}},
			code:    codes.InvalidArgument,
			message: "cluster name is required",
		},
		{
			name:    "no selector",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer"},
			code:    codes.InvalidArgument,
			message: "exactly one of namespace name or ID is required",
		},
		{
			name:    "empty namespace name",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceName{}},
			code:    codes.InvalidArgument,
			message: "namespace name is empty",
		},
		{
			name:    "empty namespace ID",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{}},
			code:    codes.InvalidArgument,
			message: "namespace ID is empty",
		},
		{
			name:    "current cluster",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: s.currentClusterName, Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}},
			code:    codes.InvalidArgument,
			message: "is the current cluster",
		},
		{
			name:     "unknown cluster",
			request:  &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "other", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}},
			clusters: s.remoteNamespaceTestClusters(true),
			code:     codes.FailedPrecondition,
			message:  `unknown remote cluster "other"`,
		},
		{
			name:     "connection disabled",
			request:  &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}},
			clusters: s.remoteNamespaceTestClusters(false),
			code:     codes.FailedPrecondition,
			message:  `connection to remote cluster "peer" is disabled`,
		},
		{
			name:    "no frontend address",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}},
			clusters: map[string]cluster.ClusterInformation{
				"peer": {Enabled: true},
			},
			code:    codes.FailedPrecondition,
			message: `remote cluster "peer" has no frontend address`,
		},
	} {
		s.Run(test.name, func() {
			if test.clusters != nil {
				s.mockMetadata.EXPECT().GetAllClusterInfo().Return(test.clusters).Times(1)
			}
			// No remote client is created for a rejected request: the mock
			// factory fails the test on any unexpected NewRemoteAdminClientWithTimeout.
			_, err := s.handler.GetRemoteClusterNamespace(context.Background(), test.request)
			s.Equal(test.code, serviceerror.ToStatus(err).Code(), err)
			s.ErrorContains(err, test.message)
		})
	}
}

func (s *adminHandlerSuite) Test_GetRemoteClusterNamespace_ReadsVerifiedPeer() {
	for _, test := range []struct {
		name    string
		request *adminservice.GetRemoteClusterNamespaceRequest
		want    *adminservice.GetNamespaceRequest
	}{
		{
			name:    "by ID",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}},
			want:    &adminservice.GetNamespaceRequest{Attributes: &adminservice.GetNamespaceRequest_Id{Id: "id"}},
		},
		{
			name:    "by name",
			request: &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceName{RemoteNamespaceName: "name"}},
			want:    &adminservice.GetNamespaceRequest{Attributes: &adminservice.GetNamespaceRequest_Namespace{Namespace: "name"}},
		},
	} {
		s.Run(test.name, func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			deadline, _ := ctx.Deadline()
			requireCallerDeadline := func(callCtx context.Context) {
				callDeadline, ok := callCtx.Deadline()
				s.True(ok)
				s.Equal(deadline, callDeadline)
			}
			s.mockMetadata.EXPECT().GetAllClusterInfo().Return(s.remoteNamespaceTestClusters(true)).Times(1)
			s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(remoteNamespaceTestAddress, gomock.Any(), gomock.Any()).Return(s.mockAdminClient).Times(1)
			describe := s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), gomock.Any()).DoAndReturn(
				func(callCtx context.Context, request *adminservice.DescribeClusterRequest, _ ...grpc.CallOption) (*adminservice.DescribeClusterResponse, error) {
					requireCallerDeadline(callCtx)
					s.Empty(request.GetClusterName(), "the peer must describe itself")
					return &adminservice.DescribeClusterResponse{ClusterName: "peer"}, nil
				},
			).Times(1)
			s.mockAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
				func(callCtx context.Context, request *adminservice.GetNamespaceRequest, _ ...grpc.CallOption) (*adminservice.GetNamespaceResponse, error) {
					requireCallerDeadline(callCtx)
					s.True(proto.Equal(test.want, request))
					return remoteNamespaceTestResponse(), nil
				},
			).After(describe).Times(1)

			response, err := s.handler.GetRemoteClusterNamespace(ctx, test.request)
			s.NoError(err)
			s.Equal("peer", response.GetClusterName())
			s.True(proto.Equal(remoteNamespaceTestResponse(), response.GetNamespace()))
		})
	}
}

func (s *adminHandlerSuite) Test_GetRemoteClusterNamespace_PeerErrors() {
	request := &adminservice.GetRemoteClusterNamespaceRequest{ClusterName: "peer", Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: "id"}}
	for _, test := range []struct {
		name         string
		describe     *adminservice.DescribeClusterResponse
		describeErr  error
		namespaceErr error
		code         codes.Code
		notFound     bool
		message      string
	}{
		{
			// The wrong cluster lacks the namespace; identity is checked first,
			// so this can never surface as NamespaceNotFound.
			name:     "identity mismatch",
			describe: &adminservice.DescribeClusterResponse{ClusterName: "other"},
			code:     codes.FailedPrecondition,
			message:  `remote cluster identity mismatch: expected "peer", got "other"`,
		},
		{
			name:        "describe returns not found",
			describeErr: serviceerror.NewNotFound("cluster metadata not found"),
			code:        codes.Unavailable,
			message:     `describe remote cluster "peer"`,
		},
		{
			name:        "describe unavailable",
			describeErr: serviceerror.NewUnavailable("connection refused"),
			code:        codes.Unavailable,
			message:     "connection refused",
		},
		{
			name:        "describe permission denied",
			describeErr: serviceerror.NewPermissionDenied("denied", ""),
			code:        codes.PermissionDenied,
			message:     "denied",
		},
		{
			name:         "verified peer lacks the namespace",
			describe:     &adminservice.DescribeClusterResponse{ClusterName: "peer"},
			namespaceErr: serviceerror.NewNamespaceNotFound("id"),
			code:         codes.NotFound,
			notFound:     true,
		},
		{
			name:         "verified peer denies the read",
			describe:     &adminservice.DescribeClusterResponse{ClusterName: "peer"},
			namespaceErr: serviceerror.NewPermissionDenied("denied", ""),
			code:         codes.PermissionDenied,
			message:      "denied",
		},
	} {
		s.Run(test.name, func() {
			s.mockMetadata.EXPECT().GetAllClusterInfo().Return(s.remoteNamespaceTestClusters(true)).Times(1)
			s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(remoteNamespaceTestAddress, gomock.Any(), gomock.Any()).Return(s.mockAdminClient).Times(1)
			s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), gomock.Any()).Return(test.describe, test.describeErr).Times(1)
			if test.describeErr == nil && test.describe.GetClusterName() == "peer" {
				s.mockAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, test.namespaceErr).Times(1)
			}

			_, err := s.handler.GetRemoteClusterNamespace(context.Background(), request)
			s.Equal(test.code, serviceerror.ToStatus(err).Code(), err)
			_, isNamespaceNotFound := errors.AsType[*serviceerror.NamespaceNotFound](err)
			s.Equal(test.notFound, isNamespaceNotFound, err)
			if test.message != "" {
				s.ErrorContains(err, test.message)
			}
		})
	}
}
