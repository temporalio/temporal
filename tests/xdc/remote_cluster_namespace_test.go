package xdc

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type (
	// RemoteClusterNamespaceTestSuite exercises the GetRemoteClusterNamespace
	// relay over real gRPC on both hops: test client -> cluster0 frontend ->
	// cluster1 frontend. Operator tools rely on typed errors surviving that path.
	RemoteClusterNamespaceTestSuite struct {
		xdcBaseSuite
	}
)

func TestRemoteClusterNamespaceTestSuite(t *testing.T) {
	t.Parallel()
	s := new(RemoteClusterNamespaceTestSuite)
	suite.Run(t, s)
}

func (s *RemoteClusterNamespaceTestSuite) SetupSuite() {
	s.setupSuite()
}

func (s *RemoteClusterNamespaceTestSuite) SetupTest() {
	s.setupTest()
}

func (s *RemoteClusterNamespaceTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *RemoteClusterNamespaceTestSuite) TestRelayReadsVerifiedPeer() {
	ctx := testcore.NewContext()
	source, peer := s.clusters[0], s.clusters[1]
	ns := s.createGlobalNamespace()
	sourceNamespace, err := source.AdminClient().GetNamespace(ctx, &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Namespace{Namespace: ns},
	})
	s.Require().NoError(err)
	id := sourceNamespace.GetInfo().GetId()

	for _, request := range []*adminservice.GetRemoteClusterNamespaceRequest{
		{ClusterName: peer.ClusterName(), Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: id}},
		{ClusterName: peer.ClusterName(), Selector: &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceName{RemoteNamespaceName: ns}},
	} {
		response, err := source.AdminClient().GetRemoteClusterNamespace(ctx, request)
		s.Require().NoError(err)
		s.Equal(peer.ClusterName(), response.GetClusterName())
		s.Equal(id, response.GetNamespace().GetInfo().GetId())
		s.Equal(ns, response.GetNamespace().GetInfo().GetName())
		s.True(response.GetNamespace().GetIsGlobalNamespace())
	}
}

func (s *RemoteClusterNamespaceTestSuite) TestNamespaceNotFoundSurvivesBothHops() {
	ctx := testcore.NewContext()
	source, peer := s.clusters[0], s.clusters[1]

	_, err := source.AdminClient().GetRemoteClusterNamespace(ctx, &adminservice.GetRemoteClusterNamespaceRequest{
		ClusterName: peer.ClusterName(),
		Selector:    &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: uuid.NewString()},
	})
	_, isNamespaceNotFound := errors.AsType[*serviceerror.NamespaceNotFound](err)
	s.True(isNamespaceNotFound, "a verified peer's NamespaceNotFound must stay typed across both hops: %v", err)

	_, err = source.AdminClient().GetRemoteClusterNamespace(ctx, &adminservice.GetRemoteClusterNamespaceRequest{
		ClusterName: peer.ClusterName(),
		Selector:    &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceName{RemoteNamespaceName: "missing-" + uuid.NewString()},
	})
	_, isNamespaceNotFound = errors.AsType[*serviceerror.NamespaceNotFound](err)
	s.True(isNamespaceNotFound, "a verified peer's NamespaceNotFound must stay typed across both hops: %v", err)
}

func (s *RemoteClusterNamespaceTestSuite) TestRelayErrorsAreNeverNamespaceNotFound() {
	ctx := testcore.NewContext()
	source := s.clusters[0]
	for _, test := range []struct {
		cluster string
		code    codes.Code
	}{
		{cluster: source.ClusterName(), code: codes.InvalidArgument},
		{cluster: "unknown-" + uuid.NewString(), code: codes.FailedPrecondition},
	} {
		_, err := source.AdminClient().GetRemoteClusterNamespace(ctx, &adminservice.GetRemoteClusterNamespaceRequest{
			ClusterName: test.cluster,
			Selector:    &adminservice.GetRemoteClusterNamespaceRequest_RemoteNamespaceId{RemoteNamespaceId: uuid.NewString()},
		})
		s.Equal(test.code, serviceerror.ToStatus(err).Code(), err)
		_, isNamespaceNotFound := errors.AsType[*serviceerror.NamespaceNotFound](err)
		s.False(isNamespaceNotFound, err)
	}
}
